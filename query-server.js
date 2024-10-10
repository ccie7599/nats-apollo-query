const { ApolloServer, gql } = require('apollo-server-express');
const express = require('express');
const https = require('https'); // Use https instead of http
const fs = require('fs');
const { WebSocketServer } = require('ws');
const { useServer } = require('graphql-ws/lib/use/ws');
const { makeExecutableSchema } = require('@graphql-tools/schema');
const NatsPubSub = require('./NatsPubSub');

// Create an instance of NatsPubSub with your NATS server URL
const pubsub = new NatsPubSub('nats://localhost:4222');

// Define your GraphQL schema
const typeDefs = gql`
  type Query {
    hello: String
  }
  type Subscription {
    messageAdded: String
  }
`;

// Define your resolvers
const resolvers = {
  Query: {
    hello: () => 'Hello world!',
  },
  Subscription: {
    messageAdded: {
      subscribe: () => pubsub.asyncIterator('MESSAGE_ADDED'),
    },
  },
};

// Create the executable schema
const schema = makeExecutableSchema({ typeDefs, resolvers });

const startServer = async () => {
  const app = express();

  // Load TLS certificate and private key
  const server = https.createServer({
    key: fs.readFileSync('/certs/privkey.pem'),
    cert: fs.readFileSync('/certs/fullchain.pem'),
  }, app);

  // Create the Apollo Server instance
  const apolloServer = new ApolloServer({ schema });
  await apolloServer.start();
  apolloServer.applyMiddleware({ app });

  // Set up the WebSocket server for handling GraphQL subscriptions
  const wsServer = new WebSocketServer({
    server,
    path: '/graphql',
  });

  useServer({ schema }, wsServer);

  // Start the HTTPS server
  server.listen(8445, () => {
    console.log(`🚀 Server ready at https://localhost:8445${apolloServer.graphqlPath}`);
  });

  // Simulate message publishing
  setInterval(() => {
    pubsub.publish('MESSAGE_ADDED', { messageAdded: 'New message from NATS!' });
  }, 5000);
};

// Start the server
startServer().catch(err => {
  console.error('Error starting server:', err);
});
