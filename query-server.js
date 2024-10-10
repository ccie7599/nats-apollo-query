const { ApolloServer } = require('apollo-server-express');
const express = require('express');
const https = require('https'); // Use https for TLS
const fs = require('fs');
const path = require('path');
const { gql } = require('apollo-server');
const { connect, StringCodec } = require('nats');

// Define the GraphQL schema
const typeDefs = gql`
  type Query {
    getOrder(orderId: ID!): Order
  }

  type Order {
    id: ID!
    customer: String
    product: String
    quantity: Int
    status: String
  }
`;

// Ensure cache directory exists
const ensureCacheDir = () => {
  const cacheDir = path.join('/tmp', 'cache', 'orders');
  if (!fs.existsSync(cacheDir)) {
    fs.mkdirSync(cacheDir, { recursive: true });
  }
  return cacheDir;
};

// Write order data to local cache file
const writeOrderToFile = (orderId, data) => {
  const cacheDir = ensureCacheDir();
  const filePath = path.join(cacheDir, `${orderId}.json`);
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
};

// Read order data from local cache file
const readOrderFromFile = (orderId) => {
  const cacheDir = ensureCacheDir();
  const filePath = path.join(cacheDir, `${orderId}.json`);
  if (fs.existsSync(filePath)) {
    return JSON.parse(fs.readFileSync(filePath));
  }
  return null;
};

// Keep a persistent NATS connection and subscribe to the wildcard pattern
const connectNATS = async () => {
  const nc = await connect({ servers: "nats://host.docker.internal:4222"});
  const sc = StringCodec();

  // Subscribe to all orders in the NATS cache
  const sub = nc.subscribe('cache.orders.>', {
    callback: (err, msg) => {
      if (err) {
        console.error('Error receiving message:', err);
      } else {
        const decodedMessage = sc.decode(msg.data);
        const orderId = msg.subject.split('.')[2]; // Extract orderId from subject
        console.log(`Received message for order ${orderId}:`, decodedMessage);

        // Write the received order to local disk
        writeOrderToFile(orderId, JSON.parse(decodedMessage));
      }
    },
  });

  console.log('Subscribed to NATS wildcard pattern cache.orders.#');

  return { nc, sc };
};

// Resolver that checks local file cache or fetches from the "database"
const resolvers = {
  Query: {
    getOrder: async (_, { orderId }, { nats }) => {
      const subject = `cache.orders.${orderId}`;
      const { nc, sc } = nats;

      // Check if order exists in local cache
      const cachedOrder = readOrderFromFile(orderId);

      if (cachedOrder) {
        console.log(`Order ${orderId} found in local cache.`);
        return cachedOrder;
      }

      console.log(`Cache miss for order ${orderId}, fetching from database...`);

      // Simulate fetching order from a database
      const orderFromDB = {
        id: orderId,
        customer: "John Doe",
        product: "Laptop",
        quantity: 2,
        status: "shipped",
      };

      // Write the order to local disk cache
      writeOrderToFile(orderId, orderFromDB);

      // Publish the result to NATS for future caching
      const encodedOrder = sc.encode(JSON.stringify(orderFromDB));
      nc.publish(subject, encodedOrder);
      console.log(`Published order ${orderId} to subject ${subject}`);

      // Return the order from the database
      return orderFromDB;
    },
  },
};

// Initialize Apollo Server with HTTPS and NATS in the context
const startApolloServer = async () => {
  const nats = await connectNATS();

  const app = express();

  // Load TLS certificate and private key
  const httpsServer = https.createServer({
    key: fs.readFileSync('/certs/privkey.pem'),  // Private key
    cert: fs.readFileSync('/certs/fullchain.pem') // Certificate
  }, app);

  // Create Apollo Server
  const apolloServer = new ApolloServer({
    typeDefs,
    resolvers,
    context: () => ({ nats }), // Pass NATS instance in context
  });

  await apolloServer.start();
  apolloServer.applyMiddleware({ app });

  // Start the HTTPS server
  httpsServer.listen(8445, () => {
    console.log(`🚀 Server ready at https://localhost:8445${apolloServer.graphqlPath}`);
  });
};

// Start the server
startApolloServer().catch(err => {
  console.error('Error starting server:', err);
});
