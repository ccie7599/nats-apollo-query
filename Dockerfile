# Use the official Node.js image.
FROM node:18

# Create and change to the app directory.
WORKDIR /usr/src/app

# Copy application dependency manifests to the container image.
COPY package*.json ./

# Install dependencies.
RUN npm install

# Copy application code to the container image.
COPY . .

# Expose the port the app runs on.
EXPOSE 8445

# Run the web service on container startup.
CMD [ "node", "server.js" ]
