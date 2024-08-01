# Use the official Node.js image.
FROM node:14

# Create and change to the app directory.
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY package.json package-lock.json ./

# Install dependencies.
RUN npm install

# Copy the rest of the application code.
COPY . .

# Set the command to run the backend service.
CMD ["node", "--max-old-space-size=2048", "server.js"]

