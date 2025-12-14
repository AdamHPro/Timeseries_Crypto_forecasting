# ./front/Dockerfile

# Use a Node image (LTS version recommended)
FROM node:18-alpine

WORKDIR /app

# Copy package.json to install dependencies
COPY package.json .
# Occasionally package-lock.json too if you have it
# COPY package-lock.json .

RUN npm install

# Copy the rest (though your volume in docker-compose will override this in dev)
COPY . .

# Start React in development mode
CMD ["npm", "start"]