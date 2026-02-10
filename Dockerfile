# --- Stage 1: Build Stage ---
FROM node:20-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build


# --- Stage 1.1: Binary Downloader ---
# We use a separate stage to grab the Shaka Packager binary safely
FROM alpine:latest AS tool-downloader
RUN apk add --no-cache curl
# Download the latest Linux x64 binary for Shaka Packager
RUN curl -L https://github.com/shaka-project/shaka-packager/releases/latest/download/packager-linux-x64 -o /usr/local/bin/packager
RUN chmod +x /usr/local/bin/packager

# --- Stage 2: Production Stage ---
FROM node:20-alpine
WORKDIR /app

# Install FFmpeg at the OS level (Critical for transcoding)
# Alpine uses 'apk' to manage packages
RUN apk update && apk add --no-cache ffmpeg

COPY --from=tool-downloader /usr/local/bin/packager /usr/local/bin/packager

# Install production dependencies
COPY package*.json ./
RUN npm install --omit=dev

# Copy compiled code from the builder stage
COPY --from=builder /app/dist ./dist

# Copy .env file for environment variables
COPY .env .env

# Create the temp directory for video processing with proper permissions
RUN mkdir -p temp && chown -R node:node temp

# Switch to a non-root user for better security on ECS Fargate
USER node

# Set production environment
ENV NODE_ENV=production

# Health check (optional but recommended for ECS)
# HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
#   CMD node -e "require('net').connect(6379, process.env.REDIS_HOST || 'localhost').on('connect', () => process.exit(0)).on('error', () => process.exit(1))"

# Start the worker
CMD ["node", "dist/index.js"]