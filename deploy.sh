#!/bin/bash
set -e

echo "DSH ETL Search Deployment Script"

# Check if .env exists
if [ ! -f .env ]; then
    echo "ERROR: .env file not found!"
    echo "Copy .env.example and fill in the values:"
    echo "  cp .env.example .env"
    echo "  # then set:"
    echo "  MONGODB_URI=your_mongodb_atlas_uri"
    echo "  JWT_SECRET=your_secret_key"
    exit 1
fi

# Validate required variables
source .env

if [ -z "$MONGODB_URI" ]; then
    echo "ERROR: MONGODB_URI is not set in .env"
    exit 1
fi

if [ -z "$JWT_SECRET" ]; then
    echo "ERROR: JWT_SECRET is not set in .env"
    exit 1
fi

# Build and start
echo "Building containers..."
docker-compose build

echo "Starting services..."
docker-compose up -d

echo "Waiting for services to start..."
sleep 10

# Health check
echo "Checking health..."
if curl -sf http://localhost:8000/health > /dev/null; then
    echo "Backend: OK"
else
    echo "Backend: not ready yet (check: docker-compose logs backend)"
fi

if curl -sf http://localhost:80 > /dev/null; then
    echo "Frontend: OK"
else
    echo "Frontend: not ready yet (check: docker-compose logs frontend)"
fi

echo ""
echo "Deployment complete!"
echo "  Frontend: http://localhost"
echo "  Backend:  http://localhost:8000"
echo "  Health:   http://localhost:8000/health"
echo "  Logs:     docker-compose logs -f"
