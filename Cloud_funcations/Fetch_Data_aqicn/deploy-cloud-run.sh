#!/bin/bash

# Exit on error
set -e

# Load environment variables
if [ -f .env ]; then
  echo "Loading environment variables from .env"
  export $(grep -v '^#' .env | xargs)
else
  echo "Warning: .env file not found. Make sure to create it from .env.example"
  exit 1
fi

# Set default service name if not provided
SERVICE_NAME=${SERVICE_NAME:-"fetch-aqicn-service"}
REGION=${REGION:-"us-central1"}
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest"

echo "Deploying to Cloud Run with the following configuration:"
echo "Project ID: ${PROJECT_ID}"
echo "Service Name: ${SERVICE_NAME}"
echo "Region: ${REGION}"

# Ensure gcloud is configured with the correct project
gcloud config set project ${PROJECT_ID}

# Build the container image
echo "Building container image..."
gcloud builds submit --tag ${IMAGE_NAME} .

# Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image ${IMAGE_NAME} \
  --platform managed \
  --region ${REGION} \
  --allow-unauthenticated \
  --set-env-vars="BUCKET_NAME=${BUCKET_NAME},TOKEN=${TOKEN}"

echo "Deployment complete! Your service is now running at:"
gcloud run services describe ${SERVICE_NAME} --platform managed --region ${REGION} --format 'value(status.url)'
