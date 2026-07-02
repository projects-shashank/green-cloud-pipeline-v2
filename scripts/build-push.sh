#!/bin/bash
# Build all Docker images and push to Artifact Registry in both regions
# Run from repo root: bash scripts/build-push.sh

set -e  # exit on any error

PROJECT_ID="green-cloud-pipeline-v2"
MUMBAI_REGISTRY="asia-south1-docker.pkg.dev/${PROJECT_ID}/green-cloud-pipeline"
MONTREAL_REGISTRY="northamerica-northeast1-docker.pkg.dev/${PROJECT_ID}/green-cloud-pipeline"
TAG="latest"

SERVICES=("job-generator" "carbon-forecaster" "admission-controller" "worker")

echo "=== Authenticating Docker with Artifact Registry ==="
gcloud auth configure-docker \
  asia-south1-docker.pkg.dev \
  northamerica-northeast1-docker.pkg.dev \
  --quiet

echo ""
echo "=== Building images ==="
for SERVICE in "${SERVICES[@]}"; do
  echo "--- Building ${SERVICE} ---"
  docker build \
    -f services/${SERVICE}/Dockerfile \
    -t ${MUMBAI_REGISTRY}/${SERVICE}:${TAG} \
    -t ${MONTREAL_REGISTRY}/${SERVICE}:${TAG} \
    .
  echo "Built ${SERVICE} successfully"
done

echo ""
echo "=== Pushing to Mumbai (asia-south1) ==="
for SERVICE in "${SERVICES[@]}"; do
  echo "--- Pushing ${SERVICE} to Mumbai ---"
  docker push ${MUMBAI_REGISTRY}/${SERVICE}:${TAG}
done

echo ""
echo "=== Pushing to Montreal (northamerica-northeast1) ==="
for SERVICE in "${SERVICES[@]}"; do
  echo "--- Pushing ${SERVICE} to Montreal ---"
  docker push ${MONTREAL_REGISTRY}/${SERVICE}:${TAG}
done

echo ""
echo "=== All images built and pushed successfully ==="
echo "Mumbai:   ${MUMBAI_REGISTRY}"
echo "Montreal: ${MONTREAL_REGISTRY}"
