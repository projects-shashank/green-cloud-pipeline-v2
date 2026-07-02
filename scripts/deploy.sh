#!/bin/bash
set -e

PROJECT_ID="green-cloud-pipeline-v2"
EMAPS_API_KEY="${ELECTRICITY_MAPS_API_KEY}"

if [ -z "$EMAPS_API_KEY" ]; then
  echo "ERROR: ELECTRICITY_MAPS_API_KEY env var not set"
  exit 1
fi

echo "=== Deploying to Mumbai (asia-south1-a) ==="
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a \
  --project ${PROJECT_ID}

# Create secret for Electricity Maps API key
kubectl create secret generic electricity-maps-secret \
  --from-literal=api-key="${EMAPS_API_KEY}" \
  --namespace green-cloud \
  --dry-run=client -o yaml | kubectl apply -f -

# Apply all Mumbai manifests
kubectl apply -f infra/k8s/mumbai/redis.yaml
kubectl apply -f infra/k8s/mumbai/carbon-forecaster.yaml
kubectl apply -f infra/k8s/mumbai/job-generator.yaml
kubectl apply -f infra/k8s/mumbai/admission-controller.yaml
kubectl apply -f infra/k8s/mumbai/worker.yaml

echo ""
echo "=== Deploying to Montreal (northamerica-northeast1-a) ==="
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a \
  --project ${PROJECT_ID}

# Apply all Montreal manifests
kubectl apply -f infra/k8s/montreal/redis.yaml
kubectl apply -f infra/k8s/montreal/job-generator.yaml
kubectl apply -f infra/k8s/montreal/worker.yaml

echo ""
echo "=== Waiting for pods to be ready ==="

echo "--- Mumbai pods ---"
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT_ID} --quiet
kubectl rollout status deployment/redis              -n green-cloud --timeout=120s
kubectl rollout status deployment/carbon-forecaster  -n green-cloud --timeout=120s
kubectl rollout status deployment/job-generator      -n green-cloud --timeout=120s
kubectl rollout status deployment/admission-controller -n green-cloud --timeout=120s
kubectl rollout status deployment/worker-mumbai      -n green-cloud --timeout=120s

echo ""
echo "--- Montreal pods ---"
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT_ID} --quiet
kubectl rollout status deployment/redis              -n green-cloud --timeout=120s
kubectl rollout status deployment/job-generator      -n green-cloud --timeout=120s
kubectl rollout status deployment/worker-montreal    -n green-cloud --timeout=120s

echo ""
echo "=== Deploy complete ==="
echo ""
echo "--- Mumbai pod status ---"
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT_ID} --quiet
kubectl get pods -n green-cloud

echo ""
echo "--- Montreal pod status ---"
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT_ID} --quiet
kubectl get pods -n green-cloud
