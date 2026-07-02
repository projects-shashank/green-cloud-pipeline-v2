#!/bin/bash
# Stop and delete all application pods from both clusters
# Keeps infrastructure (clusters, pubsub, bigquery, gcs) intact

set -e
echo "=== Stopping all pods ==="

gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project green-cloud-pipeline-v2 --quiet

kubectl delete deployment --all -n green-cloud --ignore-not-found
kubectl delete service carbon-forecaster carbon-forecaster-external \
  -n green-cloud --ignore-not-found 2>/dev/null || true

gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project green-cloud-pipeline-v2 --quiet

kubectl delete deployment --all -n green-cloud --ignore-not-found
kubectl delete hpa --all -n green-cloud --ignore-not-found

echo "All pods deleted at: $(date -u)"
echo ""
echo "Next: run one of:"
echo "  bash scripts/run-baseline.sh"
echo "  bash scripts/run-green-reactive.sh"
echo "  bash scripts/run-green-predictive.sh"
