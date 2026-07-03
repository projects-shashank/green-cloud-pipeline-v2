#!/bin/bash
# Teardown — destroy both GKE clusters and clean subscriptions
# BigQuery, GCS, Artifact Registry, Pub/Sub topics are preserved

set -e
echo "=== Teardown: Destroying clusters ==="
echo "Started at: $(date -u)"

# Delete all pods first
echo "--- Deleting Mumbai pods ---"
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project green-cloud-pipeline-v2 --quiet 2>/dev/null || true
kubectl delete deployment --all -n green-cloud --ignore-not-found 2>/dev/null || true
kubectl delete service carbon-forecaster \
  -n green-cloud --ignore-not-found 2>/dev/null || true

echo "--- Deleting Montreal pods ---"
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project green-cloud-pipeline-v2 --quiet 2>/dev/null || true
kubectl delete deployment --all -n green-cloud --ignore-not-found 2>/dev/null || true
kubectl delete hpa --all -n green-cloud --ignore-not-found 2>/dev/null || true

echo "Waiting 30s for pods to terminate..."
sleep 30

# Destroy Montreal cluster
echo "--- Destroying Montreal cluster ---"
cd terraform/environments/montreal
terraform destroy -auto-approve
cd ../../..

# Destroy Mumbai cluster
echo "--- Destroying Mumbai cluster ---"
cd terraform/environments/mumbai
terraform destroy -auto-approve
cd ../../..


# Check quota back to 0
bash scripts/quota-check.sh

echo ""
echo "=== Teardown complete at: $(date -u) ==="
echo "Preserved: BigQuery, GCS, Pub/Sub topics, Artifact Registry"
echo "Next: bash scripts/redeploy.sh"
