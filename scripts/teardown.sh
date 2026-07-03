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
kubectl delete service carbon-forecaster carbon-forecaster-external \
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

# Recreate subscriptions clean
echo "--- Recreating subscriptions ---"
for sub in mumbai-jobs-sub mumbai-process-sub montreal-jobs-sub; do
  case $sub in
    mumbai-jobs-sub)    TOPIC="mumbai-jobs";    DLQ="mumbai-dlq" ;;
    mumbai-process-sub) TOPIC="mumbai-process"; DLQ="mumbai-dlq" ;;
    montreal-jobs-sub)  TOPIC="montreal-jobs";  DLQ="montreal-dlq" ;;
  esac
  echo "Recreating $sub..."
  gcloud pubsub subscriptions delete $sub \
    --project green-cloud-pipeline-v2 --quiet 2>/dev/null || true
  gcloud pubsub subscriptions create $sub \
    --topic=projects/green-cloud-pipeline-v2/topics/${TOPIC} \
    --project=green-cloud-pipeline-v2 \
    --ack-deadline=60 \
    --message-retention-duration=86400s \
    --dead-letter-topic=projects/green-cloud-pipeline-v2/topics/${DLQ} \
    --max-delivery-attempts=5 \
    --min-retry-delay=10s \
    --max-retry-delay=300s
done

# Check quota back to 0
bash scripts/quota-check.sh

echo ""
echo "=== Teardown complete at: $(date -u) ==="
echo "Preserved: BigQuery, GCS, Pub/Sub topics, Artifact Registry"
echo "Next: bash scripts/redeploy.sh"
