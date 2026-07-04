#!/bin/bash
set -e
echo "=== Stopping experiment pods (keeping monitoring) ==="

gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project green-cloud-pipeline-v2 --quiet

# Delete only experiment pods — keep prometheus and grafana
kubectl delete deployment job-generator admission-controller \
  worker-mumbai carbon-forecaster redis \
  -n green-cloud --ignore-not-found
kubectl delete service carbon-forecaster \
  -n green-cloud --ignore-not-found 2>/dev/null || true

gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project green-cloud-pipeline-v2 --quiet

kubectl delete deployment job-generator worker-montreal \
  carbon-forecaster redis \
  -n green-cloud --ignore-not-found
kubectl delete hpa --all -n green-cloud --ignore-not-found

echo "Experiment pods deleted. Monitoring still running."
echo "Waiting 30s for generators to fully stop..."
sleep 30

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

echo "All subscriptions recreated."
echo "All stopped at: $(date -u)"
echo ""
echo "Next: truncate BQ then run experiment:"
echo "  bq query --project_id green-cloud-pipeline-v2 --use_legacy_sql=false 'TRUNCATE TABLE green_cloud_pipeline.jobs'"
echo "  ELECTRICITY_MAPS_API_KEY=your_key bash scripts/run-baseline.sh"
