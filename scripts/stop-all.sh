#!/bin/bash
# Stop all generators and workers — use before switching runs

echo "=== Stopping all services ==="

gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project green-cloud-pipeline-v2 --quiet

kubectl scale deployment job-generator        --replicas=0 -n green-cloud
kubectl scale deployment admission-controller --replicas=0 -n green-cloud
kubectl scale deployment worker-mumbai        --replicas=0 -n green-cloud

gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project green-cloud-pipeline-v2 --quiet

kubectl scale deployment job-generator   --replicas=0 -n green-cloud
kubectl scale deployment worker-montreal --replicas=0 -n green-cloud

echo "All services stopped at: $(date -u)"
echo "Run: bq query --project_id green-cloud-pipeline-v2 --use_legacy_sql=false 'TRUNCATE TABLE green_cloud_pipeline.jobs'"
echo "Then run the next experiment script"
