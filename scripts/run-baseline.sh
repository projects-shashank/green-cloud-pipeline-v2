#!/bin/bash
# Run 1: Baseline mode
# Carbon-blind — all jobs process locally, no carbon check
# ADMISSION_MODE=baseline, SCALING_MODE=reactive

set -e
echo "=== Starting Run 1: Baseline Mode ==="
echo "Started at: $(date -u)"

# Mumbai
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project green-cloud-pipeline-v2 --quiet

kubectl scale deployment job-generator        --replicas=1 -n green-cloud
kubectl scale deployment admission-controller --replicas=1 -n green-cloud
kubectl scale deployment worker-mumbai        --replicas=1 -n green-cloud

kubectl set env deployment/job-generator \
  ADMISSION_MODE=baseline SCALING_MODE=reactive -n green-cloud

kubectl set env deployment/admission-controller \
  ADMISSION_MODE=baseline SCALING_MODE=reactive -n green-cloud

kubectl set env deployment/worker-mumbai \
  ADMISSION_MODE=baseline SCALING_MODE=reactive -n green-cloud

# Montreal
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project green-cloud-pipeline-v2 --quiet

kubectl scale deployment job-generator    --replicas=1 -n green-cloud
kubectl scale deployment worker-montreal  --replicas=1 -n green-cloud

kubectl set env deployment/job-generator \
  ADMISSION_MODE=baseline SCALING_MODE=reactive -n green-cloud

kubectl set env deployment/worker-montreal \
  ADMISSION_MODE=baseline SCALING_MODE=reactive -n green-cloud

echo ""
echo "=== Baseline mode active ==="
echo "Routing: all jobs stay in origin region"
echo "Run for at least 30 minutes then check BigQuery"
echo "Started at: $(date -u)"
