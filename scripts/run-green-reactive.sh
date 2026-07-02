#!/bin/bash
# Run 2: Green mode + reactive scaling
# Carbon-aware routing, GKE autoscaler handles scaling reactively
# ADMISSION_MODE=green, SCALING_MODE=reactive

set -e
echo "=== Starting Run 2: Green Mode + Reactive Scaling ==="
echo "Started at: $(date -u)"

# Mumbai
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project green-cloud-pipeline-v2 --quiet

kubectl scale deployment job-generator        --replicas=1 -n green-cloud
kubectl scale deployment admission-controller --replicas=1 -n green-cloud
kubectl scale deployment worker-mumbai        --replicas=1 -n green-cloud
kubectl scale deployment carbon-forecaster    --replicas=1 -n green-cloud

kubectl set env deployment/job-generator \
  ADMISSION_MODE=green SCALING_MODE=reactive -n green-cloud

kubectl set env deployment/admission-controller \
  ADMISSION_MODE=green SCALING_MODE=reactive -n green-cloud

kubectl set env deployment/worker-mumbai \
  ADMISSION_MODE=green SCALING_MODE=reactive -n green-cloud

kubectl set env deployment/carbon-forecaster \
  SCALING_MODE=reactive -n green-cloud

# Montreal
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project green-cloud-pipeline-v2 --quiet

kubectl scale deployment job-generator    --replicas=1 -n green-cloud
kubectl scale deployment worker-montreal  --replicas=1 -n green-cloud

kubectl set env deployment/job-generator \
  ADMISSION_MODE=green SCALING_MODE=reactive -n green-cloud

kubectl set env deployment/worker-montreal \
  ADMISSION_MODE=green SCALING_MODE=reactive -n green-cloud

echo ""
echo "=== Green + Reactive mode active ==="
echo "Routing: batch jobs redirect when Montreal in green window or Mumbai dirty"
echo "Scaling: GKE autoscaler reacts to pod pressure"
echo "Run for at least 30 minutes then check BigQuery"
echo "Started at: $(date -u)"
