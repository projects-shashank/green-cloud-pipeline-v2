#!/bin/bash
# Run 3: Green mode + predictive scaling
# Carbon-aware routing + proactive scale-up before green window
# ADMISSION_MODE=green, SCALING_MODE=predictive

set -e
echo "=== Starting Run 3: Green Mode + Predictive Scaling ==="
echo "Started at: $(date -u)"

# Mumbai
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project green-cloud-pipeline-v2 --quiet

kubectl scale deployment job-generator        --replicas=1 -n green-cloud
kubectl scale deployment admission-controller --replicas=1 -n green-cloud
kubectl scale deployment worker-mumbai        --replicas=1 -n green-cloud
kubectl scale deployment carbon-forecaster    --replicas=1 -n green-cloud

kubectl set env deployment/job-generator \
  ADMISSION_MODE=green SCALING_MODE=predictive -n green-cloud

kubectl set env deployment/admission-controller \
  ADMISSION_MODE=green SCALING_MODE=predictive -n green-cloud

kubectl set env deployment/worker-mumbai \
  ADMISSION_MODE=green SCALING_MODE=predictive -n green-cloud

kubectl set env deployment/carbon-forecaster \
  SCALING_MODE=predictive -n green-cloud

# Montreal
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project green-cloud-pipeline-v2 --quiet

kubectl scale deployment job-generator    --replicas=1 -n green-cloud
kubectl scale deployment worker-montreal  --replicas=1 -n green-cloud

kubectl set env deployment/job-generator \
  ADMISSION_MODE=green SCALING_MODE=predictive -n green-cloud

kubectl set env deployment/worker-montreal \
  ADMISSION_MODE=green SCALING_MODE=predictive -n green-cloud

echo ""
echo "=== Green + Predictive mode active ==="
echo "Routing: batch jobs redirect when Montreal in green window or Mumbai dirty"
echo "Scaling: forecaster proactively scales Montreal before green window arrives"
echo "Run for at least 30 minutes then check BigQuery"
echo "Started at: $(date -u)"
