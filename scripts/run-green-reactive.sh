#!/bin/bash
# Run 2: Green + Reactive — carbon-aware routing + HPA (CPU>80%)
# Scaling triggered by CPU load AFTER jobs arrive
set -e
PROJECT="green-cloud-pipeline-v2"
EMAPS_KEY="${ELECTRICITY_MAPS_API_KEY}"
if [ -z "$EMAPS_KEY" ]; then echo "ERROR: ELECTRICITY_MAPS_API_KEY not set"; exit 1; fi

echo "=== Run 2: Green + Reactive (HPA) Mode ==="
echo "Started at: $(date -u)"

gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet

kubectl create secret generic electricity-maps-secret \
  --from-literal=api-key="${EMAPS_KEY}" \
  --namespace green-cloud --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f infra/k8s/mumbai/redis.yaml
kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/carbon-forecaster.yaml | sed 's/value: "predictive"/value: "reactive"/')
YAML
kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/job-generator.yaml | sed 's/value: "baseline"/value: "green"/' | sed 's/value: "predictive"/value: "reactive"/')
YAML
kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/admission-controller.yaml | sed 's/value: "baseline"/value: "green"/' | sed 's/value: "predictive"/value: "reactive"/')
YAML
kubectl apply -f infra/k8s/mumbai/worker.yaml

gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet

kubectl apply -f infra/k8s/montreal/redis.yaml
kubectl apply -f - << YAML
$(cat infra/k8s/montreal/job-generator.yaml | sed 's/value: "baseline"/value: "green"/' | sed 's/value: "predictive"/value: "reactive"/')
YAML
# Apply full worker.yaml including HPA
kubectl apply -f infra/k8s/montreal/worker-deployment.yaml
kubectl apply -f infra/k8s/montreal/worker-hpa.yaml

echo "=== Waiting for pods ==="
gcloud container clusters get-credentials gcp-mumbai --zone asia-south1-a --project ${PROJECT} --quiet
kubectl rollout status deployment/redis -n green-cloud --timeout=180s
kubectl rollout status deployment/carbon-forecaster -n green-cloud --timeout=180s
kubectl rollout status deployment/job-generator -n green-cloud --timeout=180s
kubectl rollout status deployment/admission-controller -n green-cloud --timeout=180s
kubectl rollout status deployment/worker-mumbai -n green-cloud --timeout=180s

gcloud container clusters get-credentials gcp-montreal --zone northamerica-northeast1-a --project ${PROJECT} --quiet
kubectl rollout status deployment/redis -n green-cloud --timeout=180s
kubectl rollout status deployment/job-generator -n green-cloud --timeout=180s
kubectl rollout status deployment/worker-montreal -n green-cloud --timeout=180s

echo ""
echo "=== Mumbai pods ==="
gcloud container clusters get-credentials gcp-mumbai --zone asia-south1-a --project ${PROJECT} --quiet
kubectl get pods -n green-cloud

echo ""
echo "=== Montreal pods ==="
gcloud container clusters get-credentials gcp-montreal --zone northamerica-northeast1-a --project ${PROJECT} --quiet
kubectl get pods -n green-cloud
kubectl get hpa -n green-cloud

echo ""
echo "=== Run 2: Green + Reactive (HPA) active ==="
echo "Mumbai:   120 jobs/min | ADMISSION_MODE=green | SCALING_MODE=reactive"
echo "Montreal: 160 jobs/min | ADMISSION_MODE=green | HPA scales at CPU>80%"
echo "Scaling:  HPA reacts AFTER CPU spikes from redirected jobs"
echo "Started at: $(date -u)"
echo "Run for 30 minutes then: bash scripts/check-results.sh"
