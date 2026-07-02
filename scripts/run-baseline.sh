#!/bin/bash
# Run 1: Baseline — carbon-blind, all jobs stay local, no scaling
set -e
PROJECT="green-cloud-pipeline-v2"
EMAPS_KEY="${ELECTRICITY_MAPS_API_KEY}"
if [ -z "$EMAPS_KEY" ]; then echo "ERROR: ELECTRICITY_MAPS_API_KEY not set"; exit 1; fi

echo "=== Run 1: Baseline Mode ==="
echo "Started at: $(date -u)"

# ── Mumbai ────────────────────────────────────────────────────────────────────
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet

kubectl create secret generic electricity-maps-secret \
  --from-literal=api-key="${EMAPS_KEY}" \
  --namespace green-cloud --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f infra/k8s/mumbai/redis.yaml
kubectl apply -f infra/k8s/mumbai/carbon-forecaster.yaml

kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/job-generator.yaml \
  | sed 's/value: "green"/value: "baseline"/' \
  | sed 's/value: "predictive"/value: "reactive"/')
YAML

kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/admission-controller.yaml \
  | sed 's/value: "green"/value: "baseline"/' \
  | sed 's/value: "predictive"/value: "reactive"/')
YAML

kubectl apply -f infra/k8s/mumbai/worker.yaml

# ── Montreal ──────────────────────────────────────────────────────────────────
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet

kubectl delete hpa worker-montreal-hpa -n green-cloud --ignore-not-found

kubectl apply -f infra/k8s/montreal/redis.yaml

kubectl apply -f - << YAML
$(cat infra/k8s/montreal/job-generator.yaml \
  | sed 's/value: "green"/value: "baseline"/' \
  | sed 's/value: "predictive"/value: "reactive"/')
YAML

kubectl apply -f infra/k8s/montreal/worker-deployment.yaml

# ── Wait ──────────────────────────────────────────────────────────────────────
echo "=== Waiting for Mumbai pods ==="
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet
kubectl rollout status deployment/redis              -n green-cloud --timeout=180s
kubectl rollout status deployment/carbon-forecaster  -n green-cloud --timeout=180s
kubectl rollout status deployment/job-generator      -n green-cloud --timeout=180s
kubectl rollout status deployment/admission-controller -n green-cloud --timeout=180s
kubectl rollout status deployment/worker-mumbai      -n green-cloud --timeout=180s

echo "=== Waiting for Montreal pods ==="
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet
kubectl rollout status deployment/redis           -n green-cloud --timeout=180s
kubectl rollout status deployment/job-generator   -n green-cloud --timeout=180s
kubectl rollout status deployment/worker-montreal -n green-cloud --timeout=180s

# ── Status ────────────────────────────────────────────────────────────────────
echo ""
echo "=== Mumbai pods ==="
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet
kubectl get pods -n green-cloud

echo ""
echo "=== Montreal pods ==="
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet
kubectl get pods -n green-cloud

echo ""
echo "=== Run 1: Baseline active ==="
echo "Mumbai:   120 jobs/min | ADMISSION_MODE=baseline | No scaling"
echo "Montreal: 160 jobs/min | ADMISSION_MODE=baseline | No scaling"
echo "Processing: interactive 0.5-2s | batch 2-8s"
echo "SLA threshold: 5000ms"
echo "Started at: $(date -u)"
echo "Run for 5 minutes then: bash scripts/check-results.sh"
