#!/bin/bash
# Run 1: Baseline — carbon-blind, all jobs stay local
# Full deploy + start in one script

set -e
PROJECT="green-cloud-pipeline-v2"
MUMBAI_REG="asia-south1-docker.pkg.dev/${PROJECT}/green-cloud-pipeline"
MONTREAL_REG="northamerica-northeast1-docker.pkg.dev/${PROJECT}/green-cloud-pipeline"
EMAPS_KEY="${ELECTRICITY_MAPS_API_KEY}"

if [ -z "$EMAPS_KEY" ]; then
  echo "ERROR: ELECTRICITY_MAPS_API_KEY not set"
  exit 1
fi

echo "=== Run 1: Baseline Mode ==="
echo "Started at: $(date -u)"

# ── Mumbai ────────────────────────────────────────────────────────────────────
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet

kubectl create secret generic electricity-maps-secret \
  --from-literal=api-key="${EMAPS_KEY}" \
  --namespace green-cloud --dry-run=client -o yaml | kubectl apply -f -

# Redis
kubectl apply -f infra/k8s/mumbai/redis.yaml

# Carbon forecaster (runs but admission controller ignores it in baseline)
cat infra/k8s/mumbai/carbon-forecaster.yaml | \
  sed 's/value: "reactive"/value: "reactive"/' | kubectl apply -f -

# Job generator
cat infra/k8s/mumbai/job-generator.yaml | \
  sed 's/value: "baseline"/value: "baseline"/' | \
  sed 's/value: "reactive"/value: "reactive"/' | kubectl apply -f -

# Admission controller
cat infra/k8s/mumbai/admission-controller.yaml | \
  sed 's/value: "baseline"/value: "baseline"/' | \
  sed 's/value: "reactive"/value: "reactive"/' | kubectl apply -f -

# Worker
cat infra/k8s/mumbai/worker.yaml | \
  sed 's/value: "baseline"/value: "baseline"/' | \
  sed 's/value: "reactive"/value: "reactive"/' | kubectl apply -f -

# ── Montreal ──────────────────────────────────────────────────────────────────
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet

kubectl apply -f infra/k8s/montreal/redis.yaml

cat infra/k8s/montreal/job-generator.yaml | \
  sed 's/value: "baseline"/value: "baseline"/' | \
  sed 's/value: "reactive"/value: "reactive"/' | kubectl apply -f -

cat infra/k8s/montreal/worker.yaml | \
  sed 's/value: "baseline"/value: "baseline"/' | \
  sed 's/value: "reactive"/value: "reactive"/' | kubectl apply -f -

# ── Wait for pods ─────────────────────────────────────────────────────────────
echo ""
echo "=== Waiting for Mumbai pods ==="
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet

kubectl rollout status deployment/redis -n green-cloud --timeout=180s
kubectl rollout status deployment/carbon-forecaster -n green-cloud --timeout=180s
kubectl rollout status deployment/job-generator -n green-cloud --timeout=180s
kubectl rollout status deployment/admission-controller -n green-cloud --timeout=180s
kubectl rollout status deployment/worker-mumbai -n green-cloud --timeout=180s

echo ""
echo "=== Waiting for Montreal pods ==="
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet

kubectl rollout status deployment/redis -n green-cloud --timeout=180s
kubectl rollout status deployment/job-generator -n green-cloud --timeout=180s
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
echo "ADMISSION_MODE=baseline | SCALING_MODE=reactive"
echo "Routing: all jobs stay in origin region — no carbon check"
echo "Started at: $(date -u)"
echo ""
echo "Let run for 30 minutes then:"
echo "  bash scripts/check-results.sh"
echo "  bash scripts/stop-all.sh"
