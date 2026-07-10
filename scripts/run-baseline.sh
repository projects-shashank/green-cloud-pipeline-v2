#!/bin/bash
# Run 1: Baseline — carbon-blind, all jobs stay local, no scaling
set -e
PROJECT="green-cloud-pipeline-v2"
EMAPS_KEY="${ELECTRICITY_MAPS_API_KEY}"
if [ -z "$EMAPS_KEY" ]; then echo "ERROR: ELECTRICITY_MAPS_API_KEY not set"; exit 1; fi

echo "=== Run 1: Baseline Mode ==="
echo "Started at: $(date -u)"

# ── Step 1: Deploy ALL workers and supporting pods (NO generators yet) ────────

# Mumbai
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet

kubectl create secret generic electricity-maps-secret \
  --from-literal=api-key="${EMAPS_KEY}" \
  --namespace green-cloud --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f infra/k8s/mumbai/redis.yaml
kubectl apply -f infra/k8s/mumbai/carbon-forecaster.yaml

kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/admission-controller.yaml \
  | sed 's/value: "green"/value: "baseline"/' \
  | sed 's/value: "predictive"/value: "reactive"/')
YAML

kubectl apply -f infra/k8s/mumbai/worker.yaml

# Montreal
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet

kubectl delete hpa worker-montreal-hpa -n green-cloud --ignore-not-found
kubectl create secret generic electricity-maps-secret --from-literal=api-key="${EMAPS_KEY}" --namespace green-cloud --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f infra/k8s/montreal/redis.yaml
kubectl apply -f infra/k8s/montreal/carbon-forecaster.yaml
kubectl apply -f infra/k8s/montreal/worker-deployment.yaml

# ── Step 2: Wait for ALL workers on BOTH clusters to be ready ────────────────
echo ""
echo "=== Waiting for Mumbai workers ==="
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet
kubectl rollout status deployment/redis                -n green-cloud --timeout=180s
kubectl rollout status deployment/carbon-forecaster    -n green-cloud --timeout=180s
kubectl rollout status deployment/admission-controller -n green-cloud --timeout=180s
kubectl rollout status deployment/worker-mumbai        -n green-cloud --timeout=180s

echo ""
echo "=== Waiting for Montreal workers ==="
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet
kubectl rollout status deployment/redis            -n green-cloud --timeout=180s
kubectl rollout status deployment/carbon-forecaster -n green-cloud --timeout=180s
kubectl rollout status deployment/worker-montreal  -n green-cloud --timeout=180s

# ── Step 3: Wait for Pub/Sub streaming pulls to establish ────────────────────
# rollout status = pod running, NOT = worker actively consuming from Pub/Sub
# The streaming pull client needs ~10s to connect and start pulling
echo ""
echo "All workers running. Waiting 3m for Pub/Sub streaming pulls to establish..."
sleep 180

# ── Step 4: Start BOTH generators simultaneously ─────────────────────────────
echo "Starting job generators on both clusters simultaneously..."

gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet

kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/job-generator.yaml \
  | sed 's/value: "green"/value: "baseline"/' \
  | sed 's/value: "predictive"/value: "reactive"/')
YAML

gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet

kubectl apply -f - << YAML
$(cat infra/k8s/montreal/job-generator.yaml \
  | sed 's/value: "green"/value: "baseline"/' \
  | sed 's/value: "predictive"/value: "reactive"/')
YAML

# Wait for both generators to be running
echo "Waiting for generators to start..."
kubectl rollout status deployment/job-generator -n green-cloud --timeout=120s || true

gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet
kubectl rollout status deployment/job-generator -n green-cloud --timeout=120s || true

# ── Step 5: Status ────────────────────────────────────────────────────────────
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
echo "Mumbai:   180 jobs/min  | ADMISSION_MODE=baseline | No scaling"
echo "Montreal: 240 jobs/min  | ADMISSION_MODE=baseline | No scaling"
echo "Processing: interactive 100-500ms | batch 500-2000ms"
echo "SLA threshold: 5000ms"
echo "Started at: $(date -u)"
echo "Run for 24 hours then: bash scripts/check-results.sh"
