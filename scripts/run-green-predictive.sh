#!/bin/bash
# Run 3: Green + Predictive — carbon-aware routing + proactive scaling
# Full deploy + start in one script

set -e
PROJECT="green-cloud-pipeline-v2"
EMAPS_KEY="${ELECTRICITY_MAPS_API_KEY}"

if [ -z "$EMAPS_KEY" ]; then
  echo "ERROR: ELECTRICITY_MAPS_API_KEY not set"
  exit 1
fi

echo "=== Run 3: Green + Predictive Mode ==="
echo "Started at: $(date -u)"

# ── Mumbai ────────────────────────────────────────────────────────────────────
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet

kubectl create secret generic electricity-maps-secret \
  --from-literal=api-key="${EMAPS_KEY}" \
  --namespace green-cloud --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f infra/k8s/mumbai/redis.yaml

# Carbon forecaster — predictive mode
kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/carbon-forecaster.yaml | \
  sed 's/value: "reactive"/value: "predictive"/')
YAML

# Job generator — green mode
kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/job-generator.yaml | \
  sed 's/value: "baseline"/value: "green"/')
YAML

# Admission controller — green mode
kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/admission-controller.yaml | \
  sed 's/value: "baseline"/value: "green"/')
YAML

# Worker — green mode
kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/worker.yaml | \
  sed 's/value: "baseline"/value: "green"/')
YAML

# ── Montreal ──────────────────────────────────────────────────────────────────
gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet

kubectl apply -f infra/k8s/montreal/redis.yaml

kubectl apply -f - << YAML
$(cat infra/k8s/montreal/job-generator.yaml | \
  sed 's/value: "baseline"/value: "green"/')
YAML

kubectl apply -f - << YAML
$(cat infra/k8s/montreal/worker.yaml | \
  sed 's/value: "baseline"/value: "green"/')
YAML

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
echo "=== Run 3: Green + Predictive active ==="
echo "ADMISSION_MODE=green | SCALING_MODE=predictive"
echo "Routing: batch jobs redirect when Montreal in green window or Mumbai dirty"
echo "Scaling: forecaster proactively scales Montreal before green window"
echo "Started at: $(date -u)"
echo ""
echo "To test predictive scaling, run in a new terminal:"
echo "  kubectl config use-context gke_${PROJECT}_asia-south1-a_gcp-mumbai"
echo "  kubectl port-forward -n green-cloud svc/carbon-forecaster 18080:8080 &"
echo "  curl -X POST http://localhost:18080/admin/simulate-green-window \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"duration_seconds\": 300, \"type\": \"approaching\"}'"
echo ""
echo "Let run for 30 minutes then:"
echo "  bash scripts/check-results.sh"
echo "  bash scripts/stop-all.sh"
