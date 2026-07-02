#!/bin/bash
# Run 3: Green + Predictive — carbon-aware routing + forecaster scaling
# Scaling triggered BEFORE green window by carbon intensity forecast
set -e
PROJECT="green-cloud-pipeline-v2"
EMAPS_KEY="${ELECTRICITY_MAPS_API_KEY}"
if [ -z "$EMAPS_KEY" ]; then echo "ERROR: ELECTRICITY_MAPS_API_KEY not set"; exit 1; fi

echo "=== Run 3: Green + Predictive Mode ==="
echo "Started at: $(date -u)"

gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet

kubectl create secret generic electricity-maps-secret \
  --from-literal=api-key="${EMAPS_KEY}" \
  --namespace green-cloud --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f infra/k8s/mumbai/redis.yaml
kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/carbon-forecaster.yaml | sed 's/value: "reactive"/value: "predictive"/')
YAML
kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/job-generator.yaml | sed 's/value: "baseline"/value: "green"/')
YAML
kubectl apply -f - << YAML
$(cat infra/k8s/mumbai/admission-controller.yaml | sed 's/value: "baseline"/value: "green"/')
YAML
kubectl apply -f infra/k8s/mumbai/worker.yaml

gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet

# Remove HPA — predictive mode uses forecaster scaling only
kubectl delete hpa worker-montreal-hpa -n green-cloud --ignore-not-found

kubectl apply -f infra/k8s/montreal/redis.yaml
kubectl apply -f - << YAML
$(cat infra/k8s/montreal/job-generator.yaml | sed 's/value: "baseline"/value: "green"/')
YAML
# Apply worker WITHOUT HPA
kubectl apply -f - << YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker-montreal
  namespace: green-cloud
spec:
  replicas: 1
  selector:
    matchLabels:
      app: worker-montreal
  template:
    metadata:
      labels:
        app: worker-montreal
    spec:
      serviceAccountName: green-cloud-sa
      containers:
      - name: worker
        image: northamerica-northeast1-docker.pkg.dev/${PROJECT}/green-cloud-pipeline/worker:latest
        env:
        - name: PROJECT_ID
          value: "${PROJECT}"
        - name: REGION
          value: "montreal"
        - name: SUBSCRIPTION_ID
          value: "montreal-jobs-sub"
        - name: CARBON_FORECASTER_URL
          value: "http://35.200.248.98:8080/carbon"
        - name: GCS_BUCKET
          value: "${PROJECT}-data-lake"
        - name: REDIS_HOST
          value: "redis"
        - name: SLA_THRESHOLD_MS
          value: "5000"
        resources:
          requests:
            cpu: "600m"
            memory: "512Mi"
          limits:
            cpu: "900m"
            memory: "1Gi"
YAML

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

echo ""
echo "=== Run 3: Green + Predictive active ==="
echo "Mumbai:   120 jobs/min | ADMISSION_MODE=green | SCALING_MODE=predictive"
echo "Montreal: 160 jobs/min | ADMISSION_MODE=green | Forecaster scales BEFORE green window"
echo "Scaling:  Proactive — second worker ready before jobs arrive"
echo ""
echo "To trigger simulation:"
echo "  kubectl config use-context gke_${PROJECT}_asia-south1-a_gcp-mumbai"
echo "  kubectl port-forward -n green-cloud svc/carbon-forecaster 18080:8080 &"
echo "  curl -X POST http://localhost:18080/admin/simulate-green-window \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"duration_seconds\": 600, \"type\": \"approaching\"}'"
echo ""
echo "Started at: $(date -u)"
echo "Run for 30 minutes then: bash scripts/check-results.sh"
