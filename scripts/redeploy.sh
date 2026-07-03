#!/bin/bash
# Redeploy — provision clusters and push Docker images
# Does NOT start any services — use run-baseline/reactive/predictive for that

set -e
echo "=== Redeploy: Provisioning infrastructure ==="
echo "Started at: $(date -u)"

PROJECT="green-cloud-pipeline-v2"
MUMBAI_REG="asia-south1-docker.pkg.dev/${PROJECT}/green-cloud-pipeline"
MONTREAL_REG="northamerica-northeast1-docker.pkg.dev/${PROJECT}/green-cloud-pipeline"

# ── Step 1: Quota check ───────────────────────────────────────────────────────
echo "--- Checking quota ---"
bash scripts/quota-check.sh

# ── Step 2: Provision Mumbai cluster ─────────────────────────────────────────
echo ""
echo "--- Provisioning Mumbai cluster ---"
cd terraform/environments/mumbai
terraform init -upgrade -input=false
terraform apply -auto-approve
cd ../../..

bash scripts/quota-check.sh

# ── Step 3: Provision Montreal cluster ───────────────────────────────────────
echo ""
echo "--- Provisioning Montreal cluster ---"
cd terraform/environments/montreal
terraform init -upgrade -input=false
terraform apply -auto-approve
cd ../../..

bash scripts/quota-check.sh

# ── Step 4: Set up Workload Identity on both clusters ────────────────────────
echo ""
echo "--- Setting up Workload Identity ---"
SA_EMAIL="green-cloud-pipeline-sa@${PROJECT}.iam.gserviceaccount.com"

gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet

kubectl create namespace green-cloud --dry-run=client -o yaml | kubectl apply -f -
kubectl create serviceaccount green-cloud-sa --namespace green-cloud \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl annotate serviceaccount green-cloud-sa --namespace green-cloud \
  iam.gke.io/gcp-service-account=${SA_EMAIL} --overwrite

gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet

kubectl create namespace green-cloud --dry-run=client -o yaml | kubectl apply -f -
kubectl create serviceaccount green-cloud-sa --namespace green-cloud \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl annotate serviceaccount green-cloud-sa --namespace green-cloud \
  iam.gke.io/gcp-service-account=${SA_EMAIL} --overwrite

# ── Step 5: Build and push Docker images ─────────────────────────────────────
echo ""
echo "--- Building and pushing Docker images ---"

gcloud auth configure-docker asia-south1-docker.pkg.dev --quiet
gcloud auth configure-docker northamerica-northeast1-docker.pkg.dev --quiet

SERVICES=("job-generator" "carbon-forecaster" "admission-controller" "worker")

for SERVICE in "${SERVICES[@]}"; do
  echo "Building ${SERVICE}..."
  docker build \
    -f services/${SERVICE}/Dockerfile \
    -t ${MUMBAI_REG}/${SERVICE}:latest \
    -t ${MONTREAL_REG}/${SERVICE}:latest \
    .
done

echo "Pushing to Mumbai..."
for SERVICE in "${SERVICES[@]}"; do
  docker push ${MUMBAI_REG}/${SERVICE}:latest
done

echo "Pushing to Montreal..."
for SERVICE in "${SERVICES[@]}"; do
  docker push ${MONTREAL_REG}/${SERVICE}:latest
done

# ── Step 6: Verify clusters ───────────────────────────────────────────────────
echo ""
echo "--- Verifying clusters ---"
gcloud container clusters get-credentials gcp-mumbai \
  --zone asia-south1-a --project ${PROJECT} --quiet
echo "Mumbai nodes:"
kubectl get nodes

gcloud container clusters get-credentials gcp-montreal \
  --zone northamerica-northeast1-a --project ${PROJECT} --quiet
echo "Montreal nodes:"
kubectl get nodes

echo ""
echo "=== Redeploy complete at: $(date -u) ==="
echo ""
echo "Infrastructure ready. Start an experiment run:"
echo "  ELECTRICITY_MAPS_API_KEY=your_key bash scripts/run-baseline.sh"
echo "  ELECTRICITY_MAPS_API_KEY=your_key bash scripts/run-green-reactive.sh"
echo "  ELECTRICITY_MAPS_API_KEY=your_key bash scripts/run-green-predictive.sh"
