# Green Cloud Pipeline v2

Carbon-aware ML/GenAI job scheduling across two real GCP regions.

## Regions
- Mumbai (asia-south1) — origin region
- Montreal (northamerica-northeast1) — green redirect target

## Architecture
Two real GKE clusters, one per region. Jobs are routed by an admission
controller that compares live carbon intensity (Electricity Maps API)
against dynamic thresholds derived from 24h historical data.

## Modes
- `ADMISSION_MODE=baseline` — carbon-blind, jobs stay in origin region
- `ADMISSION_MODE=green` — carbon-aware, batch jobs redirected when origin is dirty

## Setup
See docs/ for phase-by-phase setup instructions.

## Tech Stack
GKE · Pub/Sub · BigQuery · GCS · Redis · Prometheus · Grafana · Terraform · GitHub Actions
