"""
Worker — Green Cloud Pipeline v2

Receives routed jobs from Pub/Sub, estimates energy,
computes carbon accounting, and writes results to
BigQuery, GCS, and Redis.

Runs in both Mumbai and Montreal clusters.
Same code, different subscription via env var.

Environment variables:
  PROJECT_ID               GCP project ID (required)
  REGION                   mumbai or montreal (required)
  SUBSCRIPTION_ID          Pub/Sub subscription to pull from (required)
  CARBON_FORECASTER_URL    forecaster endpoint
  BQ_DATASET               BigQuery dataset (default: green_cloud_pipeline)
  BQ_TABLE                 BigQuery table (default: jobs)
  GCS_BUCKET               GCS bucket name (required)
  REDIS_HOST               Redis host (default: redis)
  SLA_THRESHOLD_MS         SLA violation threshold in ms (default: 5000)
  MAX_WORKERS              concurrent message processors (default: 4)
"""

import os
import sys
import json
import time
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

import redis
import requests
from google.cloud import pubsub_v1, bigquery, storage
from prometheus_client import Counter, Histogram, start_http_server, REGISTRY

sys.path.insert(0, "/app/shared")
from lineage import now_utc_iso
from energy_estimator import estimate_energy_kwh
from carbon_accounting import compute_carbon_emitted, compute_carbon_saved

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Prometheus metrics ────────────────────────────────────────────────────────

jobs_processed_total = Counter(
    'jobs_processed_total',
    'Total jobs processed',
    ['region', 'job_type', 'was_redirected']
)

job_latency_ms = Histogram(
    'job_latency_ms',
    'Job end-to-end latency in milliseconds',
    ['region', 'job_type', 'was_redirected'],
    buckets=[100, 250, 500, 1000, 2000, 3000, 5000, 10000, 30000, 60000]
)

carbon_emitted_grams_total = Counter(
    'carbon_emitted_grams_total',
    'Total carbon emitted in grams',
    ['region']
)

carbon_saved_grams_total = Counter(
    'carbon_saved_grams_total',
    'Total carbon saved in grams',
    ['region']
)

sla_violations_total = Counter(
    'sla_violations_total',
    'Total SLA violations',
    ['region', 'job_type']
)

energy_kwh_total = Counter(
    'energy_kwh_total',
    'Total energy consumed in kWh',
    ['region', 'job_type']
)

# ── Config ────────────────────────────────────────────────────────────────────

def get_env(key, default=None):
    val = os.environ.get(key, default)
    if val is None:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val

PROJECT_ID            = get_env("PROJECT_ID")
REGION                = get_env("REGION").lower()
SUBSCRIPTION_ID       = get_env("SUBSCRIPTION_ID")
CARBON_FORECASTER_URL = get_env("CARBON_FORECASTER_URL",
                                 "http://carbon-forecaster:8080/carbon")
BQ_DATASET            = get_env("BQ_DATASET", "green_cloud_pipeline")
BQ_TABLE              = get_env("BQ_TABLE", "jobs")
GCS_BUCKET            = get_env("GCS_BUCKET")
REDIS_HOST            = get_env("REDIS_HOST", "redis")
REDIS_PORT            = int(os.environ.get("REDIS_SERVICE_PORT", "6379"))
SLA_THRESHOLD_MS      = int(get_env("SLA_THRESHOLD_MS", "5000"))
MAX_WORKERS           = int(get_env("MAX_WORKERS", "4"))

SUBSCRIPTION_PATH = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
BQ_TABLE_PATH     = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

# ── GCP clients — thread-local ────────────────────────────────────────────────
# Each thread gets its own BigQuery and GCS client instance.
# Shared clients with default connection pools serialize concurrent I/O
# causing queue buildup when multiple threads write simultaneously.

_thread_local = threading.local()

def get_bq_client():
    if not hasattr(_thread_local, "bq_client"):
        _thread_local.bq_client = bigquery.Client(project=PROJECT_ID)
    return _thread_local.bq_client

def get_gcs_bucket():
    if not hasattr(_thread_local, "gcs_bucket"):
        gcs_client = storage.Client(project=PROJECT_ID)
        _thread_local.gcs_bucket = gcs_client.bucket(GCS_BUCKET)
    return _thread_local.gcs_bucket

# ── Redis client ──────────────────────────────────────────────────────────────

def get_redis_client():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                        decode_responses=True, socket_timeout=3)
        r.ping()
        log.info("Redis connected at %s:%d", REDIS_HOST, REDIS_PORT)
        return r
    except Exception as e:
        log.warning("Redis unavailable: %s — hot cache disabled", e)
        return None

redis_client = get_redis_client()

# ── Carbon intensity — thread-safe cache ──────────────────────────────────────
# Bug fixed: without a lock, all 4 threads simultaneously check cache expiry
# and all make HTTP calls at the same time (thundering herd).
# Previously Montreal called an external IP (35.200.248.98) causing cross-region
# HTTP delays. Now both regions use a local forecaster — lock prevents thundering herd.

_carbon_cache      = {"data": None, "fetched_at": 0.0}
_carbon_cache_lock = threading.Lock()
_CARBON_TTL        = 60

REGIONAL_DEFAULTS = {
    "mumbai":   380.0,
    "montreal": 55.0,
}


def get_carbon_intensity(region: str) -> float:
    """
    Get carbon intensity for a region in gCO2/kWh.
    Thread-safe: only one thread fetches from forecaster at a time.
    All other threads wait for the lock and use the refreshed cache.
    """
    with _carbon_cache_lock:
        age = time.time() - _carbon_cache["fetched_at"]
        if _carbon_cache["data"] is not None and age < _CARBON_TTL:
            return _carbon_cache["data"][region]["current_intensity"]

    # Cache is stale — fetch fresh data (lock released during HTTP call)
    try:
        resp = requests.get(CARBON_FORECASTER_URL, timeout=3)
        resp.raise_for_status()
        data = resp.json()
        with _carbon_cache_lock:
            _carbon_cache["data"]       = data
            _carbon_cache["fetched_at"] = time.time()
        return data[region]["current_intensity"]
    except Exception as e:
        log.warning("Carbon fetch failed: %s — using cached/default", e)
        with _carbon_cache_lock:
            if _carbon_cache["data"] is not None:
                return _carbon_cache["data"][region]["current_intensity"]
        return REGIONAL_DEFAULTS.get(region, 300.0)

# ── Processing delay ──────────────────────────────────────────────────────────

def simulate_processing(job: dict) -> None:
    """
    Simulate realistic ML job processing time.

    Makes processing compute-bound so worker scaling is meaningful.
    Without this, jobs complete in ~150ms (I/O-bound BigQuery/GCS writes)
    and adding workers gives minimal throughput improvement.

    interactive_inference: 100ms-500ms based on output token count
        ~0.2ms per output token
    batch_image_gen: 500ms-2000ms based on number of images
        ~100ms per image
    """
    job_type = job.get("job_type", "")

    if job_type == "interactive_inference":
        output_tokens = job.get("expected_output_token_count", 512)
        delay = output_tokens * 0.0002       # 0.2ms per token
        delay = max(0.1, min(delay, 0.5))   # clamp 100ms-500ms

    elif job_type == "batch_image_gen":
        num_images = job.get("num_images_requested", 4)
        delay = num_images * 0.1             # 100ms per image
        delay = max(0.5, min(delay, 2.0))   # clamp 500ms-2000ms

    else:
        delay = 1.0

    time.sleep(delay)

# ── Storage writers ───────────────────────────────────────────────────────────

def write_to_bigquery(record: dict) -> None:
    errors = get_bq_client().insert_rows_json(BQ_TABLE_PATH, [record])
    if errors:
        log.error("BigQuery insert errors: %s", errors)
        raise RuntimeError(f"BigQuery insert failed: {errors}")


def write_to_gcs(record: dict) -> None:
    ts   = datetime.fromisoformat(record["arrival_timestamp"])
    path = (
        f"{record['processed_region']}/"
        f"{ts.year}/{ts.month:02d}/{ts.day:02d}/{ts.hour:02d}/"
        f"{record['job_id']}.json"
    )
    blob = get_gcs_bucket().blob(path)
    blob.upload_from_string(
        json.dumps(record),
        content_type="application/json",
    )


def write_to_redis(record: dict) -> None:
    if redis_client is None:
        return
    try:
        key = f"job:{record['job_id']}"
        redis_client.setex(
            key, 3600,
            json.dumps({
                "job_id":           record["job_id"],
                "job_type":         record["job_type"],
                "origin_region":    record["origin_region"],
                "processed_region": record["processed_region"],
                "was_redirected":   record["was_redirected"],
                "energy_kwh":       record["energy_kwh"],
                "carbon_emitted_g": record["carbon_emitted_g"],
                "carbon_saved_g":   record["carbon_saved_g"],
                "latency_ms":       record["latency_ms"],
                "sla_violated":     record["sla_violated"],
                "processed_at":     record["processed_timestamp"],
            })
        )
    except Exception as e:
        log.warning("Redis write failed: %s", e)

# ── Job processor ─────────────────────────────────────────────────────────────

def process_job(job: dict) -> dict:
    # Simulate realistic ML processing time before recording timestamps
    simulate_processing(job)

    processed_timestamp = now_utc_iso()
    processed_region    = REGION
    origin_region       = job["origin_region"]
    was_redirected      = origin_region != processed_region

    # Energy estimation
    try:
        energy_kwh = estimate_energy_kwh(job)
    except Exception as e:
        log.warning("Energy estimation failed: %s — using 0.0", e)
        energy_kwh = 0.0

    # Carbon intensity at time of processing
    carbon_intensity = get_carbon_intensity(processed_region)

    # Carbon accounting
    carbon_emitted_g = compute_carbon_emitted(energy_kwh, carbon_intensity)

    if was_redirected:
        origin_intensity = get_carbon_intensity(origin_region)
        carbon_saved_g   = compute_carbon_saved(
            energy_kwh,
            origin_intensity,
            carbon_intensity,
            origin_region,
            processed_region,
        )
    else:
        carbon_saved_g = 0.0

    # Latency measured after full processing including storage writes
    arrival_dt   = datetime.fromisoformat(job["arrival_timestamp"])
    processed_dt = datetime.fromisoformat(processed_timestamp)
    latency_ms   = (processed_dt - arrival_dt).total_seconds() * 1000
    sla_violated = (
        job.get("priority_tier") == "latency_sla"
        and latency_ms > SLA_THRESHOLD_MS
    )

    if sla_violated:
        log.warning(
            "SLA VIOLATION: job_id=%s latency=%.0fms threshold=%dms",
            job["job_id"], latency_ms, SLA_THRESHOLD_MS,
        )

    return {
        "job_id":              job["job_id"],
        "job_type":            job["job_type"],
        "task":                job.get("task"),
        "model_name":          job["model_name"],
        "priority_tier":       job["priority_tier"],
        "origin_region":       origin_region,
        "processed_region":    processed_region,
        "was_redirected":      was_redirected,
        "admission_mode":      job.get("admission_mode", "unknown"),
        "arrival_timestamp":   job["arrival_timestamp"],
        "processed_timestamp": processed_timestamp,
        "latency_ms":          round(latency_ms, 2),
        "input_token_count":   job.get("input_token_count"),
        "output_token_count":  job.get("expected_output_token_count"),
        "num_images":          job.get("num_images_requested"),
        "energy_kwh":          round(energy_kwh, 8),
        "carbon_intensity":    round(carbon_intensity, 2),
        "carbon_emitted_g":    round(carbon_emitted_g, 6),
        "carbon_saved_g":      round(carbon_saved_g, 6),
        "sla_violated":        sla_violated,
    }

# ── Message handler ───────────────────────────────────────────────────────────

def handle_message(message) -> None:
    try:
        job    = json.loads(message.data.decode("utf-8"))
        job_id = job.get("job_id", "unknown")
        record = process_job(job)

        write_to_bigquery(record)
        write_to_gcs(record)
        write_to_redis(record)

        log.info(
            "processed job_id=%s type=%s origin=%s processed=%s "
            "redirected=%s energy=%.8f kwh carbon=%.4fg saved=%.4fg "
            "latency=%.0fms sla_ok=%s",
            job_id, record["job_type"],
            record["origin_region"], record["processed_region"],
            record["was_redirected"], record["energy_kwh"],
            record["carbon_emitted_g"], record["carbon_saved_g"],
            record["latency_ms"], not record["sla_violated"],
        )
        # Update Prometheus metrics
        was_redirected_str = str(record["was_redirected"]).lower()
        job_type           = record["job_type"]
        region             = record["processed_region"]

        jobs_processed_total.labels(
            region=region,
            job_type=job_type,
            was_redirected=was_redirected_str
        ).inc()

        job_latency_ms.labels(
            region=region,
            job_type=job_type,
            was_redirected=was_redirected_str
        ).observe(record["latency_ms"])

        carbon_emitted_grams_total.labels(region=region).inc(record["carbon_emitted_g"])
        carbon_saved_grams_total.labels(region=region).inc(record["carbon_saved_g"])
        energy_kwh_total.labels(region=region, job_type=job_type).inc(record["energy_kwh"])

        if record["sla_violated"]:
            sla_violations_total.labels(region=region, job_type=job_type).inc()

        message.ack()

    except Exception as e:
        log.error("Failed to process job %s: %s",
                  job.get("job_id", "unknown"), e)
        message.nack()

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info(
        "Worker starting — region=%s subscription=%s sla_threshold=%dms max_workers=%d",
        REGION, SUBSCRIPTION_PATH, SLA_THRESHOLD_MS, MAX_WORKERS,
    )
    subscriber   = pubsub_v1.SubscriberClient()
    flow_control = pubsub_v1.types.FlowControl(max_messages=MAX_WORKERS)
    streaming_pull = subscriber.subscribe(
        SUBSCRIPTION_PATH,
        callback=handle_message,
        flow_control=flow_control,
    )
    # Start Prometheus metrics server on port 9090
    start_http_server(9090)
    log.info("Prometheus metrics server started on port 9090")
    log.info("Worker listening on %s", SUBSCRIPTION_PATH)
    try:
        streaming_pull.result()
    except KeyboardInterrupt:
        streaming_pull.cancel()
        log.info("Worker stopped.")
    except Exception as e:
        streaming_pull.cancel()
        log.error("Streaming pull error: %s", e)
        raise


if __name__ == "__main__":
    main()