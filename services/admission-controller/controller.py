"""
Admission Controller — Green Cloud Pipeline v2

Runs in Mumbai cluster only.
Pulls jobs from mumbai-jobs-sub, applies routing logic,
then either:
  - Publishes to mumbai-process (local worker handles it)
  - Publishes to montreal-jobs (Montreal worker handles it)

Routing logic:
  Interactive jobs → always local (mumbai-process)
  Batch jobs:
    baseline mode → always local (mumbai-process)
    green mode:
      Montreal in green window? → montreal-jobs (maximise carbon saving)
      Mumbai dirty?             → montreal-jobs
      Otherwise                 → mumbai-process

Environment variables:
  PROJECT_ID           GCP project ID (required)
  ADMISSION_MODE       baseline or green (required)
  SCALING_MODE         reactive or predictive (default: reactive)
  CARBON_FORECASTER_URL URL of forecaster /carbon endpoint
                        (default: http://carbon-forecaster:8080/carbon)
  CARBON_CACHE_TTL     seconds to cache /carbon response (default: 60)
  MAX_WORKERS          concurrent Pub/Sub message processors (default: 4)
"""

import os
import sys
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from google.cloud import pubsub_v1

sys.path.insert(0, str(Path(__file__).parent.parent / "shared"))
from lineage import now_utc_iso

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

def get_env(key, default=None):
    val = os.environ.get(key, default)
    if val is None:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val

PROJECT_ID            = get_env("PROJECT_ID")
ADMISSION_MODE        = get_env("ADMISSION_MODE", "baseline").lower()
SCALING_MODE          = get_env("SCALING_MODE", "reactive").lower()
CARBON_FORECASTER_URL = get_env("CARBON_FORECASTER_URL",
                                 "http://carbon-forecaster:8080/carbon")
CARBON_CACHE_TTL      = int(get_env("CARBON_CACHE_TTL", "60"))
MAX_WORKERS           = int(get_env("MAX_WORKERS", "4"))

if ADMISSION_MODE not in ("baseline", "green"):
    raise ValueError(f"ADMISSION_MODE must be 'baseline' or 'green'")

# Pub/Sub topic paths
INBOUND_SUB      = f"projects/{PROJECT_ID}/subscriptions/mumbai-jobs-sub"
LOCAL_TOPIC      = f"projects/{PROJECT_ID}/topics/mumbai-process"
MONTREAL_TOPIC   = f"projects/{PROJECT_ID}/topics/montreal-jobs"

# ── Carbon cache ──────────────────────────────────────────────────────────────
# The forecaster is polled every 5 minutes — no need to call it
# on every single job. Cache the response for CARBON_CACHE_TTL seconds.

_carbon_cache = {
    "data":       None,
    "fetched_at": 0.0,
}
_carbon_lock = threading.Lock()


def get_carbon_state() -> dict | None:
    """
    Return cached carbon state, refreshing if stale.
    Returns None if forecaster is unreachable — caller handles fallback.
    """
    with _carbon_lock:
        age = time.time() - _carbon_cache["fetched_at"]
        if _carbon_cache["data"] is not None and age < CARBON_CACHE_TTL:
            return _carbon_cache["data"]

    try:
        resp = requests.get(CARBON_FORECASTER_URL, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        with _carbon_lock:
            _carbon_cache["data"]       = data
            _carbon_cache["fetched_at"] = time.time()
        return data
    except Exception as e:
        log.warning("Failed to fetch carbon state: %s — using cached value", e)
        with _carbon_lock:
            return _carbon_cache["data"]  # may be None if never fetched

# ── Routing decision ──────────────────────────────────────────────────────────

def decide_routing(job: dict) -> tuple[str, str]:
    """
    Decide where to route a job.

    Returns:
        (destination, reason) where destination is 'local' or 'montreal'
    """
    job_type     = job["job_type"]
    priority     = job["priority_tier"]

    # Interactive jobs — always local, no carbon check, ever
    if priority == "latency_sla" or job_type == "interactive_inference":
        return "local", "interactive_job_always_local"

    # Batch jobs in baseline mode — always local
    if ADMISSION_MODE == "baseline":
        return "local", "baseline_mode_no_carbon_check"

    # Batch jobs in green mode — check carbon
    carbon = get_carbon_state()

    if carbon is None:
        # Forecaster unreachable — fail safe: process locally
        log.warning("Carbon state unavailable — routing locally as fallback")
        return "local", "carbon_unavailable_fallback"

    if carbon.get("error") and carbon["montreal"] is None:
        return "local", "carbon_data_incomplete_fallback"

    montreal = carbon.get("montreal", {})
    mumbai   = carbon.get("mumbai", {})

    # Priority 1: Montreal in green window → migrate immediately
    # Don't even check Mumbai — maximise carbon saving
    if montreal.get("in_green_window") is True:
        return "montreal", "montreal_in_green_window"

    # Priority 2: Mumbai dirty → migrate
    if mumbai.get("is_dirty") is True:
        return "montreal", "mumbai_dirty"

    # Priority 3: Neither condition → process locally
    return "local", "neither_condition_met"

# ── Message handler ───────────────────────────────────────────────────────────

publisher = pubsub_v1.PublisherClient()


def enrich_job(job: dict, destination: str, reason: str) -> dict:
    """Add routing metadata to job before forwarding."""
    job["routed_at"]       = now_utc_iso()
    job["routed_to"]       = destination
    job["routing_reason"]  = reason
    job["admission_mode"]  = ADMISSION_MODE
    job["scaling_mode"]    = SCALING_MODE
    return job


def forward_job(job: dict, destination: str) -> None:
    """Publish job to the appropriate downstream topic."""
    topic = LOCAL_TOPIC if destination == "local" else MONTREAL_TOPIC
    data  = json.dumps(job).encode("utf-8")

    future = publisher.publish(
        topic,
        data,
        job_id=job["job_id"],
        job_type=job["job_type"],
        destination=destination,
        routing_reason=job["routing_reason"],
    )
    future.result(timeout=10)


def handle_message(message) -> None:
    """
    Process one Pub/Sub message:
    1. Parse job
    2. Decide routing
    3. Enrich job with routing metadata
    4. Forward to correct topic
    5. Acknowledge message
    """
    try:
        job = json.loads(message.data.decode("utf-8"))
        job_id   = job.get("job_id", "unknown")
        job_type = job.get("job_type", "unknown")

        destination, reason = decide_routing(job)
        job = enrich_job(job, destination, reason)
        forward_job(job, destination)

        log.info(
            "job_id=%s type=%s priority=%s → %s (%s)",
            job_id, job_type, job.get("priority_tier"), destination, reason,
        )
        message.ack()

    except Exception as e:
        log.error("Failed to handle message: %s", e)
        # nack — message will be redelivered up to DLQ max_delivery_attempts
        message.nack()

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info(
        "Admission controller starting — mode=%s/%s subscription=%s",
        ADMISSION_MODE, SCALING_MODE, INBOUND_SUB,
    )
    log.info("Local topic:    %s", LOCAL_TOPIC)
    log.info("Montreal topic: %s", MONTREAL_TOPIC)
    log.info("Forecaster URL: %s", CARBON_FORECASTER_URL)

    subscriber = pubsub_v1.SubscriberClient()

    flow_control = pubsub_v1.types.FlowControl(max_messages=MAX_WORKERS)

    streaming_pull = subscriber.subscribe(
        INBOUND_SUB,
        callback=handle_message,
        flow_control=flow_control,
    )

    log.info("Listening for messages...")

    try:
        streaming_pull.result()
    except KeyboardInterrupt:
        streaming_pull.cancel()
        log.info("Admission controller stopped.")
    except Exception as e:
        streaming_pull.cancel()
        log.error("Streaming pull error: %s", e)
        raise


if __name__ == "__main__":
    main()
