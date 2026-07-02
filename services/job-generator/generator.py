"""
Job Generator — Green Cloud Pipeline v2

Publishes a steady stream of synthetic ML/GenAI job requests
to the origin region's Pub/Sub topic.

Environment variables:
  PROJECT_ID        GCP project ID (required)
  REGION            mumbai or montreal (required)
  ADMISSION_MODE    baseline or green (default: baseline)
  SCALING_MODE      reactive or predictive (default: reactive)
  JOBS_PER_MINUTE   steady publish rate (default: 30)
  INTERACTIVE_RATIO fraction of interactive jobs (default: 0.7)
  INPUT_TOKEN_MIN   minimum input token count (default: 64)
  INPUT_TOKEN_MAX   maximum input token count (default: 2048)
"""

import os
import sys
import json
import time
import random
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "shared"))

from lineage import generate_job_id, now_utc_iso
from model_catalog import (
    sample_llm_model,
    sample_diffusion_model,
    sample_output_token_count,
    get_priority_tier,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── Job builders (no env vars needed — safe to import and test) ───────────────

def build_interactive_job(region: str, admission_mode: str,
                          scaling_mode: str,
                          input_token_min: int, input_token_max: int) -> dict:
    model = sample_llm_model()
    return {
        "job_id":                      generate_job_id(),
        "job_type":                    "interactive_inference",
        "task":                        model["task"],
        "model_name":                  model["model_name"],
        "architecture":                model["architecture"],
        "total_params_billions":       model["total_params_billions"],
        "activated_params_billions":   model["activated_params_billions"],
        "model_id":                    None,
        "gpu_model":                   model["gpu_model"],
        "priority_tier":               get_priority_tier("interactive_inference"),
        "origin_region":               region,
        "arrival_timestamp":           now_utc_iso(),
        "input_token_count":           random.randint(input_token_min, input_token_max),
        "expected_output_token_count": sample_output_token_count(model),
        "num_images_requested":        None,
        "current_concurrency":         model["avg_batch_size"],
        "admission_mode":              admission_mode,
        "scaling_mode":                scaling_mode,
    }


def build_batch_image_job(region: str, admission_mode: str,
                          scaling_mode: str) -> dict:
    model = sample_diffusion_model()
    return {
        "job_id":                      generate_job_id(),
        "job_type":                    "batch_image_gen",
        "task":                        model["task"],
        "model_name":                  model["model_name"],
        "architecture":                None,
        "total_params_billions":       None,
        "activated_params_billions":   None,
        "model_id":                    model["model_id"],
        "gpu_model":                   model["gpu_model"],
        "priority_tier":               get_priority_tier("batch_image_gen"),
        "origin_region":               region,
        "arrival_timestamp":           now_utc_iso(),
        "input_token_count":           None,
        "expected_output_token_count": None,
        "num_images_requested":        model["typical_batch_size"],
        "current_concurrency":         model["typical_batch_size"],
        "admission_mode":              admission_mode,
        "scaling_mode":                scaling_mode,
    }


def build_job(region: str, admission_mode: str, scaling_mode: str,
              interactive_ratio: float,
              input_token_min: int, input_token_max: int) -> dict:
    if random.random() < interactive_ratio:
        return build_interactive_job(region, admission_mode, scaling_mode,
                                     input_token_min, input_token_max)
    return build_batch_image_job(region, admission_mode, scaling_mode)


# ── Publisher ─────────────────────────────────────────────────────────────────

def publish_job(publisher, topic_name: str, job: dict) -> None:
    data = json.dumps(job).encode("utf-8")
    future = publisher.publish(
        topic_name,
        data,
        job_id=job["job_id"],
        job_type=job["job_type"],
        priority_tier=job["priority_tier"],
        origin_region=job["origin_region"],
    )
    message_id = future.result(timeout=10)
    log.info(
        "published job_id=%s type=%s task=%s priority=%s message_id=%s",
        job["job_id"], job["job_type"],
        job.get("task", "n/a"), job["priority_tier"], message_id,
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Load and validate config here — not at module level
    def get_env(key, default=None):
        val = os.environ.get(key, default)
        if val is None:
            raise EnvironmentError(f"Required env var '{key}' is not set.")
        return val

    project_id        = get_env("PROJECT_ID")
    region            = get_env("REGION").lower()
    admission_mode    = get_env("ADMISSION_MODE", "baseline").lower()
    scaling_mode      = get_env("SCALING_MODE", "reactive").lower()
    jobs_per_minute   = float(get_env("JOBS_PER_MINUTE", "30"))
    interactive_ratio = float(get_env("INTERACTIVE_RATIO", "0.7"))
    input_token_min   = int(get_env("INPUT_TOKEN_MIN", "64"))
    input_token_max   = int(get_env("INPUT_TOKEN_MAX", "2048"))

    if region not in ("mumbai", "montreal"):
        raise ValueError(f"REGION must be 'mumbai' or 'montreal', got '{region}'")
    if admission_mode not in ("baseline", "green"):
        raise ValueError(f"ADMISSION_MODE must be 'baseline' or 'green'")
    if scaling_mode not in ("reactive", "predictive"):
        raise ValueError(f"SCALING_MODE must be 'reactive' or 'predictive'")

    sleep_seconds = 60.0 / jobs_per_minute
    topic_name = f"projects/{project_id}/topics/{region}-jobs"

    from google.cloud import pubsub_v1
    publisher = pubsub_v1.PublisherClient()

    log.info(
        "Job generator starting — region=%s mode=%s/%s rate=%.1f jobs/min (%.2fs interval)",
        region, admission_mode, scaling_mode, jobs_per_minute, sleep_seconds,
    )
    log.info("Publishing to topic: %s", topic_name)

    jobs_published = 0
    errors = 0

    while True:
        try:
            job = build_job(region, admission_mode, scaling_mode,
                            interactive_ratio, input_token_min, input_token_max)
            publish_job(publisher, topic_name, job)
            jobs_published += 1

            if jobs_published % 10 == 0:
                log.info("--- %d jobs published, %d errors ---",
                         jobs_published, errors)

        except Exception as e:
            errors += 1
            log.error("Failed to publish job (error %d): %s", errors, e)
            if errors > 10:
                log.error("Too many errors — sleeping 30s")
                time.sleep(30)
                errors = 0

        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
