"""
Carbon Forecaster — Green Cloud Pipeline v2

Runs in Mumbai cluster only. Polls Electricity Maps API for both
Mumbai and Montreal carbon intensity, maintains rolling history,
computes dynamic thresholds, and exposes a /carbon HTTP endpoint
for Mumbai's admission controller.

Green window definition:
    green_threshold = min(last 36h Montreal) + std(last 36h Montreal)
    if current_intensity <= green_threshold → in green window

Migration threshold (Mumbai dirty check):
    migration_threshold = mean(last 24h Mumbai)
    if current_intensity > migration_threshold → Mumbai is dirty

Routing priority (enforced in admission controller):
    1. Green window? → migrate immediately (maximise carbon saving)
    2. Mumbai dirty? → migrate
    3. Otherwise   → process locally

Predictive scaling:
    If SCALING_MODE=predictive and green window is approaching
    (current within 15% of threshold AND slope non-positive),
    proactively scale Montreal worker to PREDICTIVE_REPLICAS.

Environment variables:
  ELECTRICITY_MAPS_API_KEY   API key (required)
  PROJECT_ID                 GCP project ID (required)
  SCALING_MODE               reactive or predictive (default: reactive)
  POLL_INTERVAL_SECONDS      how often to poll API (default: 300 = 5 min)
  PORT                       HTTP server port (default: 8080)
  MONTREAL_WORKER_NAMESPACE  k8s namespace (default: green-cloud)
  MONTREAL_WORKER_DEPLOYMENT k8s deployment name (default: worker-montreal)
  PREDICTIVE_REPLICAS        replicas on green window (default: 3)
"""

import os
import sys
import json
import time
import logging
import threading
import statistics
from collections import deque
from pathlib import Path

import numpy as np
import requests
from flask import Flask, jsonify

sys.path.insert(0, "/app/shared")
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

API_KEY             = get_env("ELECTRICITY_MAPS_API_KEY")
PROJECT_ID          = get_env("PROJECT_ID")
SCALING_MODE        = get_env("SCALING_MODE", "reactive").lower()
POLL_INTERVAL       = int(get_env("POLL_INTERVAL_SECONDS", "300"))
PORT                = int(get_env("PORT", "8080"))
MONTREAL_NAMESPACE  = get_env("MONTREAL_WORKER_NAMESPACE", "green-cloud")
MONTREAL_DEPLOYMENT = get_env("MONTREAL_WORKER_DEPLOYMENT", "worker-montreal")
PREDICTIVE_REPLICAS = int(get_env("PREDICTIVE_REPLICAS", "3"))

# Electricity Maps zone codes
ZONES = {
    "mumbai":   "IN-SO",  # Southern India grid
    "montreal": "CA-QC",  # Quebec — predominantly hydroelectric
}

# History sizes
# 36h at 5-min intervals = 432 readings (for green window threshold)
# 24h at 5-min intervals = 288 readings (for migration threshold)
HISTORY_36H = 432
HISTORY_24H = 288

# ── State ─────────────────────────────────────────────────────────────────────

# Store raw (timestamp, intensity) tuples
# Montreal needs 36h for green window; Mumbai needs 24h for migration threshold
_history = {
    "mumbai":   deque(maxlen=HISTORY_36H),  # use last 288 for threshold
    "montreal": deque(maxlen=HISTORY_36H),  # use all 432 for green window
}

_state = {
    "mumbai":       None,
    "montreal":     None,
    "last_updated": None,
    "error":        None,
}

_fallback = {
    "mumbai":   None,
    "montreal": None,
}

_state_lock = threading.Lock()
_currently_scaled_up = False

# ── Electricity Maps API ──────────────────────────────────────────────────────

def fetch_carbon_intensity(zone: str) -> float:
    url = "https://api.electricitymap.org/v3/carbon-intensity/latest"
    resp = requests.get(
        url,
        headers={"auth-token": API_KEY},
        params={"zone": zone},
        timeout=10,
    )
    resp.raise_for_status()
    return float(resp.json()["carbonIntensity"])

# ── Threshold computation ─────────────────────────────────────────────────────

def compute_montreal_thresholds(history: deque) -> dict | None:
    """
    Green window threshold for Montreal.

    Uses last 36h of readings:
        green_threshold = min(36h) + std(36h)

    If current_intensity <= green_threshold → in green window.

    Why min + std:
        min captures the cleanest the grid gets.
        Adding one std gives a realistic buffer above that minimum
        so we don't only trigger on the absolute lowest reading.
    """
    if len(history) < 2:  # need at least 1h of data
        return None

    values = [v for _, v in history]
    h_min  = min(values)
    h_std  = statistics.stdev(values) if len(values) > 1 else 0.0
    green_threshold = h_min + h_std

    return {
        "green_threshold":  round(green_threshold, 2),
        "history_min":      round(h_min, 2),
        "history_std":      round(h_std, 2),
        "history_size":     len(values),
    }


def compute_mumbai_thresholds(history: deque) -> dict | None:
    """
    Migration threshold for Mumbai.

    Uses last 24h of readings:
        migration_threshold = mean(24h)

    If current_intensity > migration_threshold → Mumbai is dirty.
    """
    # Only use the last 288 readings (24h) even if deque is larger
    values = [v for _, v in list(history)[-HISTORY_24H:]]
    if len(values) < 12:
        return None

    mean = statistics.mean(values)
    std  = statistics.stdev(values) if len(values) > 1 else 0.0

    return {
        "migration_threshold": round(mean, 2),
        "history_mean":        round(mean, 2),
        "history_std":         round(std, 2),
        "history_size":        len(values),
    }

# ── Green window approach detection ──────────────────────────────────────────

def is_green_window_approaching(history: deque, green_threshold: float) -> bool:
    """
    Predict whether Montreal is about to enter a green window.

    Two conditions must both be true:
        1. Proximity: current intensity is within 15% above the green threshold
           "We're close — could tip below threshold soon"
        2. Trend: short-term slope (last 6 readings) is flat or negative
           "We're not moving away from it"

    Why not LSTM:
        Montreal's grid is predominantly hydro — carbon intensity is already
        low and stable (typically 40-80 gCO2/kWh). The signal doesn't have
        enough variance to justify a complex model. A proximity+slope check
        gives the same practical result with zero training data.

    Why numpy polyfit for slope:
        More robust than "every reading lower than previous" — one noisy
        reading doesn't break the detection.
    """
    if len(history) < 6:
        return False

    recent = [v for _, v in list(history)[-6:]]
    current = recent[-1]

    # Condition 1: proximity
    proximity_ceiling = green_threshold * 1.15
    near_threshold = current <= proximity_ceiling

    # Condition 2: slope via linear regression on last 6 readings
    x = np.arange(len(recent))
    slope = np.polyfit(x, recent, 1)[0]
    not_rising = slope <= 0

    if near_threshold and not_rising:
        log.info(
            "Green window approaching: current=%.1f threshold=%.1f slope=%.3f",
            current, green_threshold, slope,
        )

    return near_threshold and not_rising

# ── Kubernetes scaling ────────────────────────────────────────────────────────

def scale_montreal_worker(replicas: int) -> None:
    """
    Scale Montreal worker deployment via kubectl.
    Only called in SCALING_MODE=predictive.
    Uses Workload Identity — no key files needed.
    """
    global _currently_scaled_up

    if replicas > 1 and _currently_scaled_up:
        return
    if replicas == 1 and not _currently_scaled_up:
        return

    try:
        import subprocess
        cmd = [
            "kubectl", "scale", "deployment", MONTREAL_DEPLOYMENT,
            f"--replicas={replicas}",
            f"--namespace={MONTREAL_NAMESPACE}",
            "--context=gke_green-cloud-pipeline-v2_northamerica-northeast1-a_gcp-montreal",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            log.info("Scaled Montreal worker to %d replicas", replicas)
            _currently_scaled_up = replicas > 1
        else:
            log.error("Failed to scale Montreal worker: %s", result.stderr)
    except Exception as e:
        log.error("Exception scaling Montreal worker: %s", e)

# ── Region state builder ──────────────────────────────────────────────────────

def compute_mumbai_state(intensity: float) -> dict:
    thresholds = compute_mumbai_thresholds(_history["mumbai"])
    if thresholds is None:
        return {
            "current_intensity":    round(intensity, 2),
            "migration_threshold":  None,
            "is_dirty":             None,
            "history_size":         len(_history["mumbai"]),
            "warming_up":           True,
        }
    is_dirty = intensity > thresholds["migration_threshold"]
    return {
        "current_intensity":    round(intensity, 2),
        "migration_threshold":  thresholds["migration_threshold"],
        "is_dirty":             is_dirty,
        "history_mean":         thresholds["history_mean"],
        "history_std":          thresholds["history_std"],
        "history_size":         thresholds["history_size"],
        "warming_up":           False,
    }


def compute_montreal_state(intensity: float) -> dict:
    thresholds = compute_montreal_thresholds(_history["montreal"])
    if thresholds is None:
        return {
            "current_intensity":  round(intensity, 2),
            "green_threshold":    None,
            "in_green_window":    None,
            "approaching":        None,
            "history_size":       len(_history["montreal"]),
            "warming_up":         True,
        }
    green_threshold = thresholds["green_threshold"]
    in_green_window = intensity <= green_threshold
    approaching     = (
        False if in_green_window
        else is_green_window_approaching(_history["montreal"], green_threshold)
    )
    return {
        "current_intensity":  round(intensity, 2),
        "green_threshold":    green_threshold,
        "in_green_window":    in_green_window,
        "approaching":        approaching,
        "history_min":        thresholds["history_min"],
        "history_std":        thresholds["history_std"],
        "history_size":       thresholds["history_size"],
        "warming_up":         False,
    }

# ── Poller thread ─────────────────────────────────────────────────────────────

def poll_once() -> None:
    results = {}
    errors  = []

    for region, zone in ZONES.items():
        try:
            intensity = fetch_carbon_intensity(zone)
            _history[region].append((now_utc_iso(), intensity))
            _fallback[region] = intensity
            results[region]   = intensity
            log.info("Fetched %s: %.1f gCO2/kWh", region, intensity)
        except Exception as e:
            log.warning("Failed to fetch %s: %s — using fallback", region, e)
            errors.append(str(e))
            if _fallback[region] is not None:
                results[region] = _fallback[region]
            else:
                log.error("No fallback for %s — skipping poll", region)
                return

    with _state_lock:
        _state["mumbai"]       = compute_mumbai_state(results["mumbai"])
        _state["montreal"]     = compute_montreal_state(results["montreal"])
        _state["last_updated"] = now_utc_iso()
        _state["error"]        = errors if errors else None

    # Predictive scaling
    if SCALING_MODE == "predictive":
        montreal = _state["montreal"]
        if montreal and not montreal.get("warming_up"):
            should_scale = (
                montreal["in_green_window"] or
                montreal["approaching"]
            )
            scale_montreal_worker(PREDICTIVE_REPLICAS if should_scale else 1)


def poller_loop() -> None:
    log.info(
        "Poller starting — interval=%ds scaling_mode=%s",
        POLL_INTERVAL, SCALING_MODE,
    )
    while True:
        try:
            poll_once()
        except Exception as e:
            log.error("Unexpected error in poll_once: %s", e)
        time.sleep(POLL_INTERVAL)

# ── HTTP server ───────────────────────────────────────────────────────────────

app = Flask(__name__)

@app.route("/carbon", methods=["GET"])
def carbon():
    """
    Return carbon state for both regions.
    Called by Mumbai admission controller before each routing decision.

    Routing logic (enforced in admission controller, documented here):
        if montreal.in_green_window → migrate (maximise carbon saving)
        elif mumbai.is_dirty        → migrate
        else                        → process locally
    """
    with _state_lock:
        state = dict(_state)

    if state["mumbai"] is None or state["montreal"] is None:
        return jsonify({
            "error": "Carbon data not yet available — still initializing",
            "last_updated": state.get("last_updated"),
        }), 503

    return jsonify(state)


@app.route("/health", methods=["GET"])
def health():
    with _state_lock:
        last = _state.get("last_updated")
    return jsonify({"status": "ok", "last_updated": last})


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    log.info(
        "Carbon forecaster starting — scaling_mode=%s port=%d poll_interval=%ds",
        SCALING_MODE, PORT, POLL_INTERVAL,
    )
    t = threading.Thread(target=poller_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
