"""
Carbon Forecaster — Green Cloud Pipeline v2

Runs in Mumbai cluster only. Polls Electricity Maps API for both
Mumbai and Montreal carbon intensity, maintains 24h rolling history,
computes dynamic thresholds, and exposes a /carbon HTTP endpoint
for Mumbai's admission controller.

In SCALING_MODE=predictive, also scales Montreal worker deployment
proactively when a green window is forecast.

Environment variables:
  ELECTRICITY_MAPS_API_KEY   API key (required)
  PROJECT_ID                 GCP project ID (required)
  SCALING_MODE               reactive or predictive (default: reactive)
  POLL_INTERVAL_SECONDS      how often to poll API (default: 300 = 5 min)
  PORT                       HTTP server port (default: 8080)
  MONTREAL_WORKER_NAMESPACE  k8s namespace for Montreal worker (default: green-cloud)
  MONTREAL_WORKER_DEPLOYMENT k8s deployment name (default: worker-montreal)
  PREDICTIVE_REPLICAS        replicas to scale to on green window (default: 3)
  GREEN_WINDOW_LOOKAHEAD     readings to check for trend (default: 6)
"""

import os
import sys
import json
import time
import logging
import threading
import statistics
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, jsonify

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

API_KEY                    = get_env("ELECTRICITY_MAPS_API_KEY")
PROJECT_ID                 = get_env("PROJECT_ID")
SCALING_MODE               = get_env("SCALING_MODE", "reactive").lower()
POLL_INTERVAL              = int(get_env("POLL_INTERVAL_SECONDS", "300"))
PORT                       = int(get_env("PORT", "8080"))
MONTREAL_NAMESPACE         = get_env("MONTREAL_WORKER_NAMESPACE", "green-cloud")
MONTREAL_DEPLOYMENT        = get_env("MONTREAL_WORKER_DEPLOYMENT", "worker-montreal")
PREDICTIVE_REPLICAS        = int(get_env("PREDICTIVE_REPLICAS", "3"))
GREEN_WINDOW_LOOKAHEAD     = int(get_env("GREEN_WINDOW_LOOKAHEAD", "6"))

# Electricity Maps zone codes
ZONES = {
    "mumbai":   "IN-SO",   # Southern India grid
    "montreal": "CA-QC",   # Quebec — predominantly hydroelectric
}

# 24h rolling history — at 5 min intervals, 24h = 288 readings
HISTORY_SIZE = 288

# ── State — shared between poller thread and HTTP server ──────────────────────

# Deques store (timestamp, intensity_gco2_per_kwh) tuples
_history = {
    "mumbai":   deque(maxlen=HISTORY_SIZE),
    "montreal": deque(maxlen=HISTORY_SIZE),
}

# Latest computed state — updated by poller, read by HTTP server
_state = {
    "mumbai":   None,
    "montreal": None,
    "last_updated": None,
    "error": None,
}

# Fallback cache — last known good values if API fails
_fallback = {
    "mumbai":   None,
    "montreal": None,
}

_state_lock = threading.Lock()

# ── Electricity Maps API ──────────────────────────────────────────────────────

def fetch_carbon_intensity(zone: str) -> float:
    """
    Fetch current carbon intensity for a zone from Electricity Maps.
    Returns gCO2/kWh.
    Raises requests.HTTPError on non-200 responses.
    """
    url = "https://api.electricitymap.org/v3/carbon-intensity/latest"
    headers = {"auth-token": API_KEY}
    params = {"zone": zone}

    resp = requests.get(url, headers=headers, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return float(data["carbonIntensity"])

# ── Threshold computation ─────────────────────────────────────────────────────

def compute_thresholds(history: deque) -> dict:
    """
    Compute dynamic thresholds from 24h rolling history.

    migration_threshold:    24h mean — if current > this, region is dirty
    green_window_threshold: 24h mean - 1 std — genuinely clean period
    """
    if len(history) < 2:
        return None

    values = [v for _, v in history]
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0

    return {
        "migration_threshold":    round(mean, 2),
        "green_window_threshold": round(mean - std, 2),
        "history_size":           len(values),
        "history_mean":           round(mean, 2),
        "history_std":            round(std, 2),
    }

# ── Green window prediction ───────────────────────────────────────────────────

def is_green_window_approaching(history: deque, green_threshold: float) -> bool:
    """
    Simple trend detection: check if carbon intensity is falling
    consistently over the last GREEN_WINDOW_LOOKAHEAD readings
    and approaching the green window threshold.

    Returns True if a green window is predicted to arrive soon.
    This triggers proactive scaling in predictive mode.
    """
    if len(history) < GREEN_WINDOW_LOOKAHEAD:
        return False

    recent = [v for _, v in list(history)[-GREEN_WINDOW_LOOKAHEAD:]]

    # Check trend: each reading lower than the previous
    is_falling = all(recent[i] > recent[i+1] for i in range(len(recent)-1))
    if not is_falling:
        return False

    # Check proximity: current value within 20% of green threshold
    current = recent[-1]
    proximity_threshold = green_threshold * 1.2
    approaching = current <= proximity_threshold

    if is_falling and approaching:
        log.info(
            "Green window approaching: current=%.1f threshold=%.1f trend=falling",
            current, green_threshold
        )
    return is_falling and approaching

# ── Kubernetes scaling ────────────────────────────────────────────────────────

_currently_scaled_up = False

def scale_montreal_worker(replicas: int) -> None:
    """
    Scale the Montreal worker deployment by patching via kubectl.
    Only called in SCALING_MODE=predictive.
    Uses Workload Identity — no key files needed.
    """
    global _currently_scaled_up

    if replicas > 1 and _currently_scaled_up:
        return  # Already scaled up — don't re-patch
    if replicas == 1 and not _currently_scaled_up:
        return  # Already at baseline — don't re-patch

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

# ── Poller thread ─────────────────────────────────────────────────────────────

def compute_region_state(region: str, intensity: float) -> dict:
    """Compute full state dict for a region given current intensity."""
    thresholds = compute_thresholds(_history[region])

    if thresholds is None:
        # Not enough history yet — use raw intensity, no threshold decisions
        return {
            "current_intensity":      round(intensity, 2),
            "migration_threshold":    None,
            "green_window_threshold": None,
            "is_dirty":               None,
            "in_green_window":        None,
            "history_size":           len(_history[region]),
        }

    is_dirty = intensity > thresholds["migration_threshold"]
    in_green_window = intensity <= thresholds["green_window_threshold"]

    return {
        "current_intensity":      round(intensity, 2),
        "migration_threshold":    thresholds["migration_threshold"],
        "green_window_threshold": thresholds["green_window_threshold"],
        "is_dirty":               is_dirty,
        "in_green_window":        in_green_window,
        "history_size":           thresholds["history_size"],
        "history_mean":           thresholds["history_mean"],
        "history_std":            thresholds["history_std"],
    }


def poll_once() -> None:
    """Poll Electricity Maps for both regions and update state."""
    results = {}
    errors = []

    for region, zone in ZONES.items():
        try:
            intensity = fetch_carbon_intensity(zone)
            _history[region].append((now_utc_iso(), intensity))
            _fallback[region] = intensity
            results[region] = intensity
            log.info("Fetched %s: %.1f gCO2/kWh", region, intensity)

        except Exception as e:
            log.warning("Failed to fetch %s: %s — using fallback", region, e)
            errors.append(str(e))

            if _fallback[region] is not None:
                # Use last known good value — don't crash the forecaster
                results[region] = _fallback[region]
                log.warning("Using fallback for %s: %.1f", region, _fallback[region])
            else:
                log.error("No fallback available for %s — skipping", region)
                return

    with _state_lock:
        _state["mumbai"]       = compute_region_state("mumbai", results["mumbai"])
        _state["montreal"]     = compute_region_state("montreal", results["montreal"])
        _state["last_updated"] = now_utc_iso()
        _state["error"]        = errors if errors else None

    # Predictive scaling — only if mode is set and we have enough history
    if SCALING_MODE == "predictive":
        montreal_state = _state["montreal"]
        if montreal_state and montreal_state["green_window_threshold"] is not None:
            approaching = is_green_window_approaching(
                _history["montreal"],
                montreal_state["green_window_threshold"],
            )
            if approaching or montreal_state["in_green_window"]:
                scale_montreal_worker(PREDICTIVE_REPLICAS)
            else:
                scale_montreal_worker(1)


def poller_loop() -> None:
    """Background thread: poll on startup then every POLL_INTERVAL seconds."""
    log.info("Poller starting — interval=%ds zones=%s", POLL_INTERVAL, ZONES)
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
    Return current carbon intensity state for both regions.
    Called by Mumbai admission controller before each routing decision.
    """
    with _state_lock:
        state = dict(_state)

    if state["mumbai"] is None or state["montreal"] is None:
        return jsonify({
            "error": "Carbon data not yet available — poller still initializing",
            "last_updated": state.get("last_updated"),
        }), 503

    return jsonify(state)


@app.route("/health", methods=["GET"])
def health():
    """Health check for Kubernetes liveness probe."""
    with _state_lock:
        last = _state.get("last_updated")
    return jsonify({"status": "ok", "last_updated": last})


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    log.info(
        "Carbon forecaster starting — scaling_mode=%s port=%d",
        SCALING_MODE, PORT
    )

    # Start poller in background thread
    t = threading.Thread(target=poller_loop, daemon=True)
    t.start()

    # Start HTTP server (blocks)
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
