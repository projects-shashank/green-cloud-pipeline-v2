"""
Carbon Forecaster — Green Cloud Pipeline v2

On startup:
  1. Fetches 24h historical carbon intensity from Electricity Maps
  2. Computes thresholds immediately — no warm-up period
  3. Fetches live current intensity
  4. Starts polling every POLL_INTERVAL seconds to keep data fresh

Green window definition (Montreal):
    green_threshold = min(24h) + std(24h)
    if current <= green_threshold → in green window

Migration threshold (Mumbai dirty check):
    migration_threshold = mean(24h)
    if current > migration_threshold → Mumbai is dirty

Routing priority (enforced in admission controller):
    1. Montreal in green window → migrate (maximise carbon saving)
    2. Mumbai dirty            → migrate
    3. Otherwise              → process locally

Predictive scaling:
    If SCALING_MODE=predictive and green window approaching
    (current within 15% of threshold AND slope non-positive),
    proactively scale Montreal worker to PREDICTIVE_REPLICAS.

Environment variables:
  ELECTRICITY_MAPS_API_KEY    API key (required)
  PROJECT_ID                  GCP project ID (required)
  SCALING_MODE                reactive or predictive (default: reactive)
  POLL_INTERVAL_SECONDS       live poll interval (default: 300)
  PORT                        HTTP server port (default: 8080)
  MONTREAL_WORKER_NAMESPACE   k8s namespace (default: green-cloud)
  MONTREAL_WORKER_DEPLOYMENT  k8s deployment name (default: worker-montreal)
  PREDICTIVE_REPLICAS         replicas on green window (default: 3)
"""

import os
import sys
import time
import logging
import threading
import statistics
from collections import deque
from pathlib import Path

import numpy as np
import requests
from flask import Flask, jsonify, request

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

ZONES = {
    "mumbai":   "IN-SO",
    "montreal": "CA-QC",
}

# Rolling history — 24h at 5min intervals = 288 readings max
HISTORY_SIZE = 288

# ── State ─────────────────────────────────────────────────────────────────────

_history = {
    "mumbai":   deque(maxlen=HISTORY_SIZE),
    "montreal": deque(maxlen=HISTORY_SIZE),
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

_state_lock          = threading.Lock()
_currently_scaled_up = False

# ── Electricity Maps API ──────────────────────────────────────────────────────

def fetch_live(zone: str) -> float:
    """Fetch current carbon intensity for a zone."""
    resp = requests.get(
        "https://api.electricitymap.org/v3/carbon-intensity/latest",
        headers={"auth-token": API_KEY},
        params={"zone": zone},
        timeout=10,
    )
    resp.raise_for_status()
    return float(resp.json()["carbonIntensity"])


def fetch_history(zone: str) -> list:
    """
    Fetch 24h historical carbon intensity for a zone.
    Returns list of (datetime_str, intensity) tuples sorted oldest first.
    """
    resp = requests.get(
        "https://api.electricitymap.org/v3/carbon-intensity/history",
        headers={"auth-token": API_KEY},
        params={"zone": zone},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()["history"]
    data.sort(key=lambda x: x["datetime"])
    return [(entry["datetime"], float(entry["carbonIntensity"])) for entry in data]

# ── Threshold computation ─────────────────────────────────────────────────────

def compute_montreal_thresholds(history: deque) -> dict | None:
    """
    Green window threshold:
        green_threshold = min(24h) + std(24h)
    """
    if len(history) < 2:
        return None
    values      = [v for _, v in history]
    h_min       = min(values)
    h_std       = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "green_threshold": round(h_min + h_std, 2),
        "history_min":     round(h_min, 2),
        "history_std":     round(h_std, 2),
        "history_size":    len(values),
    }


def compute_mumbai_thresholds(history: deque) -> dict | None:
    """
    Migration threshold:
        migration_threshold = mean(24h)
    """
    if len(history) < 2:
        return None
    values = [v for _, v in history]
    mean   = statistics.mean(values)
    std    = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "migration_threshold": round(mean, 2),
        "history_mean":        round(mean, 2),
        "history_std":         round(std, 2),
        "history_size":        len(values),
    }

# ── Green window approach detection ──────────────────────────────────────────

def is_green_window_approaching(history: deque, green_threshold: float) -> bool:
    """
    Proximity + slope check:
      1. Current within 15% above green_threshold
      2. Short-term slope (last 6 readings) is flat or negative
    """
    if len(history) < 6:
        return False
    recent  = [v for _, v in list(history)[-6:]]
    current = recent[-1]

    near_threshold = current <= green_threshold * 1.15
    slope          = np.polyfit(np.arange(len(recent)), recent, 1)[0]
    not_rising     = slope <= 0

    if near_threshold and not_rising:
        log.info(
            "Green window approaching: current=%.1f threshold=%.1f slope=%.3f",
            current, green_threshold, slope,
        )
    return near_threshold and not_rising

# ── Kubernetes scaling ────────────────────────────────────────────────────────

def scale_montreal_worker(replicas: int) -> None:
    global _currently_scaled_up
    if replicas > 1 and _currently_scaled_up:
        return
    if replicas == 1 and not _currently_scaled_up:
        return
    try:
        import subprocess
        result = subprocess.run([
            "kubectl", "scale", "deployment", MONTREAL_DEPLOYMENT,
            f"--replicas={replicas}",
            f"--namespace={MONTREAL_NAMESPACE}",
            "--context=gke_green-cloud-pipeline-v2_northamerica-northeast1-a_gcp-montreal",
        ], capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            log.info("Scaled Montreal worker to %d replicas", replicas)
            _currently_scaled_up = replicas > 1
        else:
            log.error("Failed to scale: %s", result.stderr)
    except Exception as e:
        log.error("Exception scaling: %s", e)

# ── State builders ────────────────────────────────────────────────────────────

def compute_mumbai_state(intensity: float) -> dict:
    t = compute_mumbai_thresholds(_history["mumbai"])
    if t is None:
        return {
            "current_intensity":   round(intensity, 2),
            "migration_threshold": None,
            "is_dirty":            None,
            "warming_up":          True,
        }
    return {
        "current_intensity":   round(intensity, 2),
        "migration_threshold": t["migration_threshold"],
        "is_dirty":            intensity > t["migration_threshold"],
        "history_mean":        t["history_mean"],
        "history_std":         t["history_std"],
        "history_size":        t["history_size"],
        "warming_up":          False,
    }


def compute_montreal_state(intensity: float) -> dict:
    t = compute_montreal_thresholds(_history["montreal"])
    if t is None:
        return {
            "current_intensity": round(intensity, 2),
            "green_threshold":   None,
            "in_green_window":   None,
            "approaching":       None,
            "warming_up":        True,
        }
    green_threshold = t["green_threshold"]
    in_green_window = intensity <= green_threshold
    approaching     = (
        False if in_green_window
        else is_green_window_approaching(_history["montreal"], green_threshold)
    )
    return {
        "current_intensity": round(intensity, 2),
        "green_threshold":   green_threshold,
        "in_green_window":   in_green_window,
        "approaching":       approaching,
        "history_min":       t["history_min"],
        "history_std":       t["history_std"],
        "history_size":      t["history_size"],
        "warming_up":        False,
    }

# ── Startup: load 24h history immediately ────────────────────────────────────

def load_history_on_startup() -> None:
    """
    Called once at startup before the poller loop begins.
    Fetches 24h historical data for both regions so thresholds
    are available immediately — no warm-up period needed.
    """
    log.info("Loading 24h history on startup...")
    for region, zone in ZONES.items():
        try:
            readings = fetch_history(zone)
            for ts, intensity in readings:
                _history[region].append((ts, intensity))
            _fallback[region] = readings[-1][1] if readings else None
            log.info(
                "Loaded %d historical readings for %s "
                "(range %.0f-%.0f gCO2/kWh)",
                len(readings), region,
                min(r[1] for r in readings),
                max(r[1] for r in readings),
            )
        except Exception as e:
            log.error("Failed to load history for %s: %s", region, e)

# ── Poller ────────────────────────────────────────────────────────────────────

def poll_once() -> None:
    """Fetch live intensity for both regions and update state."""
    results = {}
    errors  = []

    for region, zone in ZONES.items():
        try:
            intensity           = fetch_live(zone)
            _history[region].append((now_utc_iso(), intensity))
            _fallback[region]   = intensity
            results[region]     = intensity
            log.info("Live %s: %.1f gCO2/kWh", region, intensity)
        except Exception as e:
            log.warning("Failed to fetch live %s: %s — using fallback", region, e)
            errors.append(str(e))
            if _fallback[region] is not None:
                results[region] = _fallback[region]
            else:
                log.error("No fallback for %s — skipping", region)
                return

    with _state_lock:
        _state["mumbai"]       = compute_mumbai_state(results["mumbai"])
        _state["montreal"]     = compute_montreal_state(results["montreal"])
        _state["last_updated"] = now_utc_iso()
        _state["error"]        = errors if errors else None

    if SCALING_MODE == "predictive":
        montreal = _state["montreal"]
        if montreal and not montreal.get("warming_up"):
            should_scale = montreal["in_green_window"] or montreal["approaching"]
            scale_montreal_worker(PREDICTIVE_REPLICAS if should_scale else 1)


def poller_loop() -> None:
    log.info("Poller starting — interval=%ds", POLL_INTERVAL)
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
    Routing logic (enforced in admission controller):
        if montreal.in_green_window → migrate (maximise carbon saving)
        elif mumbai.is_dirty        → migrate
        else                        → process locally
    """
    with _state_lock:
        state = dict(_state)
    if state["mumbai"] is None or state["montreal"] is None:
        return jsonify({
            "error": "Initializing — poll not yet complete",
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

    # Load 24h history immediately on startup
    load_history_on_startup()

    # Run one live poll immediately so state is populated before HTTP server starts
    poll_once()

    # Start background poller
    t = threading.Thread(target=poller_loop, daemon=True)
    t.start()

    # Start HTTP server
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
