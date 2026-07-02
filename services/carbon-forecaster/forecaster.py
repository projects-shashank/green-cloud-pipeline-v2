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
  PREDICTIVE_REPLICAS         replicas on green window (default: 2)
"""

import os
import sys
import time
import logging
import threading
import statistics
from collections import deque
from datetime import datetime, timezone
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
PREDICTIVE_REPLICAS = int(get_env("PREDICTIVE_REPLICAS", "2"))

ZONES = {
    "mumbai":   "IN-SO",
    "montreal": "CA-QC",
}

HISTORY_SIZE = 288  # 24h at 5min intervals

# ── State ─────────────────────────────────────────────────────────────────────

_simulation = {
    "active": False,
    "until":  0.0,
    "type":   None,
}

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
    resp = requests.get(
        "https://api.electricitymap.org/v3/carbon-intensity/latest",
        headers={"auth-token": API_KEY},
        params={"zone": zone},
        timeout=10,
    )
    resp.raise_for_status()
    return float(resp.json()["carbonIntensity"])


def fetch_history(zone: str) -> list:
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
    if len(history) < 2:
        return None
    values = [v for _, v in history]
    h_min  = min(values)
    h_std  = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "green_threshold": round(h_min + h_std, 2),
        "history_min":     round(h_min, 2),
        "history_std":     round(h_std, 2),
        "history_size":    len(values),
    }


def compute_mumbai_thresholds(history: deque) -> dict | None:
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
    """
    Scale Montreal worker deployment using Kubernetes Python client.
    Uses Workload Identity — no key files needed.
    Connects to Montreal cluster via GKE API endpoint.
    """
    global _currently_scaled_up
    if replicas > 1 and _currently_scaled_up:
        return
    if replicas == 1 and not _currently_scaled_up:
        return
    try:
        from google.cloud import container_v1
        from google.auth import default as google_auth_default
        from google.auth.transport.requests import Request
        import google.auth
        from kubernetes import client as k8s_client

        # Get Montreal cluster endpoint
        cluster_client = container_v1.ClusterManagerClient()
        cluster = cluster_client.get_cluster(
            name=f"projects/{PROJECT_ID}/locations/northamerica-northeast1-a/clusters/gcp-montreal"
        )

        # Build k8s client with GCP credentials
        credentials, _ = google_auth_default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        credentials.refresh(Request())

        configuration = k8s_client.Configuration()
        configuration.host = f"https://{cluster.endpoint}"
        configuration.verify_ssl = False
        configuration.api_key = {"authorization": f"Bearer {credentials.token}"}

        with k8s_client.ApiClient(configuration) as api_client:
            apps_v1 = k8s_client.AppsV1Api(api_client)
            body = {"spec": {"replicas": replicas}}
            apps_v1.patch_namespaced_deployment_scale(
                name=MONTREAL_DEPLOYMENT,
                namespace=MONTREAL_NAMESPACE,
                body=body,
            )
            log.info("Scaled Montreal worker to %d replicas", replicas)
            _currently_scaled_up = replicas > 1

    except Exception as e:
        log.error("Exception scaling Montreal worker: %s", e)

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

    # Check simulation override
    sim_active = _simulation["active"] and time.time() < _simulation["until"]
    if sim_active and _simulation["type"] == "green_window":
        in_green_window = True
        approaching     = False
        log.info("SIMULATION: forcing in_green_window=True")
    elif sim_active and _simulation["type"] == "approaching":
        in_green_window = False
        approaching     = True
        log.info("SIMULATION: forcing approaching=True")
    else:
        _simulation["active"] = False  # expired
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
        "simulated":         sim_active,
    }

# ── Startup: load 24h history immediately ────────────────────────────────────

def load_history_on_startup() -> None:
    log.info("Loading 24h history on startup...")
    for region, zone in ZONES.items():
        try:
            readings = fetch_history(zone)
            for ts, intensity in readings:
                _history[region].append((ts, intensity))
            _fallback[region] = readings[-1][1] if readings else None
            log.info(
                "Loaded %d historical readings for %s (range %.0f-%.0f gCO2/kWh)",
                len(readings), region,
                min(r[1] for r in readings),
                max(r[1] for r in readings),
            )
        except Exception as e:
            log.error("Failed to load history for %s: %s", region, e)

# ── Poller ────────────────────────────────────────────────────────────────────

def poll_once() -> None:
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


@app.route("/admin/simulate-green-window", methods=["POST"])
def simulate_green_window():
    """
    Force a green window simulation for testing predictive scaling.
    POST JSON: {"duration_seconds": 300, "type": "approaching" or "green_window"}

    type=approaching  → sets approaching=True — triggers proactive scale-up
    type=green_window → sets in_green_window=True — triggers job redirection
    """
    data     = request.get_json() or {}
    duration = int(data.get("duration_seconds", 300))
    sim_type = data.get("type", "approaching")

    _simulation["active"] = True
    _simulation["until"]  = time.time() + duration
    _simulation["type"]   = sim_type

    log.info(
        "Simulation started: type=%s duration=%ds",
        sim_type, duration,
    )
    return jsonify({
        "status":       "ok",
        "type":         sim_type,
        "duration":     duration,
        "active_until": datetime.fromtimestamp(
            _simulation["until"], tz=timezone.utc
        ).isoformat()
    })


@app.route("/admin/simulation-status", methods=["GET"])
def simulation_status():
    active = _simulation["active"] and time.time() < _simulation["until"]
    return jsonify({
        "active":           active,
        "type":             _simulation["type"] if active else None,
        "remaining_seconds": max(0, int(_simulation["until"] - time.time())) if active else 0,
    })


@app.route("/admin/stop-simulation", methods=["POST"])
def stop_simulation():
    _simulation["active"] = False
    log.info("Simulation stopped")
    return jsonify({"status": "ok"})


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    log.info(
        "Carbon forecaster starting — scaling_mode=%s port=%d poll_interval=%ds",
        SCALING_MODE, PORT, POLL_INTERVAL,
    )
    load_history_on_startup()
    poll_once()

    t = threading.Thread(target=poller_loop, daemon=True)
    t.start()

    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()