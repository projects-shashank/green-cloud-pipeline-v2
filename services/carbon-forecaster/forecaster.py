"""
Carbon Forecaster — Green Cloud Pipeline v2

On startup:
  1. Fetches 24h historical carbon intensity from Electricity Maps
  2. Computes thresholds immediately — no warm-up period
  3. Fetches live current intensity
  4. Starts polling every POLL_INTERVAL seconds to keep data fresh

Green window definition (Montreal):
    green_threshold = p25(24h history)
    if current <= green_threshold → in green window
    Approaching: current <= green_threshold * 1.05 AND slope falling

Migration threshold (Mumbai dirty check):
    migration_threshold = p75(24h history)
    if current > migration_threshold → Mumbai is dirty
    Approaching: current >= migration_threshold * 0.97 AND slope rising

Routing priority (enforced in admission controller):
    1. Montreal in green window → migrate (maximise carbon saving)
    2. Mumbai dirty            → migrate
    3. Otherwise              → process locally

Predictive scaling triggers (SCALING_MODE=predictive):
    1. Montreal in green window          → scale up immediately
    2. Montreal approaching green window → scale up pre-emptively
    3. Mumbai is dirty                   → scale up immediately
    4. Mumbai approaching dirty          → scale up pre-emptively
    Scale-down hysteresis: requires 2 consecutive polls with no trigger
    before scaling back to 1 replica — prevents flapping on threshold oscillation.

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

_state_lock           = threading.Lock()
_currently_scaled_up  = False
_scale_down_counter   = 0   # hysteresis: require 2 consecutive false polls before scaling down
SCALE_DOWN_THRESHOLD  = 2   # number of consecutive false polls required to scale down

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
    p25    = float(np.percentile(values, 25))
    h_min  = min(values)
    h_max  = max(values)
    h_mean = statistics.mean(values)
    return {
        "green_threshold": round(p25, 2),
        "history_p25":     round(p25, 2),
        "history_min":     round(h_min, 2),
        "history_max":     round(h_max, 2),
        "history_mean":    round(h_mean, 2),
        "history_size":    len(values),
    }


def compute_mumbai_thresholds(history: deque) -> dict | None:
    if len(history) < 2:
        return None
    values = [v for _, v in history]
    p75    = float(np.percentile(values, 75))
    h_min  = min(values)
    h_max  = max(values)
    h_mean = statistics.mean(values)
    return {
        "migration_threshold": round(p75, 2),
        "history_p75":         round(p75, 2),
        "history_min":         round(h_min, 2),
        "history_max":         round(h_max, 2),
        "history_mean":        round(h_mean, 2),
        "history_size":        len(values),
    }

# ── Approach detection ────────────────────────────────────────────────────────

def _compute_slope(recent: list) -> float:
    """Compute slope of recent readings via linear regression. Returns Python float."""
    x = np.arange(len(recent), dtype=float)
    y = np.array(recent, dtype=float)
    return float(np.polyfit(x, y, 1)[0])


def is_green_window_approaching(history: deque, green_threshold: float) -> bool:
    """
    Predict whether Montreal is about to enter a green window.

    Conditions (all must be true):
        1. Proximity: current intensity within 5% above green_threshold
           i.e. current <= green_threshold * 1.05
        2. Sustained trend: short-term slope (last 6 readings) is flat or negative
        3. Latest movement: current reading is below or equal to previous reading
           (avoids pre-scaling after the falling trend has already reversed)

    Returns plain Python bool — safe for JSON serialization.
    """
    if len(history) < 6:
        return False

    recent   = [float(v) for _, v in list(history)[-6:]]
    current  = recent[-1]
    previous = recent[-2]

    near_threshold   = bool(current <= green_threshold * 1.05)
    slope            = _compute_slope(recent)
    trend_falling    = bool(slope <= 0)
    latest_falling   = bool(current <= previous)

    if near_threshold and trend_falling and latest_falling:
        log.info(
            "Green window approaching: current=%.1f threshold=%.1f slope=%.3f proximity=5%%",
            current, green_threshold, slope,
        )

    return near_threshold and trend_falling and latest_falling


def is_mumbai_approaching_dirty(history: deque, migration_threshold: float) -> bool:
    """
    Predict whether Mumbai is about to become dirty.

    Conditions (all must be true):
        1. Proximity: current intensity within 3% below migration_threshold
           i.e. current >= migration_threshold * 0.97
        2. Sustained trend: short-term slope (last 6 readings) is positive (rising)
        3. Latest movement: current reading is above or equal to previous reading
           (avoids pre-scaling after the rising trend has already reversed)

    When Mumbai goes dirty, all batch jobs redirect to Montreal.
    Pre-scaling Montreal before this happens eliminates the reactive
    scaling delay (metric lag + HPA reaction time + worker startup).

    Returns plain Python bool — safe for JSON serialization.
    """
    if len(history) < 6:
        return False

    recent   = [float(v) for _, v in list(history)[-6:]]
    current  = recent[-1]
    previous = recent[-2]

    near_threshold  = bool(current >= migration_threshold * 0.97)
    slope           = _compute_slope(recent)
    trend_rising    = bool(slope > 0)
    latest_rising   = bool(current >= previous)

    if near_threshold and trend_rising and latest_rising:
        log.info(
            "Mumbai approaching dirty: current=%.1f threshold=%.1f slope=%.3f proximity=3%%",
            current, migration_threshold, slope,
        )

    return near_threshold and trend_rising and latest_rising

# ── Kubernetes scaling ────────────────────────────────────────────────────────

def scale_montreal_worker(replicas: int) -> None:
    """
    Scale Montreal worker deployment using Kubernetes Python client.
    Uses Workload Identity — no key files needed.
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
        from kubernetes import client as k8s_client

        cluster_client = container_v1.ClusterManagerClient()
        cluster = cluster_client.get_cluster(
            name=f"projects/{PROJECT_ID}/locations/northamerica-northeast1-a/clusters/gcp-montreal"
        )

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
    """
    Compute Mumbai state dict with all values as JSON-serializable Python types.
    All booleans explicitly cast with bool() to avoid numpy.bool_ issues.
    """
    t = compute_mumbai_thresholds(_history["mumbai"])
    if t is None:
        return {
            "current_intensity":   round(float(intensity), 2),
            "migration_threshold": None,
            "is_dirty":            None,
            "approaching_dirty":   None,
            "warming_up":          True,
        }

    is_dirty        = bool(float(intensity) > t["migration_threshold"])
    approaching_dirty = (
        False if is_dirty
        else is_mumbai_approaching_dirty(_history["mumbai"], t["migration_threshold"])
    )

    return {
        "current_intensity":   round(float(intensity), 2),
        "migration_threshold": t["migration_threshold"],
        "is_dirty":            bool(is_dirty),
        "approaching_dirty":   bool(approaching_dirty),
        "history_p75":         t["history_p75"],
        "history_min":         t["history_min"],
        "history_max":         t["history_max"],
        "history_mean":        t["history_mean"],
        "history_size":        int(t["history_size"]),
        "warming_up":          False,
    }


def compute_montreal_state(intensity: float) -> dict:
    """
    Compute Montreal state dict with all values as JSON-serializable Python types.
    All booleans explicitly cast with bool() to avoid numpy.bool_ issues.
    """
    t = compute_montreal_thresholds(_history["montreal"])
    if t is None:
        return {
            "current_intensity": round(float(intensity), 2),
            "green_threshold":   None,
            "in_green_window":   None,
            "approaching":       None,
            "warming_up":        True,
        }

    green_threshold = t["green_threshold"]

    # Check simulation override
    sim_active = bool(_simulation["active"] and time.time() < _simulation["until"])

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
        in_green_window = bool(float(intensity) <= green_threshold)
        approaching     = (
            False if in_green_window
            else is_green_window_approaching(_history["montreal"], green_threshold)
        )

    return {
        "current_intensity": round(float(intensity), 2),
        "green_threshold":   green_threshold,
        "in_green_window":   bool(in_green_window),
        "approaching":       bool(approaching),
        "history_p25":       t["history_p25"],
        "history_min":       t["history_min"],
        "history_max":       t["history_max"],
        "history_mean":      t["history_mean"],
        "history_size":      int(t["history_size"]),
        "warming_up":        False,
        "simulated":         bool(sim_active),
    }

# ── Startup: load 24h history immediately ────────────────────────────────────

def load_history_on_startup() -> None:
    log.info("Loading 24h history on startup...")
    for region, zone in ZONES.items():
        try:
            readings = fetch_history(zone)
            for ts, intensity in readings:
                _history[region].append((ts, float(intensity)))
            _fallback[region] = float(readings[-1][1]) if readings else None
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
            intensity           = float(fetch_live(zone))
            _history[region].append((now_utc_iso(), intensity))
            _fallback[region]   = intensity
            results[region]     = intensity
            log.info("Live %s: %.1f gCO2/kWh", region, intensity)
        except Exception as e:
            log.warning("Failed to fetch live %s: %s — using fallback", region, e)
            errors.append(str(e))
            if _fallback[region] is not None:
                results[region] = float(_fallback[region])
            else:
                log.error("No fallback for %s — skipping", region)
                return

    with _state_lock:
        _state["mumbai"]       = compute_mumbai_state(results["mumbai"])
        _state["montreal"]     = compute_montreal_state(results["montreal"])
        _state["last_updated"] = now_utc_iso()
        _state["error"]        = errors if errors else None

    if SCALING_MODE == "predictive":
        global _scale_down_counter
        montreal = _state["montreal"]
        mumbai   = _state["mumbai"]
        if montreal and not montreal.get("warming_up"):
            should_scale = bool(
                montreal.get("in_green_window", False) or
                montreal.get("approaching", False) or
                mumbai.get("is_dirty", False) or
                mumbai.get("approaching_dirty", False)
            )
            if should_scale:
                # Scale up immediately — reset hysteresis counter
                _scale_down_counter = 0
                scale_montreal_worker(PREDICTIVE_REPLICAS)
            else:
                # Scale down only after SCALE_DOWN_THRESHOLD consecutive false polls
                # Prevents flapping when intensity oscillates around the threshold
                _scale_down_counter += 1
                if _scale_down_counter >= SCALE_DOWN_THRESHOLD:
                    log.info(
                        "Scale-down hysteresis satisfied (%d/%d) — scaling to 1",
                        _scale_down_counter, SCALE_DOWN_THRESHOLD,
                    )
                    scale_montreal_worker(1)
                else:
                    log.info(
                        "Scale-down hysteresis: %d/%d polls without trigger — holding at %d replicas",
                        _scale_down_counter, SCALE_DOWN_THRESHOLD,
                        PREDICTIVE_REPLICAS if _currently_scaled_up else 1,
                    )


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

    log.info("Simulation started: type=%s duration=%ds", sim_type, duration)

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
    active = bool(_simulation["active"] and time.time() < _simulation["until"])
    return jsonify({
        "active":            active,
        "type":              _simulation["type"] if active else None,
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
