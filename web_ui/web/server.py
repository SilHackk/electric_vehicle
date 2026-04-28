"""
Full Web UI server with SSE (/api/stream) for live updates.

Features:
- Monitors Central via CentralUIClient and holds live state in UIState (monitor_state).
- /api/dashboard returns the current snapshot (does NOT modify timestamp).
- /api/stream is an SSE endpoint that emits when monitor_state.timestamp changes.
- One-shot fallback (_fetch_full_state_once) to request FULL_STATE if monitor client is down.
- /api/stats, /api/history, /api/monitor_status and /api/driver_action endpoints.
"""
from flask import Flask, jsonify, send_file, request, Response, render_template
from flask_cors import CORS
import os
import time
import threading
import logging
import socket
import ast
import sys
import requests
import math

# path hack to allow import shared.* (as in your repo layout)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.file_storage import FileStorage
from web_ui.web.state import UIState
from web_ui.web.socket_client import CentralUIClient
from shared.protocol import Protocol, MessageTypes

# Logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

app = Flask(__name__)
CORS(app)

# Live state and storage
monitor_state = UIState()
_storage = FileStorage("data")
ui_client = None
MODEL_URL = os.environ.get("MODEL_URL", "http://model_service:8000/predict")

CITY_COORDS = {
    "vilnius": (5.0, 7.0),
    "kaunas": (4.0, 5.0),
    "klaipeda": (1.0, 3.0),
    "šiauliai": (3.0, 8.0),
    "siauliai": (3.0, 8.0),
    "panevezys": (6.0, 8.0),
    "panevėžys": (6.0, 8.0),
    "alytus": (5.0, 4.0),
}

STATIONS = {
    "A": {"name": "CP-A", "city": "Vilnius", "x": 5.5, "y": 7.2, "power": 22, "price": 0.25, "free": 1},
    "B": {"name": "CP-B", "city": "Kaunas", "x": 4.2, "y": 5.2, "power": 50, "price": 0.38, "free": 1},
    "C": {"name": "CP-C", "city": "Klaipeda", "x": 1.2, "y": 3.1, "power": 11, "price": 0.18, "free": 1},
}


def _distance(x1, y1, x2, y2):
    return round(math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2), 2)


def _build_ai_features(battery_level, city, energy_needed_kwh, priority):
    location_x, location_y = CITY_COORDS.get(city.lower().strip(), (5.0, 5.0))
    data = {
        "battery_level": int(battery_level),
        "energy_needed_kwh": float(energy_needed_kwh),
        "location_x": float(location_x),
        "location_y": float(location_y),
        "priority": int(priority),
    }

    for key, station in STATIONS.items():
        dist = _distance(location_x, location_y, station["x"], station["y"])
        duration = round(float(energy_needed_kwh) / station["power"], 2)
        cost = round(float(energy_needed_kwh) * station["price"], 2)

        data[f"station_{key}_free"] = station["free"]
        data[f"station_{key}_distance"] = dist
        data[f"station_{key}_power"] = station["power"]
        data[f"station_{key}_price"] = station["price"]
        data[f"station_{key}_duration"] = duration
        data[f"station_{key}_cost"] = cost
    return data


def _fallback_ai_recommendation(features):
    candidates = []
    for key, station in STATIONS.items():
        is_free = features[f"station_{key}_free"]
        if not is_free:
            continue
        dist = features[f"station_{key}_distance"]
        duration = features[f"station_{key}_duration"]
        cost = features[f"station_{key}_cost"]
        priority = features["priority"]
        score = cost + dist * 0.3 + duration * 0.2 if priority == 0 else duration * 2 + dist * 0.2 + cost * 0.2
        candidates.append((key, score, cost, duration, is_free))

    if not candidates:
        return {"best_station": "NONE", "station_label": "No free station", "cost": None, "duration": None, "free": 0}

    best = min(candidates, key=lambda x: x[1])
    key = best[0]
    return {"best_station": key, "station_label": STATIONS[key]["name"], "cost": best[2], "duration": best[3], "free": best[4]}


def _run_ai_recommendation(battery, city, energy, priority):
    features = _build_ai_features(
        battery_level=battery,
        city=city,
        energy_needed_kwh=energy,
        priority=priority,
    )
    try:
        response = requests.post(MODEL_URL, json=features, timeout=5)
        response.raise_for_status()
        model_result = response.json()
        station_key = str(model_result.get("best_station", "")).replace("CP-", "").replace("station_", "")
        station_key = station_key[-1:].upper() if station_key else ""
    except Exception:
        model_result = _fallback_ai_recommendation(features)
        station_key = model_result["best_station"]

    # Post-process model output to keep recommendation logical:
    # if model picks unknown/busy station, fallback to deterministic best free station.
    if station_key not in STATIONS or not features.get(f"station_{station_key}_free", 0):
        fallback = _fallback_ai_recommendation(features)
        station_key = fallback["best_station"] if fallback["best_station"] in STATIONS else station_key
        if station_key in STATIONS:
            model_result = fallback

    if station_key in STATIONS:
        return {
            "station_key": station_key,
            "station": STATIONS[station_key]["name"],
            "station_city": STATIONS[station_key]["city"],
            "cost": float(model_result.get("cost", features.get(f"station_{station_key}_cost", 0.0)) or 0.0),
            "duration": float(model_result.get("duration", features.get(f"station_{station_key}_duration", 0.0)) or 0.0),
            "free": int(model_result.get("free", features.get(f"station_{station_key}_free", 0)) or 0),
        }

    return {
        "station_key": "NONE",
        "station": "No free station",
        "station_city": "-",
        "cost": None,
        "duration": None,
        "free": 0,
    }

# --- Initialization ---
def init_monitor():
    """Start the CentralUIClient in background to receive live updates."""
    global ui_client
    central_host = os.environ.get("CENTRAL_HOST", "central")
    central_port = int(os.environ.get("CENTRAL_PORT", 5000))
    try:
        ui_client = CentralUIClient(monitor_state, host=central_host, port=central_port)
        logging.info(f"[Web UI] Monitor client started (connects to {central_host}:{central_port})")
    except Exception as e:
        ui_client = None
        logging.exception(f"[Web UI] Failed to initialize monitor client: {e}")

def _parse_full_state_fields(fields):
    cps, drivers, history = [], [], []
    if len(fields) > 1:
        try:
            cps = ast.literal_eval(fields[1])
        except Exception:
            cps = []
    if len(fields) > 2:
        try:
            drivers = ast.literal_eval(fields[2])
        except Exception:
            drivers = []
    if len(fields) > 3:
        try:
            history = ast.literal_eval(fields[3])
        except Exception:
            history = []
    return cps, drivers, history

def _fetch_full_state_once(host, port, timeout=2.0):
    """One-shot TCP register -> wait for FULL_STATE and apply it."""
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, int(port)))

        reg = Protocol.build_message(MessageTypes.REGISTER, "MONITOR", "WEB_UI_TEMP")
        s.send(Protocol.encode(reg))

        buf = b""
        start = time.time()
        while time.time() - start < timeout:
            try:
                data = s.recv(4096)
            except socket.timeout:
                continue

            if not data:
                break

            buf += data
            msg, valid = Protocol.decode(buf)
            if not valid or msg is None:
                continue

            fields = Protocol.parse_message(msg)
            if not fields:
                continue

            if fields[0] == "FULL_STATE":
                cps, drivers, history = _parse_full_state_fields(fields)
                try:
                    monitor_state.set_full_state(cps, drivers, history)
                    return True
                except Exception:
                    return False
    except Exception:
        return False
    finally:
        try:
            if s:
                s.close()
        except Exception:
            pass
    return False

# --- Routes ---
@app.route("/")
def index():
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if os.path.exists(html_path):
        return send_file(html_path)
    return "<h1>Dashboard not found</h1>", 404


@app.route("/ai", methods=["GET", "POST"])
def ai_page():
    result = None
    form_data = {"battery": 35, "city": "Vilnius", "energy": 20, "priority": 1}

    if request.method == "POST":
        form_data = {
            "battery": request.form.get("battery", 35),
            "city": request.form.get("city", "Vilnius"),
            "energy": request.form.get("energy", 20),
            "priority": request.form.get("priority", 1),
        }
        result = _run_ai_recommendation(
            battery=form_data["battery"],
            city=form_data["city"],
            energy=form_data["energy"],
            priority=form_data["priority"],
        )
        result["priority_label"] = "Fastest" if int(form_data["priority"]) == 1 else "Cheapest"
        result["user_city"] = form_data["city"]

    return render_template("index.html", result=result, form_data=form_data, stations=STATIONS)


@app.route("/api/ai/recommend", methods=["POST"])
def api_ai_recommend():
    data = request.json or {}
    battery = int(data.get("battery", 35))
    city = str(data.get("city", "Vilnius"))
    energy = float(data.get("energy", 20))
    priority = int(data.get("priority", 1))

    result = _run_ai_recommendation(
        battery=battery,
        city=city,
        energy=energy,
        priority=priority,
    )
    result["priority_label"] = "Fastest" if priority == 1 else "Cheapest"
    result["user_city"] = city
    return jsonify(result)

@app.route("/api/dashboard")
def api_dashboard():
    snap = monitor_state.snapshot() or {}

    # If no live data at all – try one-shot fetch
    if not snap.get("charging_points") and not snap.get("drivers"):
        try:
            central_host = os.environ.get("CENTRAL_HOST", "central")
            central_port = int(os.environ.get("CENTRAL_PORT", 5000))
            _fetch_full_state_once(central_host, central_port, timeout=2.0)
            snap = monitor_state.snapshot() or {}
        except Exception:
            pass

    # Do not invent state; only ensure key exists
    if "timestamp" not in snap:
        snap["timestamp"] = None

    return jsonify(snap)

@app.route("/api/history")
def api_history():
    snap = monitor_state.snapshot() or {}
    return jsonify({"history": snap.get("history", [])})

@app.route("/api/stats")
def api_stats():
    snap = monitor_state.snapshot() or {}
    cps = snap.get("charging_points", {}) or {}
    drivers = snap.get("drivers", {}) or {}
    history = snap.get("history", []) or []

    total_energy = sum(float(h.get("kwh_delivered", 0) or 0) for h in history)
    total_revenue = sum(float(h.get("total_amount", 0) or 0) for h in history)

    active_charges = sum(1 for c in cps.values() if c.get("state") == "SUPPLYING")

    return jsonify({
        "total_cps": len(cps),
        "active_charges": active_charges,
        "total_drivers": len(drivers),
        "total_energy": total_energy,
        "total_revenue": total_revenue
    })

@app.route("/api/monitor_status")
def api_monitor_status():
    connected = False
    try:
        connected = bool(ui_client and getattr(ui_client, "sock", None))
    except Exception:
        connected = False

    snap = monitor_state.snapshot() or {}
    return jsonify({
        "monitor_connected": connected,
        "last_update": snap.get("timestamp")
    })

@app.route("/api/driver_action", methods=["POST"])
def driver_action():
    data = request.json or {}
    driver_id = data.get("driver_id")
    action = data.get("action")
    cp_id = data.get("cp_id")
    if isinstance(cp_id, str) and cp_id.strip():
        cp_id = cp_id.strip().upper()
    kwh_needed = float(data.get("kwh_needed", 10))

    if not driver_id or not action:
        return jsonify({"success": False, "error": "Missing driver_id or action"}), 400

    if not ui_client or not getattr(ui_client, "sock", None):
        return jsonify({"success": False, "error": "Not connected to Central"}), 503

    try:
        if action == "request_charge":
            if not cp_id:
                return jsonify({"success": False, "error": "Missing cp_id"}), 400
            ui_client.send_command(
                "REQUEST_CHARGE",
                driver_id=driver_id,
                cp_id=cp_id,
                kwh_needed=kwh_needed
            )
            logging.info(f"[Web UI] Sent REQUEST_CHARGE: {driver_id} -> {cp_id}")

        elif action == "finish_charging":
            snap = monitor_state.snapshot() or {}
            driver = (snap.get("drivers", {}) or {}).get(driver_id, {}) or {}
            cp_id = cp_id or driver.get("current_cp")
            if not cp_id:
                return jsonify({"success": False, "error": "No active charging session"}), 400
            ui_client.send_command("END_CHARGE", driver_id=driver_id, cp_id=cp_id)
            logging.info(f"[Web UI] Sent END_CHARGE: {driver_id} -> {cp_id}")

        else:
            return jsonify({"success": False, "error": f"Unknown action: {action}"}), 400

        return jsonify({"success": True})

    except Exception as e:
        logging.exception(f"Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/register_cp", methods=["POST"])
def register_cp():
    """Proxy CP registration to Central REST API."""
    data = request.json or {}
    central_api = os.environ.get("CENTRAL_API_URL", "http://central:5003")
    try:
        r = requests.post(f"{central_api}/api/register_cp", json=data, timeout=8)
        return jsonify(r.json()), r.status_code
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "Central API timeout"}), 504
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# --- SSE stream endpoint ---
@app.route("/api/stream")
def api_stream():
    """
    SSE endpoint: emits a data line when monitor_state.timestamp changes.
    Emits a comment keepalive every ~10s so connections survive proxies.
    """
    def event_stream():
        last_ts = None
        keepalive = 0
        while True:
            try:
                snap = monitor_state.snapshot() or {}
                ts = snap.get("timestamp")
                if ts is not None and ts != last_ts:
                    last_ts = ts
                    yield f"data: {ts}\n\n"
                    keepalive = 0
                else:
                    time.sleep(0.5)
                    keepalive += 1
                    if keepalive >= 20:
                        keepalive = 0
                        yield ": keepalive\n\n"
            except GeneratorExit:
                break
            except Exception:
                time.sleep(1)
                continue

    return Response(event_stream(), mimetype="text/event-stream")

# --- Server Run ---
def run_server(host="0.0.0.0", port=8000):
    logging.info("Starting Web UI server...")
    threading.Thread(target=init_monitor, daemon=True).start()
    app.run(host=host, port=port, debug=False, threaded=True)

if __name__ == "__main__":
    run_server()
