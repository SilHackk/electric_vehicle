"""
Full Web UI server with SSE (/api/stream) for live updates.

Features:
- Monitors Central via CentralUIClient and holds live state in UIState (monitor_state).
- /api/dashboard returns the current snapshot (does NOT modify timestamp).
- /api/stream is an SSE endpoint that emits when monitor_state.timestamp changes.
- One-shot fallback (_fetch_full_state_once) to request FULL_STATE if monitor client is down.
- /api/stats, /api/history, /api/monitor_status and /api/driver_action endpoints.
"""
from flask import Flask, jsonify, send_file, request, Response
from flask_cors import CORS
import os
import time
import threading
import json
import logging
import socket
import ast
import sys
import requests

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

def _fetch_full_state_once(host, port, timeout=2.0):
    """One-shot TCP register -> wait for FULL_STATE and apply it.
       Returns True if applied, False otherwise."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, int(port)))
        # send REGISTER MONITOR temporary id
        reg = Protocol.build_message(MessageTypes.REGISTER, "MONITOR", "WEB_UI_TEMP")
        s.send(Protocol.encode(reg))

        buf = b''
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
            if fields[0] == 'FULL_STATE':
                cps = []
                drivers = []
                history = []
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
                try:
                    s.close()
                except Exception:
                    pass
                try:
                    monitor_state.set_full_state(cps, drivers, history)
                    return True
                except Exception:
                    return False
        try:
            s.close()
        except Exception:
            pass
    except Exception:
        return False
    return False

# --- Routes ---
@app.route('/')
def index():
    html_path = os.path.join(os.path.dirname(__file__), 'dashboard.html')
    if os.path.exists(html_path):
        return send_file(html_path)
    return "<h1>Dashboard not found</h1>", 404

@app.route('/api/dashboard')
def api_dashboard():
    """
    Returns the live state snapshot from the Central Service.
    This endpoint DOES NOT change the state's timestamp. Timestamp must be set by UIState when Central sends updates.
    As a convenience, if the monitor client is not connected and we have empty state, try a one-shot fetch.
    """
    snap = monitor_state.snapshot() or {}
    if not snap.get('timestamp'):
        snap['timestamp'] = time.time()

    # Check ui_client socket presence
    try:
        client_sock = getattr(ui_client, 'sock', None)
    except Exception:
        client_sock = None

    need_fallback = (not snap.get('charging_points') or not snap.get('drivers') or snap.get('timestamp') is None)
    if need_fallback and not client_sock:
        central_host = os.environ.get('CENTRAL_HOST', 'central')
        central_port = int(os.environ.get('CENTRAL_PORT', 5000))
        try:
            applied = _fetch_full_state_once(central_host, central_port, timeout=2.0)
            if applied:
                snap = monitor_state.snapshot()
        except Exception:
            pass

    # Drivers fallback from file
    if not snap.get("drivers"):
        snap["drivers"] = {}
        try:
            drivers_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'drivers.txt')
            if os.path.exists(drivers_file):
                with open(drivers_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            d = json.loads(line)
                            snap["drivers"][d["driver_id"]] = {}
        except Exception:
            pass

    for driver_id, driver in snap.get("drivers", {}).items():
        driver.setdefault("status", "IDLE")
        driver.setdefault("current_cp", None)

    if not snap.get("charging_points"):
        snap["charging_points"] = {}
        try:
            cps_list = _storage.get_all_cps() or []
            for cp in cps_list:
                cp_id = cp.get("cp_id")
                snap["charging_points"][cp_id] = {
                    "state": "ACTIVATED",
                    "location": [cp.get("latitude"), cp.get("longitude")],
                    "price_per_kwh": float(cp.get("price_per_kwh", 0.0)),
                    "current_driver": None,
                    "kwh_delivered": 0.0,
                    "amount_euro": 0.0
                }
        except Exception:
            pass

    for cp in snap.get("charging_points", {}).values():
        cp.setdefault("current_driver", None)
        cp.setdefault("kwh_delivered", 0.0)
        cp.setdefault("amount_euro", 0.0)
        cp.setdefault("state", "ACTIVATED")

    # IMPORTANT: do NOT overwrite snap['timestamp'] here. The timestamp must reflect real updates from Central.
    if 'timestamp' not in snap:
        snap['timestamp'] = None

    return jsonify(snap)

@app.route('/api/history')
def api_history():
    snap = monitor_state.snapshot()
    return jsonify({"history": snap.get("history", [])})

@app.route('/api/stats')
def api_stats():
    snap = monitor_state.snapshot()
    cps = snap.get("charging_points", {})
    drivers = snap.get("drivers", {})
    total_energy = sum(float(c.get("kwh_delivered", 0)) for c in cps.values())
    total_revenue = sum(float(c.get("amount_euro", 0)) for c in cps.values())
    active_charges = sum(1 for c in cps.values() if c.get("state") == "SUPPLYING")
    return jsonify({
        "total_cps": len(cps),
        "active_charges": active_charges,
        "total_drivers": len(drivers),
        "total_energy": total_energy,
        "total_revenue": total_revenue
    })

@app.route('/api/monitor_status')
def api_monitor_status():
    connected = False
    try:
        connected = bool(ui_client and getattr(ui_client, 'sock', None))
    except Exception:
        connected = False
    snap = monitor_state.snapshot()
    return jsonify({
        "monitor_connected": connected,
        "last_update": snap.get('timestamp')
    })

@app.route('/api/driver_action', methods=['POST'])
def driver_action():
    data = request.json or {}
    driver_id = data.get('driver_id')
    action = data.get('action')
    cp_id = data.get('cp_id')
    kwh_needed = float(data.get('kwh_needed', 10))

    if not driver_id or not action:
        return jsonify({"success": False, "error": "Missing driver_id or action"}), 400

    if not ui_client or not ui_client.sock:
        return jsonify({"success": False, "error": "Not connected to Central"}), 503

    try:
        if action == 'request_charge':
            if not cp_id:
                return jsonify({"success": False, "error": "Missing cp_id"}), 400
            ui_client.send_command("REQUEST_CHARGE", driver_id=driver_id, cp_id=cp_id, kwh_needed=kwh_needed)
            logging.info(f"[Web UI] Sent REQUEST_CHARGE: {driver_id} -> {cp_id}")

        elif action == 'finish_charging':
            snap = monitor_state.snapshot()
            driver = snap.get("drivers", {}).get(driver_id, {})
            cp_id = cp_id or driver.get("current_cp")
            if not cp_id:
                return jsonify({"success": False, "error": "No active charging session"}), 400
            ui_client.send_command("FINISH_CHARGE", driver_id=driver_id, cp_id=cp_id)
            logging.info(f"[Web UI] Sent FINISH_CHARGE: {driver_id} -> {cp_id}")
        else:
            return jsonify({"success": False, "error": f"Unknown action: {action}"}), 400

        return jsonify({"success": True})
    except Exception as e:
        logging.exception(f"Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
# --- SSE stream endpoint ---
@app.route('/api/stream')
def api_stream():
    """
    SSE endpoint: emits a data line when monitor_state.timestamp changes.
    Emits a comment keepalive every N seconds so connections survive proxies.
    """
    def event_stream():
        last_ts = None
        keepalive = 0
        while True:
            try:
                snap = monitor_state.snapshot()
                ts = snap.get('timestamp')
                # Emit only when timestamp changed and is not None
                if ts is not None and ts != last_ts:
                    last_ts = ts
                    yield f"data: {ts}\n\n"
                    keepalive = 0
                else:
                    time.sleep(0.5)
                    keepalive += 1
                    # every ~10s send a comment keepalive (does not trigger fetch)
                    if keepalive >= 20:
                        keepalive = 0
                        yield ": keepalive\n\n"
            except GeneratorExit:
                break
            except Exception:
                time.sleep(1)
                continue
    return Response(event_stream(), mimetype='text/event-stream')

@app.route('/api/register_cp', methods=['POST'])
def register_cp():
    data = request.json or {}

    required = ['cp_id', 'latitude', 'longitude', 'price_per_kwh']
    for f in required:
        if f not in data:
            return jsonify({"success": False, "error": f"Missing {f}"}), 400

    try:
        registry_url = os.environ.get("REGISTRY_URL", "http://registry:5001")
        r = requests.post(
            f"{registry_url}/register",
            json={
                "cp_id": data["cp_id"],
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "price_per_kwh": data["price_per_kwh"]
            },
            timeout=5
        )

        return jsonify(r.json()), r.status_code

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/register_driver', methods=['POST'])
def register_driver():
    """Forward driver registration to Registry"""
    data = request.json or {}

    if 'driver_id' not in data:
        return jsonify({"success": False, "error": "Missing driver_id"}), 400

    try:
        import requests  # Pridėti import viršuje jei dar nėra
        
        registry_url = os.environ.get("REGISTRY_URL", "http://registry:5001")
        r = requests.post(
            f"{registry_url}/register_driver",
            json={"driver_id": data["driver_id"]},
            timeout=5
        )

        return jsonify(r.json()), r.status_code

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# --- Server Run ---
def run_server(host='0.0.0.0', port=8000):
    logging.info("Starting Web UI server...")
    threading.Thread(target=init_monitor, daemon=True).start()
    app.run(host=host, port=port, debug=False, threaded=True)

if __name__ == "__main__":
    run_server()
