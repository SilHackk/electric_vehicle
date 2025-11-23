# web_ui/web/server.py
from flask import Flask, jsonify, send_file, request
from flask_cors import CORS
import os
import time
import threading
import json # Used for loading drivers.txt

# path hack to allow import shared.* (as in your repo layout)
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.file_storage import FileStorage
from web_ui.web.state import UIState
from web_ui.web.socket_client import CentralUIClient


app = Flask(__name__)
# ui_state is not used, monitor_state holds the live data feed
ui_state = UIState() 
monitor_state = UIState()
CORS(app)

# Global variables
ui_client = None
_storage = FileStorage("data")

# --- Initialization ---
def init_monitor():
    global ui_client
    # read CENTRAL_HOST/CENTRAL_PORT from env (docker-compose sets them)
    central_host = os.environ.get("CENTRAL_HOST", "central")
    central_port = int(os.environ.get("CENTRAL_PORT", 5000))
    
    # Initialize the client to connect to the Central service via WebSocket
    ui_client = CentralUIClient(monitor_state, host=central_host, port=central_port)
    print(f"[Web UI] Monitor client started (connects to ws://{central_host}:{central_port})")

# --- Routes ---

@app.route('/')
def index():
    """Serves the dashboard HTML file."""
    html_path = os.path.join(os.path.dirname(__file__), 'dashboard.html')
    if os.path.exists(html_path):
        return send_file(html_path)
    return "<h1>Dashboard not found</h1>", 404

@app.route('/api/dashboard')
def api_dashboard():
    """
    Returns the live state snapshot from the Central Service.
    Includes fallback logic to load from file if the live state is empty.
    """
    snap = monitor_state.snapshot()
    
    # 1. Fallback for Drivers: Load all drivers from file first if the live state is empty
    if not snap.get("drivers"):
        snap["drivers"] = {} 
        try:
            drivers_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'drivers.txt')
            if os.path.exists(drivers_file):
                with open(drivers_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            d = json.loads(line)
                            snap["drivers"][d["driver_id"]] = {} # Add driver ID to snap
        except Exception:
            pass
            
    # 2. Apply default driver fields (this uses the live data if present, or defaults if loaded from file)
    for driver_id, driver in snap.get("drivers", {}).items():
        driver.setdefault("status", "IDLE")
        driver.setdefault("current_cp", None)

    # 3. Fallback for Charging Points: Load CPs from file storage if live state is empty
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

    # 4. Apply default CP fields (redundant, but ensures backward compatibility if live data is partial)
    for cp in snap.get("charging_points", {}).values():
        cp.setdefault("current_driver", None)
        cp.setdefault("kwh_delivered", 0.0)
        cp.setdefault("amount_euro", 0.0)
        cp.setdefault("state", "ACTIVATED")

    # always add a timestamp
    snap["timestamp"] = time.time()

    return jsonify(snap)

@app.route('/api/history')
def api_history():
    """Returns the history log."""
    snap = monitor_state.snapshot()
    return jsonify({"history": snap.get("history", [])})

@app.route('/api/stats')
def api_stats():
    """Calculates and returns simple aggregate statistics."""
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

@app.route('/api/driver_action', methods=['POST'])
def driver_action():
    """
    Relays driver commands to the Central Service via the WebSocket client.
    This is the crucial fix for correct system interaction.
    """
    data = request.json
    driver_id = data.get('driver_id')
    action = data.get('action')
    cp_id = data.get('cp_id')
    
    if not driver_id or not action:
        return jsonify({"success": False, "error": "Missing driver_id or action"}), 400

    # Ensure the WebSocket client is running
    if not ui_client:
        print("[Web UI] WARNING: UI client not initialized or connected.")
        return jsonify({"success": False, "error": "System monitor client is offline."}), 503

    try:
        if action == 'request_charge':
            if not cp_id:
                return jsonify({"success": False, "error": "Missing cp_id for request_charge"}), 400
                
            # Use the ui_client to send the command to the Central service
            ui_client.send_command("REQUEST_CHARGE", driver_id=driver_id, cp_id=cp_id)
            print(f"[Web UI] Sent command REQUEST_CHARGE for {driver_id} at {cp_id}")

        elif action == 'finish_charging':
            if not cp_id:
                # Attempt to find the CP the driver is currently using from local state
                driver_info = monitor_state.snapshot().get("drivers", {}).get(driver_id)
                cp_id = driver_info.get("current_cp") if driver_info else None
                
            if not cp_id:
                return jsonify({"success": False, "error": "Cannot find current CP to finish charge"}), 400
                
            # Use the ui_client to send the command to the Central service
            ui_client.send_command("FINISH_CHARGE", driver_id=driver_id, cp_id=cp_id)
            print(f"[Web UI] Sent command FINISH_CHARGE for {driver_id} at {cp_id}")
            
        else:
             return jsonify({"success": False, "error": f"Unknown action: {action}"}), 400

        # The Web UI now expects the Central service to process the command 
        # and send an updated state back via WebSocket, which is picked up by init_monitor.
        return jsonify({"success": True, "message": f"Action {action} relayed to Central."})
    
    except AttributeError:
        # This catches errors if ui_client doesn't have a send_command method 
        # (needs confirmation based on the CentralUIClient implementation)
        return jsonify({"success": False, "error": "Client error: Central communication failed."}), 500
    except Exception as e:
        print(f"[Web UI] Error processing driver action: {e}")
        return jsonify({"success": False, "error": "Internal server error during action."}), 500


# --- Server Run ---

def run_server(host='0.0.0.0', port=8000):
    print("Starting Web UI server...")
    # Start the monitor client in a background thread to handle WebSocket connection
    threading.Thread(target=init_monitor, daemon=True).start()  
    app.run(host=host, port=port, debug=False, threaded=True)
    
if __name__ == "__main__":
    run_server()