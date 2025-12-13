# ============================================================================
# EV_Registry - REST API for CP Registration & Credential Management
# ============================================================================

from flask import Flask, request, jsonify
import json
import os
import secrets
import hashlib
from datetime import datetime
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.audit_logger import log_auth

app = Flask(__name__)

# File storage for registry data
REGISTRY_FILE = "data/registry.txt"

def load_registry():
    """Load all registered CPs"""
    if not os.path.exists(REGISTRY_FILE):
        return {}
    
    registry = {}
    try:
        with open(REGISTRY_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    cp_data = json.loads(line)
                    registry[cp_data['cp_id']] = cp_data
    except Exception as e:
        print(f"[Registry] Error loading: {e}")
    return registry

def save_registry(registry):
    """Save all registered CPs"""
    try:
        os.makedirs("data", exist_ok=True)
        with open(REGISTRY_FILE, 'w') as f:
            for cp_data in registry.values():
                f.write(json.dumps(cp_data) + "\n")
    except Exception as e:
        print(f"[Registry] Error saving: {e}")

def generate_credentials():
    """Generate random username and password"""
    username = f"cp_user_{secrets.token_hex(4)}"
    password = secrets.token_urlsafe(16)
    return username, password

def hash_password(password):
    """Hash password for storage"""
    return hashlib.sha256(password.encode()).hexdigest()

# ============================================================================
# REST API ENDPOINTS
# ============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "ok", "service": "EV_Registry"}), 200

@app.route('/register', methods=['POST'])
def register_cp():
    """
    Register a new CP
    Body: {"cp_id": "CP-001", "latitude": "40.5", "longitude": "-3.1", "price_per_kwh": 0.30}
    Returns: {"username": "...", "password": "...", "cp_id": "..."}
    """
    data = request.get_json()
    
    if not data or 'cp_id' not in data:
        return jsonify({"error": "cp_id required"}), 400
    
    cp_id = data['cp_id']
    latitude = data.get('latitude', '0')
    longitude = data.get('longitude', '0')
    price_per_kwh = data.get('price_per_kwh', 0.30)
    
    registry = load_registry()
    
    # Generate credentials
    username, password = generate_credentials()
    password_hash = hash_password(password)
    
    # Store CP data
    registry[cp_id] = {
        "cp_id": cp_id,
        "username": username,
        "password_hash": password_hash,
        "latitude": latitude,
        "longitude": longitude,
        "price_per_kwh": price_per_kwh,
        "registered_at": datetime.now().isoformat()
    }
    
    save_registry(registry)
    
    print(f"[Registry] ✅ Registered CP: {cp_id} with username: {username}")
    
    # Return credentials (password in plaintext ONCE)
    return jsonify({
        "cp_id": cp_id,
        "username": username,
        "password": password,  # Only returned once!
        "message": "Registration successful. Save these credentials!"
    }), 201

@app.route('/unregister/<cp_id>', methods=['DELETE'])
def unregister_cp(cp_id):
    """Delete a CP registration"""
    registry = load_registry()
    
    if cp_id not in registry:
        return jsonify({"error": "CP not found"}), 404
    
    del registry[cp_id]
    save_registry(registry)
    
    print(f"[Registry] ❌ Unregistered CP: {cp_id}")
    
    return jsonify({"message": f"CP {cp_id} unregistered"}), 200

@app.route('/verify', methods=['POST'])
def verify_credentials():
    """
    Verify CP credentials (used by Central)
    Body: {"cp_id": "CP-001", "username": "...", "password": "..."}
    Returns: {"valid": true/false}
    """
    data = request.get_json()
    
    if not data or 'cp_id' not in data or 'username' not in data or 'password' not in data:
        return jsonify({"valid": False, "error": "Missing fields"}), 400
    
    cp_id = data['cp_id']
    username = data['username']
    password = data['password']
    
    registry = load_registry()
    
    if cp_id not in registry:
        log_auth(request.remote_addr, cp_id, success=False, reason="NOT_REGISTERED")
        return jsonify({"valid": False, "error": "CP not registered"}), 401
    
    cp_data = registry[cp_id]
    
    if cp_data['username'] != username:
        log_auth(request.remote_addr, cp_id, success=False, reason="INVALID_USERNAME")
        return jsonify({"valid": False, "error": "Invalid username"}), 401
    
    if cp_data['password_hash'] != hash_password(password):
        log_auth(request.remote_addr, cp_id, success=False, reason="INVALID_USERNAME")
        return jsonify({"valid": False, "error": "Invalid password"}), 401
    
    log_auth(request.remote_addr, cp_id, success=True)

    return jsonify({"valid": True, "cp_id": cp_id}), 200

@app.route('/list', methods=['GET'])
def list_cps():
    """List all registered CPs (without credentials)"""
    registry = load_registry()
    
    cps = []
    for cp_id, cp_data in registry.items():
        cps.append({
            "cp_id": cp_id,
            "username": cp_data['username'],
            "latitude": cp_data['latitude'],
            "longitude": cp_data['longitude'],
            "registered_at": cp_data['registered_at']
        })
    
    return jsonify({"charging_points": cps}), 200

@app.route('/register_driver', methods=['POST'])
def register_driver():
    """
    Register a new Driver
    Body: {"driver_id": "DRIVER-001"}
    Returns: {"driver_id": "...", "message": "..."}
    """
    data = request.get_json()
    
    if not data or 'driver_id' not in data:
        return jsonify({"error": "driver_id required"}), 400
    
    driver_id = data['driver_id']
    
    # Load existing drivers
    driver_file = "data/registry_drivers.txt"
    drivers = {}
    
    if os.path.exists(driver_file):
        with open(driver_file, 'r') as f:
            for line in f:
                if line.strip():
                    d = json.loads(line)
                    drivers[d['driver_id']] = d
    
    # Check if already exists
    if driver_id in drivers:
        return jsonify({"error": "Driver already registered"}), 409
    
    # Save driver
    drivers[driver_id] = {
        "driver_id": driver_id,
        "registered_at": datetime.now().isoformat()
    }
    
    os.makedirs("data", exist_ok=True)
    with open(driver_file, 'w') as f:
        for d in drivers.values():
            f.write(json.dumps(d) + "\n")
    
    print(f"[Registry] ✅ Registered Driver: {driver_id}")
    
    return jsonify({
        "driver_id": driver_id,
        "message": "Driver registered successfully"
    }), 201

@app.route('/list_drivers', methods=['GET'])
def list_drivers():
    """List all registered drivers"""
    driver_file = "data/registry_drivers.txt"
    drivers = []
    
    if os.path.exists(driver_file):
        with open(driver_file, 'r') as f:
            for line in f:
                if line.strip():
                    drivers.append(json.loads(line))
    
    return jsonify({"drivers": drivers}), 200

if __name__ == "__main__":
    print("[EV_Registry] Starting on port 5001...")
    app.run(host='0.0.0.0', port=5001, debug=True)