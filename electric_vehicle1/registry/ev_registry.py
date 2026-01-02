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
import requests
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
    data = request.get_json()
    
    if not data or 'cp_id' not in data:
        return jsonify({"error": "cp_id required"}), 400
    
    cp_id = data['cp_id']
    city = data.get('city', 'Madrid')
    price_per_kwh = data.get('price_per_kwh', 0.30)
    latitude = data.get('latitude', None)  # ✅ Accept coordinates if provided
    longitude = data.get('longitude', None)
    
    registry = load_registry()
    
    if cp_id in registry:  # ✅ Check if already exists
        return jsonify({"error": "CP already registered"}), 409
    
    username, password = generate_credentials()
    password_hash = hash_password(password)
    
    registry[cp_id] = {
        "cp_id": cp_id,
        "city": city,
        "latitude": latitude,  # ✅ Store coordinates
        "longitude": longitude,
        "username": username,
        "password": password,
        "password_hash": password_hash,
        "price_per_kwh": price_per_kwh,
        "registered_at": datetime.now().isoformat()
    }
    
    save_registry(registry)
    
    print(f"[Registry] ✅ Registered {cp_id} in {city}")
    
    return jsonify({
        "cp_id": cp_id,
        "city": city,
        "latitude": latitude,
        "longitude": longitude,
        "username": username,
        "password": password
    }), 201


def init_default_cps():
    """Initialize default CPs with ACTUAL coordinates"""
    registry = load_registry()
    default_cps = [
        {"cp_id": "CP-001", "city": "Madrid", "latitude": 40.4168, "longitude": -3.7038, "price_per_kwh": 0.30},
        {"cp_id": "CP-002", "city": "Vilnius", "latitude": 54.6872, "longitude": 25.2797, "price_per_kwh": 0.35}
    ]
    for cp in default_cps:
        if cp["cp_id"] not in registry:
            username, password = generate_credentials()
            registry[cp["cp_id"]] = {
                "cp_id": cp["cp_id"],
                "username": username,
                "password": password,
                "password_hash": hash_password(password),
                "city": cp["city"],
                "latitude": cp["latitude"],   # ✅ Add coordinates
                "longitude": cp["longitude"],
                "price_per_kwh": cp["price_per_kwh"],
                "registered_at": datetime.now().isoformat()
            }
    save_registry(registry)
    print(f"[Registry] ✅ Initialized {len(default_cps)} default CPs")
@app.route('/verify', methods=['POST'])
def verify_cp():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    username = data.get("username")
    password = data.get("password")

    registry = load_registry()

    if cp_id not in registry:
        log_auth("CP", cp_id, success=False, reason="CP_NOT_FOUND")
        return jsonify({"valid": False, "error": "CP not found"}), 404

    cp = registry[cp_id]

    if cp.get("username") != username:
        log_auth("CP", cp_id, success=False, reason="INVALID_USERNAME")
        return jsonify({"valid": False, "error": "Invalid username"}), 401

    if cp.get("password_hash") != hash_password(password):
        log_auth("CP", cp_id, success=False, reason="INVALID_PASSWORD")
        return jsonify({"valid": False, "error": "Invalid password"}), 401

    log_auth("CP", cp_id, success=True)

    return jsonify({
        "valid": True,
        "price_per_kwh": cp.get("price_per_kwh", 0.30),
        "latitude": cp.get("latitude"),
        "longitude": cp.get("longitude")
    }), 200


@app.route('/list', methods=['GET'])
def list_cps():
    registry = load_registry()
    cps = []
    for cp_id, cp_data in registry.items():
        cps.append({
            "cp_id": cp_id,
            "city": cp_data.get("city", "Unknown"),
            "username": cp_data['username'],
            "password": cp_data.get('password', ''),
            "price_per_kwh": cp_data.get("price_per_kwh", 0.30),
            "registered_at": cp_data['registered_at']
        })
    return jsonify({"charging_points": cps}), 200


if __name__ == "__main__":
    init_default_cps()
    app.run(host='0.0.0.0', port=5001, debug=True)