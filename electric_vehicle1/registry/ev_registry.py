# ============================================================================
# EV_Registry - FIXED VERSION
# Issues fixed:
# 1. Registration actually saves to file
# 2. Credentials properly generated and retrievable
# 3. CORS enabled for web UI access
# 4. Proper error handling
# ============================================================================

from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import os
import secrets
import hashlib
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__)
CORS(app)  # Enable CORS for web UI

# File storage
REGISTRY_FILE = "data/registry.txt"

def ensure_data_dir():
    """Make sure data directory exists"""
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(REGISTRY_FILE):
        with open(REGISTRY_FILE, 'w') as f:
            pass  # Create empty file

def load_registry():
    """Load all registered CPs from file"""
    ensure_data_dir()
    registry = {}
    
    try:
        with open(REGISTRY_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        cp_data = json.loads(line)
                        registry[cp_data['cp_id']] = cp_data
                    except json.JSONDecodeError as e:
                        print(f"[Registry] Error parsing line: {line[:50]}... - {e}")
                        continue
    except FileNotFoundError:
        print("[Registry] Registry file not found, creating new one")
        ensure_data_dir()
    except Exception as e:
        print(f"[Registry] Error loading registry: {e}")
    
    return registry

def save_registry(registry):
    """Save all registered CPs to file"""
    ensure_data_dir()
    
    try:
        # Write to temp file first (atomic write)
        temp_file = REGISTRY_FILE + ".tmp"
        with open(temp_file, 'w') as f:
            for cp_data in registry.values():
                f.write(json.dumps(cp_data) + "\n")
        
        # Replace old file with new one
        if os.path.exists(REGISTRY_FILE):
            os.remove(REGISTRY_FILE)
        os.rename(temp_file, REGISTRY_FILE)
        
        print(f"[Registry] ✅ Saved {len(registry)} CPs to {REGISTRY_FILE}")
        return True
    except Exception as e:
        print(f"[Registry] ❌ Error saving registry: {e}")
        return False

def generate_credentials():
    """Generate random username and password"""
    username = f"cp_user_{secrets.token_hex(4)}"
    password = secrets.token_urlsafe(16)
    return username, password

def hash_password(password):
    """Hash password with SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "ok", "service": "EV_Registry"}), 200

@app.route('/register', methods=['POST'])
def register_cp():
    """Register a new CP - generates credentials and saves to registry"""
    data = request.get_json()
    
    if not data or 'cp_id' not in data:
        return jsonify({"error": "cp_id required"}), 400
    
    cp_id = data['cp_id']
    city = data.get('city', 'Madrid')
    price_per_kwh = float(data.get('price_per_kwh', 0.30))
    latitude = data.get('latitude')
    longitude = data.get('longitude')
    
    # Load current registry
    registry = load_registry()
    
    # Check if already exists
    if cp_id in registry:
        return jsonify({
            "error": "CP already registered",
            "cp_id": cp_id,
            "username": registry[cp_id].get('username'),
            "password": registry[cp_id].get('password')  # Return existing credentials
        }), 409
    
    # Generate new credentials
    username, password = generate_credentials()
    password_hash = hash_password(password)
    
    # Create CP entry
    cp_entry = {
        "cp_id": cp_id,
        "city": city,
        "latitude": latitude,
        "longitude": longitude,
        "username": username,
        "password": password,  # Store plaintext (for demo only!)
        "password_hash": password_hash,
        "price_per_kwh": price_per_kwh,
        "registered_at": datetime.now().isoformat()
    }
    
    # Add to registry
    registry[cp_id] = cp_entry
    
    # Save to file
    if save_registry(registry):
        print(f"[Registry] ✅ Registered {cp_id} in {city}")
        print(f"[Registry]    Username: {username}")
        print(f"[Registry]    Password: {password}")
        
        return jsonify({
            "success": True,
            "cp_id": cp_id,
            "city": city,
            "latitude": latitude,
            "longitude": longitude,
            "username": username,
            "password": password,
            "message": "CP registered successfully"
        }), 201
    else:
        return jsonify({"error": "Failed to save registry"}), 500

@app.route('/verify', methods=['POST'])
def verify_cp():
    """Verify CP credentials"""
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    username = data.get("username")
    password = data.get("password")
    
    if not all([cp_id, username, password]):
        return jsonify({"valid": False, "error": "Missing credentials"}), 400
    
    registry = load_registry()
    
    if cp_id not in registry:
        print(f"[Registry] ❌ Verify failed: CP {cp_id} not found")
        return jsonify({"valid": False, "error": "CP not found"}), 404
    
    cp = registry[cp_id]
    
    # Check username
    if cp.get("username") != username:
        print(f"[Registry] ❌ Verify failed: Invalid username for {cp_id}")
        return jsonify({"valid": False, "error": "Invalid username"}), 401
    
    # Check password
    if cp.get("password_hash") != hash_password(password):
        print(f"[Registry] ❌ Verify failed: Invalid password for {cp_id}")
        return jsonify({"valid": False, "error": "Invalid password"}), 401
    
    print(f"[Registry] ✅ Verified credentials for {cp_id}")
    
    return jsonify({
        "valid": True,
        "cp_id": cp_id,
        "price_per_kwh": cp.get("price_per_kwh", 0.30),
        "latitude": cp.get("latitude"),
        "longitude": cp.get("longitude"),
        "city": cp.get("city")
    }), 200

@app.route('/list', methods=['GET'])
def list_cps():
    """List all registered CPs"""
    registry = load_registry()
    
    cps = []
    for cp_id, cp_data in registry.items():
        cps.append({
            "cp_id": cp_id,
            "city": cp_data.get("city", "Unknown"),
            "latitude": cp_data.get("latitude"),
            "longitude": cp_data.get("longitude"),
            "username": cp_data.get('username'),
            "password": cp_data.get('password', ''),  # Include for demo
            "price_per_kwh": cp_data.get("price_per_kwh", 0.30),
            "registered_at": cp_data.get('registered_at')
        })
    
    return jsonify({"charging_points": cps}), 200

@app.route('/get/<cp_id>', methods=['GET'])
def get_cp(cp_id):
    """Get specific CP details"""
    registry = load_registry()
    
    if cp_id not in registry:
        return jsonify({"error": "CP not found"}), 404
    
    return jsonify(registry[cp_id]), 200

def init_default_cps():
    """Initialize default CPs if registry is empty"""
    registry = load_registry()
    
    default_cps = [
        {"cp_id": "CP-001", "city": "Madrid", "latitude": 40.4168, "longitude": -3.7038, "price_per_kwh": 0.30},
        {"cp_id": "CP-002", "city": "Vilnius", "latitude": 54.6872, "longitude": 25.2797, "price_per_kwh": 0.35}
    ]
    
    added = 0
    for cp_data in default_cps:
        if cp_data["cp_id"] not in registry:
            username, password = generate_credentials()
            registry[cp_data["cp_id"]] = {
                "cp_id": cp_data["cp_id"],
                "username": username,
                "password": password,
                "password_hash": hash_password(password),
                "city": cp_data["city"],
                "latitude": cp_data["latitude"],
                "longitude": cp_data["longitude"],
                "price_per_kwh": cp_data["price_per_kwh"],
                "registered_at": datetime.now().isoformat()
            }
            added += 1
    
    if added > 0:
        save_registry(registry)
        print(f"[Registry] ✅ Initialized {added} default CPs")

if __name__ == "__main__":
    print("=" * 60)
    print("🔐 EV_Registry Starting")
    print("=" * 60)
    
    ensure_data_dir()
    init_default_cps()
    
    print(f"[Registry] Listening on http://0.0.0.0:5001")
    print(f"[Registry] Registry file: {os.path.abspath(REGISTRY_FILE)}")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)