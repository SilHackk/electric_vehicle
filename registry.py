# ev_registry.py
from flask import Flask, request, jsonify
from cryptography.fernet import Fernet
import uuid
import datetime

app = Flask(__name__)

# Generate a master key for this registry (in practice, store securely)
MASTER_KEY = Fernet.generate_key()
fernet = Fernet(MASTER_KEY)

# In-memory storage for CPs
cp_registry = {}

# Audit log
audit_log = []

def log_event(action, cp_id=None, description=None):
    event = {
        "timestamp": datetime.datetime.now().isoformat(),
        "cp_id": cp_id,
        "action": action,
        "description": description
    }
    audit_log.append(event)
    print("AUDIT:", event)

@app.route("/register", methods=["POST"])
def register_cp():
    data = request.json
    cp_id = str(uuid.uuid4())
    location = data.get("location")
    password = data.get("password", "default_pass")
    
    encrypted_pass = fernet.encrypt(password.encode()).decode()
    cp_registry[cp_id] = {"location": location, "password": encrypted_pass, "active": True}
    
    log_event("REGISTER", cp_id, f"Registered CP at {location}")
    
    return jsonify({"cp_id": cp_id, "status": "registered"}), 201

@app.route("/deregister/<cp_id>", methods=["DELETE"])
def deregister_cp(cp_id):
    if cp_id in cp_registry:
        cp_registry.pop(cp_id)
        log_event("DEREGISTER", cp_id, "CP deregistered")
        return jsonify({"status": "deregistered"}), 200
    return jsonify({"error": "CP not found"}), 404

@app.route("/authenticate", methods=["POST"])
def authenticate_cp():
    data = request.json
    cp_id = data.get("cp_id")
    password = data.get("password")
    
    if cp_id not in cp_registry:
        log_event("AUTH_FAIL", cp_id, "CP not registered")
        return jsonify({"error": "CP not registered"}), 401
    
    stored_encrypted = cp_registry[cp_id]["password"]
    stored_password = fernet.decrypt(stored_encrypted.encode()).decode()
    
    if password == stored_password:
        log_event("AUTH_SUCCESS", cp_id, "CP authenticated successfully")
        # In real case, generate a session token or encryption key
        return jsonify({"status": "authenticated"}), 200
    else:
        log_event("AUTH_FAIL", cp_id, "Wrong password")
        return jsonify({"error": "Authentication failed"}), 401

@app.route("/audit", methods=["GET"])
def get_audit():
    return jsonify(audit_log), 200

if __name__ == "__main__":
    app.run(port=5001, debug=True)
