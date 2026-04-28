# ============================================================================
# EVCharging System - Simple TXT File Storage
# ============================================================================

import os
import json
import threading
from datetime import datetime


class FileStorage:
    """Simple file-based storage using .txt files"""

    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.lock = threading.Lock()
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # File paths
        self.cp_file = os.path.join(data_dir, "charging_points.txt")
        self.driver_file = os.path.join(data_dir, "drivers.txt")
        self.history_file = os.path.join(data_dir, "charging_history.txt")
        
        # Initialize files if they don't exist
        self._init_files()

    def _init_files(self):
        """Create empty files if they don't exist"""
        for filepath in [self.cp_file, self.driver_file, self.history_file]:
            if not os.path.exists(filepath):
                with open(filepath, 'w') as f:
                    f.write("")

    # ========================================================================
    # CHARGING POINTS
    # ========================================================================

    def save_cp(self, cp_id, latitude, longitude, price_per_kwh, state="ACTIVATED"):
        """Save charging point to file (now accepts lat/lon instead of city)"""
        with self.lock:
            cps = self._read_cps()
            cps[cp_id] = {
                "cp_id": cp_id,
                "latitude": float(latitude),   # ✅ Store coordinates
                "longitude": float(longitude),
                "price_per_kwh": float(price_per_kwh),
                "state": state,
                "registered_at": datetime.now().isoformat()
            }
            self._write_cps(cps)


    def save_cp_secret(self, cp_id, secret):
        """Save CP secret/password for authentication"""
        secrets_file = os.path.join(self.data_dir, "cp_secrets.txt")
        
        with self.lock:
            secrets = {}
            
            # Read existing secrets
            if os.path.exists(secrets_file):
                try:
                    with open(secrets_file, 'r') as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                data = json.loads(line)
                                secrets[data['cp_id']] = data['secret']
                except Exception as e:
                    print(f"[FileStorage] Error reading secrets: {e}")
            
            # Update secret
            secrets[cp_id] = secret
            
            # Write back
            try:
                with open(secrets_file, 'w') as f:
                    for cid, sec in secrets.items():
                        f.write(json.dumps({"cp_id": cid, "secret": sec}) + "\n")
            except Exception as e:
                print(f"[FileStorage] Error writing secrets: {e}")


    def get_cp_secret(self, cp_id):
        """Get CP secret for authentication"""
        secrets_file = os.path.join(self.data_dir, "cp_secrets.txt")
        
        if not os.path.exists(secrets_file):
            return None
        
        with self.lock:
            try:
                with open(secrets_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            data = json.loads(line)
                            if data['cp_id'] == cp_id:
                                return data['secret']
            except Exception as e:
                print(f"[FileStorage] Error reading secret: {e}")
        
        return None

    def get_cp(self, cp_id):
        """Get a specific charging point"""
        with self.lock:
            cps = self._read_cps()
            return cps.get(cp_id)

    def get_all_cps(self):
        """Get all charging points"""
        with self.lock:
            return list(self._read_cps().values())

    def _read_cps(self):
        """Read all CPs from file"""
        cps = {}
        try:
            with open(self.cp_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        cp_data = json.loads(line)
                        cps[cp_data['cp_id']] = cp_data
        except Exception as e:
            print(f"[FileStorage] Error reading CPs: {e}")
        return cps

    def _write_cps(self, cps):
        """Write all CPs to file"""
        try:
            with open(self.cp_file, 'w') as f:
                for cp_data in cps.values():
                    f.write(json.dumps(cp_data) + "\n")
        except Exception as e:
            print(f"[FileStorage] Error writing CPs: {e}")

    # ========================================================================
    # DRIVERS
    # ========================================================================

    def save_driver(self, driver_id, status="IDLE"):
        """Save or update driver to file"""
        with self.lock:
            drivers = self._read_drivers()
            
            if driver_id not in drivers:
                drivers[driver_id] = {
                    "driver_id": driver_id,
                    "status": status,
                    "total_charges": 0,
                    "total_spent": 0.0,
                    "registered_at": datetime.now().isoformat()
                }
            else:
                drivers[driver_id]["status"] = status
            
            self._write_drivers(drivers)

    def get_driver(self, driver_id):
        """Get a specific driver"""
        with self.lock:
            drivers = self._read_drivers()
            return drivers.get(driver_id)

    def update_driver_stats(self, driver_id, spent_amount):
        """Update driver statistics after charging"""
        with self.lock:
            drivers = self._read_drivers()
            
            if driver_id in drivers:
                drivers[driver_id]["total_charges"] += 1
                drivers[driver_id]["total_spent"] += spent_amount
                self._write_drivers(drivers)

    def _read_drivers(self):
        """Read all drivers from file"""
        drivers = {}
        try:
            with open(self.driver_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        driver_data = json.loads(line)
                        drivers[driver_data['driver_id']] = driver_data
        except Exception as e:
            print(f"[FileStorage] Error reading drivers: {e}")
        return drivers

    def _write_drivers(self, drivers):
        """Write all drivers to file"""
        try:
            with open(self.driver_file, 'w') as f:
                for driver_data in drivers.values():
                    f.write(json.dumps(driver_data) + "\n")
        except Exception as e:
            print(f"[FileStorage] Error writing drivers: {e}")

    # ========================================================================
    # CHARGING HISTORY
    # ========================================================================

    def save_charging_session(self, cp_id, driver_id, kwh_delivered, total_amount, duration_seconds):
        """Save completed charging session to history"""
        with self.lock:
            session = {
                "timestamp": datetime.now().isoformat(),
                "cp_id": cp_id,
                "driver_id": driver_id,
                "kwh_delivered": kwh_delivered,
                "total_amount": total_amount,
                "duration_seconds": duration_seconds
            }
            
            try:
                with open(self.history_file, 'a') as f:
                    f.write(json.dumps(session) + "\n")
            except Exception as e:
                print(f"[FileStorage] Error saving history: {e}")

    def get_recent_history(self, limit=10):
        """Get recent charging sessions"""
        with self.lock:
            sessions = []
            try:
                with open(self.history_file, 'r') as f:
                    lines = f.readlines()
                    # Get last N lines
                    for line in lines[-limit:]:
                        line = line.strip()
                        if line:
                            sessions.append(json.loads(line))
            except Exception as e:
                print(f"[FileStorage] Error reading history: {e}")
            return sessions

    def get_driver_history(self, driver_id, limit=10):
        """Get charging history for specific driver"""
        with self.lock:
            sessions = []
            try:
                with open(self.history_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            session = json.loads(line)
                            if session["driver_id"] == driver_id:
                                sessions.append(session)
                                if len(sessions) >= limit:
                                    break
            except Exception as e:
                print(f"[FileStorage] Error reading driver history: {e}")
            return sessions