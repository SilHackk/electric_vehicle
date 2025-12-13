# ============================================================================
# EVCharging System - EV_Central (Control Center) - UPDATED MESSAGES
# ============================================================================

import socket
import threading
import time
import sys
import json  

import requests
from config import REGISTRY_URL, REGISTRY_POLL_INTERVAL

from datetime import datetime
from config import (
    CENTRAL_HOST, CENTRAL_PORT, CP_STATES, COLORS
)
from shared.protocol import Protocol, MessageTypes
from shared.kafka_client import KafkaClient
from shared.file_storage import FileStorage
from flask import Flask, request, jsonify
from shared.encryption import EncryptionManager

from shared.audit_logger import log_auth, log_charge, log_fault, log_state
class EVCentral:
    def __init__(self, host=CENTRAL_HOST, port=CENTRAL_PORT):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = True

        # File storage instead of database
        self.storage = FileStorage("data")

        # Runtime data (in-memory for active sessions)
        self.charging_points = {}     
        self.drivers = {}             
        self.active_connections = {}  
        self.entity_to_socket = {}    
        self.monitors = {}    
        self.logs = []  
                
        self.encryption = EncryptionManager()
        self.cp_encryption_keys = {}  # cp_id -> simetrinis raktas
        
        # NAUJAS: CP kredencialai (saugomi iš Registry)
        self.cp_credentials = {}  # cp_id -> {"username": ..., "secret": ...}

        # Kafka client
        self.kafka = KafkaClient("EV_Central")

        # Lock for thread safety
        self.lock = threading.Lock()

        print("[EV_Central] Initializing with file storage...")
        
        # Load existing CPs from file on startup
        self._load_stored_cps()

        rest_thread = threading.Thread(target=self.start_rest_api, daemon=True)
        rest_thread.start()
        print("[EV_Central] REST API started on port 5003")

    def _is_authenticated(self, cp_id, secret):
        stored = self.storage.get_cp_secret(cp_id)
        return stored is not None and stored == secret

    def start_rest_api(self):
        """Start Flask REST API for weather alerts"""
        app = Flask(__name__)
        
        @app.route('/api/weather_alert', methods=['POST'])
        def weather_alert():
            data = request.json
            cp_id = data.get('cp_id')
            alert = data.get('alert')
            temp = data.get('temperature')
            
            self.add_log("EV_Weather", f"{cp_id}: {alert} ({temp}°C)")
            
            if alert == "ALERT_COLD":
                # Shutdown CP
                with self.lock:
                    if cp_id in self.charging_points:
                        self.charging_points[cp_id]["state"] = CP_STATES["OUT_OF_ORDER"]
                print(f"[EV_Central] ❌ CP {cp_id} OUT OF SERVICE - Cold alert")
            
            elif alert == "WEATHER_OK":
                # Restore CP
                with self.lock:
                    if cp_id in self.charging_points:
                        self.charging_points[cp_id]["state"] = CP_STATES["ACTIVATED"]
                print(f"[EV_Central] ✅ CP {cp_id} RESTORED - Weather OK")
            
            return jsonify({"status": "received"}), 200
        
        app.run(host='0.0.0.0', port=5003, debug=False, threaded=True)
    def _load_stored_cps(self):
        """Load charging points from file on startup"""
        stored_cps = self.storage.get_all_cps()
        
        if stored_cps:
            print(f"[EV_Central] Loading {len(stored_cps)} charging points from storage...")
            
            for cp_data in stored_cps:
                cp_id = cp_data['cp_id']
                self.charging_points[cp_id] = {
                    "state": CP_STATES["DISCONNECTED"],
                    "location": (cp_data['latitude'], cp_data['longitude']),
                    "price_per_kwh": cp_data['price_per_kwh'],
                    "current_driver": None,
                    "kwh_delivered": 0,
                    "amount_euro": 0,
                    "session_start": None,
                    "charging_complete": False
                }
                print(f"  - {cp_id} at ({cp_data['latitude']}, {cp_data['longitude']})")
        else:
            print("[EV_Central] No stored charging points found")
        
    def add_log(self, source, text):
        """Store a log entry and broadcast to UI monitor if connected."""
        timestamp = datetime.now().isoformat()
        entry = {
            "time": timestamp,
            "source": str(source),
            "text": str(text)
        }
        # keep bounded size
        self.logs.append(entry)
        if len(self.logs) > 500:
            self.logs = self.logs[-500:]

        # also print to console (optional)
        print(f"[LOG] {timestamp} {source}: {text}")

        # broadcast to UI (if connected)
        try:
            # send structured log: event_type LOG, cp_id/source, text (single field)
            if "WEB_UI" in self.monitors:
                msg = Protocol.build_message("LOG", entry["source"], entry["text"], entry["time"])
                self.monitors["WEB_UI"].send(Protocol.encode(msg))
        except Exception as e:
            print(f"[EV_Central] ⚠️ Failed to forward log to WEB_UI: {e}")

    def start(self):
        print(f"[EV_Central] Starting on {self.host}:{self.port}")
    
        # Start registry polling
        registry_thread = threading.Thread(target=self.poll_registry_loop, daemon=True)
        registry_thread.start()
        print("[EV_Central] Registry polling started")
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)

        print(f"[EV_Central] Listening on port {self.port}")
        print(f"[EV_Central] File storage location: data/")

        try:
            while self.running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    client_id = f"{client_address[0]}:{client_address[1]}"
                    self.active_connections[client_id] = client_socket

                    thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, client_id),
                        daemon=True
                    )
                    thread.start()

                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        print(f"[EV_Central] Accept error: {e}")

        except KeyboardInterrupt:
            print("[EV_Central] Shutting down...")
        finally:
            self.shutdown()

    def _handle_client(self, client_socket, client_id):
        """Handle individual client connection"""
        print(f"[EV_Central] Client connected: {client_id}")

        try:
            buffer = b''
            while self.running:
                data = client_socket.recv(4096)
                if not data:
                    break

                buffer += data

                while len(buffer) > 0:
                    message, is_valid = Protocol.decode(buffer)

                    if is_valid:
                        etx_pos = buffer.find(b'\x03')
                        buffer = buffer[etx_pos + 2:]
                        self._process_message(message, client_socket, client_id)
                    else:
                        break

        except Exception as e:
            print(f"[EV_Central] Error handling {client_id}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
            if client_id in self.active_connections:
                del self.active_connections[client_id]
            print(f"[EV_Central] Client disconnected: {client_id}")

    def _process_message(self, message, client_socket, client_id):
        """Process incoming message"""
        fields = Protocol.parse_message(message)
        msg_type = fields[0]


        # CP turi prasidėti "CP"
        if fields[1].startswith("CP"):  
            cp_id = fields[1]

            # Secret key ateina formato SECRET=xxxx
            last_field = fields[-1]
            secret = last_field.replace("SECRET=","") if last_field.startswith("SECRET=") else None

            # Autentifikavimas
            if not self._is_authenticated(cp_id, secret):
                print(f"[EV_Central] ❌ Authentication FAILED for {cp_id}")
                self.add_log("AUTH-FAIL", f"{cp_id} provided wrong secret")
                return  # STOP – toliau neapdorojama


        # print(f"[EV_Central] 📨 Received: {msg_type} from {client_id}")

        if msg_type == MessageTypes.REGISTER:
            self._handle_register(fields, client_socket, client_id)
        elif msg_type == MessageTypes.HEARTBEAT:
            self._handle_heartbeat(fields, client_socket, client_id)
        elif msg_type == MessageTypes.REQUEST_CHARGE:
            self._handle_charge_request(fields, client_socket, client_id)
        elif msg_type == MessageTypes.QUERY_AVAILABLE_CPS:
            self._handle_query_available_cps(fields, client_socket, client_id)
        elif msg_type == MessageTypes.SUPPLY_UPDATE:
            self._handle_supply_update(fields, client_socket)
        elif msg_type == MessageTypes.SUPPLY_END:
            self._handle_supply_end(fields, client_socket)
        elif msg_type == MessageTypes.END_CHARGE:
            self._handle_end_charge(fields, client_socket, client_id)
        elif msg_type == MessageTypes.FAULT:
            self._handle_fault(fields, client_socket, client_id)
        elif msg_type == MessageTypes.RECOVERY:
            self._handle_recovery(fields, client_socket, client_id)
        elif msg_type == "LOG" or msg_type == MessageTypes.LOG if hasattr(MessageTypes, 'LOG') else False:
            # Expected fields: ["LOG", source, text, timestamp?]
            try:
                source = fields[1] if len(fields) > 1 else "UNKNOWN"
                text = fields[2] if len(fields) > 2 else ""
                timestamp = fields[3] if len(fields) > 3 else None
                self.add_log(source, text)
            except Exception as e:
                print(f"[EV_Central] Failed to process LOG message: {e}")

    def _broadcast_to_ui(self, event_type, cp_id, driver_id=None, payload=None):
        """
        Broadcast a message to WEB_UI monitor socket.
        payload: optional string or JSON-able data that will be sent as extra fields.
        """
        if "WEB_UI" in self.monitors:
            try:
                # If payload is present, include it as extra fields (convert to string)
                if payload is not None:
                    # build_message can accept multiple args — we pass payload as string
                    msg = Protocol.build_message(event_type, cp_id, driver_id or "", json.dumps(payload))
                else:
                    msg = Protocol.build_message(event_type, cp_id, driver_id or "")
                self.monitors["WEB_UI"].send(Protocol.encode(msg))
            except Exception as e:
                print(f"[EV_Central] ⚠️ Failed to broadcast {event_type} to UI: {e}")

    def _handle_register(self, fields, client_socket, client_id):
        """Handle CP or Driver registration"""
        if len(fields) < 3:
            return

        entity_type = fields[1]
        entity_id = fields[2]

        if entity_type == "CP":
            # NAUJAS: Tikrinti kredencialus per Registry API
            username = fields[6] if len(fields) > 6 else None
            password = fields[7] if len(fields) > 7 else None
            
            if not self._verify_cp_credentials(entity_id, username, password):
                print(f"[EV_Central] ❌ Authentication FAILED for {entity_id}")
                self.add_log("AUTH-FAIL", f"{entity_id} wrong credentials")
                log_auth(client_id, entity_id, success=False, reason="INVALID_CREDENTIALS")
                # Siųsti DENY atsakymą
                deny_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.DENY, entity_id, "AUTH_FAILED")
                )
                client_socket.send(deny_msg)
                return  # STOP - nepriimti registracijos
            
            log_auth(client_id, entity_id, success=True)
            
            # NAUJAS: Generuoti simetrinį šifravimo raktą
            symmetric_key = self.encryption.generate_key(password)
            self.cp_encryption_keys[entity_id] = symmetric_key
            
            print(f"[EV_Central] 🔐 Generated encryption key for {entity_id}")
        
            lat = fields[3] if len(fields) > 3 else "0"
            lon = fields[4] if len(fields) > 4 else "0"
            price = float(fields[5]) if len(fields) > 5 else 0.30

            with self.lock:
                self.charging_points[entity_id] = {
                    "state": CP_STATES["ACTIVATED"],
                    "location": (lat, lon),
                    "price_per_kwh": price,
                    "connected_at": datetime.now().isoformat(),
                    "current_driver": None,
                    "kwh_delivered": 0,
                    "amount_euro": 0,
                    "session_start": None,
                    "charging_complete": False
                }
                
                self.entity_to_socket[entity_id] = client_socket

            self.storage.save_cp(entity_id, lat, lon, price, CP_STATES["ACTIVATED"])
            
            secret = fields[6] if len(fields) > 6 else None
            self.storage.save_cp_secret(entity_id, secret)
            
            print(f"[EV_Central] ✅ CP Registered: {entity_id} at ({lat}, {lon}) - Saved to file")
            self.add_log("EV_Central", f"CP Registered: {entity_id} at ({lat}, {lon})")
            response = Protocol.encode(
                Protocol.build_message(
                    MessageTypes.ACKNOWLEDGE, 
                    entity_id, 
                    "OK",
                    symmetric_key.decode()  # Grąžinti raktą CP
                )
            )
            client_socket.send(response)
            
            self.kafka.publish_event("system_events", "CP_REGISTERED", {
                "cp_id": entity_id,
                "location": (lat, lon),
                "price": price
            })
            
            response = Protocol.encode(
                Protocol.build_message(MessageTypes.ACKNOWLEDGE, entity_id, "OK")
            )
            client_socket.send(response)

        elif entity_type == "DRIVER":
            with self.lock:
                self.drivers[entity_id] = {
                    "status": "IDLE",
                    "current_cp": None,
                    "charge_amount": 0
                }
                
                self.entity_to_socket[entity_id] = client_socket
                print(f"[EV_Central] 🔑 Mapped driver {entity_id} to socket")

            self.storage.save_driver(entity_id, "IDLE")

            print(f"[EV_Central] ✅ Driver Registered: {entity_id} - Saved to file")
            self.add_log(f"[EV_Central] ✅ Driver Registered: {entity_id} - Saved to file")

            response = Protocol.encode(
                Protocol.build_message(MessageTypes.ACKNOWLEDGE, entity_id, "OK")
            )
            client_socket.send(response)

        elif entity_type == "MONITOR":
            monitor_name = entity_id   # e.g. WEB_UI

            # SPECIAL case: Web dashboard
            if monitor_name == "WEB_UI":
                with self.lock:
                    self.monitors["WEB_UI"] = client_socket

                print(f"[EV_Central] 🖥️  Web UI connected.")

                # Send ACK
                response = Protocol.encode(
                    Protocol.build_message(MessageTypes.ACKNOWLEDGE, "WEB_UI", "OK")
                )
                client_socket.send(response)

                # Immediately send full system snapshot
                self._send_full_state_to_ui(client_socket)

                return

            # OLD CP monitor logic (keep for CP monitoring)
            monitor_cp_id = fields[3] if len(fields) > 3 else None
            if monitor_cp_id:
                with self.lock:
                    self.monitors[monitor_cp_id] = client_socket

                print(f"[EV_Central] 📡 CP monitor registered for {monitor_cp_id}")

                response = Protocol.encode(
                    Protocol.build_message(MessageTypes.ACKNOWLEDGE, monitor_cp_id, "MONITOR_OK")
                )
                client_socket.send(response)

    def _verify_cp_credentials(self, cp_id, username, password):
        """Verify CP credentials via Registry API"""
        try:
            response = requests.post(
                f"{REGISTRY_URL}/verify",
                json={
                    "cp_id": cp_id,
                    "username": username,
                    "password": password
                },
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("valid"):
                    # Saugoti kredencialus atminiuje
                    self.cp_credentials[cp_id] = {
                        "username": username,
                        "secret": password  # Bus naudojamas šifravimui
                    }
                    return True
            
            return False
        
        except Exception as e:
            print(f"[EV_Central] Registry verification error: {e}")
            return False

    # 5. NAUJAS METODAS: Pranešimų šifravimas/dešifravimas
    def _encrypt_message(self, cp_id, message):
        """Encrypt message for specific CP using their symmetric key"""
        if cp_id not in self.cp_encryption_keys:
            return message  # Jei nėra rakto, siųsti nešifruotą
        
        key = self.cp_encryption_keys[cp_id]
        return self.encryption.encrypt(message, key)

    def _decrypt_message(self, cp_id, encrypted_message):
        """Decrypt message from specific CP"""
        if cp_id not in self.cp_encryption_keys:
            return encrypted_message  # Jei nėra rakto, laikyti nešifruotu
        
        key = self.cp_encryption_keys[cp_id]
        try:
            return self.encryption.decrypt(encrypted_message, key)
        except Exception as e:
            print(f"[EV_Central] Decryption error for {cp_id}: {e}")
            return None

    # 6. NAUJAS METODAS: Rakto atšaukimas (Key Revocation)
    def revoke_cp_key(self, cp_id):
        """Revoke encryption key for CP (security incident)"""
        with self.lock:
            if cp_id in self.cp_encryption_keys:
                del self.cp_encryption_keys[cp_id]
                print(f"[EV_Central] 🔐 Revoked encryption key for {cp_id}")
                self.add_log("SECURITY", f"Key revoked for {cp_id}")
                
                # Atjungti CP
                if cp_id in self.entity_to_socket:
                    try:
                        self.entity_to_socket[cp_id].close()
                        del self.entity_to_socket[cp_id]
                    except:
                        pass
                
                # CP turės iš naujo autentifikuotis

    def _handle_charge_request(self, fields, client_socket, client_id):
        """Handle driver charging request"""
        if len(fields) < 4:
            print(f"[EV_Central] ⚠️  Invalid REQUEST_CHARGE: {fields}")
            self.add_log("EV_Central", f"Invalid REQUEST_CHARGE from {client_id}: {fields}")
            return

        driver_id = fields[1]
        cp_id = fields[2]
        kwh_needed = float(fields[3])

        print(f"[EV_Central] 🔌 {driver_id} requesting {kwh_needed} kWh at {cp_id}")

        with self.lock:
            if cp_id not in self.charging_points:
                response = Protocol.encode(
                    Protocol.build_message(MessageTypes.DENY, driver_id, cp_id, "CP_NOT_FOUND")
                )
                client_socket.send(response)
                print(f"[EV_Central] ❌ Denied: CP not found")
                return

            cp = self.charging_points[cp_id]

            if cp["state"] != CP_STATES["ACTIVATED"] or cp["current_driver"] is not None:
                reason = f"CP_STATE_{cp['state']}" if cp["state"] != CP_STATES["ACTIVATED"] else "CP_ALREADY_IN_USE"
                response = Protocol.encode(
                    Protocol.build_message(MessageTypes.DENY, driver_id, cp_id, reason)
                )
                client_socket.send(response)
                print(f"[EV_Central] ❌ Denied: {reason}")
                return

            # Authorization granted
            cp["state"] = CP_STATES["SUPPLYING"]
            cp["current_driver"] = driver_id
            cp["session_start"] = time.time()
            cp["kwh_delivered"] = 0
            cp["amount_euro"] = 0
            cp["kwh_needed"] = kwh_needed
            cp["charging_complete"] = False
            
            self.drivers[driver_id]["status"] = "CHARGING"
            self.drivers[driver_id]["current_cp"] = cp_id

        log_charge(client_id, cp_id, driver_id, "CHARGE_START")

        print(f"[EV_Central] ✅ Charge authorized: Driver {driver_id} → CP {cp_id}")
        self.add_log("EV_Central", f"Charge authorized Driver {driver_id} -> {cp_id}")

        # Send AUTHORIZE to driver
        response = Protocol.encode(
            Protocol.build_message(MessageTypes.AUTHORIZE, driver_id, cp_id, kwh_needed, cp["price_per_kwh"])
        )
        try:
            client_socket.send(response)
            print(f"[EV_Central] 📤 Sent AUTHORIZE to driver {driver_id}")
        except Exception as e:
            print(f"[EV_Central] ⚠️  Failed to send AUTHORIZE to driver: {e}")

        # Send AUTHORIZE to CP Engine
        if cp_id in self.entity_to_socket:
            cp_auth_msg = Protocol.encode(
                Protocol.build_message(MessageTypes.AUTHORIZE, driver_id, cp_id, kwh_needed)
            )
            try:
                self.entity_to_socket[cp_id].send(cp_auth_msg)
                print(f"[EV_Central] 📤 Sent AUTHORIZE to CP {cp_id}")
            except Exception as e:
                print(f"[EV_Central] ⚠️  Failed to send AUTHORIZE to CP: {e}")
        
        #Notify WEB
        if "WEB_UI" in self.monitors:
            try:
                ui_msg = Protocol.encode(
                    Protocol.build_message("DRIVER_START", cp_id, driver_id)
                )
                self.monitors["WEB_UI"].send(ui_msg)
                print(f"[EV_Central] 📤 Notified UI: DRIVER_START")
            except Exception as e:
                print(f"[EV_Central] UI notify failed: {e}")

        # Notify monitor
        if cp_id in self.monitors:
            try:
                monitor_notify = Protocol.encode(
                    Protocol.build_message("DRIVER_START", cp_id, driver_id)
                )
                self.monitors[cp_id].send(monitor_notify)
                print(f"[EV_Central] 📤 Notified monitor: {driver_id} started at {cp_id}")
            except Exception as e:
                print(f"[EV_Central] Failed to notify monitor: {e}")

        self.kafka.publish_event("charging_logs", "CHARGE_AUTHORIZED", {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "kwh_needed": kwh_needed
        })

    def _handle_supply_update(self, fields, client_socket):
        """Handle real-time supply updates from CP"""
        if len(fields) < 4:
            print(f"[EV_Central] ⚠️  Invalid SUPPLY_UPDATE: {fields}")
            self.add_log("[EV_Central] ⚠️  Invalid SUPPLY_UPDATE: {fields}")
            return

        cp_id = fields[1]
        kwh_increment = float(fields[2])
        amount = float(fields[3])

        driver_id = None
        with self.lock:
            if cp_id in self.charging_points:
                cp = self.charging_points[cp_id]
                cp["kwh_delivered"] += kwh_increment
                cp["amount_euro"] = amount
                driver_id = cp["current_driver"]
                
                # Check if 100% reached
                if cp["kwh_delivered"] >= cp.get("kwh_needed", 10):
                    if not cp.get("charging_complete", False):
                        cp["charging_complete"] = True
                        print(f"\n[EV_Central] 🔋 {driver_id} finished charging at {cp_id}, waiting for driver to unplug\n")
                        
                        # Notify monitor of completion
                        if cp_id in self.monitors:
                            try:
                                complete_msg = Protocol.encode(
                                    Protocol.build_message("CHARGING_COMPLETE", cp_id, driver_id)
                                )
                                self.monitors[cp_id].send(complete_msg)
                            except Exception as e:
                                print(f"[EV_Central] Failed to notify monitor of completion: {e}")
                
                print(f"[EV_Central] 📊 CP {cp_id}: {cp['kwh_delivered']:.3f} kWh, {amount:.2f}€")
                self.add_log(cp_id, f"{cp['kwh_delivered']:.3f} kWh, {amount:.2f}€")

        # Forward update to driver
        if driver_id and driver_id in self.entity_to_socket:
            try:
                update_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.SUPPLY_UPDATE, cp_id, kwh_increment, amount)
                )
                self.entity_to_socket[driver_id].send(update_msg)
            except Exception as e:
                print(f"[EV_Central] Failed to forward update to {driver_id}: {e}")

        if "WEB_UI" in self.monitors:
            try:
                ui_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.SUPPLY_UPDATE, cp_id, kwh_increment, amount)
                )
                self.monitors["WEB_UI"].send(ui_msg)
            except:
                pass

    def _handle_supply_end(self, fields, client_socket):
        """Handle supply completion from CP"""
        if len(fields) < 5:
            return

        cp_id = fields[1]
        driver_id = fields[2]
        total_kwh = float(fields[3])
        total_amount = float(fields[4])

        log_charge(client_id, cp_id, driver_id, "CHARGE_END", kwh=total_kwh, amount=total_amount)

        duration_seconds = 0
        
        with self.lock:
            if cp_id in self.charging_points:
                cp = self.charging_points[cp_id]
                
                if cp["session_start"]:
                    duration_seconds = int(time.time() - cp["session_start"])
                
                cp["state"] = CP_STATES["ACTIVATED"]
                cp["current_driver"] = None
                cp["kwh_delivered"] = 0
                cp["amount_euro"] = 0
                cp["session_start"] = None
                cp["charging_complete"] = False

            if driver_id in self.drivers:
                self.drivers[driver_id]["status"] = "IDLE"
                self.drivers[driver_id]["current_cp"] = None

        self.storage.save_charging_session(cp_id, driver_id, total_kwh, total_amount, duration_seconds)
        self.storage.update_driver_stats(driver_id, total_amount)

        print(f"\n[EV_Central] ✅ {driver_id} unplugged from {cp_id}")
        self.add_log("EV_Central", f"{driver_id} unplugged from {cp_id} - {total_kwh} kWh")
        print(f"[EV_Central]    → {total_kwh:.2f} kWh, {total_amount:.2f}€, {duration_seconds}s")
        print(f"[EV_Central]    → CP {cp_id} now ACTIVATED\n")

        if driver_id in self.entity_to_socket:
            try:
                ticket_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.TICKET, cp_id, total_kwh, total_amount)
                )
                self.entity_to_socket[driver_id].send(ticket_msg)
                print(f"[EV_Central] 📤 Sent TICKET to driver {driver_id}")
            except Exception as e:
                print(f"[EV_Central] Failed to send ticket to {driver_id}: {e}")

        if cp_id in self.monitors:
            try:
                monitor_notify = Protocol.encode(
                    Protocol.build_message("DRIVER_STOP", cp_id, driver_id)
                )
                self.monitors[cp_id].send(monitor_notify)
                print(f"[EV_Central] 📤 Notified monitor: {driver_id} unplugged from {cp_id}")
            except Exception as e:
                print(f"[EV_Central] Failed to notify monitor: {e}")

        if "WEB_UI" in self.monitors:
            try:
                # Send supply end
                ui_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.SUPPLY_END, cp_id, driver_id, total_kwh, total_amount)
                )
                self.monitors["WEB_UI"].send(ui_msg)
                # Send driver stop
                ui_msg2 = Protocol.encode(
                    Protocol.build_message("DRIVER_STOP", cp_id, driver_id)
                )
                self.monitors["WEB_UI"].send(ui_msg2)
                print(f"[EV_Central] 📤 Notified UI: SUPPLY_END + DRIVER_STOP")
            except Exception as e:
                print(f"[EV_Central] UI notify failed: {e}")

        self.kafka.publish_event("charging_logs", "CHARGE_COMPLETED", {
            "cp_id": cp_id,
            "driver_id": driver_id,
            "total_kwh": total_kwh,
            "total_amount": total_amount
        })

    def _handle_end_charge(self, fields, client_socket, client_id):
        """Handle manual end charge from driver"""
        if len(fields) < 3:
            return

        driver_id = fields[1]
        cp_id = fields[2]

        print(f"[EV_Central] 🔌 Driver {driver_id} manually ending charge at {cp_id}")

        total_kwh = 0
        total_amount = 0
        duration_seconds = 0

        with self.lock:
            if cp_id not in self.charging_points:
                print(f"[EV_Central] ❌ CP {cp_id} not found")
                return
            
            cp = self.charging_points[cp_id]
            
            if cp["current_driver"] != driver_id:
                print(f"[EV_Central] ❌ Driver {driver_id} not charging at {cp_id}")
                return

            duration_seconds = int(time.time() - cp["session_start"]) if cp["session_start"] else 0
            total_seconds = 14.0
            kwh_needed = cp.get("kwh_needed", 10)
            total_kwh = min(kwh_needed, (duration_seconds / total_seconds) * kwh_needed)
            total_amount = round(total_kwh * cp["price_per_kwh"], 2)

            cp["state"] = CP_STATES["ACTIVATED"]
            cp["current_driver"] = None
            cp["kwh_delivered"] = 0
            cp["amount_euro"] = 0
            cp["session_start"] = None
            cp["charging_complete"] = False

            if driver_id in self.drivers:
                self.drivers[driver_id]["status"] = "IDLE"
                self.drivers[driver_id]["current_cp"] = None

        self.storage.save_charging_session(cp_id, driver_id, total_kwh, total_amount, duration_seconds)
        self.storage.update_driver_stats(driver_id, total_amount)

        print(f"\n[EV_Central] ✅ {driver_id} unplugged from {cp_id}")
        print(f"[EV_Central]    → {total_kwh:.2f} kWh, {total_amount:.2f}€, {duration_seconds}s")
        print(f"[EV_Central]    → CP {cp_id} now ACTIVATED\n")

        if cp_id in self.entity_to_socket:
            try:
                end_supply_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.END_SUPPLY, cp_id)
                )
                self.entity_to_socket[cp_id].send(end_supply_msg)
                print(f"[EV_Central] 📤 Sent END_SUPPLY to CP {cp_id}")
            except Exception as e:
                print(f"[EV_Central] ⚠️  Failed to send END_SUPPLY to {cp_id}: {e}")

        if driver_id in self.entity_to_socket:
            try:
                ticket_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.TICKET, cp_id, total_kwh, total_amount)
                )
                self.entity_to_socket[driver_id].send(ticket_msg)
                print(f"[EV_Central] 📤 Sent ticket to driver {driver_id}")
            except Exception as e:
                print(f"[EV_Central] ⚠️  Failed to send ticket to {driver_id}: {e}")

        if cp_id in self.monitors:
            try:
                monitor_notify = Protocol.encode(
                    Protocol.build_message("DRIVER_STOP", cp_id, driver_id)
                )
                self.monitors[cp_id].send(monitor_notify)
                print(f"[EV_Central] 📤 Notified monitor: {driver_id} unplugged from {cp_id}")
            except Exception as e:
                print(f"[EV_Central] Failed to notify monitor: {e}")

        if "WEB_UI" in self.monitors:
            try:
                ui_msg = Protocol.encode(
                    Protocol.build_message("DRIVER_STOP", cp_id, driver_id)
                )
                self.monitors["WEB_UI"].send(ui_msg)
                print(f"[EV_Central] 📤 Notified UI: DRIVER_STOP")
            except Exception as e:
                print(f"[EV_Central] UI notify failed: {e}")

        self.kafka.publish_event("charging_logs", "CHARGE_MANUALLY_ENDED", {
            "cp_id": cp_id,
            "driver_id": driver_id,
            "total_kwh": total_kwh,
            "total_amount": total_amount,
            "duration_seconds": duration_seconds
        })

    def _handle_heartbeat(self, fields, client_socket, client_id):
        """Handle heartbeat from CP"""
        if len(fields) < 3:
            return

        cp_id = fields[1]
        state = fields[2]

        with self.lock:
            if cp_id in self.charging_points:
                if self.charging_points[cp_id]["state"] != CP_STATES["SUPPLYING"]:
                    self.charging_points[cp_id]["state"] = state

    def _handle_fault(self, fields, client_socket, client_id):
        """Handle fault notification from CP monitor"""
        if len(fields) < 2:
            return

        cp_id = fields[1]

        driver_id = None
        was_supplying = False

        with self.lock:
            if cp_id in self.charging_points:
                cp = self.charging_points[cp_id]
                was_supplying = (cp["state"] == CP_STATES["SUPPLYING"])
                driver_id = cp["current_driver"]
                
                cp["state"] = CP_STATES["OUT_OF_ORDER"]
                
                if was_supplying and driver_id:
                    total_kwh = cp["kwh_delivered"]
                    total_amount = cp["amount_euro"]
                    duration_seconds = int(time.time() - cp["session_start"]) if cp["session_start"] else 0
                    
                    self.storage.save_charging_session(cp_id, driver_id, total_kwh, total_amount, duration_seconds)
                    self.storage.update_driver_stats(driver_id, total_amount)
                    
                    cp["current_driver"] = None
                    cp["kwh_delivered"] = 0
                    cp["amount_euro"] = 0
                    cp["session_start"] = None
                    cp["charging_complete"] = False
                    
                    if driver_id in self.drivers:
                        self.drivers[driver_id]["status"] = "IDLE"
                        self.drivers[driver_id]["current_cp"] = None

        log_fault(client_id, cp_id, "CP_FAULT", "Health check failed")
    
        print(f"[EV_Central] ⚠️ FAULT reported for CP {cp_id}")
        self.add_log("EV_Central", f"FAULT {cp_id}")
        
        if was_supplying and driver_id:
            print(f"[EV_Central] ⚠️  Charging session interrupted for driver {driver_id}")
            
            if driver_id in self.entity_to_socket:
                try:
                    fault_msg = Protocol.encode(
                        Protocol.build_message(MessageTypes.DENY, driver_id, cp_id, "CP_FAULT_EMERGENCY_STOP")
                    )
                    self.entity_to_socket[driver_id].send(fault_msg)
                except Exception as e:
                    print(f"[EV_Central] Failed to notify driver of fault: {e}")
        
        if "WEB_UI" in self.monitors:
            try:
                ui_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.FAULT, cp_id)
                )
                self.monitors["WEB_UI"].send(ui_msg)
            except:
                pass   

            self.kafka.publish_event("system_events", "CP_FAULT", {"cp_id": cp_id})

    def _handle_recovery(self, fields, client_socket, client_id):
        """Handle recovery notification from CP monitor"""
        if len(fields) < 2:
            return

        cp_id = fields[1]

        with self.lock:
            if cp_id in self.charging_points:
                self.charging_points[cp_id]["state"] = CP_STATES["ACTIVATED"]
        
        log_fault(client_id, cp_id, "CP_RECOVERY", "System restored")

        print(f"[EV_Central] ✅ CP {cp_id} recovered")
        
        if "WEB_UI" in self.monitors:
            try:
                ui_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.RECOVERY, cp_id)
                )
                self.monitors["WEB_UI"].send(ui_msg)
            except:
                pass
            
        self.add_log("EV_Central", f"RECOVERY {cp_id}")
        self.kafka.publish_event("system_events", "CP_RECOVERED", {"cp_id": cp_id})

    def _handle_query_available_cps(self, fields, client_socket, client_id):
        """Handle driver query for available CPs"""
        if len(fields) < 2:
            return

        driver_id = fields[1]

        available_cps = []
        with self.lock:
            for cp_id, cp_data in self.charging_points.items():
                if cp_data["state"] == CP_STATES["ACTIVATED"] and cp_data["current_driver"] is None:
                    available_cps.append({
                        "cp_id": cp_id,
                        "location": cp_data["location"],
                        "price_per_kwh": cp_data["price_per_kwh"]
                    })

        response_fields = [MessageTypes.AVAILABLE_CPS]
        for cp in available_cps:
            response_fields.extend([
                cp["cp_id"],
                cp["location"][0],
                cp["location"][1],
                cp["price_per_kwh"]
            ])

        response = Protocol.encode(Protocol.build_message(*response_fields))
        client_socket.send(response)

        print(f"[EV_Central] Sent {len(available_cps)} available CPs to {driver_id}")

    def display_dashboard(self):
        """Display monitoring dashboard periodically"""
        while self.running:
            time.sleep(2)

            with self.lock:
                print("\n" + "="*80)
                print("EV_CENTRAL MONITORING DASHBOARD")
                print("="*80)

                print("\n[CHARGING POINTS]")
                if not self.charging_points:
                    print("  No charging points registered")
                else:
                    for cp_id, cp_data in self.charging_points.items():
                        color = COLORS.get(cp_data["state"], "?")
                        print(f"  [{color}] {cp_id}: {cp_data['state']}")
                        if cp_data["state"] == CP_STATES["SUPPLYING"]:
                            print(f"      Driver: {cp_data['current_driver']}")
                            print(f"      kWh: {cp_data['kwh_delivered']:.2f} kWh")
                            print(f"      Amount: {cp_data['amount_euro']:.2f}€")

                print("\n[DRIVERS]")
                if not self.drivers:
                    print("  No drivers registered")
                else:
                    for driver_id, driver_data in self.drivers.items():
                        print(f"  {driver_id}: {driver_data['status']}")
                        if driver_data['status'] == "CHARGING":
                            print(f"      At: {driver_data['current_cp']}")

                print("="*80 + "\n")

    def _send_full_state_to_ui(self, ui_socket):
        """Send full system snapshot to the web dashboard."""
        with self.lock:
            cps = []
            for cp_id, cp in self.charging_points.items():
                cps.append([
                    cp_id,
                    cp["state"],
                    cp["location"][0],
                    cp["location"][1],
                    cp["price_per_kwh"],
                    cp["current_driver"] or "",
                    cp["kwh_delivered"],
                    cp["amount_euro"]
                ])

            drivers = []
            for d_id, d in self.drivers.items():
                drivers.append([
                    d_id,
                    d["status"],
                    d["current_cp"] or ""
                ])

            history = self.storage.get_recent_history(20)

        msg = Protocol.build_message(
            "FULL_STATE",
            str(cps),
            str(drivers),
            str(history)
        )
        try:
            ui_socket.send(Protocol.encode(msg))
            print("[EV_Central] 📤 Sent full system state to UI")
        except:
            print("[EV_Central] ⚠️ Failed to send system state to UI")

    def handle_admin_commands(self):
        """Handle admin commands"""
        while self.running:
            try:
                cmd = input("\n[ADMIN] Command (stop/resume <CP_ID>, list, history, quit): ").strip()

                if cmd == "help":
                    print("Commands:")
                    print("  stop <CP_ID>    - Stop a charging point")
                    print("  resume <CP_ID>  - Resume a charging point")
                    print("  list            - List all charging points")
                    print("  history         - Show recent charging history")
                    print("  quit            - Shutdown system")
                    continue

                if cmd == "list":
                    with self.lock:
                        print("\n=== CHARGING POINTS ===")
                        for cp_id, cp_data in self.charging_points.items():
                            print(f"  {cp_id}: {cp_data['state']}")
                            if cp_data["current_driver"]:
                                print(f"    └─ Charging: {cp_data['current_driver']}")
                    continue

                if cmd == "history":
                    history = self.storage.get_recent_history(10)
                    print("\n=== RECENT CHARGING HISTORY ===")
                    if not history:
                        print("  No history yet")
                    else:
                        for session in history:
                            print(f"  {session['timestamp'][:19]}: {session['driver_id']} @ {session['cp_id']}")
                            print(f"     → {session['kwh_delivered']} kWh, {session['total_amount']}€, {session['duration_seconds']}s")
                    continue

                if cmd == "quit":
                    print("Shutting down...")
                    self.running = False
                    break

                if cmd.startswith("stop"):
                    parts = cmd.split()
                    if len(parts) < 2:
                        print("❌ Usage: stop <CP_ID>")
                        continue
                    
                    cp_id = parts[1]
                    
                    driver_id = None
                    was_charging = False
                    
                    with self.lock:
                        if cp_id not in self.charging_points:
                            print(f"❌ CP {cp_id} not found")
                            continue
                        
                        cp = self.charging_points[cp_id]
                        was_charging = (cp["state"] == CP_STATES["SUPPLYING"])
                        driver_id = cp["current_driver"]
                        
                        if was_charging and driver_id:
                            total_kwh = cp["kwh_delivered"]
                            total_amount = total_kwh * cp["price_per_kwh"]
                            duration_seconds = int(time.time() - cp["session_start"]) if cp["session_start"] else 0
                            
                            self.storage.save_charging_session(cp_id, driver_id, total_kwh, total_amount, duration_seconds)
                            self.storage.update_driver_stats(driver_id, total_amount)
                            
                            print(f"⚠️  Charging session at {cp_id} interrupted ({total_kwh:.2f} kWh, {total_amount:.2f}€)")
                            
                            cp["current_driver"] = None
                            cp["kwh_delivered"] = 0
                            cp["amount_euro"] = 0
                            cp["session_start"] = None
                            cp["charging_complete"] = False
                            
                            if driver_id in self.drivers:
                                self.drivers[driver_id]["status"] = "IDLE"
                                self.drivers[driver_id]["current_cp"] = None
                        
                        cp["state"] = CP_STATES["STOPPED"]
                    
                    if cp_id in self.entity_to_socket:
                        try:
                            stop_msg = Protocol.encode(
                                Protocol.build_message(MessageTypes.STOP_COMMAND, cp_id)
                            )
                            self.entity_to_socket[cp_id].send(stop_msg)
                            print(f"✅ CP {cp_id} stopped")
                            
                            if was_charging and driver_id and driver_id in self.entity_to_socket:
                                ticket_msg = Protocol.encode(
                                    Protocol.build_message(MessageTypes.TICKET, cp_id, 
                                                          total_kwh, total_amount)
                                )
                                self.entity_to_socket[driver_id].send(ticket_msg)
                                print(f"📤 Ticket sent to driver {driver_id}")
                                
                        except Exception as e:
                            print(f"❌ Failed to stop CP: {e}")
                    else:
                        print(f"❌ CP {cp_id} not connected")
                    continue

                if cmd.startswith("resume"):
                    parts = cmd.split()
                    if len(parts) < 2:
                        print("❌ Usage: resume <CP_ID>")
                        continue
                    
                    cp_id = parts[1]
                    if cp_id in self.entity_to_socket:
                        with self.lock:
                            if cp_id in self.charging_points:
                                self.charging_points[cp_id]["state"] = CP_STATES["ACTIVATED"]
                        
                        try:
                            resume_msg = Protocol.encode(
                                Protocol.build_message(MessageTypes.RESUME_COMMAND, cp_id)
                            )
                            self.entity_to_socket[cp_id].send(resume_msg)
                            print(f"✅ CP {cp_id} resumed")
                        except Exception as e:
                            print(f"❌ Failed to resume CP: {e}")
                    else:
                        print(f"❌ CP {cp_id} not found or not connected")
                    continue

                print("❌ Unknown command. Type 'help' for commands.")

            except Exception as e:
                print(f"❌ Command error: {e}")

                if cmd.startswith("revoke"):
                    parts = cmd.split()
                    if len(parts) < 2:
                        print("❌ Usage: revoke <CP_ID>")
                        continue
                    
                    cp_id = parts[1]
                    self.revoke_cp_key(cp_id)
                    print(f"✅ Key revoked for {cp_id}. CP must re-authenticate.")
                    continue
    def shutdown(self):
        """Shutdown the central system"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.kafka.close()
        print("[EV_Central] Shutdown complete")

    def poll_registry_loop(self):
        """Poll registry but don't auto-add disconnected CPs to dashboard"""
        while self.running:
            try:
                time.sleep(REGISTRY_POLL_INTERVAL)
                response = requests.get(f"{REGISTRY_URL}/list", timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    registry_cps = {cp['cp_id']: cp for cp in data.get('charging_points', [])}
                    
                    with self.lock:
                        for cp_id, cp_data in registry_cps.items():
                            # ONLY add if NOT already present
                            # Don't create DISCONNECTED entries automatically
                            if cp_id not in self.charging_points:
                                # Just log it, don't add to self.charging_points yet
                                print(f"[EV_Central] 📋 CP {cp_id} registered in registry (not yet connected)")
                                # It will be added when it actually connects via REGISTER message
            except Exception as e:
                print(f"[EV_Central] Registry poll error: {e}")

if __name__ == "__main__":
    central = EVCentral()

    # Start server in separate thread
    server_thread = threading.Thread(target=central.start, daemon=True)
    server_thread.start()

    # Start dashboard in separate thread
    dashboard_thread = threading.Thread(target=central.display_dashboard, daemon=True)
    dashboard_thread.start()

    # Start admin console
    try:
        central.handle_admin_commands()
    except KeyboardInterrupt:
        print("\n[EV_Central] Shutting down...")
        central.shutdown()