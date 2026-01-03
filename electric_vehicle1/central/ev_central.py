# ============================================================================
# EVCharging System - EV_Central (Control Center) - OPTION A (Full parity)
# FIXES:
# - No random CP disconnects due to parse/decrypt errors
# - Robust socket lifecycle (cleans mappings only on real disconnect)
# - CP encryption supported: decrypt incoming CP messages using socket->cp_id map
# - Encrypt outgoing CP commands (AUTHORIZE, END_SUPPLY, STOP, RESUME)
# - FULL_STATE always uses str(list) so UI ast.literal_eval never crashes
# - REST API includes /api/register_cp and /api/weather_alert on port 5003
# - Registry polling does NOT create fake DISCONNECTED entries
# - UI broadcasts kept compatible (FULL_STATE, SUPPLY_UPDATE, SUPPLY_END, DRIVER_START/STOP, LOG)
# ============================================================================

import socket
import threading
import time
import json
import os
import logging
from datetime import datetime

import requests
from flask import Flask, request, jsonify

from config import CENTRAL_HOST, CENTRAL_PORT, CP_STATES, COLORS
from config import REGISTRY_URL, REGISTRY_POLL_INTERVAL
from shared.protocol import Protocol, MessageTypes
from shared.kafka_client import KafkaClient
from shared.file_storage import FileStorage
from shared.encryption import EncryptionManager
from shared.audit_logger import log_auth, log_charge, log_fault, log_state

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


class EVCentral:
    def __init__(self, host=CENTRAL_HOST, port=CENTRAL_PORT):
        self.host = host
        self.port = port

        self.server_socket = None
        self.running = True

        # Storage
        self.storage = FileStorage("data")

        # Runtime state
        self.charging_points = {}      # cp_id -> dict
        self.drivers = {}              # driver_id -> dict
        self.logs = []                 # list of dict entries

        # Socket maps
        self.active_connections = {}   # client_id -> socket
        self.entity_to_socket = {}     # entity_id -> socket
        self.socket_to_entity = {}     # socket -> entity_id
        self.socket_to_type = {}       # socket -> "CP"/"DRIVER"/"MONITOR"

        # Monitors
        self.monitors = {}             # "WEB_UI" -> socket, optionally per-CP monitors

        # Crypto
        self.encryption = EncryptionManager()
        self.cp_encryption_keys = {}   # cp_id -> symmetric key bytes
        self.cp_credentials = {}       # cp_id -> {"username":..., "secret":...}

        # Kafka
        self.kafka = KafkaClient("EV_Central")

        # Lock
        self.lock = threading.Lock()

        print("[EV_Central] Initializing with file storage...")
        self._load_stored_cps()

        # Start REST API thread
        rest_thread = threading.Thread(target=self.start_rest_api, daemon=True)
        rest_thread.start()
        print("[EV_Central] REST API started on port 5003")

    # -------------------------------------------------------------------------
    # REST API (CP register + weather alert)
    # -------------------------------------------------------------------------
    def start_rest_api(self):
        app = Flask(__name__)

        @app.route("/api/register_cp", methods=["POST"])
        def register_cp():
            data = request.json or {}
            cp_id = data.get("cp_id")
            city = data.get("city")
            price = float(data.get("price_per_kwh", 0.30))

            if not cp_id or not city:
                return jsonify({"error": "cp_id and city are required"}), 400

            lat, lon = self.get_coordinates_from_city(city)
            if lat is None or lon is None:
                logging.warning(f"[EV_Central] Geocoding failed for {city}, using fallback")
                lat, lon = 54.6872, 25.2797  # fallback

            # Forward to Registry best-effort
            try:
                r = requests.post(
                    f"{REGISTRY_URL}/register_cp",
                    json={"cp_id": cp_id, "city": city, "price_per_kwh": price},
                    timeout=5,
                )
                if r.status_code >= 400:
                    logging.warning(f"[EV_Central] Registry error {r.status_code}: {r.text}")
            except Exception as e:
                logging.warning(f"[EV_Central] Registry unreachable: {e}")

            with self.lock:
                # Don't overwrite if exists (avoid losing runtime fields)
                if cp_id not in self.charging_points:
                    self.charging_points[cp_id] = {
                        "state": CP_STATES["DISCONNECTED"],
                        "location": (lat, lon),
                        "price_per_kwh": price,
                        "current_driver": None,
                        "kwh_delivered": 0.0,
                        "amount_euro": 0.0,
                        "session_start": None,
                        "charging_complete": False,
                        "kwh_needed": 0.0,
                    }

            self.storage.save_cp(cp_id, lat, lon, price, CP_STATES["DISCONNECTED"])
            self.add_log("EV_Central", f"CP {cp_id} registered in {city}")

            return jsonify({
                "cp_id": cp_id,
                "city": city,
                "latitude": lat,
                "longitude": lon,
                "message": "CP registered successfully"
            }), 201

        @app.route("/api/weather_alert", methods=["POST"])
        def weather_alert():
            data = request.json or {}
            cp_id = data.get("cp_id")
            alert = data.get("alert")
            temp = data.get("temperature")

            if not cp_id or not alert:
                return jsonify({"error": "cp_id and alert required"}), 400

            self.add_log("EV_Weather", f"{cp_id}: {alert} ({temp}°C)")

            with self.lock:
                cp = self.charging_points.get(cp_id)
                if not cp:
                    # Do not create fake CP entries
                    return jsonify({"status": "received (cp unknown in central)"}), 200

                if alert == "ALERT_COLD":
                    cp["state"] = CP_STATES["OUT_OF_ORDER"]
                    self._broadcast_to_ui("CP_STATE", cp_id, payload={"state": CP_STATES["OUT_OF_ORDER"]})
                    print(f"[EV_Central] ❌ CP {cp_id} OUT OF SERVICE - Cold alert")

                elif alert == "WEATHER_OK":
                    # Restore only if not supplying
                    if cp["state"] != CP_STATES["SUPPLYING"]:
                        cp["state"] = CP_STATES["ACTIVATED"]
                        self._broadcast_to_ui("CP_STATE", cp_id, payload={"state": CP_STATES["ACTIVATED"]})
                        print(f"[EV_Central] ✅ CP {cp_id} RESTORED - Weather OK")

            return jsonify({"status": "received"}), 200

        app.run(host="0.0.0.0", port=5003, debug=False, threaded=True)

    # -------------------------------------------------------------------------
    # Geocoding (optional)
    # -------------------------------------------------------------------------
    def get_coordinates_from_city(self, city):
        try:
            api_key = os.getenv("OPENWEATHER_API_KEY")
            if not api_key:
                return None, None

            r = requests.get(
                "https://api.openweathermap.org/geo/1.0/direct",
                params={"q": city, "limit": 1, "appid": api_key},
                timeout=5,
            )
            if r.status_code != 200:
                return None, None

            data = r.json()
            if not data:
                return None, None

            return data[0].get("lat"), data[0].get("lon")
        except Exception:
            return None, None

    # -------------------------------------------------------------------------
    # Storage load
    # -------------------------------------------------------------------------
    def _load_stored_cps(self):
        stored = self.storage.get_all_cps()
        if not stored:
            print("[EV_Central] No stored charging points found")
            return

        print(f"[EV_Central] Loading {len(stored)} charging points from storage...")
        for cp in stored:
            cp_id = cp["cp_id"]
            self.charging_points[cp_id] = {
                "state": CP_STATES["DISCONNECTED"],
                "location": (cp["latitude"], cp["longitude"]),
                "price_per_kwh": cp["price_per_kwh"],
                "current_driver": None,
                "kwh_delivered": 0.0,
                "amount_euro": 0.0,
                "session_start": None,
                "charging_complete": False,
                "kwh_needed": 0.0,
            }

    # -------------------------------------------------------------------------
    # Logging + UI broadcast
    # -------------------------------------------------------------------------
    def add_log(self, source, text):
        ts = datetime.now().isoformat()
        entry = {"time": ts, "source": str(source), "text": str(text)}
        self.logs.append(entry)
        if len(self.logs) > 500:
            self.logs = self.logs[-500:]

        print(f"[LOG] {ts} {source}: {text}")

        ui_sock = self.monitors.get("WEB_UI")
        if ui_sock:
            try:
                msg = Protocol.build_message("LOG", entry["source"], entry["text"], entry["time"])
                ui_sock.send(Protocol.encode(msg))
            except Exception as e:
                print(f"[EV_Central] ⚠️ Failed to forward log to WEB_UI: {e}")

    def _broadcast_to_ui(self, event_type, cp_id, driver_id=None, payload=None):
        ui_sock = self.monitors.get("WEB_UI")
        if not ui_sock:
            return
        try:
            if payload is not None:
                msg = Protocol.build_message(event_type, cp_id, driver_id or "", json.dumps(payload))
            else:
                msg = Protocol.build_message(event_type, cp_id, driver_id or "")
            ui_sock.send(Protocol.encode(msg))
        except Exception as e:
            print(f"[EV_Central] ⚠️ UI broadcast failed ({event_type}): {e}")

    def _send_full_state_to_ui(self, ui_socket):
        # IMPORTANT: send stringified Python lists for ast.literal_eval compatibility
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
                    cp["amount_euro"],
                ])

            drivers = []
            for d_id, d in self.drivers.items():
                drivers.append([d_id, d["status"], d.get("current_cp") or ""])

            history = self.storage.get_recent_history(20)

        msg = Protocol.build_message("FULL_STATE", str(cps), str(drivers), str(history))
        try:
            ui_socket.send(Protocol.encode(msg))
            print("[EV_Central] 📤 Sent full system state to UI")
        except Exception as e:
            print(f"[EV_Central] ⚠️ Failed to send system state to UI: {e}")

    # -------------------------------------------------------------------------
    # Crypto helpers
    # -------------------------------------------------------------------------
    def _encrypt_message_for_cp(self, cp_id, plain_message):
        key = self.cp_encryption_keys.get(cp_id)
        if not key:
            return plain_message
        return self.encryption.encrypt(plain_message, key)

    def _decrypt_message_from_cp(self, cp_id, maybe_encrypted):
        key = self.cp_encryption_keys.get(cp_id)
        if not key:
            return maybe_encrypted
        try:
            return self.encryption.decrypt(maybe_encrypted, key)
        except Exception as e:
            print(f"[EV_Central] Decryption error for {cp_id}: {e}")
            return None

    # -------------------------------------------------------------------------
    # Server start
    # -------------------------------------------------------------------------
    def start(self):
        print(f"[EV_Central] Starting TCP server on {self.host}:{self.port}")

        # Registry polling (best-effort)
        threading.Thread(target=self.poll_registry_loop, daemon=True).start()

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(20)

        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                client_socket.settimeout(None)
                client_id = f"{addr[0]}:{addr[1]}"

                with self.lock:
                    self.active_connections[client_id] = client_socket

                threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, client_id),
                    daemon=True,
                ).start()
            except Exception as e:
                if self.running:
                    print(f"[EV_Central] Accept error: {e}")

    def _handle_client(self, client_socket, client_id):
        print(f"[EV_Central] Client connected: {client_id}")
        buffer = b""

        try:
            while self.running:
                data = client_socket.recv(4096)
                if not data:
                    break
                buffer += data

                while buffer:
                    msg, valid = Protocol.decode(buffer)
                    if not valid:
                        break

                    etx = buffer.find(b"\x03")
                    buffer = buffer[etx + 2:] if etx != -1 else b""

                    # 1) Try parse as plaintext protocol
                    fields = None
                    try:
                        fields = Protocol.parse_message(msg)
                    except Exception:
                        fields = None

                    # 2) If parse fails or unknown -> try decrypt if socket is CP
                    if not fields or not fields[0]:
                        cp_id = self.socket_to_entity.get(client_socket)
                        if cp_id and self.socket_to_type.get(client_socket) == "CP":
                            dec = self._decrypt_message_from_cp(cp_id, msg)
                            if not dec:
                                continue
                            try:
                                fields = Protocol.parse_message(dec)
                            except Exception:
                                continue
                        else:
                            continue

                    # 3) Process message safely (DO NOT kill socket on handler error)
                    try:
                        self._process_fields(fields, client_socket, client_id)
                    except Exception as e:
                        print(f"[EV_Central] Handler error (ignored) from {client_id}: {e}")
                        continue

        except Exception as e:
            print(f"[EV_Central] Error handling {client_id}: {e}")
        finally:
            self._cleanup_socket(client_socket, client_id)

    def _cleanup_socket(self, client_socket, client_id):
        # Close socket once
        try:
            client_socket.close()
        except Exception:
            pass

        with self.lock:
            ent = self.socket_to_entity.pop(client_socket, None)
            typ = self.socket_to_type.pop(client_socket, None)

            # Remove entity mapping only if still points to this socket
            if ent and self.entity_to_socket.get(ent) == client_socket:
                del self.entity_to_socket[ent]

            # UI monitor cleanup
            if typ == "MONITOR":
                if self.monitors.get(ent) == client_socket:
                    del self.monitors[ent]

            # CP cleanup
            if typ == "CP" and ent:
                cp = self.charging_points.get(ent)
                if cp:
                    cp["state"] = CP_STATES["DISCONNECTED"]
                    cp["current_driver"] = None
                    cp["charging_complete"] = False
                    self._broadcast_to_ui("CP_STATE", ent, payload={"state": CP_STATES["DISCONNECTED"]})
                print(f"[EV_Central] 🔌 CP {ent} marked DISCONNECTED")

            # Remove active connection id
            self.active_connections.pop(client_id, None)

        print(f"[EV_Central] Client disconnected: {client_id}")

    # -------------------------------------------------------------------------
    # Dispatch
    # -------------------------------------------------------------------------
    def _process_fields(self, fields, client_socket, client_id):
        t = fields[0]

        if t == MessageTypes.REGISTER:
            self._handle_register(fields, client_socket, client_id)
        elif t == MessageTypes.HEARTBEAT:
            self._handle_heartbeat(fields, client_socket, client_id)
        elif t == MessageTypes.REQUEST_CHARGE:
            self._handle_charge_request(fields, client_socket, client_id)
        elif t == MessageTypes.QUERY_AVAILABLE_CPS:
            self._handle_query_available_cps(fields, client_socket, client_id)
        elif t == MessageTypes.SUPPLY_UPDATE:
            self._handle_supply_update(fields, client_socket, client_id)
        elif t == MessageTypes.SUPPLY_END:
            self._handle_supply_end(fields, client_socket, client_id)
        elif t == MessageTypes.END_CHARGE:
            self._handle_end_charge(fields, client_socket, client_id)
        elif t == MessageTypes.FAULT:
            self._handle_fault(fields, client_socket, client_id)
        elif t == MessageTypes.RECOVERY:
            self._handle_recovery(fields, client_socket, client_id)
        elif t == "LOG":
            src = fields[1] if len(fields) > 1 else "UNKNOWN"
            txt = fields[2] if len(fields) > 2 else ""
            self.add_log(src, txt)

    # -------------------------------------------------------------------------
    # REGISTER
    # -------------------------------------------------------------------------
    def _handle_register(self, fields, client_socket, client_id):
        if len(fields) < 3:
            return

        entity_type = fields[1]
        entity_id = fields[2]

        if entity_type == "MONITOR":
            if entity_id == "WEB_UI":
                with self.lock:
                    self.monitors["WEB_UI"] = client_socket
                    self.socket_to_entity[client_socket] = "WEB_UI"
                    self.socket_to_type[client_socket] = "MONITOR"

                print("[EV_Central] 🖥 UI connected")
                ack = Protocol.build_message(MessageTypes.ACKNOWLEDGE, "WEB_UI", "OK")
                client_socket.send(Protocol.encode(ack))
                self._send_full_state_to_ui(client_socket)
                return

            with self.lock:
                self.monitors[entity_id] = client_socket
                self.socket_to_entity[client_socket] = entity_id
                self.socket_to_type[client_socket] = "MONITOR"

            ack = Protocol.build_message(MessageTypes.ACKNOWLEDGE, entity_id, "OK")
            client_socket.send(Protocol.encode(ack))
            return

        if entity_type == "DRIVER":
            with self.lock:
                self.drivers[entity_id] = {"status": "IDLE", "current_cp": None, "charge_amount": 0}
                self.entity_to_socket[entity_id] = client_socket
                self.socket_to_entity[client_socket] = entity_id
                self.socket_to_type[client_socket] = "DRIVER"

            self.storage.save_driver(entity_id, "IDLE")
            print(f"[EV_Central] ✅ Driver Registered: {entity_id}")
            self.add_log("EV_Central", f"Driver {entity_id} registered")
            ack = Protocol.build_message(MessageTypes.ACKNOWLEDGE, entity_id, "OK")
            client_socket.send(Protocol.encode(ack))
            return

        if entity_type == "CP":
            # CP sends: REGISTER|CP|CP-001|lat|lon|price|username|password
            username = fields[6] if len(fields) > 6 else None
            password = fields[7] if len(fields) > 7 else None

            if not self._verify_cp_credentials(entity_id, username, password):
                deny = Protocol.build_message(MessageTypes.DENY, entity_id, "AUTH_FAILED")
                client_socket.send(Protocol.encode(deny))
                return

            log_auth(client_id, entity_id, success=True)

            # Generate encryption key based on secret (same scheme as your other code)
            symmetric_key = self.encryption.generate_key(password)

            with self.lock:
                self.cp_encryption_keys[entity_id] = symmetric_key
                self.entity_to_socket[entity_id] = client_socket
                self.socket_to_entity[client_socket] = entity_id
                self.socket_to_type[client_socket] = "CP"

            stored = self.storage.get_cp(entity_id)
            if stored:
                lat = stored["latitude"]
                lon = stored["longitude"]
                price = stored["price_per_kwh"]
            else:
                # prefer lat/lon from message
                try:
                    lat = float(fields[3]) if len(fields) > 3 else 40.5
                    lon = float(fields[4]) if len(fields) > 4 else -3.1
                except Exception:
                    lat, lon = 40.5, -3.1
                try:
                    price = float(fields[5]) if len(fields) > 5 else 0.30
                except Exception:
                    price = 0.30

            with self.lock:
                # Preserve if exists, update fields
                cp = self.charging_points.get(entity_id) or {}
                cp.update({
                    "state": CP_STATES["ACTIVATED"],
                    "location": (lat, lon),
                    "price_per_kwh": price,
                    "current_driver": None,
                    "kwh_delivered": 0.0,
                    "amount_euro": 0.0,
                    "session_start": None,
                    "charging_complete": False,
                    "kwh_needed": 0.0,
                })
                self.charging_points[entity_id] = cp

            self.storage.save_cp(entity_id, lat, lon, price, CP_STATES["ACTIVATED"])

            # Broadcast connect (UI may use it, harmless if not)
            self._broadcast_to_ui(
                "CP_CONNECTED",
                entity_id,
                payload={"state": "ACTIVATED", "location": (lat, lon), "price_per_kwh": price},
            )

            ack = Protocol.build_message(
                MessageTypes.ACKNOWLEDGE,
                entity_id,
                "OK",
                symmetric_key.decode(),
                str(lat),
                str(lon),
            )
            client_socket.send(Protocol.encode(ack))

            print(f"[EV_Central] ✅ CP Connected: {entity_id}")
            self.add_log("EV_Central", f"CP {entity_id} connected")
            return

    def _verify_cp_credentials(self, cp_id, username, password):
        try:
            r = requests.post(
                f"{REGISTRY_URL}/verify",
                json={"cp_id": cp_id, "username": username, "password": password},
                timeout=5,
            )
            if r.status_code != 200:
                return False

            with self.lock:
                self.cp_credentials[cp_id] = {"username": username, "secret": password}
            return True
        except Exception as e:
            print(f"[EV_Central] Registry verify error: {e}")
            return False

    # -------------------------------------------------------------------------
    # HEARTBEAT
    # -------------------------------------------------------------------------
    def _handle_heartbeat(self, fields, client_socket, client_id):
        if len(fields) < 3:
            return

        cp_id = fields[1]
        state = fields[2]

        with self.lock:
            cp = self.charging_points.get(cp_id)
            if not cp:
                return
            # Never overwrite SUPPLYING by heartbeat
            if cp["state"] != CP_STATES["SUPPLYING"]:
                cp["state"] = state

    # -------------------------------------------------------------------------
    # AVAILABLE CPS
    # -------------------------------------------------------------------------
    def _handle_query_available_cps(self, fields, client_socket, client_id):
        if len(fields) < 2:
            return
        driver_id = fields[1]

        available = []
        with self.lock:
            for cp_id, cp in self.charging_points.items():
                if cp["state"] == CP_STATES["ACTIVATED"] and cp["current_driver"] is None:
                    available.append({
                        "cp_id": cp_id,
                        "location": cp["location"],
                        "price_per_kwh": cp["price_per_kwh"],
                    })

        response_fields = [MessageTypes.AVAILABLE_CPS]
        for cp in available:
            response_fields.extend([cp["cp_id"], cp["location"][0], cp["location"][1], cp["price_per_kwh"]])

        msg = Protocol.build_message(*response_fields)
        try:
            client_socket.send(Protocol.encode(msg))
        except Exception as e:
            print(f"[EV_Central] Failed to send AVAILABLE_CPS to {driver_id}: {e}")

    # -------------------------------------------------------------------------
    # SEND TO CP (safe + encrypted)
    # -------------------------------------------------------------------------
    def _send_to_cp(self, cp_id, plain_msg):
        with self.lock:
            sock = self.entity_to_socket.get(cp_id)

        if not sock:
            print(f"[EV_Central] ⚠️ CP {cp_id} socket missing")
            return False

        try:
            enc = self._encrypt_message_for_cp(cp_id, plain_msg)
            sock.send(Protocol.encode(enc))
            return True
        except Exception as e:
            print(f"[EV_Central] ⚠️ Failed to send to CP {cp_id}: {e}")
            # Don't crash; cleanup mapping to avoid repeated bad file descriptor
            try:
                self._cleanup_socket(sock, f"cp:{cp_id}")
            except Exception:
                pass
            return False

    # -------------------------------------------------------------------------
    # CHARGE REQUEST
    # -------------------------------------------------------------------------
    def _handle_charge_request(self, fields, client_socket, client_id):
        if len(fields) < 4:
            self.add_log("EV_Central", f"Invalid REQUEST_CHARGE from {client_id}: {fields}")
            return

        driver_id = fields[1]
        cp_id = fields[2]
        kwh_needed = float(fields[3])

        print(f"[EV_Central] 🔌 {driver_id} requesting {kwh_needed} kWh at {cp_id}")

        with self.lock:
            cp = self.charging_points.get(cp_id)
            if not cp:
                deny = Protocol.build_message(MessageTypes.DENY, driver_id, cp_id, "CP_NOT_FOUND")
                client_socket.send(Protocol.encode(deny))
                return

            if cp["state"] != CP_STATES["ACTIVATED"] or cp["current_driver"] is not None:
                reason = f"CP_STATE_{cp['state']}" if cp["state"] != CP_STATES["ACTIVATED"] else "CP_ALREADY_IN_USE"
                deny = Protocol.build_message(MessageTypes.DENY, driver_id, cp_id, reason)
                client_socket.send(Protocol.encode(deny))
                return

            # Start session
            cp["state"] = CP_STATES["SUPPLYING"]
            cp["current_driver"] = driver_id
            cp["session_start"] = time.time()
            cp["kwh_delivered"] = 0.0
            cp["amount_euro"] = 0.0
            cp["kwh_needed"] = kwh_needed
            cp["charging_complete"] = False

            if driver_id in self.drivers:
                self.drivers[driver_id]["status"] = "CHARGING"
                self.drivers[driver_id]["current_cp"] = cp_id

            price = cp["price_per_kwh"]

        log_charge(client_id, cp_id, driver_id, "CHARGE_START")

        self.add_log("EV_Central", f"Charge authorized Driver {driver_id} -> {cp_id}")
        print(f"[EV_Central] ✅ Charge authorized: Driver {driver_id} → CP {cp_id}")

        # AUTHORIZE to driver
        try:
            drv_msg = Protocol.build_message(MessageTypes.AUTHORIZE, driver_id, cp_id, kwh_needed, price)
            client_socket.send(Protocol.encode(drv_msg))
            print(f"[EV_Central] 📤 Sent AUTHORIZE to driver {driver_id}")
        except Exception as e:
            print(f"[EV_Central] ⚠️ Failed to send AUTHORIZE to driver: {e}")

        # AUTHORIZE to CP (encrypted)
        self._send_to_cp(cp_id, Protocol.build_message(MessageTypes.AUTHORIZE, driver_id, cp_id, kwh_needed))

        # Notify UI
        self._broadcast_to_ui("DRIVER_START", cp_id, driver_id)

        # Kafka (best effort)
        try:
            self.kafka.publish_event("charging_logs", "CHARGE_AUTHORIZED", {
                "driver_id": driver_id,
                "cp_id": cp_id,
                "kwh_needed": kwh_needed
            })
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # SUPPLY UPDATE / END
    # -------------------------------------------------------------------------
    def _handle_supply_update(self, fields, client_socket, client_id):
        if len(fields) < 4:
            self.add_log("EV_Central", f"Invalid SUPPLY_UPDATE: {fields}")
            return

        cp_id = fields[1]
        kwh_inc = float(fields[2])
        amount = float(fields[3])

        driver_id = None
        with self.lock:
            cp = self.charging_points.get(cp_id)
            if not cp:
                return
            cp["kwh_delivered"] += kwh_inc
            cp["amount_euro"] = amount
            driver_id = cp.get("current_driver")

            # Optional completion mark
            if cp["kwh_delivered"] >= cp.get("kwh_needed", 0) and cp.get("kwh_needed", 0) > 0:
                if not cp.get("charging_complete", False):
                    cp["charging_complete"] = True
                    print(f"[EV_Central] 🔋 {driver_id} finished charging at {cp_id}, waiting unplug")
                    self._broadcast_to_ui("CHARGING_COMPLETE", cp_id, driver_id)

        # Forward to driver
        if driver_id:
            with self.lock:
                dsock = self.entity_to_socket.get(driver_id)
            if dsock:
                try:
                    msg = Protocol.build_message(MessageTypes.SUPPLY_UPDATE, cp_id, kwh_inc, amount)
                    dsock.send(Protocol.encode(msg))
                except Exception:
                    pass

        # Forward to UI as the UI expects SUPPLY_UPDATE protocol fields
        self._broadcast_to_ui(MessageTypes.SUPPLY_UPDATE, cp_id, payload={"kwh_inc": kwh_inc, "amount": amount})

    def _handle_supply_end(self, fields, client_socket, client_id):
        if len(fields) < 5:
            return

        cp_id = fields[1]
        driver_id = fields[2]
        total_kwh = float(fields[3])
        total_amount = float(fields[4])

        duration = 0
        with self.lock:
            cp = self.charging_points.get(cp_id)
            if cp and cp.get("session_start"):
                duration = int(time.time() - cp["session_start"])

            if cp:
                cp["state"] = CP_STATES["ACTIVATED"]
                cp["current_driver"] = None
                cp["kwh_delivered"] = 0.0
                cp["amount_euro"] = 0.0
                cp["session_start"] = None
                cp["charging_complete"] = False
                cp["kwh_needed"] = 0.0

            if driver_id in self.drivers:
                self.drivers[driver_id]["status"] = "IDLE"
                self.drivers[driver_id]["current_cp"] = None

        # Persist
        try:
            log_charge(client_id, cp_id, driver_id, "CHARGE_END", kwh=total_kwh, amount=total_amount)
        except Exception:
            pass

        self.storage.save_charging_session(cp_id, driver_id, total_kwh, total_amount, duration)
        self.storage.update_driver_stats(driver_id, total_amount)

        print(f"[EV_Central] ✅ {driver_id} unplugged from {cp_id} ({total_kwh:.2f} kWh, {total_amount:.2f}€)")

        # Ticket to driver
        with self.lock:
            dsock = self.entity_to_socket.get(driver_id)
        if dsock:
            try:
                ticket = Protocol.build_message(MessageTypes.TICKET, cp_id, total_kwh, total_amount)
                dsock.send(Protocol.encode(ticket))
            except Exception:
                pass

        # UI
        self._broadcast_to_ui("DRIVER_STOP", cp_id, driver_id)
        # also send SUPPLY_END protocol for UI history
        ui_sock = self.monitors.get("WEB_UI")
        if ui_sock:
            try:
                ui_msg = Protocol.build_message(MessageTypes.SUPPLY_END, cp_id, driver_id, total_kwh, total_amount)
                ui_sock.send(Protocol.encode(ui_msg))
            except Exception:
                pass

        # Kafka
        try:
            self.kafka.publish_event("charging_logs", "CHARGE_COMPLETED", {
                "cp_id": cp_id,
                "driver_id": driver_id,
                "total_kwh": total_kwh,
                "total_amount": total_amount
            })
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # END CHARGE (manual finish)
    # -------------------------------------------------------------------------
    def _handle_end_charge(self, fields, client_socket, client_id):
        if len(fields) < 3:
            return

        driver_id = fields[1]
        cp_id = fields[2]

        print(f"[EV_Central] 🔌 Driver {driver_id} manually ending charge at {cp_id}")

        with self.lock:
            cp = self.charging_points.get(cp_id)
            if not cp or cp.get("current_driver") != driver_id:
                return

            duration = int(time.time() - cp["session_start"]) if cp.get("session_start") else 0
            total_seconds = 14.0
            kwh_needed = float(cp.get("kwh_needed", 0.0) or 0.0)
            if kwh_needed > 0:
                total_kwh = min(kwh_needed, (duration / total_seconds) * kwh_needed)
            else:
                total_kwh = float(cp.get("kwh_delivered", 0.0))
            total_amount = round(total_kwh * cp["price_per_kwh"], 2)

            # reset
            cp["state"] = CP_STATES["ACTIVATED"]
            cp["current_driver"] = None
            cp["kwh_delivered"] = 0.0
            cp["amount_euro"] = 0.0
            cp["session_start"] = None
            cp["charging_complete"] = False
            cp["kwh_needed"] = 0.0

            if driver_id in self.drivers:
                self.drivers[driver_id]["status"] = "IDLE"
                self.drivers[driver_id]["current_cp"] = None

        self.storage.save_charging_session(cp_id, driver_id, total_kwh, total_amount, duration)
        self.storage.update_driver_stats(driver_id, total_amount)

        # Notify CP to end supply (encrypted)
        self._send_to_cp(cp_id, Protocol.build_message(MessageTypes.END_SUPPLY, cp_id))

        # Ticket to driver
        try:
            ticket = Protocol.build_message(MessageTypes.TICKET, cp_id, total_kwh, total_amount)
            client_socket.send(Protocol.encode(ticket))
        except Exception:
            pass

        self._broadcast_to_ui("DRIVER_STOP", cp_id, driver_id)

    # -------------------------------------------------------------------------
    # FAULT / RECOVERY
    # -------------------------------------------------------------------------
    def _handle_fault(self, fields, client_socket, client_id):
        if len(fields) < 2:
            return
        cp_id = fields[1]

        with self.lock:
            cp = self.charging_points.get(cp_id)
            if cp:
                cp["state"] = CP_STATES["OUT_OF_ORDER"]

        try:
            log_fault(client_id, cp_id, "CP_FAULT", "Health check failed")
        except Exception:
            pass

        self.add_log("EV_Central", f"FAULT {cp_id}")
        self._broadcast_to_ui(MessageTypes.FAULT, cp_id, payload={"state": CP_STATES["OUT_OF_ORDER"]})

        try:
            self.kafka.publish_event("system_events", "CP_FAULT", {"cp_id": cp_id})
        except Exception:
            pass

    def _handle_recovery(self, fields, client_socket, client_id):
        if len(fields) < 2:
            return
        cp_id = fields[1]

        with self.lock:
            cp = self.charging_points.get(cp_id)
            if cp and cp["state"] != CP_STATES["SUPPLYING"]:
                cp["state"] = CP_STATES["ACTIVATED"]

        try:
            log_fault(client_id, cp_id, "CP_RECOVERY", "System restored")
        except Exception:
            pass

        self.add_log("EV_Central", f"RECOVERY {cp_id}")
        self._broadcast_to_ui(MessageTypes.RECOVERY, cp_id, payload={"state": CP_STATES["ACTIVATED"]})

        try:
            self.kafka.publish_event("system_events", "CP_RECOVERED", {"cp_id": cp_id})
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # Dashboard printing
    # -------------------------------------------------------------------------
    def display_dashboard(self):
        while self.running:
            time.sleep(2)
            with self.lock:
                print("\n" + "=" * 80)
                print("EV_CENTRAL MONITORING DASHBOARD")
                print("=" * 80)

                print("\n[CHARGING POINTS]")
                if not self.charging_points:
                    print("  No charging points registered")
                else:
                    for cp_id, cp in self.charging_points.items():
                        color = COLORS.get(cp["state"], "?")
                        print(f"  [{color}] {cp_id}: {cp['state']}")
                        if cp["state"] == CP_STATES["SUPPLYING"]:
                            print(f"      Driver: {cp.get('current_driver')}")
                            print(f"      kWh: {cp.get('kwh_delivered', 0):.2f} kWh")
                            print(f"      Amount: {cp.get('amount_euro', 0):.2f}€")

                print("\n[DRIVERS]")
                if not self.drivers:
                    print("  No drivers registered")
                else:
                    for d_id, d in self.drivers.items():
                        print(f"  {d_id}: {d['status']}")
                        if d["status"] == "CHARGING":
                            print(f"      At: {d.get('current_cp')}")

                print("=" * 80 + "\n")

    # -------------------------------------------------------------------------
    # Registry polling
    # -------------------------------------------------------------------------
    def poll_registry_loop(self):
        while self.running:
            time.sleep(REGISTRY_POLL_INTERVAL)
            try:
                r = requests.get(f"{REGISTRY_URL}/list", timeout=5)
                if r.status_code != 200:
                    continue
                data = r.json()
                registry_cps = {cp["cp_id"]: cp for cp in data.get("charging_points", [])}
                for cp_id in registry_cps.keys():
                    if cp_id not in self.charging_points:
                        print(f"[EV_Central] 📋 CP {cp_id} exists in registry (not connected yet)")
            except Exception as e:
                print(f"[EV_Central] Registry poll error: {e}")

    # -------------------------------------------------------------------------
    # Shutdown
    # -------------------------------------------------------------------------
    def shutdown(self):
        self.running = False
        try:
            if self.server_socket:
                self.server_socket.close()
        except Exception:
            pass
        try:
            self.kafka.close()
        except Exception:
            pass
        print("[EV_Central] Shutdown complete")


if __name__ == "__main__":
    central = EVCentral()

    threading.Thread(target=central.start, daemon=True).start()
    threading.Thread(target=central.display_dashboard, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        central.shutdown()
