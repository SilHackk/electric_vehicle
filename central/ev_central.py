# ============================================================================
# EVCharging System - EV_Central (Control Center) - OPTION A (Full parity)
# FIXES (REAL):
# - UI receives cumulative kWh and € (NOT increment-only JSON)
# - SUPPLY_UPDATE broadcast uses Protocol.build_message(...) so UI updates CP rows
# - Money accumulates correctly (supports both CP-sends-total and CP-sends-increment)
# - Progress bar can fill (UI can compute % from kwh_delivered / kwh_needed)
# - Registry forward uses /register (fixes 404 POST /register_cp)
# - CP registration via UI appears immediately (stored + FULL_STATE works)
# - No fake CPs created by registry polling
# - Robust socket lifecycle + safe handler errors
# - CP encryption supported (decrypt inbound if needed, encrypt outbound commands)
# - FULL_STATE always uses str(list) for ast.literal_eval UI compatibility
# - REST API: /api/register_cp + /api/weather_alert on port 5003
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
from shared.audit_logger import log_auth, log_charge, log_fault

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

        # Monitors (UI)
        self.monitors = {}             # "WEB_UI" -> socket

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

            # Forward to Registry (IMPORTANT: registry endpoint is /register, not /register_cp)
            try:
                r = requests.post(
                    f"{REGISTRY_URL}/register",
                    json={"cp_id": cp_id, "city": city, "price_per_kwh": price, "latitude": lat, "longitude": lon},
                    timeout=5,
                )
                if r.status_code >= 400:
                    logging.warning(f"[EV_Central] Registry error {r.status_code}: {r.text}")
            except Exception as e:
                logging.warning(f"[EV_Central] Registry unreachable: {e}")

            # Add CP locally immediately so UI sees it right away
            with self.lock:
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

            # If UI is connected, push updated full state immediately (so CP appears next to others)
            ui_sock = self.monitors.get("WEB_UI")
            if ui_sock:
                try:
                    self._send_full_state_to_ui(ui_sock)
                except Exception:
                    pass

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
                    # If CP is charging, let it finish, then stop
                    if cp["state"] == CP_STATES["SUPPLYING"]:
                        cp["pending_weather_stop"] = True
                        print(f"[EV_Central] ❄️ CP {cp_id} will stop AFTER charging completes (weather)")
                    else:
                        cp["state"] = CP_STATES["OUT_OF_ORDER"]
                        self._send_to_cp(cp_id, Protocol.build_message("STOP_SUPPLY", cp_id))
                        self._broadcast_to_ui_protocol(
                            Protocol.build_message("CP_STATE", cp_id,
                                                json.dumps({"state": CP_STATES["OUT_OF_ORDER"]}))
                        )
                        print(f"[EV_Central] ❌ CP {cp_id} OUT OF SERVICE - Cold alert")

                elif alert == "WEATHER_OK":
                    # Clear pending stop
                    cp.pop("pending_weather_stop", None)

                    if cp["state"] != CP_STATES["SUPPLYING"]:
                        cp["state"] = CP_STATES["ACTIVATED"]
                        self._send_to_cp(cp_id, Protocol.build_message("RESUME_SUPPLY", cp_id))
                        self._broadcast_to_ui_protocol(
                            Protocol.build_message("CP_STATE", cp_id,
                                                json.dumps({"state": CP_STATES["ACTIVATED"]}))
                        )
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
    # Logging
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

    # -------------------------------------------------------------------------
    # UI helpers
    # -------------------------------------------------------------------------
    def _broadcast_to_ui_protocol(self, protocol_message_str):
        """Send already-built protocol string to WEB_UI."""
        ui_sock = self.monitors.get("WEB_UI")
        if not ui_sock:
            return
        try:
            ui_sock.send(Protocol.encode(protocol_message_str))
        except Exception as e:
            print(f"[EV_Central] ⚠️ UI broadcast failed: {e}")

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
                    cp.get("kwh_needed", 0.0),
                ])

            drivers = []
            for d_id, d in self.drivers.items():
                drivers.append([d_id, d["status"], d.get("current_cp") or ""])

            history = self.storage.get_recent_history(20)

        msg = Protocol.build_message("FULL_STATE", str(cps), str(drivers), str(history))
        ui_socket.send(Protocol.encode(msg))
        print("[EV_Central] 📤 Sent full system state to UI")

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

                    # 2) If parse fails -> try decrypt if socket is CP
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

                    # 3) Process message safely
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
        try:
            client_socket.close()
        except Exception:
            pass

        with self.lock:
            ent = self.socket_to_entity.pop(client_socket, None)
            typ = self.socket_to_type.pop(client_socket, None)

            if ent and self.entity_to_socket.get(ent) == client_socket:
                del self.entity_to_socket[ent]

            if typ == "MONITOR":
                if self.monitors.get(ent) == client_socket:
                    del self.monitors[ent]

            if typ == "CP" and ent:
                cp = self.charging_points.get(ent)
                # Preserve session if actively charging
                if cp and cp["state"] == CP_STATES["SUPPLYING"]:
                    return
                if cp:
                    cp["state"] = CP_STATES["DISCONNECTED"]
                    cp["current_driver"] = None
                    cp["charging_complete"] = False
                    # UI update
                    self._broadcast_to_ui_protocol(
                        Protocol.build_message("CP_STATE", ent, json.dumps({"state": CP_STATES["DISCONNECTED"]}))
                    )
                print(f"[EV_Central] 🔌 CP {ent} marked DISCONNECTED")

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
            self._handle_heartbeat(fields)
        elif t == MessageTypes.REQUEST_CHARGE:
            self._handle_charge_request(fields, client_socket, client_id)
        elif t == MessageTypes.QUERY_AVAILABLE_CPS:
            self._handle_query_available_cps(fields, client_socket)
        elif t == MessageTypes.SUPPLY_UPDATE:
            self._handle_supply_update(fields)
        elif t == MessageTypes.SUPPLY_END:
            self._handle_supply_end(fields, client_socket, client_id)
        elif t == MessageTypes.END_CHARGE:
            self._handle_end_charge(fields, client_socket, client_id)
        elif t == MessageTypes.FAULT:
            self._handle_fault(fields, client_id)
        elif t == MessageTypes.RECOVERY:
            self._handle_recovery(fields)
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
            # Your UI uses REGISTER|MONITOR|WEB_UI
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

        if entity_type == "DRIVER":
            with self.lock:
                self.drivers[entity_id] = {"status": "IDLE", "current_cp": None, "charge_amount": 0}
                self.entity_to_socket[entity_id] = client_socket
                self.socket_to_entity[client_socket] = entity_id
                self.socket_to_type[client_socket] = "DRIVER"

            self.storage.save_driver(entity_id, "IDLE")
            self.add_log("EV_Central", f"Driver {entity_id} registered")
            ack = Protocol.build_message(MessageTypes.ACKNOWLEDGE, entity_id, "OK")
            client_socket.send(Protocol.encode(ack))
            return

        if entity_type == "CP":
            # CP sends: REGISTER|CP|CP-001|lat|lon|price|username|password
            username = fields[6] if len(fields) > 6 else None
            password = fields[7] if len(fields) > 7 else None

            # Verify against Registry
            if not self._verify_cp_credentials(entity_id, username, password):
                deny = Protocol.build_message(MessageTypes.DENY, entity_id, "AUTH_FAILED")
                client_socket.send(Protocol.encode(deny))
                return

            log_auth(client_id, entity_id, success=True)

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
                cp = self.charging_points.get(entity_id) or {}
                # Preserve money/kwh if reconnecting during session
                prev_kwh = float(cp.get("kwh_delivered", 0.0) or 0.0)
                prev_amt = float(cp.get("amount_euro", 0.0) or 0.0)

                cp.update({
                    "state": CP_STATES["ACTIVATED"],
                    "location": (lat, lon),
                    "price_per_kwh": price,
                    "current_driver": cp.get("current_driver"),
                    "kwh_delivered": prev_kwh,
                    "amount_euro": prev_amt,
                    "session_start": cp.get("session_start"),
                    "charging_complete": bool(cp.get("charging_complete", False)),
                    "kwh_needed": float(cp.get("kwh_needed", 0.0) or 0.0),
                })
                # If it had an active driver, restore SUPPLYING
                if cp.get("current_driver"):
                    cp["state"] = CP_STATES["SUPPLYING"]

                self.charging_points[entity_id] = cp

            self.storage.save_cp(entity_id, lat, lon, price, self.charging_points[entity_id]["state"])

            # ACK to CP
            ack = Protocol.build_message(
                MessageTypes.ACKNOWLEDGE,
                entity_id,
                "OK",
                symmetric_key.decode(),
                str(lat),
                str(lon),
            )
            client_socket.send(Protocol.encode(ack))

            self.add_log("EV_Central", f"CP {entity_id} connected")

            # Push full state to UI so CP appears/updates immediately
            ui_sock = self.monitors.get("WEB_UI")
            if ui_sock:
                try:
                    self._send_full_state_to_ui(ui_sock)
                except Exception:
                    pass

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
    def _handle_heartbeat(self, fields):
        if len(fields) < 3:
            return
        cp_id = fields[1]
        state = fields[2]
        with self.lock:
            cp = self.charging_points.get(cp_id)
            if not cp:
                return
            if cp["state"] != CP_STATES["SUPPLYING"]:
                cp["state"] = state

    # -------------------------------------------------------------------------
    # AVAILABLE CPS
    # -------------------------------------------------------------------------
    def _handle_query_available_cps(self, fields, client_socket):
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

        # AUTHORIZE to driver
        drv_msg = Protocol.build_message(MessageTypes.AUTHORIZE, driver_id, cp_id, kwh_needed, price)
        client_socket.send(Protocol.encode(drv_msg))

        # AUTHORIZE to CP (encrypted)
        self._send_to_cp(cp_id, Protocol.build_message(MessageTypes.AUTHORIZE, driver_id, cp_id, kwh_needed))

        # Notify UI (driver started)
        self._broadcast_to_ui_protocol(Protocol.build_message("DRIVER_START", cp_id, driver_id))

    # -------------------------------------------------------------------------
    # SUPPLY UPDATE (CRITICAL FIX)
    # -------------------------------------------------------------------------
    def _handle_supply_update(self, fields):
        """
        fields: SUPPLY_UPDATE|cp_id|kwh_inc|amount
        BUT some CP engines send:
          - amount = TOTAL amount so far
          - OR amount = increment amount
        We support BOTH.

        UI MUST receive cumulative totals via real protocol message:
          SUPPLY_UPDATE|cp_id|kwh_total|amount_total
        """
        if len(fields) < 4:
            self.add_log("EV_Central", f"Invalid SUPPLY_UPDATE: {fields}")
            return

        cp_id = fields[1]
        kwh_inc = float(fields[2])
        amount_field = float(fields[3])

        with self.lock:
            cp = self.charging_points.get(cp_id)
            if not cp:
                return

            # Always accumulate kWh
            cp["kwh_delivered"] = float(cp.get("kwh_delivered", 0.0) or 0.0) + kwh_inc

            # Money: support both modes
            # If CP sends TOTAL, it will usually be >= our computed total.
            computed_total = cp["kwh_delivered"] * float(cp["price_per_kwh"])
            prev_amt = float(cp.get("amount_euro", 0.0) or 0.0)

            if amount_field >= prev_amt and amount_field >= (computed_total * 0.7):
                # treat as TOTAL
                cp["amount_euro"] = amount_field
            else:
                # treat as INCREMENT
                cp["amount_euro"] = prev_amt + (kwh_inc * float(cp["price_per_kwh"]))

            cp["amount_euro"] = round(cp["amount_euro"], 2)

            driver_id = cp.get("current_driver")

            # Completion flag
            kwh_needed = float(cp.get("kwh_needed", 0.0) or 0.0)
            if kwh_needed > 0 and cp["kwh_delivered"] >= kwh_needed:
                if not cp.get("charging_complete", False):
                    cp["charging_complete"] = True
                    # 🔴 CENTRAL decides to stop charging
                    self._send_to_cp(
                        cp_id,
                        Protocol.build_message(MessageTypes.END_SUPPLY, cp_id)
                    )
                    self._broadcast_to_ui_protocol(Protocol.build_message("CHARGING_COMPLETE", cp_id, driver_id or ""))

            # Prepare totals for messages
            kwh_total = round(cp["kwh_delivered"], 6)
            eur_total = cp["amount_euro"]

        # Forward to driver (most drivers want live totals; if yours wants increment, change here)
        if driver_id:
            with self.lock:
                dsock = self.entity_to_socket.get(driver_id)
            if dsock:
                try:
                    # Send TOTALS to driver (so driver UI can show progress)
                    msg = Protocol.build_message(MessageTypes.SUPPLY_UPDATE, cp_id, kwh_total, eur_total)
                    dsock.send(Protocol.encode(msg))
                except Exception:
                    pass

        # ✅ CRITICAL: send TOTALS to WEB_UI using protocol message (NOT JSON payload)
        self._broadcast_to_ui_protocol(
            Protocol.build_message(MessageTypes.SUPPLY_UPDATE, cp_id, kwh_total, eur_total)
        )

    # -------------------------------------------------------------------------
    # SUPPLY END
    # -------------------------------------------------------------------------
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
        self._broadcast_to_ui_protocol(Protocol.build_message("DRIVER_STOP", cp_id, driver_id))
        self._broadcast_to_ui_protocol(Protocol.build_message(MessageTypes.SUPPLY_END, cp_id, driver_id, total_kwh, total_amount))

        # ---------------------------------------------------------
# Weather-triggered shutdown AFTER charging completes
# ---------------------------------------------------------
        with self.lock:
            cp = self.charging_points.get(cp_id)
            if cp and cp.pop("pending_weather_stop", False):
                cp["state"] = CP_STATES["OUT_OF_ORDER"]

                # Tell CP engine to stop
                self._send_to_cp(
                    cp_id,
                    Protocol.build_message("STOP_SUPPLY", cp_id)
                )

                # Update UI
                self._broadcast_to_ui_protocol(
                    Protocol.build_message(
                        "CP_STATE",
                        cp_id,
                        json.dumps({"state": CP_STATES["OUT_OF_ORDER"]})
                    )
                )

                print(f"[EV_Central] ❄️ CP {cp_id} stopped after charging due to weather")

    # -------------------------------------------------------------------------
    # END CHARGE (manual)
    # -------------------------------------------------------------------------
    def _handle_end_charge(self, fields, client_socket, client_id):
        if len(fields) < 3:
            return

        driver_id = fields[1]
        cp_id = fields[2]

        with self.lock:
            cp = self.charging_points.get(cp_id)
        pending_weather_stop = False
        if cp:
            pending_weather_stop = bool(cp.pop("pending_weather_stop", False))

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

            cp["state"] = CP_STATES["ACTIVATED"]
            # Tell UI CP is ACTIVATED again (so card changes from SUPPLYING)
            self._broadcast_to_ui_protocol(
                Protocol.build_message("CP_STATE", cp_id, json.dumps({"state": CP_STATES["ACTIVATED"]}))
            )

            # Also broadcast SUPPLY_END so UI closes the session visually
            self._broadcast_to_ui_protocol(
                Protocol.build_message(MessageTypes.SUPPLY_END, cp_id, driver_id, total_kwh, total_amount)
            )

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

        self._send_to_cp(cp_id, Protocol.build_message(MessageTypes.END_SUPPLY, cp_id))

        try:
            ticket = Protocol.build_message(MessageTypes.TICKET, cp_id, total_kwh, total_amount)
            client_socket.send(Protocol.encode(ticket))
        except Exception:
            pass

        self._broadcast_to_ui_protocol(Protocol.build_message("DRIVER_STOP", cp_id, driver_id))

    # -------------------------------------------------------------------------
    # FAULT / RECOVERY
    # -------------------------------------------------------------------------
    def _handle_fault(self, fields, client_id):
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
        self._broadcast_to_ui_protocol(Protocol.build_message(MessageTypes.FAULT, cp_id, json.dumps({"state": CP_STATES["OUT_OF_ORDER"]})))

    def _handle_recovery(self, fields):
        if len(fields) < 2:
            return
        cp_id = fields[1]
        with self.lock:
            cp = self.charging_points.get(cp_id)
            if cp and cp["state"] != CP_STATES["SUPPLYING"]:
                cp["state"] = CP_STATES["ACTIVATED"]

        self.add_log("EV_Central", f"RECOVERY {cp_id}")
        self._broadcast_to_ui_protocol(Protocol.build_message(MessageTypes.RECOVERY, cp_id, json.dumps({"state": CP_STATES["ACTIVATED"]})))

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
    # Registry polling (NO fake entries)
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
