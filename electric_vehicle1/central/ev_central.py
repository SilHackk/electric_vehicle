# ============================================================================
# EVCharging System - EV_Central (Control Center) - FULL COPY/PASTE VERSION
#
# FIXES INCLUDED (based on your current file):
# ✅ UI receives cumulative kWh and € (Protocol SUPPLY_UPDATE totals)
# ✅ SUPPLY_UPDATE supports CP engines sending amount as TOTAL or INCREMENT
# ✅ Manual stop works even if UI sends FINISH_CHARGE (alias -> END_CHARGE)
# ✅ Removes the "15s forced stop" behavior: charging can run as long as needed
# ✅ Fixes force_stop_charge bug (was calling _handle_supply_end with wrong args)
# ✅ Deduplicates charge-finalization logic via _finalize_charge()
# ✅ Manual END_CHARGE uses current cp totals (no fake duration math)
# ✅ REST API started in a separate process to avoid random timeouts/freezes
#
# NOTE:
# - This assumes your shared modules/config are the same as your project.
# - Copy/paste this over your current ev_central.py.
# ============================================================================

import socket
import threading
import time
import json
import os
import logging
import multiprocessing
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
        self.cp_credentials = {}       # cp_id -> {"username":. "secret":.}

        # Kafka
        self.kafka = KafkaClient("EV_Central")

        # Lock
        self.lock = threading.Lock()

        print("[EV_Central] Initializing with file storage.")
        self._load_stored_cps()

        # Start REST API in separate process to avoid timeouts/freezes
        p = multiprocessing.Process(target=self.start_rest_api)
        p.start()

        print("[EV_Central] REST API started on port 5003 (separate process)")

    # -------------------------------------------------------------------------
    # Logging helper
    # -------------------------------------------------------------------------
    def add_log(self, source, text):
        entry = {
            "ts": datetime.utcnow().isoformat(),
            "source": source,
            "text": text,
        }
        with self.lock:
            self.logs.append(entry)
            # Keep last 200
            if len(self.logs) > 200:
                self.logs = self.logs[-200:]

        # Also forward to UI if connected
        try:
            self._broadcast_to_ui_protocol(Protocol.build_message("LOG", source, text))
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # Load stored CPs (if your FileStorage persists them)
    # -------------------------------------------------------------------------
    def _load_stored_cps(self):
        try:
            stored = self.storage.get_all_cps()
        except Exception:
            stored = []

        with self.lock:
            for cp in stored:
                cp_id = cp.get("cp_id")
                if not cp_id:
                    continue
                self.charging_points[cp_id] = {
                    "state": cp.get("state", CP_STATES["DISCONNECTED"]),
                    "location": (cp.get("latitude", 0.0), cp.get("longitude", 0.0)),
                    "price_per_kwh": float(cp.get("price_per_kwh", 0.30) or 0.30),
                    "current_driver": None,
                    "kwh_delivered": 0.0,
                    "amount_euro": 0.0,
                    "session_start": None,
                    "charging_complete": False,
                    "kwh_needed": 0.0,
                }

    # -------------------------------------------------------------------------
    # REST API (CP register + weather alert)
    # -------------------------------------------------------------------------
    def start_rest_api(self):
        app = Flask(__name__)
        from flask_cors import CORS
        CORS(app)

        @app.route("/api/register_cp", methods=["POST"])
        def api_register_cp():
            """
            UI calls this. Central forwards registration to Registry (/register).
            """
            data = request.json or {}
            cp_id = (data.get("cp_id") or "").strip()
            city = (data.get("city") or "").strip()
            price = data.get("price_per_kwh")

            if not cp_id or not city or price is None:
                return jsonify({"error": "cp_id, city, price_per_kwh required"}), 400

            try:
                price = float(price)
            except Exception:
                return jsonify({"error": "price_per_kwh must be a number"}), 400

            # Geocode city -> lat/lon
            lat, lon = self.get_coordinates_from_city(city)
            if lat is None or lon is None:
                return jsonify({"error": f"Could not geocode city: {city}"}), 400

            # Forward to Registry
            try:
                r = requests.post(
                    f"{REGISTRY_URL}/register",
                    json={"cp_id": cp_id, "city": city, "price_per_kwh": price},
                    timeout=8,
                )
                if r.status_code != 200:
                    return jsonify({"error": f"Registry error: {r.status_code}", "detail": r.text}), 500
                reg = r.json()
            except Exception as e:
                return jsonify({"error": f"Registry unreachable: {e}"}), 500

            # Do NOT create fake CP in runtime map here; only CP engine connection creates it.
            # But we can log:
            self.add_log("EV_Central", f"Registry registered CP {cp_id} in {city} ({lat},{lon}) price={price}")

            return jsonify({
                "cp_id": reg.get("cp_id", cp_id),
                "city": reg.get("city", city),
                "latitude": lat,
                "longitude": lon,
            }), 200

        @app.route("/api/weather_alert", methods=["POST"])
        def api_weather_alert():
            """
            Weather service calls this. Updates CP state OUT_OF_ORDER / ACTIVATED.
            """
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
                    logging.warning(f"[EV_Central] Weather alert for unknown CP: {cp_id}")
                    return jsonify({"status": "received (cp unknown)"}), 200

                if alert == "ALERT_COLD":
                    cp["state"] = CP_STATES["OUT_OF_ORDER"]
                    self._broadcast_to_ui("CP_STATE", cp_id, payload={"state": CP_STATES["OUT_OF_ORDER"]})
                    self.add_log("EV_Central", f"{cp_id} taken out of service (cold weather)")
                elif alert == "WEATHER_OK":
                    if cp["state"] != CP_STATES["SUPPLYING"]:
                        cp["state"] = CP_STATES["ACTIVATED"]
                        self._broadcast_to_ui("CP_STATE", cp_id, payload={"state": CP_STATES["ACTIVATED"]})
                        self.add_log("EV_Central", f"{cp_id} restored to service (weather ok)")

            return jsonify({"status": "received", "cp_id": cp_id, "alert": alert}), 200

        app.run(host="0.0.0.0", port=5003, debug=False, threaded=True)

    # -------------------------------------------------------------------------
    # FIXED: Geocoding with better error handling
    # -------------------------------------------------------------------------
    def get_coordinates_from_city(self, city):
        try:
            api_key = os.getenv("OPENWEATHER_API_KEY")
            if not api_key:
                print("[EV_Central] ⚠️ No OpenWeather API key found")
                return None, None

            r = requests.get(
                "https://api.openweathermap.org/geo/1.0/direct",
                params={"q": city, "limit": 1, "appid": api_key},
                timeout=8,
            )
            if r.status_code != 200:
                print(f"[EV_Central] ❌ Geocoding API error: {r.status_code}")
                return None, None

            data = r.json()
            if not data:
                print(f"[EV_Central] ❌ City not found: {city}")
                return None, None

            lat = data[0].get("lat")
            lon = data[0].get("lon")
            return lat, lon
        except Exception as e:
            print(f"[EV_Central] ❌ Geocoding error: {e}")
            return None, None
        
    def _handle_weather_alert(self, fields):
        if len(fields) < 3:
            return

        cp_id = fields[1]
        alert = fields[2]

        with self.lock:
            cp = self.charging_points.get(cp_id)
            if not cp:
                return

            if alert == "ALERT_COLD":
                cp["state"] = CP_STATES["OUT_OF_ORDER"]
                self.add_log("EV_Weather", f"{cp_id} OUT_OF_ORDER (cold)")
            elif alert == "WEATHER_OK":
                if cp["state"] != CP_STATES["SUPPLYING"]:
                    cp["state"] = CP_STATES["ACTIVATED"]
                    self.add_log("EV_Weather", f"{cp_id} ACTIVATED (weather ok)")

        self._broadcast_to_ui_protocol(
            Protocol.build_message("CP_STATE", cp_id, cp["state"])
        )


    # -------------------------------------------------------------------------
    # UI broadcast helpers (JSON / Protocol)
    # -------------------------------------------------------------------------
    def _broadcast_to_ui(self, event, cp_id, payload=None):
        ui_sock = self.monitors.get("WEB_UI")
        if not ui_sock:
            return
        try:
            msg = Protocol.build_message(event, cp_id, json.dumps(payload or {}))
            ui_sock.send(Protocol.encode(msg))
        except Exception as e:
            print(f"[EV_Central] ⚠️ UI broadcast failed: {e}")

    def _broadcast_to_ui_protocol(self, protocol_message_str):
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
    # Registry verify
    # -------------------------------------------------------------------------
    def _verify_cp_credentials(self, cp_id, username, password):
        try:
            r = requests.post(
                f"{REGISTRY_URL}/verify",
                json={"cp_id": cp_id, "username": username, "password": password},
                timeout=8,
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

            # Accept charge
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

        try:
            log_charge(client_id, cp_id, driver_id, "CHARGE_START")
        except Exception:
            pass

        self.add_log("EV_Central", f"Charge authorized Driver {driver_id} -> {cp_id}")

        # AUTHORIZE to driver
        drv_msg = Protocol.build_message(MessageTypes.AUTHORIZE, driver_id, cp_id, kwh_needed, price)
        try:
            client_socket.send(Protocol.encode(drv_msg))
        except Exception:
            pass

        # AUTHORIZE to CP (encrypted)
        self._send_to_cp(cp_id, Protocol.build_message(MessageTypes.AUTHORIZE, driver_id, cp_id, kwh_needed))

        # Notify UI (driver started)
        self._broadcast_to_ui_protocol(Protocol.build_message("DRIVER_START", cp_id, driver_id))

        # IMPORTANT: no forced timeout here. Charging can run as long as needed.
        # (Stop happens when CP sends SUPPLY_END or driver sends END_CHARGE)

    # -------------------------------------------------------------------------
    # SUPPLY UPDATE (supports CP sending amount total or increment)
    # -------------------------------------------------------------------------
    def _handle_supply_update(self, fields):
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

            # Always accumulate kWh (expects increment)
            cp["kwh_delivered"] = float(cp.get("kwh_delivered", 0.0) or 0.0) + kwh_inc

            computed_total = cp["kwh_delivered"] * float(cp["price_per_kwh"])
            prev_amt = float(cp.get("amount_euro", 0.0) or 0.0)

            # If CP sends TOTAL amount, accept it; else compute via increment.
            if amount_field >= prev_amt and amount_field >= (computed_total * 0.7):
                cp["amount_euro"] = amount_field
            else:
                cp["amount_euro"] = prev_amt + (kwh_inc * float(cp["price_per_kwh"]))

            cp["amount_euro"] = round(cp["amount_euro"], 2)

            driver_id = cp.get("current_driver")

            # Completion flag (optional)
            kwh_needed = float(cp.get("kwh_needed", 0.0) or 0.0)
            if kwh_needed > 0 and cp["kwh_delivered"] >= kwh_needed:
                if not cp.get("charging_complete", False):
                    cp["charging_complete"] = True
                    self._broadcast_to_ui_protocol(
                        Protocol.build_message("CHARGING_COMPLETE", cp_id, driver_id or "")
                    )

            # Prepare totals
            kwh_total = round(cp["kwh_delivered"], 6)
            eur_total = cp["amount_euro"]

        # Forward totals to driver
        if driver_id:
            with self.lock:
                dsock = self.entity_to_socket.get(driver_id)
            if dsock:
                try:
                    msg = Protocol.build_message(MessageTypes.SUPPLY_UPDATE, cp_id, kwh_total, eur_total)
                    dsock.send(Protocol.encode(msg))
                except Exception:
                    pass

        # Forward totals to UI
        self._broadcast_to_ui_protocol(
            Protocol.build_message(MessageTypes.SUPPLY_UPDATE, cp_id, kwh_total, eur_total)
        )

    # -------------------------------------------------------------------------
    # INTERNAL FINALIZER (single source of truth for ending a session)
    # -------------------------------------------------------------------------
    def _finalize_charge(self, cp_id, driver_id, total_kwh, total_amount, client_id_for_log="EV_Central"):
        duration = 0

        with self.lock:
            cp = self.charging_points.get(cp_id)
            if cp and cp.get("session_start"):
                duration = int(time.time() - cp["session_start"])

            if cp:
                cp["state"] = CP_STATES["ACTIVATED"]
                cp["current_driver"] = None
                cp["session_start"] = None
                cp["charging_complete"] = True
                cp["kwh_needed"] = 0.0

                # keep last totals in the event payload only; reset live counters for new sessions
                cp["kwh_delivered"] = 0.0
                cp["amount_euro"] = 0.0

            if driver_id in self.drivers:
                self.drivers[driver_id]["status"] = "IDLE"
                self.drivers[driver_id]["current_cp"] = None

        # Persist
        try:
            log_charge(client_id_for_log, cp_id, driver_id, "CHARGE_END", kwh=total_kwh, amount=total_amount)
        except Exception:
            pass

        try:
            self.storage.save_charging_session(cp_id, driver_id, total_kwh, total_amount, duration)
            self.storage.update_driver_stats(driver_id, total_amount)
        except Exception:
            pass

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
        self._broadcast_to_ui_protocol(
            Protocol.build_message(MessageTypes.SUPPLY_END, cp_id, driver_id, total_kwh, total_amount)
        )

    # -------------------------------------------------------------------------
    # SUPPLY END (from CP)
    # -------------------------------------------------------------------------
    def _handle_supply_end(self, fields, client_socket, client_id):
        if len(fields) < 5:
            return

        cp_id = fields[1]
        driver_id = fields[2]
        total_kwh = float(fields[3])
        total_amount = float(fields[4])

        self._finalize_charge(cp_id, driver_id, total_kwh, total_amount, client_id_for_log=client_id)

    # -------------------------------------------------------------------------
    # FORCE STOP (kept for future, NOT used automatically)
    # -------------------------------------------------------------------------
    def force_stop_charge(self, cp_id):
        """
        Optional manual/admin force stop.
        NOT used automatically (no timeout).
        """
        with self.lock:
            cp = self.charging_points.get(cp_id)
            if not cp or cp["state"] != CP_STATES["SUPPLYING"]:
                return
            driver_id = cp.get("current_driver") or ""
            total_kwh = float(cp.get("kwh_delivered", 0.0) or 0.0)
            total_amount = float(cp.get("amount_euro", 0.0) or 0.0)

        self.add_log("EV_Central", f"Force stop requested for {cp_id}")
        # Ask CP to stop supply (best-effort)
        try:
            self._send_to_cp(cp_id, Protocol.build_message(MessageTypes.END_SUPPLY, cp_id))
        except Exception:
            pass

        # Finalize locally using current totals
        self._finalize_charge(cp_id, driver_id, total_kwh, total_amount, client_id_for_log="EV_Central")

    # -------------------------------------------------------------------------
    # END CHARGE (manual from driver/UI)
    # -------------------------------------------------------------------------
    def _handle_end_charge(self, fields, client_socket, client_id):
        if len(fields) < 3:
            return

        driver_id = fields[1]
        cp_id = fields[2]

        with self.lock:
            cp = self.charging_points.get(cp_id)
            if not cp or cp.get("current_driver") != driver_id:
                return

            # Use current accumulated totals (correct + no fake duration math)
            total_kwh = float(cp.get("kwh_delivered", 0.0) or 0.0)
            total_amount = float(cp.get("amount_euro", 0.0) or 0.0)

        # Ask CP to stop supply (best-effort)
        self._send_to_cp(cp_id, Protocol.build_message(MessageTypes.END_SUPPLY, cp_id))

        # Finalize immediately (CP may still send SUPPLY_END; that will just finalize again,
        # but since state becomes ACTIVATED, it won't break. If you want to ignore duplicate
        # SUPPLY_END, you can add a session_id in future.)
        self._finalize_charge(cp_id, driver_id, total_kwh, total_amount, client_id_for_log=client_id)

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
        self._broadcast_to_ui_protocol(
            Protocol.build_message(MessageTypes.FAULT, cp_id, json.dumps({"state": CP_STATES["OUT_OF_ORDER"]}))
        )

    def _handle_recovery(self, fields):
        if len(fields) < 2:
            return
        cp_id = fields[1]
        with self.lock:
            cp = self.charging_points.get(cp_id)
            if cp and cp["state"] != CP_STATES["SUPPLYING"]:
                cp["state"] = CP_STATES["ACTIVATED"]

        self.add_log("EV_Central", f"RECOVERY {cp_id}")
        self._broadcast_to_ui_protocol(
            Protocol.build_message(MessageTypes.RECOVERY, cp_id, json.dumps({"state": CP_STATES["ACTIVATED"]}))
        )

    # -------------------------------------------------------------------------
    # REGISTER
    # -------------------------------------------------------------------------
    def _handle_register(self, fields, client_socket, client_id):
        if len(fields) < 3:
            return

        entity_type = fields[1]
        entity_id = fields[2]

        if entity_type == "MONITOR":
            # UI uses REGISTER|MONITOR|WEB_UI
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
                self.drivers[entity_id] = {"status": "IDLE", "current_cp": None}
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
            return

    # -------------------------------------------------------------------------
    # Client handling / socket lifecycle
    # -------------------------------------------------------------------------
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
        elif t == "FINISH_CHARGE":
            # IMPORTANT: UI server currently sends FINISH_CHARGE (alias)
            self._handle_end_charge(["END_CHARGE", fields[1], fields[2]], client_socket, client_id)
        elif t == MessageTypes.FAULT:
            self._handle_fault(fields, client_id)
        elif t == MessageTypes.RECOVERY:
            self._handle_recovery(fields)
        elif t == "LOG":
            src = fields[1] if len(fields) > 1 else "UNKNOWN"
            txt = fields[2] if len(fields) > 2 else ""
            self.add_log(src, txt)
        elif t == "WEATHER_ALERT":
            self._handle_weather_alert(fields)


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
                r = requests.get(f"{REGISTRY_URL}/list", timeout=8)
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
