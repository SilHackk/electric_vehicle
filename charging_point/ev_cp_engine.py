# ============================================================================
# EVCharging System - EV_CP_E (Charging Point Engine) - FIXED
# - No shutdown on temporary disconnect
# - Automatic reconnect loop
# - Threads started only once
# - Incoming messages can be encrypted (decrypt attempt)
# - Removes undefined self.connected usage
# ============================================================================

import socket
import threading
import time
import sys
import requests
import os
import json

from config import CP_STATES, SUPPLY_UPDATE_INTERVAL
from shared.protocol import Protocol, MessageTypes
from shared.kafka_client import KafkaClient
from shared.encryption import EncryptionManager
from shared.audit_logger import log_charge, log_state

REGISTRY_URL = "http://registry:5001"


class EVCPEngine:
    def __init__(self, cp_id, latitude, longitude, price_per_kwh,
                 central_host="localhost", central_port=5000,
                 username=None, password=None):

        self.cp_id = cp_id
        self.latitude = float(latitude)
        self.longitude = float(longitude)
        self.price_per_kwh = float(price_per_kwh)

        self.central_host = central_host
        self.central_port = central_port

        self.state = CP_STATES["ACTIVATED"]
        self.current_driver = None
        self.current_session = None
        self.charging_complete = False

        self.central_socket = None
        self.running = True
        self.connected = False  # ✅ define it
        self.lock = threading.Lock()

        self.username = username
        self.password = password
        self.encryption = EncryptionManager()
        self.symmetric_key = None

        if not self.username or not self.password:
            self._fetch_credentials_from_registry()

        self.kafka = KafkaClient(f"EV_CP_E_{cp_id}")

        # Thread control (start once)
        self._threads_started = False

        print(f"[{self.cp_id}] Engine initialized")
        print(f"[{self.cp_id}] Location: ({self.latitude}, {self.longitude})")
        print(f"[{self.cp_id}] Price: {self.price_per_kwh} €/kWh")

    # ---------------------------------------------------------------------

    def _fetch_credentials_from_registry(self):
        while True:
            try:
                print(f"[{self.cp_id}] 🔍 Checking Registry for credentials...")
                r = requests.get(f"{REGISTRY_URL}/list", timeout=5)

                if r.status_code != 200:
                    print(f"[{self.cp_id}] Registry HTTP {r.status_code}, retrying...")
                    time.sleep(3)
                    continue

                data = r.json()

                # Case 1: {"charging_points": [...]}
                if isinstance(data, dict) and "charging_points" in data:
                    cps = data["charging_points"]

                # Case 2: [...] (list)
                elif isinstance(data, list):
                    cps = data

                else:
                    cps = []

                for cp in cps:
                    if cp.get("cp_id") == self.cp_id:
                        self.username = cp.get("username")
                        self.password = cp.get("password")

                        if self.username and self.password:
                            print(f"[{self.cp_id}] ✅ Credentials loaded from Registry")
                            return

                print(f"[{self.cp_id}] ⏳ Not yet registered, retrying in 3s...")
                time.sleep(3)

            except Exception as e:
                print(f"[{self.cp_id}] Registry error: {e}, retrying in 3s...")
                time.sleep(3)




    # ---------------------------------------------------------------------

    def _encrypt(self, msg):
        if not self.symmetric_key:
            return msg
        return self.encryption.encrypt(msg, self.symmetric_key)

    def _decrypt_if_needed(self, msg):
        """
        Central might send plaintext or encrypted.
        Parse plaintext first, BUT only accept it if it looks like a real protocol type.
        Otherwise try decrypt.
        """
        # Types we expect to RECEIVE from Central
        valid_types = {
            MessageTypes.AUTHORIZE,
            MessageTypes.END_SUPPLY,
            "STOP_SUPPLY",
            "RESUME_SUPPLY",
            MessageTypes.DENY,
            MessageTypes.ACKNOWLEDGE,
        }

        # 1) Try plaintext parse
        try:
            fields = Protocol.parse_message(msg)
            if fields and fields[0] in valid_types:
                return fields
        except Exception:
            pass

        # 2) Try decrypt if we have a key
        if not self.symmetric_key:
            return None

        try:
            dec = self.encryption.decrypt(msg, self.symmetric_key)
            fields = Protocol.parse_message(dec)
            return fields
        except Exception:
            return None

    # ---------------------------------------------------------------------

    def _close_central_socket(self):
        try:
            if self.central_socket:
                self.central_socket.close()
        except Exception:
            pass
        self.central_socket = None
        self.connected = False

    # ---------------------------------------------------------------------

    def connect_to_central(self):
        """
        One connect attempt.
        Returns True if connected, False otherwise.
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            s.connect((self.central_host, self.central_port))
            s.settimeout(None)

            register = Protocol.build_message(
                MessageTypes.REGISTER, "CP", self.cp_id,
                str(self.latitude), str(self.longitude),
                str(self.price_per_kwh),
                self.username, self.password
            )
            s.send(Protocol.encode(register))

            data = s.recv(4096)
            msg, ok = Protocol.decode(data)
            if not ok:
                s.close()
                return False

            fields = Protocol.parse_message(msg)
            if fields[0] != MessageTypes.ACKNOWLEDGE or len(fields) < 3 or fields[2] != "OK":
                s.close()
                return False

            if len(fields) > 3:
                self.symmetric_key = fields[3].encode()

            self.central_socket = s
            self.connected = True

            log_state("CP", self.cp_id, "DISCONNECTED", "CONNECTED")
            print(f"[{self.cp_id}] ✅ Connected to CENTRAL")

            # Start threads only once
            if not self._threads_started:
                self._threads_started = True
                threading.Thread(target=self._listen_central_loop, daemon=True).start()
                threading.Thread(target=self._status_loop, daemon=True).start()
                threading.Thread(target=self._display_loop, daemon=True).start()

            return True

        except Exception as e:
            print(f"[{self.cp_id}] ❌ Central connection failed: {e}")
            self._close_central_socket()
            return False

    # ---------------------------------------------------------------------
    # LISTEN LOOP (never shuts down engine; just marks disconnected)
    # ---------------------------------------------------------------------

    def _listen_central_loop(self):
        buffer = b""
        while self.running:
            if not self.connected or not self.central_socket:
                time.sleep(1)
                continue

            try:
                data = self.central_socket.recv(4096)
                if not data:
                    print(f"[{self.cp_id}] Central closed connection (listen loop)")
                    self._close_central_socket()
                    continue

                buffer += data

                while buffer:
                    msg, ok = Protocol.decode(buffer)
                    if not ok:
                        break

                    etx = buffer.find(b"\x03")
                    buffer = buffer[etx + 2:] if etx != -1 else b""

                    fields = self._decrypt_if_needed(msg)
                    if not fields:
                        continue

                    if fields[0] == MessageTypes.AUTHORIZE:
                        self._handle_authorize(fields)

                    elif fields[0] == MessageTypes.END_SUPPLY:
                        self._handle_end_supply()

                    elif fields[0] == "STOP_SUPPLY":
                        self._handle_stop_supply()

                    elif fields[0] == "RESUME_SUPPLY":
                        self._handle_resume_supply()


            except (ConnectionResetError, BrokenPipeError, OSError):
                print(f"[{self.cp_id}] Lost connection to Central (listen loop) - reconnecting...")
                self._close_central_socket()
            except Exception as e:
                # Do not kill engine
                print(f"[{self.cp_id}] Listen error: {e}")
                self._close_central_socket()

    # ---------------------------------------------------------------------

    def _handle_authorize(self, fields):
        # expected: AUTHORIZE, driver_id, cp_id, kwh_needed, ...
        driver_id = fields[1]
        kwh_needed = float(fields[3])

        with self.lock:
            if self.state != CP_STATES["ACTIVATED"]:
                return

            self.state = CP_STATES["SUPPLYING"]
            self.current_driver = driver_id
            self.current_session = {
                "start": time.time(),
                "kwh_needed": kwh_needed,
                "kwh_delivered": 0.0
            }
            self.charging_complete = False

        log_charge("CP", self.cp_id, driver_id, "CHARGE_START", kwh=kwh_needed)
        print(f"[{self.cp_id}] ✅ Charging started for {driver_id}")

    # ---------------------------------------------------------------------

    def _handle_end_supply(self):
        with self.lock:
            if not self.current_session:
                return

            elapsed = time.time() - self.current_session["start"]
            total_kwh = min(
                self.current_session["kwh_needed"],
                (elapsed / 14.0) * self.current_session["kwh_needed"]
            )
            total_amount = round(total_kwh * self.price_per_kwh, 2)

            msg = Protocol.build_message(
                MessageTypes.SUPPLY_END,
                self.cp_id,
                self.current_driver,
                f"{total_kwh:.3f}",
                f"{total_amount:.2f}"
            )

            # send only if connected
            if self.central_socket and self.connected:
                try:
                    self.central_socket.send(Protocol.encode(self._encrypt(msg)))
                except Exception:
                    self._close_central_socket()

            self.state = CP_STATES["ACTIVATED"]
            self.current_driver = None
            self.current_session = None
            self.charging_complete = False

        print(f"[{self.cp_id}] ✅ Charging finished")

    # ---------------------------------------------------------------------
    def _handle_stop_supply(self):
        with self.lock:
            # If currently supplying, mark it to stop at end (do not kill current charge)
            if self.state == CP_STATES["SUPPLYING"]:
                self.charging_complete = True  # use as "pending stop" flag
                print(f"[{self.cp_id}] ❄️ STOP_SUPPLY received: will stop after charge ends")
                return

            # If not supplying, go out of order immediately
            self.state = CP_STATES["OUT_OF_ORDER"]
            print(f"[{self.cp_id}] ❄️ STOP_SUPPLY received: OUT_OF_ORDER")

    def _handle_resume_supply(self):
        with self.lock:
            # Resume only if not supplying
            if self.state != CP_STATES["SUPPLYING"]:
                self.state = CP_STATES["ACTIVATED"]
            self.charging_complete = False
        print(f"[{self.cp_id}] ✅ RESUME_SUPPLY received: ACTIVATED")
#-----------------------------------------------------------------------
    def _status_loop(self):
        TOTAL_SECONDS = 15.0

        while self.running:
            time.sleep(SUPPLY_UPDATE_INTERVAL)

            if not self.connected or not self.central_socket:
                continue

            try:
                # Heartbeat
                hb = Protocol.build_message("HEARTBEAT", self.cp_id, self.state)
                self.central_socket.send(Protocol.encode(self._encrypt(hb)))

                with self.lock:
                    if self.state == CP_STATES["SUPPLYING"] and self.current_session:
                        s = self.current_session
                        kwh_needed = float(s["kwh_needed"])
                        elapsed = time.time() - s["start"]

                        remaining = kwh_needed - float(s["kwh_delivered"])
                        if remaining <= 0:
                            remaining = 0.0

                        # increment per tick so total finishes in TOTAL_SECONDS
                        kwh_inc = (kwh_needed / TOTAL_SECONDS) * SUPPLY_UPDATE_INTERVAL
                        if kwh_inc > remaining:
                            kwh_inc = remaining

                        s["kwh_delivered"] += kwh_inc

                        # send SUPPLY_UPDATE as INCREMENT
                        amount_inc = round(kwh_inc * self.price_per_kwh, 2)
                        upd = Protocol.build_message(
                            MessageTypes.SUPPLY_UPDATE,
                            self.cp_id,
                            f"{kwh_inc:.6f}",
                            f"{amount_inc:.2f}"
                        )
                        self.central_socket.send(Protocol.encode(self._encrypt(upd)))

                        # if finished, send SUPPLY_END once
                        if s["kwh_delivered"] >= kwh_needed - 1e-9:
                            total_kwh = kwh_needed
                            total_amount = round(total_kwh * self.price_per_kwh, 2)

                            end_msg = Protocol.build_message(
                                MessageTypes.SUPPLY_END,
                                self.cp_id,
                                self.current_driver,
                                f"{total_kwh:.3f}",
                                f"{total_amount:.2f}"
                            )
                            self.central_socket.send(Protocol.encode(self._encrypt(end_msg)))

                            # reset state
                            self.state = CP_STATES["ACTIVATED"]
                            self.current_driver = None
                            self.current_session = None
                            self.charging_complete = False


            except Exception as e:
                print(f"[{self.cp_id}] Status loop error: {e}")
                self._close_central_socket()

    # ---------------------------------------------------------------------

    def _display_loop(self):
        while self.running:
            time.sleep(2)
            if self.state == CP_STATES["ACTIVATED"]:
                print(f"[{self.cp_id}] AVAILABLE")
            elif self.state == CP_STATES["SUPPLYING"]:
                print(f"[{self.cp_id}] SUPPLYING")

    # ---------------------------------------------------------------------

    def _shutdown(self, reason):
        # keep shutdown for manual exit only
        if not self.running:
            return
        print(f"[{self.cp_id}] ❌ Shutdown: {reason}")
        self.running = False
        self._close_central_socket()
        try:
            self.kafka.close()
        except Exception:
            pass

    # ---------------------------------------------------------------------

    def run(self):
        """
        Main loop: keep trying to connect forever.
        Engine never exits unless container stops.
        """
        while self.running:
            if not self.connected:
                self.connect_to_central()
                if not self.connected:
                    time.sleep(2)
                    continue
            time.sleep(1)


# -------------------------------------------------------------------------

if __name__ == "__main__":
    cp_id = sys.argv[1]
    lat = sys.argv[2]
    lon = sys.argv[3]
    price = sys.argv[4]
    host = sys.argv[5] if len(sys.argv) > 5 else "localhost"
    port = int(sys.argv[6]) if len(sys.argv) > 6 else 5000

    engine = EVCPEngine(cp_id, lat, lon, price, host, port)
    engine.run()
