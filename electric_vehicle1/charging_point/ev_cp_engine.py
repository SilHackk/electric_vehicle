# ============================================================================
# EVCharging System - EV_CP_E (Charging Point Engine) - UPDATED DISPLAY
# ============================================================================

import socket
import threading
import time
import sys
import json
from datetime import datetime
from config import CP_BASE_PORT, CP_STATES, COLORS, SUPPLY_UPDATE_INTERVAL
from shared.protocol import Protocol, MessageTypes
from shared.kafka_client import KafkaClient
import requests
from shared.encryption import EncryptionManager
from shared.audit_logger import log_charge, log_state

import os
REGISTRY_URL = os.environ.get("REGISTRY_URL", "http://registry:5001")


class EVCPEngine:
    def __init__(self, cp_id, latitude, longitude, price_per_kwh, 
                 central_host="localhost", central_port=5000,
                 monitor_host="localhost", monitor_port=None,
                 username=None, password=None):
        self.cp_id = cp_id
        self.latitude = latitude
        self.longitude = longitude
        self.price_per_kwh = float(price_per_kwh)

        self.central_host = central_host
        self.central_port = central_port
        self.monitor_host = monitor_host
        self.monitor_port = monitor_port or (CP_BASE_PORT + int(cp_id.split('-')[1]))

        self.state = CP_STATES["ACTIVATED"]
        self.current_driver = None
        self.current_session = None
        self.charging_complete = False  # NEW: Track when 100% reached

        self.central_socket = None
        self.monitor_socket = None
        self.running = True
        self.lock = threading.Lock()

        self.username = username
        self.password = password
        self.encryption = EncryptionManager()
        self.symmetric_key = None  # Gaus iš Central po autentifikavimo
        
        # Jei kredencialai nepateikti, gauti iš Registry
        if not self.username or not self.password:
            self._fetch_credentials_from_registry()

        # Kafka
        self.kafka = KafkaClient(f"EV_CP_E_{cp_id}")

        # Health status
        self.health_ok = True
        self.simulate_fault = False

        print(f"[{self.cp_id}] Engine initializing...")

    def _fetch_credentials_from_registry(self):
        """Fetch credentials from Registry (if CP was pre-registered)"""
        try:
            response = requests.get(f"{REGISTRY_URL}/list", timeout=5)
            response.raise_for_status()

            data = response.json()
            for cp in data.get("charging_points", []):
                if cp.get("cp_id") == self.cp_id:
                    self.username = cp.get("username")
                    self.password = cp.get("password")

                    if not self.username or not self.password:
                        print(f"[{self.cp_id}] ❌ Missing credentials in Registry")
                        sys.exit(1)

                    print(f"[{self.cp_id}] ✅ Credentials loaded from Registry")
                    print(f"[{self.cp_id}] 👤 Username: {self.username}")
                    return

            print(f"[{self.cp_id}] ❌ Not registered in Registry")
            print(f"[{self.cp_id}] 📝 Please register via Web UI / Registry API")
            sys.exit(1)

        except Exception as e:
            print(f"[{self.cp_id}] Registry fetch error: {e}")
            sys.exit(1)

    def connect_to_central(self):
        """Connect to central system via socket"""
        try:
            self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.central_socket.connect((self.central_host, self.central_port))

            # Register with CENTRAL
            register_msg = Protocol.encode(
                Protocol.build_message(
                    "REGISTER", "CP", self.cp_id,
                    self.latitude, self.longitude, self.price_per_kwh,
                    self.username,   # Naujas laukas
                    self.password    # Naujas laukas
                )
            )
            self.central_socket.send(register_msg)

            buffer = b''
            self.central_socket.settimeout(10)
            
            try:
                data = self.central_socket.recv(4096)
                buffer += data
                message, is_valid = Protocol.decode(buffer)
                
                if is_valid:
                    fields = Protocol.parse_message(message)
                    
                    if fields[0] == MessageTypes.ACKNOWLEDGE:
                        if fields[2] == "OK":
                            # Gauti simetrinį raktą
                            if len(fields) > 3:
                                key_str = fields[3]
                                self.symmetric_key = key_str.encode()
                                print(f"[{self.cp_id}] 🔐 Received encryption key")
                            
                            log_state("127.0.0.1", self.cp_id, "DISCONNECTED", "CONNECTED")

                            self.send_log("Registered & Authenticated with CENTRAL")
                            print(f"[{self.cp_id}] ✅ Authenticated with CENTRAL")
                            
                            # Start listening for messages from CENTRAL
                            thread = threading.Thread(
                                target=self._listen_central,
                                daemon=True
                            )
                            thread.start()
                            
                            return True
                        
                    elif fields[0] == MessageTypes.DENY:

                        log_state("127.0.0.1", self.cp_id, "DISCONNECTED", "DENIED")

                        print(f"[{self.cp_id}] ❌ Authentication DENIED: {fields[2]}")
                        return False
            
            except socket.timeout:
                print(f"[{self.cp_id}] ❌ Authentication timeout")
                return False

        except Exception as e:
            print(f"[{self.cp_id}] Failed to connect to CENTRAL: {e}")
            return False

    def _encrypt_message(self, message):
        """Encrypt message using symmetric key"""
        if not self.symmetric_key:
            return message  # Jei nėra rakto, siųsti nešifruotą
        
        return self.encryption.encrypt(message, self.symmetric_key)

    def _decrypt_message(self, encrypted_message):
        """Decrypt message using symmetric key"""
        if not self.symmetric_key:
            return encrypted_message
        
        try:
            return self.encryption.decrypt(encrypted_message, self.symmetric_key)
        except Exception as e:
            print(f"[{self.cp_id}] Decryption error: {e}")
            return None

    def listen_for_monitor(self):
        """Listen for Monitor connections via socket"""
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(('0.0.0.0', self.monitor_port))
            server_socket.listen(1)

            print(f"[{self.cp_id}] Listening for monitor on port {self.monitor_port}")

            while self.running:
                try:
                    client_socket, addr = server_socket.accept()
                    self.monitor_socket = client_socket
                    print(f"[{self.cp_id}] Monitor connected")

                    # Start health check listener
                    thread = threading.Thread(
                        target=self._listen_monitor,
                        daemon=True
                    )
                    thread.start()

                except Exception as e:
                    if self.running:
                        print(f"[{self.cp_id}] Monitor connection error: {e}")

        except Exception:
            if self.running:
                print(f"[{self.cp_id}] Monitor listener error")

    def _listen_central(self):
        """Listen for messages from CENTRAL via socket"""
        buffer = b''
        try:
            while self.running:
                try:
                    data = self.central_socket.recv(4096)
                    if not data:
                        break

                    buffer += data

                    while len(buffer) > 0:
                        message, is_valid = Protocol.decode(buffer)

                        if is_valid:
                            etx_pos = buffer.find(b'\x03')
                            buffer = buffer[etx_pos + 2:]

                            fields = Protocol.parse_message(message)
                            
                            if fields[0] == "AUTHORIZE":
                                self._handle_authorization(fields)

                            elif fields[0] == "STOP_COMMAND":
                                self._handle_stop_command()

                            elif fields[0] == "RESUME_COMMAND":
                                self._handle_resume_command()

                            elif fields[0] == "END_SUPPLY":
                                self._handle_end_supply()

                        else:
                            break

                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"[{self.cp_id}] CENTRAL listener error: {e}")
                    break

        except Exception as e:
            print(f"[{self.cp_id}] CENTRAL connection lost: {e}")

    def _listen_monitor(self):
        """Listen for health checks from monitor via socket"""
        try:
            while self.running and self.monitor_socket:
                data = self.monitor_socket.recv(4096)
                if not data:
                    break

                message, is_valid = Protocol.decode(data)
                if is_valid:
                    fields = Protocol.parse_message(message)

                    if fields[0] == "HEALTH_CHECK":
                        # Respond to health check
                        if self.simulate_fault:
                            response = Protocol.encode(
                                Protocol.build_message(
                                    "HEALTH_KO", self.cp_id
                                )
                            )
                        else:
                            response = Protocol.encode(
                                Protocol.build_message(
                                    "HEALTH_OK", self.cp_id
                                )
                            )
                        self.monitor_socket.send(response)

        except Exception as e:
            print(f"[{self.cp_id}] Monitor listener error: {e}")

    def _handle_authorization(self, fields):
        """Handle charging authorization from CENTRAL"""
        driver_id = fields[1]
        kwh_needed = float(fields[3]) if len(fields) > 3 else 10

        with self.lock:
            if self.state == CP_STATES["ACTIVATED"]:
                self.current_driver = driver_id
                self.state = CP_STATES["SUPPLYING"]
                log_charge("127.0.0.1", self.cp_id, driver_id, "CHARGE_START", kwh=kwh_needed)
                self.charging_complete = False
                self.current_session = {
                    "driver_id": driver_id,
                    "start_time": time.time(),
                    "kwh_needed": kwh_needed,
                    "kwh_delivered": 0.0,
                    "amount": 0.0
                }

                print(f"\n[{self.cp_id}] ✅ Charging authorized")
                self.send_log(f"Charging authorized for driver {driver_id}, kWh={kwh_needed}")
                print(f"[{self.cp_id}] → IN USE - CHARGING\n")
    def send_log(self, text):
        """Send a LOG message to CENTRAL so UI can receive it."""
        try:
            if self.central_socket:
                msg = Protocol.build_message("LOG", self.cp_id, str(text))
                self.central_socket.send(Protocol.encode(msg))
        except Exception as e:
            # keep local print as fallback
            print(f"[{self.cp_id}] Failed to send log: {e}")

    def _handle_stop_command(self):
        """Handle STOP command from CENTRAL"""
        with self.lock:
            if self.state == CP_STATES["SUPPLYING"] and self.current_session:
                driver_id = self.current_driver
                session = self.current_session
                kwh_delivered = session["kwh_delivered"]
                total_amount = round(kwh_delivered * self.price_per_kwh, 2)
                
                end_msg = Protocol.encode(
                    Protocol.build_message(
                        "SUPPLY_END", self.cp_id, driver_id,
                        kwh_delivered, total_amount
                    )
                )
                try:
                    self.central_socket.send(end_msg)
                except Exception as e:
                    print(f"[{self.cp_id}] Error notifying supply end: {e}")
                
                self.current_driver = None
                self.current_session = None
                self.charging_complete = False
            
            self.state = CP_STATES["STOPPED"]
        
        print(f"[{self.cp_id}] Received STOP command from CENTRAL - now stopped")

    def _handle_resume_command(self):
        """Handle RESUME command from CENTRAL"""
        with self.lock:
            self.state = CP_STATES["ACTIVATED"]
            self.charging_complete = False
        print(f"[{self.cp_id}] Received RESUME command from CENTRAL - now activated")

    def _handle_end_supply(self):
        """Handle END_SUPPLY command from CENTRAL"""
        with self.lock:
            if self.state == CP_STATES["SUPPLYING"] and self.current_session:
                driver_id = self.current_driver
                session = self.current_session

                elapsed = time.time() - session["start_time"]
                total_seconds = 14.0
                kwh_delivered = min(session["kwh_needed"], (elapsed / total_seconds) * session["kwh_needed"])
                total_amount = round(kwh_delivered * self.price_per_kwh, 2)

                print(f"\n[{self.cp_id}] Supply ended by CENTRAL")
                print(f"[{self.cp_id}] {kwh_delivered:.3f} kWh, {total_amount:.2f}€")
                self.send_log(f"Supply ended: {kwh_delivered:.3f} kWh, {total_amount:.2f}€")
                
                end_msg = Protocol.encode(
                    Protocol.build_message(
                        "SUPPLY_END", self.cp_id, driver_id,
                        kwh_delivered, total_amount
                    )
                )
                try:
                    self.central_socket.send(end_msg)
                except Exception as e:
                    print(f"[{self.cp_id}] Error notifying supply end: {e}")

                self.state = CP_STATES["ACTIVATED"]
                self.current_driver = None
                self.current_session = None
                self.charging_complete = False
                
                print(f"[{self.cp_id}] → AVAILABLE\n")

    def stop_charging(self):
        """Simulate driver unplugging vehicle from CP"""
        with self.lock:
            if self.state == CP_STATES["SUPPLYING"]:
                driver_id = self.current_driver
                session = self.current_session

                elapsed = time.time() - session["start_time"]
                total_seconds = 14.0
                kwh_delivered = min(session["kwh_needed"], (elapsed / total_seconds) * session["kwh_needed"])
                total_amount = round(kwh_delivered * self.price_per_kwh, 2)

                print(f"\n[{self.cp_id}] Vehicle unplugged")
                print(f"[{self.cp_id}] {kwh_delivered:.3f} kWh, {total_amount:.2f}€")

                end_msg = Protocol.encode(
                    Protocol.build_message(
                        "SUPPLY_END", self.cp_id, driver_id,
                        kwh_delivered, total_amount
                    )
                )
                self.central_socket.send(end_msg)

                self.state = CP_STATES["ACTIVATED"]
                self.current_driver = None
                self.current_session = None
                self.charging_complete = False

                print(f"[{self.cp_id}] → AVAILABLE\n")
                return True

        return False

    def send_heartbeat(self):
        """Send encrypted heartbeat"""
        heartbeat = Protocol.build_message(
            "HEARTBEAT", self.cp_id, self.state
        )
        
        # NAUJAS: Šifruoti prieš siųsti
        encrypted = self._encrypt_message(heartbeat)
        encoded = Protocol.encode(encrypted)
        
        try:
            self.central_socket.send(encoded)
        except Exception as e:
            print(f"[{self.cp_id}] ❌ Failed to send heartbeat: {e}")
    def send_status_updates(self):
        """Send status updates to CENTRAL every second"""
        while self.running:
            time.sleep(SUPPLY_UPDATE_INTERVAL)

            try:
                with self.lock:
                    # Always send heartbeat
                    try:
                        heartbeat = Protocol.encode(
                            Protocol.build_message(
                                "HEARTBEAT", self.cp_id, self.state
                            )
                        )
                        self.central_socket.send(heartbeat)
                    except Exception as e:
                        print(f"[{self.cp_id}] ❌ Failed to send HEARTBEAT: {e}")

                    # If charging, send detailed update
                    if self.state == CP_STATES["SUPPLYING"] and self.current_session:
                        session = self.current_session

                        kwh_this_second = session["kwh_needed"] / 14.0
                        session["kwh_delivered"] += kwh_this_second

                        if session["kwh_delivered"] >= session["kwh_needed"]:
                            session["kwh_delivered"] = session["kwh_needed"]
                            
                            if not self.charging_complete:
                                self.charging_complete = True
                                print(f"\n[{self.cp_id}] 🔋 Charged fully, waiting for driver to unplug")
                            
                            continue

                        amount = session["kwh_delivered"] * self.price_per_kwh
                        session["amount"] = amount

                        # Display charging progress
                        print(f"[{self.cp_id}] {session['kwh_delivered']:.3f} kWh | {amount:.2f}€ (IN USE - CHARGING)")
                        self.send_log(f"{session['kwh_delivered']:.3f} kWh | {amount:.2f}€ (CHARGING)")
                        try:
                            update_msg = Protocol.encode(
                                Protocol.build_message(
                                    "SUPPLY_UPDATE",
                                    self.cp_id,
                                    f"{kwh_this_second:.6f}",
                                    f"{amount:.2f}"
                                )
                            )
                            self.central_socket.send(update_msg)
                        except Exception as e:
                            print(f"[{self.cp_id}] ❌ Failed to send SUPPLY_UPDATE: {e}")

            except Exception as e:
                print(f"[{self.cp_id}] ❌ Error in status update loop: {e}")

    def status_display_loop(self):
        """Display status every 2 seconds"""
        while self.running:
            time.sleep(2)
            
            with self.lock:
                if self.state == CP_STATES["ACTIVATED"]:
                    print(f"[{self.cp_id}] → AVAILABLE")
                elif self.state == CP_STATES["SUPPLYING"] and self.charging_complete:
                    print(f"[{self.cp_id}] 🔋 Charged fully, waiting for driver to unplug")

    def display_menu(self):
        """Display interactive menu for CP operations"""
        while self.running:
            try:
                time.sleep(0.5)  # Small delay to allow other threads to print
                
                # Just wait, status is displayed by status_display_loop
                
            except Exception as e:
                print(f"Menu error: {e}")

    def run(self):
        """Run the CP engine"""
        if not self.connect_to_central():
            print(f"[{self.cp_id}] Cannot connect to CENTRAL")
            return

        # Start monitor listener
        monitor_thread = threading.Thread(
            target=self.listen_for_monitor,
            daemon=True
        )
        monitor_thread.start()

        # Start status updater
        updater_thread = threading.Thread(
            target=self.send_status_updates,
            daemon=True
        )
        updater_thread.start()

        # Start status display
        display_thread = threading.Thread(
            target=self.status_display_loop,
            daemon=True
        )
        display_thread.start()

        # Run menu
        try:
            self.display_menu()
        except KeyboardInterrupt:
            print(f"\n[{self.cp_id}] Shutting down...")
        finally:
            self.running = False
            if self.central_socket:
                self.central_socket.close()
            if self.monitor_socket:
                self.monitor_socket.close()
            self.kafka.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ev_cp_engine.py <CP_ID> [latitude] [longitude] [price] [central_host] [central_port]")
        sys.exit(1)

    cp_id = sys.argv[1]
    latitude = sys.argv[2] if len(sys.argv) > 2 else "40.5"
    longitude = sys.argv[3] if len(sys.argv) > 3 else "-3.1"
    price = sys.argv[4] if len(sys.argv) > 4 else "0.30"
    central_host = sys.argv[5] if len(sys.argv) > 5 else "localhost"
    central_port = int(sys.argv[6]) if len(sys.argv) > 6 else 5000

    engine = EVCPEngine(cp_id, latitude, longitude, price, central_host, central_port)
    engine.run()