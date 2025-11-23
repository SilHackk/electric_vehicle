# ============================================================================
# EVCharging System - EV_Driver (Driver Application) - MANUAL MODE ONLY
# ============================================================================

import socket
import threading
import time
import sys
import os
from config import WAIT_BETWEEN_REQUESTS
from shared.protocol import Protocol, MessageTypes
from shared.kafka_client import KafkaClient


class EVDriver:
    def __init__(self, driver_id, central_host="localhost", central_port=5000,
                 requests_file=None):
        self.driver_id = driver_id
        self.central_host = central_host
        self.central_port = central_port
        self.requests_file = requests_file

        self.central_socket = None
        self.running = True
        self.lock = threading.Lock()

        # Driver state - ALWAYS START IDLE
        self.status = "IDLE"
        self.current_cp = None
        self.charge_amount = 0
        self.requests_queue = []

        # Kafka
        self.kafka = KafkaClient(f"EV_Driver_{driver_id}")

        print(f"[{self.driver_id}] Driver initializing...")

    def connect_to_central(self):
        """Connect to CENTRAL"""
        try:
            self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.central_socket.connect((self.central_host, self.central_port))

            # Register with CENTRAL
            register_msg = Protocol.encode(
                Protocol.build_message(MessageTypes.REGISTER, "DRIVER", self.driver_id)
            )
            self.central_socket.send(register_msg)
            print(f"[{self.driver_id}] Registered with CENTRAL")

            # Start listening for messages from CENTRAL
            thread = threading.Thread(
                target=self._listen_central,
                daemon=True
            )
            thread.start()

            return True

        except Exception as e:
            print(f"[{self.driver_id}] Failed to connect to CENTRAL: {e}")
            return False

    def _reconnect_to_central(self):
        """Attempt to reconnect to CENTRAL"""
        print(f"[{self.driver_id}] Attempting to reconnect to CENTRAL...")
        try:
            # Close existing socket if any
            if self.central_socket:
                self.central_socket.close()

            # Create new socket and connect
            self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.central_socket.connect((self.central_host, self.central_port))

            # Re-register with CENTRAL
            register_msg = Protocol.encode(
                Protocol.build_message(MessageTypes.REGISTER, "DRIVER", self.driver_id)
            )
            self.central_socket.send(register_msg)
            print(f"[{self.driver_id}] Reconnected and re-registered with CENTRAL")

            # Restart listener thread
            thread = threading.Thread(
                target=self._listen_central,
                daemon=True
            )
            thread.start()

            return True

        except Exception as e:
            print(f"[{self.driver_id}] Failed to reconnect: {e}")
            return False

    def _listen_central(self):
        """Listen for messages from CENTRAL"""
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
                            msg_type = fields[0]

                            if msg_type == MessageTypes.AUTHORIZE:
                                self._handle_authorization(fields)

                            elif msg_type == MessageTypes.DENY:
                                self._handle_denial(fields)

                            elif msg_type == MessageTypes.TICKET:
                                self._handle_ticket(fields)

                            elif msg_type == MessageTypes.AVAILABLE_CPS:
                                self._handle_available_cps(fields)

                            elif msg_type == MessageTypes.SUPPLY_UPDATE:
                                self._handle_supply_update(fields)

                        else:
                            break

                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"[{self.driver_id}] Listener error: {e}")
                    break

        except Exception as e:
            print(f"[{self.driver_id}] CENTRAL connection lost: {e}")

    def _handle_authorization(self, fields):
        """Handle charging authorization from CENTRAL"""
        # AUTHORIZE#driver_id#cp_id#kwh_needed#price
        cp_id = fields[2]
        kwh_needed = fields[3]
        price = fields[4]

        with self.lock:
            self.status = "CHARGING"
            self.current_cp = cp_id

        print(f"\n[{self.driver_id}] ✓ AUTHORIZED to charge at {cp_id}")
        print(f"    kWh needed: {kwh_needed}, Price: {price}€/kWh")
        print(f"[{self.driver_id}] Vehicle plugged in automatically, starting supply...\n")

        self.kafka.publish_event("charging_logs", "DRIVER_AUTHORIZED", {
            "driver_id": self.driver_id,
            "cp_id": cp_id,
            "kwh_needed": kwh_needed
        })

    def _handle_denial(self, fields):
        """Handle charging denial from CENTRAL"""
        # DENY#driver_id#cp_id#reason
        cp_id = fields[2] if len(fields) > 2 else "?"
        reason = fields[3] if len(fields) > 3 else "UNKNOWN"

        with self.lock:
            self.status = "IDLE"
            self.current_cp = None

        print(f"\n[{self.driver_id}] ✗ DENIED charging at {cp_id}")
        print(f"    Reason: {reason}\n")

        self.kafka.publish_event("charging_logs", "DRIVER_DENIED", {
            "driver_id": self.driver_id,
            "cp_id": cp_id,
            "reason": reason
        })

    def _handle_ticket(self, fields):
        """Handle charging ticket from CENTRAL"""
        # TICKET#cp_id#total_kwh#total_amount
        cp_id = fields[1] if len(fields) > 1 else "?"
        total_kwh = fields[2] if len(fields) > 2 else "?"
        total_amount = fields[3] if len(fields) > 3 else "?"

        with self.lock:
            self.status = "IDLE"
            self.current_cp = None

        print(f"\n{'='*60}")
        print(f"CHARGING TICKET - {self.driver_id}")
        print(f"{'='*60}")
        print(f"Charging Point: {cp_id}")
        print(f"Energy: {total_kwh} kWh")
        print(f"Amount: {total_amount}€")
        print(f"{'='*60}\n")

        self.kafka.publish_event("charging_logs", "DRIVER_TICKET", {
            "driver_id": self.driver_id,
            "cp_id": cp_id,
            "total_kwh": total_kwh,
            "total_amount": total_amount
        })

    def _handle_supply_update(self, fields):
        """Handle real-time supply updates during charging"""
        # SUPPLY_UPDATE#cp_id#consumption_kw#amount_euro
        if len(fields) < 4:
            return

        cp_id = fields[1]
        consumption_kw = fields[2]
        amount_euro = fields[3]

        print(f"[{self.driver_id}] ⚡ Charging: {consumption_kw} kW - {amount_euro}€")

    def _handle_available_cps(self, fields):
        """Handle available CPs response from CENTRAL"""
        # AVAILABLE_CPS#cp_id1#lat1#lon1#price1#cp_id2#lat2#lon2#price2...
        if len(fields) < 2:
            print(f"[{self.driver_id}] No available CPs")
            return

        print(f"\n[{self.driver_id}] Available Charging Points:")
        for i in range(1, len(fields), 4):
            if i + 3 < len(fields):
                cp_id = fields[i]
                lat = fields[i + 1]
                lon = fields[i + 2]
                price = fields[i + 3]
                print(f"  - {cp_id} (€{price}/kWh) at ({lat}, {lon})")
        print()

    def request_charge(self, cp_id, kwh_needed=10):
        """Request charging from a specific CP"""
        if self.status != "IDLE":
            print(f"\n❌ Cannot request (status: {self.status})")
            return False

        with self.lock:
            self.status = "REQUESTING"

        request_msg = Protocol.encode(
            Protocol.build_message(
                MessageTypes.REQUEST_CHARGE, self.driver_id, cp_id, kwh_needed
            )
        )

        try:
            self.central_socket.send(request_msg)
            print(f"\n[{self.driver_id}] 📤 Requesting charge: {cp_id}, {kwh_needed} kWh\n")

            self.kafka.publish_event("charging_logs", "CHARGE_REQUESTED", {
                "driver_id": self.driver_id,
                "cp_id": cp_id,
                "kwh_needed": kwh_needed
            })

            return True

        except Exception as e:
            print(f"[{self.driver_id}] Error sending request: {e}")
            with self.lock:
                self.status = "IDLE"
            return False

    def finish_charging_manual(self):
        """Manually unplug and go back to IDLE"""
        if self.status != "CHARGING":
            print(f"\n❌ Not currently charging (status: {self.status})")
            return False

        with self.lock:
            cp_id = self.current_cp
            self.status = "IDLE"
            self.current_cp = None

        # Send END_CHARGE to CENTRAL
        end_charge_msg = Protocol.encode(
            Protocol.build_message(MessageTypes.END_CHARGE, self.driver_id, cp_id)
        )

        try:
            self.central_socket.send(end_charge_msg)
            print(f"\n[{self.driver_id}] 📤 Sent manual end charge request for {cp_id}\n")

            # Status will be updated when ticket is received
            return True

        except BrokenPipeError:
            print(f"[{self.driver_id}] Connection to CENTRAL lost. Attempting to reconnect...")
            # Try to reconnect
            if self._reconnect_to_central():
                # Retry sending the message
                try:
                    self.central_socket.send(end_charge_msg)
                    print(f"[{self.driver_id}] 📤 Re-sent manual end charge request for {cp_id}\n")
                    return True
                except Exception as e:
                    print(f"[{self.driver_id}] Failed to re-send: {e}")
            return False
        except Exception as e:
            print(f"[{self.driver_id}] Error sending end charge: {e}")
            return False

    def query_available_cps(self):
        """Query available CPs from CENTRAL"""
        query_msg = Protocol.encode(
            Protocol.build_message(MessageTypes.QUERY_AVAILABLE_CPS, self.driver_id)
        )

        try:
            self.central_socket.send(query_msg)
            print(f"\n[{self.driver_id}] 📤 Querying available CPs...\n")
            return True
        except Exception as e:
            print(f"[{self.driver_id}] Error sending query: {e}")
            return False

    def display_menu(self):
        """Display interactive menu for driver - MANUAL MODE ONLY"""
        while self.running:
            try:
                print(f"\n{'='*60}")
                print(f"[{self.driver_id} MENU] Status: {self.status}", end="")
                if self.current_cp:
                    print(f" | At: {self.current_cp}\n", end="")
                else:
                    print("\n", end="")
                print(f"{'='*60}")
                print("\nOptions:")
                print("  1. Request charge")
                print("  2. View status")
                print("  3. View available CPs")
                print("  4. Finish charging (unplug)")
                print("  5. Exit")
                print(f"{'='*60}")

                choice = input("\nChoice (1-5): ").strip()

                if choice == "1":
                    if self.status != "IDLE":
                        print(f"\n❌ Cannot request while {self.status}")
                        continue
                    
                    cp_id = input("Enter CP ID (e.g., CP-001): ").strip()
                    kwh_str = input("Enter kWh needed (default 10): ").strip()
                    try:
                        kwh = float(kwh_str) if kwh_str else 10
                        self.request_charge(cp_id, kwh)
                    except ValueError:
                        print("❌ Invalid kWh value")

                elif choice == "2":
                    print(f"\n  Status: {self.status}")
                    print(f"  Current CP: {self.current_cp if self.current_cp else 'None'}\n")

                elif choice == "3":
                    self.query_available_cps()

                elif choice == "4":
                    self.finish_charging_manual()

                elif choice == "5":
                    print("\nExiting driver application...")
                    self.running = False
                    break

                else:
                    print("\n❌ Invalid choice. Please enter 1-5")

            except Exception as e:
                print(f"\n❌ Menu error: {e}")

    def run(self):
        """Run the driver"""
        if not self.connect_to_central():
            print(f"[{self.driver_id}] Cannot connect to CENTRAL")
            return

        print(f"[{self.driver_id}] ✓ Connected to CENTRAL")
        print(f"[{self.driver_id}] Ready for manual commands\n")

        # Display menu only - NO automatic processing
        try:
            self.display_menu()
        except KeyboardInterrupt:
            print(f"\n[{self.driver_id}] Shutting down...")
        finally:
            self.running = False
            if self.central_socket:
                self.central_socket.close()
            self.kafka.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ev_driver.py <DRIVER_ID> [central_host] [central_port]")
        sys.exit(1)

    driver_id = sys.argv[1]
    central_host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
    central_port = int(sys.argv[3]) if len(sys.argv) > 3 else 5000

    driver = EVDriver(driver_id, central_host, central_port)
    driver.run()