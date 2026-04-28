# ============================================================================
# EVCharging System - EV_CP_M (Charging Point Monitor)
# ARCHITECTURE-CORRECT VERSION
# - Monitor talks ONLY to CENTRAL (Sockets)
# - NO direct Engine connection (per official diagram)
# - Shows charging progress, kWh and €
# ============================================================================

import socket
import threading
import time
import sys
from datetime import datetime

from shared.protocol import Protocol, MessageTypes


class EVCPMonitor:
    def __init__(self, cp_id, central_host="localhost", central_port=5000):
        self.cp_id = cp_id
        self.central_host = central_host
        self.central_port = central_port

        self.central_socket = None
        self.running = True
        self.lock = threading.Lock()
        self.recv_buffer = b""

        # Charging session state (from CENTRAL)
        self.current_driver = None
        self.state = "DISCONNECTED"
        self.kwh = 0.0
        self.amount = 0.0
        self.session_start = None

        print(f"[{self.cp_id} Monitor] Initializing...")
        print(f"[{self.cp_id} Monitor] Central: {self.central_host}:{self.central_port}")

    # ------------------------------------------------------------------
    # CENTRAL CONNECTION
    # ------------------------------------------------------------------
    def connect_to_central(self):
        while self.running:
            try:
                print(f"[{self.cp_id} Monitor] Connecting to CENTRAL...")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.central_host, self.central_port))
                self.central_socket = s

                # Register as CP monitor
                reg = Protocol.build_message(
                    MessageTypes.REGISTER, "MONITOR", self.cp_id, self.cp_id
                )
                s.send(Protocol.encode(reg))

                print(f"[{self.cp_id} Monitor] ✅ Connected to CENTRAL")
                return True

            except Exception as e:
                print(f"[{self.cp_id} Monitor] ❌ Central connection failed: {e}")
                time.sleep(3)

        return False

    # ------------------------------------------------------------------
    # LISTEN CENTRAL
    # ------------------------------------------------------------------
    def _listen_central(self):
        try:
            while self.running:
                data = self.central_socket.recv(4096)
                if not data:
                    raise ConnectionError("Central disconnected")

                self.recv_buffer += data

                while True:
                    msg, valid = Protocol.decode(self.recv_buffer)
                    if not valid:
                        break

                    etx = self.recv_buffer.find(b"\x03")
                    self.recv_buffer = self.recv_buffer[etx + 2:]

                    fields = Protocol.parse_message(msg)
                    self._handle_message(fields)

        except Exception as e:
            print(f"[{self.cp_id} Monitor] ❌ CENTRAL connection lost: {e}")
        finally:
            self.running = False

    # ------------------------------------------------------------------
    # MESSAGE HANDLER
    # ------------------------------------------------------------------
    def _handle_message(self, fields):
        t = fields[0]

        with self.lock:
            # CP state updates
            if t == "CP_STATE":
                self.state = fields[2] if len(fields) > 2 else self.state

            elif t == "CP_CONNECTED":
                self.state = "ACTIVATED"
                print(f"\n[{self.cp_id}] ✅ CONNECTED & AVAILABLE\n")

            elif t == "DRIVER_START":
                self.current_driver = fields[2]
                self.kwh = 0.0
                self.amount = 0.0
                self.session_start = datetime.now()
                self.state = "SUPPLYING"
                self._print_start()

            elif t == MessageTypes.SUPPLY_UPDATE:
                self.kwh += float(fields[2])
                self.amount = float(fields[3])
                self._print_progress()

            elif t == MessageTypes.SUPPLY_END:
                total_kwh = float(fields[3])
                total_amount = float(fields[4])
                self._print_end(total_kwh, total_amount)
                self._reset()

            elif t == "DRIVER_STOP":
                self._reset()

    # ------------------------------------------------------------------
    # DISPLAY HELPERS
    # ------------------------------------------------------------------
    def _print_start(self):
        print("\n" + "=" * 60)
        print(f"🚗 DRIVER {self.current_driver} STARTED CHARGING")
        print("=" * 60)

    def _print_progress(self):
        elapsed = int((datetime.now() - self.session_start).total_seconds())
        print(
            f"⚡ {self.cp_id} | {self.current_driver} | "
            f"{self.kwh:.2f} kWh | {self.amount:.2f} € | {elapsed}s"
        )

    def _print_end(self, kwh, amount):
        print("\n" + "=" * 60)
        print("🔌 CHARGING SESSION FINISHED")
        print(f"Driver: {self.current_driver}")
        print(f"Energy: {kwh:.2f} kWh")
        print(f"Cost:   {amount:.2f} €")
        print("=" * 60 + "\n")

    def _reset(self):
        self.current_driver = None
        self.kwh = 0.0
        self.amount = 0.0
        self.session_start = None
        self.state = "ACTIVATED"

    # ------------------------------------------------------------------
    # RUN
    # ------------------------------------------------------------------
    def run(self):
        if not self.connect_to_central():
            return

        listener = threading.Thread(target=self._listen_central, daemon=True)
        listener.start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            if self.central_socket:
                self.central_socket.close()
            print(f"[{self.cp_id} Monitor] Shutdown")


# ------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ev_cp_monitor.py <CP_ID> [central_host] [central_port]")
        sys.exit(1)

    cp_id = sys.argv[1]
    central_host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
    central_port = int(sys.argv[3]) if len(sys.argv) > 3 else 5000

    monitor = EVCPMonitor(cp_id, central_host, central_port)
    monitor.run()
