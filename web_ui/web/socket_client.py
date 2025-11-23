# web_ui/web/socket_client.py
import socket
import threading
import time
import os
import ast
import traceback

from shared.protocol import Protocol, MessageTypes
from .state import UIState

DEFAULT_HOST = os.environ.get("CENTRAL_HOST", "central")
DEFAULT_PORT = int(os.environ.get("CENTRAL_PORT", 5000))

class CentralUIClient:
    def __init__(self, state: UIState, host=DEFAULT_HOST, port=DEFAULT_PORT):
        self.state = state
        self.host = host
        self.port = port
        self.sock = None
        self.running = True
        self.recv_buffer = b''
        self.lock = threading.Lock()
        self.client_id = "WEB_UI"
        self._start_threads()

    def _start_threads(self):
        t = threading.Thread(target=self._connect_loop, daemon=True)
        t.start()

    def _connect_loop(self):
        while self.running:
            try:
                if not self.sock:
                    self._connect_and_register()
                # listen
                self._listen_loop()
            except Exception as e:
                print(f"[Web UI] Connection loop error: {e}")
                traceback.print_exc()
                self._close_socket()
                time.sleep(2)

    def _connect_and_register(self):
        try:
            print(f"[Web UI] Connecting to central {self.host}:{self.port} ...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(8)
            s.connect((self.host, int(self.port)))
            s.settimeout(None)
            self.sock = s
            # send REGISTER MONITOR WEB_UI
            msg = Protocol.build_message(MessageTypes.REGISTER, "MONITOR", self.client_id)
            s.send(Protocol.encode(msg))
            print("[Web UI] Sent REGISTER -> waiting for FULL_STATE or ACK")
        except Exception as e:
            print(f"[Web UI] Failed to connect/register: {e}")
            self._close_socket()
            raise

    def _listen_loop(self):
        if not self.sock:
            return
        try:
            while self.running:
                data = self.sock.recv(4096)
                if not data:
                    print("[Web UI] Central closed connection")
                    raise ConnectionResetError("central closed")
                self.recv_buffer += data
                # consume messages
                while True:
                    msg, valid = Protocol.decode(self.recv_buffer)
                    if not valid:
                        break
                    # find ETX and chop buffer
                    etx_pos = self.recv_buffer.find(b'\x03')
                    if etx_pos == -1:
                        self.recv_buffer = b''
                    else:
                        self.recv_buffer = self.recv_buffer[etx_pos + 2:]
                    try:
                        fields = Protocol.parse_message(msg)
                        if not fields:
                            continue
                        self._handle_message(fields)
                    except Exception as e:
                        print(f"[Web UI] Error parsing message: {e}")
        except socket.timeout:
            return
        except Exception as e:
            # bubble up to reconnect
            raise

    def _handle_message(self, fields):
        # fields is a list: [type, ...rest]
        typ = fields[0]
        # print(f"[Web UI] Received: {typ} with {len(fields)} fields")
        try:
            if typ == "FULL_STATE":
                # expected: FULL_STATE, cps_str, drivers_str, history_str
                cps = []
                drivers = []
                history = []
                if len(fields) > 1:
                    try:
                        cps = ast.literal_eval(fields[1])
                    except Exception:
                        cps = []
                if len(fields) > 2:
                    try:
                        drivers = ast.literal_eval(fields[2])
                    except Exception:
                        drivers = []
                if len(fields) > 3:
                    try:
                        history = ast.literal_eval(fields[3])
                    except Exception:
                        history = []
                self.state.set_full_state(cps, drivers, history)
                print("[Web UI] FULL_STATE received and applied")
            elif typ == MessageTypes.SUPPLY_UPDATE:
                # SUPPLY_UPDATE, cp_id, kwh_inc, amount
                if len(fields) >= 4:
                    cp_id = fields[1]
                    kwh_inc = float(fields[2])
                    amount = float(fields[3])
                    # update cp record: increment
                    snapshot = self.state.snapshot()
                    cp = snapshot["charging_points"].get(cp_id, {})
                    kwh = cp.get("kwh_delivered", 0.0) + kwh_inc
                    amt = amount
                    self.state.update_cp(cp_id, {"kwh_delivered": kwh, "amount_euro": amt})
            elif typ == MessageTypes.SUPPLY_END or typ == "DRIVER_STOP" or typ == MessageTypes.TICKET:
                # handle end-of-charge types
                try:
                    if typ == MessageTypes.SUPPLY_END and len(fields) >= 5:
                        cp_id = fields[1]
                        driver_id = fields[2]
                        total_kwh = float(fields[3])
                        total_amount = float(fields[4])
                        self.state.update_cp(cp_id, {"state": "ACTIVATED", "current_driver": None, "kwh_delivered": 0.0, "amount_euro": 0.0})
                        self.state.update_driver(driver_id, {"status": "IDLE", "current_cp": None})
                        self.state.add_history({
                            "cp_id": cp_id, "driver_id": driver_id, "kwh_delivered": total_kwh, "total_amount": total_amount, "timestamp": time.time()
                        })
                except Exception as e:
                    print(f"[Web UI] Error handling {typ}: {e}")
            elif typ == MessageTypes.REGISTER or typ == MessageTypes.ACKNOWLEDGE:
                # ACK for register - ignore or log
                pass
            else:
                # generic: handle cp status updates like DRIVER_START / DRIVER_STOP / CHARGING_COMPLETE
                if typ == "DRIVER_START" and len(fields) >= 3:
                    cp_id = fields[1]
                    driver_id = fields[2]
                    self.state.update_cp(cp_id, {"state": "SUPPLYING", "current_driver": driver_id})
                    self.state.update_driver(driver_id, {"status": "CHARGING", "current_cp": cp_id})
                    print(f"[Web UI] DRIVER_START: {driver_id} at {cp_id}")
                elif typ == "DRIVER_STOP" and len(fields) >= 3:
                    cp_id = fields[1]
                    driver_id = fields[2]
                    self.state.update_cp(cp_id, {"state": "ACTIVATED", "current_driver": None, "kwh_delivered": 0.0, "amount_euro": 0.0})
                    self.state.update_driver(driver_id, {"status": "IDLE", "current_cp": None})
                    print(f"[Web UI] DRIVER_STOP: {driver_id} from {cp_id}")
                elif typ == "CHARGING_COMPLETE" and len(fields) >= 3:
                    cp_id = fields[1]
                    driver_id = fields[2]
                    self.state.update_cp(cp_id, {"charging_complete": True})
                    print(f"[Web UI] CHARGING_COMPLETE: {driver_id} at {cp_id}")
        except Exception as e:
            print(f"[Web UI] _handle_message error: {e}")
            traceback.print_exc()

    def send_command(self, command_type, driver_id=None, cp_id=None, kwh_needed=10):
        """
        Send commands to Central system via protocol
        command_type: REQUEST_CHARGE, FINISH_CHARGE, etc.
        """
        if not self.sock:
            print("[Web UI] Cannot send command: not connected")
            return False
        
        try:
            with self.lock:
                if command_type == "REQUEST_CHARGE":
                    msg = Protocol.build_message(
                        MessageTypes.REQUEST_CHARGE,
                        driver_id,
                        cp_id,
                        kwh_needed
                    )
                elif command_type == "FINISH_CHARGE":
                    msg = Protocol.build_message(
                        MessageTypes.END_CHARGE,
                        driver_id,
                        cp_id
                    )
                else:
                    print(f"[Web UI] Unknown command type: {command_type}")
                    return False
                
                encoded = Protocol.encode(msg)
                self.sock.send(encoded)
                print(f"[Web UI] Sent {command_type} for {driver_id} at {cp_id}")
                return True
        except Exception as e:
            print(f"[Web UI] Error sending command: {e}")
            return False

    def _close_socket(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def stop(self):
        self.running = False
        self._close_socket()