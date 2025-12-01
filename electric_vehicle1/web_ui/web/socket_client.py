# web_ui/web/socket_client.py
import socket
import threading
import time
import os
import ast
import traceback
from shared.protocol import Protocol, MessageTypes

class CentralUIClient:
    def __init__(self, state, host, port):
        self.state = state
        self.host = host
        self.port = port
        self.sock = None
        self.running = True
        self.recv_buffer = b""
        self.lock = threading.Lock()
        self.client_id = "WEB_UI"
        threading.Thread(target=self._connect_loop, daemon=True).start()

    def _connect_loop(self):
        while self.running:
            try:
                if not self.sock:
                    self._connect_and_register()
                self._listen_loop()
            except Exception:
                self._close_socket()
                time.sleep(2)

    def _connect_and_register(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(8)
        s.connect((self.host, self.port))
        s.settimeout(None)
        self.sock = s
        msg = Protocol.build_message(MessageTypes.REGISTER, "MONITOR", self.client_id)
        s.send(Protocol.encode(msg))

    def _listen_loop(self):
        while self.running:
            data = self.sock.recv(4096)
            if not data:
                raise ConnectionResetError
            self.recv_buffer += data
            while True:
                msg, valid = Protocol.decode(self.recv_buffer)
                if not valid:
                    break
                etx = self.recv_buffer.find(b"\x03")
                self.recv_buffer = self.recv_buffer[etx+2:] if etx != -1 else b""
                try:
                    fields = Protocol.parse_message(msg)
                    if fields:
                        self._handle_message(fields)
                except:
                    traceback.print_exc()
    def _handle_message(self, fields):
        t = fields[0]
        if t == "FULL_STATE":
            cps = ast.literal_eval(fields[1]) if len(fields) > 1 else []
            drivers = ast.literal_eval(fields[2]) if len(fields) > 2 else []
            hist = ast.literal_eval(fields[3]) if len(fields) > 3 else []
            self.state.set_full_state(cps, drivers, hist)

        elif t == "LOG" or t == MessageTypes.LOG if hasattr(MessageTypes, "LOG") else False:
            # Expected fields: ["LOG", source, text, timestamp?]
            try:
                source = fields[1] if len(fields) > 1 else "UNKNOWN"
                text = fields[2] if len(fields) > 2 else ""
                ts = fields[3] if len(fields) > 3 else None
                entry = {
                    "source": source,
                    "text": text,
                    "time": ts or time.time()
                }
                self.state.add_log(entry)
            except Exception:
                traceback.print_exc()

        elif t == MessageTypes.SUPPLY_UPDATE and len(fields) >= 4:
            cp_id = fields[1]
            inc = float(fields[2])
            amt = float(fields[3])
            snap = self.state.snapshot()
            cp = snap["charging_points"].get(cp_id, {})
            new_kwh = cp.get("kwh_delivered", 0) + inc
            self.state.update_cp(cp_id, {"kwh_delivered": new_kwh, "amount_euro": amt})

        elif t == MessageTypes.SUPPLY_END and len(fields) >= 5:
            cp_id, driver_id = fields[1], fields[2]
            total_kwh, total_amount = float(fields[3]), float(fields[4])
            self.state.update_cp(cp_id, {
                "state": "ACTIVATED",
                "current_driver": None,
                "kwh_delivered": 0.0,
                "amount_euro": 0.0
            })
            self.state.update_driver(driver_id, {"status": "IDLE", "current_cp": None})
            self.state.add_history({
                "cp_id": cp_id,
                "driver_id": driver_id,
                "kwh_delivered": total_kwh,
                "total_amount": total_amount,
                "timestamp": time.time()
            })

        elif t == "DRIVER_START" and len(fields) >= 3:
            cp_id, driver_id = fields[1], fields[2]
            self.state.update_cp(cp_id, {"state": "SUPPLYING", "current_driver": driver_id})
            self.state.update_driver(driver_id, {"status": "CHARGING", "current_cp": cp_id})

        elif t == "DRIVER_STOP" and len(fields) >= 3:
            cp_id, driver_id = fields[1], fields[2]
            self.state.update_cp(cp_id, {
                "state": "ACTIVATED", "current_driver": None,
                "kwh_delivered": 0.0, "amount_euro": 0.0
            })
            self.state.update_driver(driver_id, {"status": "IDLE", "current_cp": None})

    def send_command(self, command, driver_id=None, cp_id=None, kwh_needed=10):
        if not self.sock:
            return False
        with self.lock:
            if command == "REQUEST_CHARGE":
                msg = Protocol.build_message(MessageTypes.REQUEST_CHARGE, driver_id, cp_id, kwh_needed)
            elif command == "FINISH_CHARGE":
                msg = Protocol.build_message(MessageTypes.END_CHARGE, driver_id, cp_id)
            else:
                return False
            self.sock.send(Protocol.encode(msg))
            return True

    def _close_socket(self):
        if self.sock:
            try: self.sock.close()
            except: pass
        self.sock = None

