# web_ui/web/socket_client.py
import socket
import threading
import time
import os
import ast
import json
import traceback
from shared.protocol import Protocol, MessageTypes


def _safe_parse(value, default):
    """
    UI receives FULL_STATE fields as strings.
    Central should send str(list), but we tolerate:
    - python literal lists/dicts
    - JSON strings
    """
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return default

    s = value.strip()
    if not s:
        return default

    # 1) python literal
    try:
        return ast.literal_eval(s)
    except Exception:
        pass

    # 2) json
    try:
        return json.loads(s)
    except Exception:
        return default


class CentralUIClient:
    def __init__(self, state, host, port):
        self.state = state
        self.host = host
        self.port = int(port)
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
        while self.running and self.sock:
            data = self.sock.recv(4096)
            if not data:
                raise ConnectionResetError("socket closed")

            self.recv_buffer += data

            while True:
                msg, valid = Protocol.decode(self.recv_buffer)
                if not valid:
                    break

                etx = self.recv_buffer.find(b"\x03")
                self.recv_buffer = self.recv_buffer[etx + 2:] if etx != -1 else b""

                try:
                    fields = Protocol.parse_message(msg)
                    if fields:
                        self._handle_message(fields)
                except Exception:
                    traceback.print_exc()

    def _handle_message(self, fields):
        t = fields[0]

        if t == "FULL_STATE":
            cps = _safe_parse(fields[1] if len(fields) > 1 else None, [])
            drivers = _safe_parse(fields[2] if len(fields) > 2 else None, [])
            hist = _safe_parse(fields[3] if len(fields) > 3 else None, [])
            try:
                self.state.set_full_state(cps, drivers, hist)
            except Exception:
                traceback.print_exc()
            return

        # LOG: ["LOG", source, text, timestamp]
        if t == "LOG" or (hasattr(MessageTypes, "LOG") and t == MessageTypes.LOG):
            try:
                source = fields[1] if len(fields) > 1 else "UNKNOWN"
                text = fields[2] if len(fields) > 2 else ""
                ts = fields[3] if len(fields) > 3 else None
                entry = {"source": source, "text": text, "time": ts or time.time()}
                self.state.add_log(entry)
            except Exception:
                traceback.print_exc()
            return

        if t == MessageTypes.SUPPLY_UPDATE and len(fields) >= 4:
            cp_id = fields[1]
            inc = float(fields[2])
            amt = float(fields[3])
            snap = self.state.snapshot()
            cp = snap.get("charging_points", {}).get(cp_id, {})
            new_kwh = float(cp.get("kwh_delivered", 0)) + inc
            self.state.update_cp(cp_id, {"kwh_delivered": new_kwh, "amount_euro": amt})
            return

        if t == MessageTypes.SUPPLY_END and len(fields) >= 5:
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
            return

        if t == "DRIVER_START" and len(fields) >= 3:
            cp_id, driver_id = fields[1], fields[2]
            self.state.update_cp(cp_id, {"state": "SUPPLYING", "current_driver": driver_id})
            self.state.update_driver(driver_id, {"status": "CHARGING", "current_cp": cp_id})
            return

        if t == "DRIVER_STOP" and len(fields) >= 3:
            cp_id, driver_id = fields[1], fields[2]
            self.state.update_cp(cp_id, {
                "state": "ACTIVATED", "current_driver": None,
                "kwh_delivered": 0.0, "amount_euro": 0.0
            })
            self.state.update_driver(driver_id, {"status": "IDLE", "current_cp": None})
            return

        if t == MessageTypes.AUTHORIZE and len(fields) >= 3:
            driver_id = fields[1]
            cp_id = fields[2]
            self.state.update_driver(driver_id, {"status": "CHARGING", "current_cp": cp_id})
            self.state.update_cp(cp_id, {"state": "SUPPLYING", "current_driver": driver_id})
            return

        if t == MessageTypes.DENY and len(fields) >= 2:
            driver_id = fields[1]
            self.state.update_driver(driver_id, {"status": "IDLE", "current_cp": None})
            return

        # Ignore unknown types safely

    def send_command(self, command, driver_id=None, cp_id=None, kwh_needed=10):
        with self.lock:
            if not self.sock:
                print("[socket_client] Socket not connected")
                return False
            try:
                if command == "REQUEST_CHARGE":
                    msg = Protocol.build_message(MessageTypes.REQUEST_CHARGE, driver_id, cp_id, kwh_needed)
                elif command == "FINISH_CHARGE":
                    msg = Protocol.build_message(MessageTypes.END_CHARGE, driver_id, cp_id)
                else:
                    return False

                self.sock.send(Protocol.encode(msg))
                return True
            except Exception as e:
                print(f"[socket_client] send_command error: {e}")
                self._close_socket()
                return False

    def _close_socket(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        self.sock = None
        self.recv_buffer = b""
