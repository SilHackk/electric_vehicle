# web_ui/web/socket_client.py - FIXED VERSION
# Fixes:
# 1. CP-002 not showing in available list
# 2. CP shows SUPPLYING when it's not
# 3. Can't stop charging properly
# 4. State synchronization issues

import socket
import threading
import time
import ast
import json
import traceback
from shared.protocol import Protocol, MessageTypes


def _safe_parse(value, default):
    """Parse strings safely - handles both Python literals and JSON"""
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return default

    s = value.strip()
    if not s:
        return default

    # Try python literal first
    try:
        return ast.literal_eval(s)
    except Exception:
        pass

    # Try JSON
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
        self.connected = False
        
        # Start connection loop
        threading.Thread(target=self._connect_loop, daemon=True).start()

    def _connect_loop(self):
        """Keep trying to connect to Central"""
        while self.running:
            try:
                if not self.sock or not self.connected:
                    self._connect_and_register()
                self._listen_loop()
            except Exception as e:
                print(f"[UIClient] Connection error: {e}")
                self._close_socket()
                time.sleep(2)

    def _connect_and_register(self):
        """Connect to Central and register as monitor"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            s.connect((self.host, self.port))
            s.settimeout(None)
            self.sock = s

            # Register as WEB_UI monitor
            msg = Protocol.build_message(MessageTypes.REGISTER, "MONITOR", self.client_id)
            s.send(Protocol.encode(msg))
            
            self.connected = True
            print(f"[UIClient] ✅ Connected to Central at {self.host}:{self.port}")
            
        except Exception as e:
            print(f"[UIClient] ❌ Failed to connect: {e}")
            self._close_socket()
            raise

    def _close_socket(self):
        """Safely close socket"""
        self.connected = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        self.sock = None
        self.recv_buffer = b""

    def _listen_loop(self):
        """Listen for messages from Central"""
        while self.running and self.sock and self.connected:
            try:
                data = self.sock.recv(4096)
                if not data:
                    raise ConnectionResetError("Socket closed by Central")

                self.recv_buffer += data

                # Process all complete messages in buffer
                while True:
                    msg, valid = Protocol.decode(self.recv_buffer)
                    if not valid:
                        break

                    # Remove processed message from buffer
                    etx = self.recv_buffer.find(b"\x03")
                    self.recv_buffer = self.recv_buffer[etx + 2:] if etx != -1 else b""

                    try:
                        fields = Protocol.parse_message(msg)
                        if fields:
                            self._handle_message(fields)
                    except Exception as e:
                        print(f"[UIClient] Message handling error: {e}")
                        traceback.print_exc()

            except Exception as e:
                print(f"[UIClient] Listen loop error: {e}")
                raise

    def _handle_message(self, fields):
        """Handle different message types from Central"""
        if not fields or len(fields) == 0:
            return

        msg_type = fields[0]

        # =====================================================================
        # FULL_STATE - Complete system state (most important!)
        # =====================================================================
        if msg_type == "FULL_STATE":
            cps_raw = fields[1] if len(fields) > 1 else None
            drivers_raw = fields[2] if len(fields) > 2 else None
            history_raw = fields[3] if len(fields) > 3 else None

            cps = _safe_parse(cps_raw, [])
            drivers = _safe_parse(drivers_raw, [])
            history = _safe_parse(history_raw, [])

            try:
                self.state.set_full_state(cps, drivers, history)
                print(f"[UIClient] 📊 FULL_STATE received: {len(cps)} CPs, {len(drivers)} drivers")
            except Exception as e:
                print(f"[UIClient] Error setting full state: {e}")
                traceback.print_exc()
            return

        # =====================================================================
        # CP_CONNECTED - New CP connected (add to state)
        # =====================================================================
        if msg_type == "CP_CONNECTED" and len(fields) >= 2:
            cp_id = fields[1]
            
            # Parse payload if exists
            if len(fields) > 2:
                try:
                    payload = json.loads(fields[2])
                    state = payload.get("state", "ACTIVATED")
                    location = payload.get("location", [0, 0])
                    price = payload.get("price_per_kwh", 0.30)
                except:
                    state = "ACTIVATED"
                    location = [0, 0]
                    price = 0.30
            else:
                state = "ACTIVATED"
                location = [0, 0]
                price = 0.30

            # Add/update CP in state
            self.state.update_cp(cp_id, {
                "state": state,
                "location": location,
                "price_per_kwh": price,
                "current_driver": None,
                "kwh_delivered": 0.0,
                "amount_euro": 0.0
            })
            print(f"[UIClient] ✅ CP_CONNECTED: {cp_id}")
            return

        # =====================================================================
        # CP_STATE - CP state changed
        # =====================================================================
        if msg_type == "CP_STATE" and len(fields) >= 3:
            cp_id = fields[1]
            new_state = fields[2]
            
            self.state.update_cp(cp_id, {"state": new_state})
            print(f"[UIClient] 🔄 CP_STATE: {cp_id} → {new_state}")
            return

        # =====================================================================
        # DRIVER_START - Driver started charging
        # =====================================================================
        if msg_type == "DRIVER_START" and len(fields) >= 3:
            cp_id = fields[1]
            driver_id = fields[2]
            
            # Update CP to SUPPLYING
            self.state.update_cp(cp_id, {
                "state": "SUPPLYING",
                "current_driver": driver_id,
                "kwh_delivered": 0.0,
                "amount_euro": 0.0
            })
            
            # Update driver to CHARGING
            self.state.update_driver(driver_id, {
                "status": "CHARGING",
                "current_cp": cp_id
            })
            
            print(f"[UIClient] 🔌 DRIVER_START: {driver_id} → {cp_id}")
            return

        # =====================================================================
        # SUPPLY_UPDATE - Real-time charging progress
        # =====================================================================
        if msg_type == MessageTypes.SUPPLY_UPDATE and len(fields) >= 4:
            cp_id = fields[1]
            kwh_inc = float(fields[2])
            amount = float(fields[3])
            
            # Get current CP state
            snap = self.state.snapshot()
            cp = snap.get("charging_points", {}).get(cp_id, {})
            
            # Add increment to current value
            new_kwh = float(cp.get("kwh_delivered", 0)) + kwh_inc
            
            # Update CP with new values
            self.state.update_cp(cp_id, {
                "kwh_delivered": new_kwh,
                "amount_euro": amount
            })
            
            # Debug print (remove in production)
            # print(f"[UIClient] ⚡ UPDATE: {cp_id} {new_kwh:.2f}kWh €{amount:.2f}")
            return

        # =====================================================================
        # SUPPLY_END - Charging completed
        # =====================================================================
        if msg_type == MessageTypes.SUPPLY_END and len(fields) >= 5:
            cp_id = fields[1]
            driver_id = fields[2]
            total_kwh = float(fields[3])
            total_amount = float(fields[4])
            
            # Reset CP to ACTIVATED
            self.state.update_cp(cp_id, {
                "state": "ACTIVATED",
                "current_driver": None,
                "kwh_delivered": 0.0,
                "amount_euro": 0.0
            })
            
            # Reset driver to IDLE
            self.state.update_driver(driver_id, {
                "status": "IDLE",
                "current_cp": None
            })
            
            # Add to history
            self.state.add_history({
                "cp_id": cp_id,
                "driver_id": driver_id,
                "kwh_delivered": total_kwh,
                "total_amount": total_amount,
                "timestamp": time.time()
            })
            
            print(f"[UIClient] ✅ SUPPLY_END: {driver_id} finished at {cp_id} ({total_kwh}kWh, €{total_amount})")
            return

        # =====================================================================
        # DRIVER_STOP - Driver manually stopped (same as SUPPLY_END cleanup)
        # =====================================================================
        if msg_type == "DRIVER_STOP" and len(fields) >= 3:
            cp_id = fields[1]
            driver_id = fields[2]
            
            # Reset CP
            self.state.update_cp(cp_id, {
                "state": "ACTIVATED",
                "current_driver": None,
                "kwh_delivered": 0.0,
                "amount_euro": 0.0
            })
            
            # Reset driver
            self.state.update_driver(driver_id, {
                "status": "IDLE",
                "current_cp": None
            })
            
            print(f"[UIClient] 🛑 DRIVER_STOP: {driver_id} at {cp_id}")
            return

        # =====================================================================
        # AUTHORIZE - Charge was authorized
        # =====================================================================
        if msg_type == MessageTypes.AUTHORIZE and len(fields) >= 3:
            driver_id = fields[1]
            cp_id = fields[2]
            
            # Mark driver as charging (UI might show this immediately)
            self.state.update_driver(driver_id, {
                "status": "CHARGING",
                "current_cp": cp_id
            })
            
            print(f"[UIClient] ✅ AUTHORIZE: {driver_id} authorized at {cp_id}")
            return

        # =====================================================================
        # DENY - Charge was denied
        # =====================================================================
        if msg_type == MessageTypes.DENY and len(fields) >= 2:
            driver_id = fields[1]
            reason = fields[3] if len(fields) > 3 else "UNKNOWN"
            
            # Reset driver to IDLE
            self.state.update_driver(driver_id, {
                "status": "IDLE",
                "current_cp": None
            })
            
            print(f"[UIClient] ❌ DENY: {driver_id} denied ({reason})")
            return

        # =====================================================================
        # LOG - System log message
        # =====================================================================
        if msg_type == "LOG" or (hasattr(MessageTypes, "LOG") and msg_type == MessageTypes.LOG):
            try:
                source = fields[1] if len(fields) > 1 else "UNKNOWN"
                text = fields[2] if len(fields) > 2 else ""
                ts = fields[3] if len(fields) > 3 else None
                
                entry = {
                    "source": str(source),
                    "text": str(text),
                    "time": ts or time.time()
                }
                
                self.state.add_log(entry)
                # Don't print every log (too noisy)
            except Exception as e:
                print(f"[UIClient] Error processing LOG: {e}")
            return

        # =====================================================================
        # CHARGING_COMPLETE - Driver finished charging (informational)
        # =====================================================================
        if msg_type == "CHARGING_COMPLETE" and len(fields) >= 3:
            cp_id = fields[1]
            driver_id = fields[2]
            print(f"[UIClient] 🔋 CHARGING_COMPLETE: {driver_id} at {cp_id}")
            # State update happens in SUPPLY_END
            return

        # =====================================================================
        # FAULT / RECOVERY - CP health status
        # =====================================================================
        if msg_type == MessageTypes.FAULT and len(fields) >= 2:
            cp_id = fields[1]
            self.state.update_cp(cp_id, {"state": "OUT_OF_ORDER"})
            print(f"[UIClient] ⚠️ FAULT: {cp_id}")
            return

        if msg_type == MessageTypes.RECOVERY and len(fields) >= 2:
            cp_id = fields[1]
            self.state.update_cp(cp_id, {"state": "ACTIVATED"})
            print(f"[UIClient] ✅ RECOVERY: {cp_id}")
            return

        # =====================================================================
        # Unknown message type - log but don't crash
        # =====================================================================
        # print(f"[UIClient] ⚠️ Unknown message type: {msg_type}")

    def send_command(self, command, driver_id=None, cp_id=None, kwh_needed=10):
        """Send command to Central (from Web UI driver control)"""
        with self.lock:
            if not self.sock or not self.connected:
                print(f"[UIClient] ❌ Cannot send command: not connected")
                return False

            try:
                if command == "REQUEST_CHARGE":
                    msg = Protocol.build_message(
                        MessageTypes.REQUEST_CHARGE,
                        driver_id,
                        cp_id,
                        kwh_needed
                    )
                elif command == "FINISH_CHARGE":
                    msg = Protocol.build_message(
                        MessageTypes.END_CHARGE,
                        driver_id,
                        cp_id
                    )
                else:
                    print(f"[UIClient] ❌ Unknown command: {command}")
                    return False

                self.sock.send(Protocol.encode(msg))
                print(f"[UIClient] 📤 Sent {command}: {driver_id} → {cp_id}")
                return True

            except Exception as e:
                print(f"[UIClient] ❌ send_command error: {e}")
                self._close_socket()
                return False

    def is_connected(self):
        """Check if currently connected to Central"""
        return self.connected and self.sock is not None