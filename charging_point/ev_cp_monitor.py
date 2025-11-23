# ============================================================================
# EVCharging System - EV_CP_M (Charging Point Monitor) - FANCY UI
# ============================================================================

import socket
import threading
import time
import sys
import os
from config import CP_BASE_PORT, HEALTH_CHECK_INTERVAL
from shared.protocol import Protocol, MessageTypes
from datetime import datetime


class EVCPMonitor:
    def __init__(self, cp_id, engine_host="localhost", engine_port=None,
                 central_host="localhost", central_port=5000):
        self.cp_id = cp_id
        self.engine_host = engine_host
        self.engine_port = engine_port or (CP_BASE_PORT + int(cp_id.split('-')[1]))
        self.central_host = central_host
        self.central_port = central_port

        self.engine_socket = None
        self.central_socket = None
        self.running = True
        self.lock = threading.Lock()

        # Status tracking
        self.engine_healthy = True
        self.consecutive_failures = 0
        self.failure_threshold = 3

        # Driver tracking
        self.current_driver = None
        self.charging_active = False
        self.charge_start_time = None
        self.charge_kwh_needed = 0
        self.charge_progress = 0
        self.last_progress_update = 0
        self.charging_complete = False

        print(f"[{self.cp_id} Monitor] Initializing...")
        print(f"[{self.cp_id} Monitor] Central: {self.central_host}:{self.central_port}")
        print(f"[{self.cp_id} Monitor] Engine: {self.engine_host}:{self.engine_port}")

    def connect_to_engine(self):
        """Connect to CP Engine"""
        print(f"[{self.cp_id} Monitor] Connecting to Engine at {self.engine_host}:{self.engine_port}...")
        
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.engine_socket.connect((self.engine_host, self.engine_port))
                print(f"[{self.cp_id} Monitor] ✅ Connected to Engine")
                return True
            except Exception as e:
                print(f"[{self.cp_id} Monitor] Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"[{self.cp_id} Monitor] Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    print(f"[{self.cp_id} Monitor] ❌ Failed to connect to Engine after {max_retries} attempts")
                    return False

    def connect_to_central(self):
        """Connect to CENTRAL"""
        print(f"[{self.cp_id} Monitor] Connecting to CENTRAL at {self.central_host}:{self.central_port}...")
        
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.central_socket.connect((self.central_host, self.central_port))

                # Register as monitor with CENTRAL
                register_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.REGISTER, "MONITOR", self.cp_id, self.cp_id)
                )
                self.central_socket.send(register_msg)

                print(f"[{self.cp_id} Monitor] ✅ Connected to CENTRAL")
                return True
            except Exception as e:
                print(f"[{self.cp_id} Monitor] Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"[{self.cp_id} Monitor] Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    print(f"[{self.cp_id} Monitor] ❌ Failed to connect to CENTRAL after {max_retries} attempts")
                    return False

    def _listen_central(self):
        """Listen for driver notifications from CENTRAL"""
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

                            # Handle driver start notification
                            if msg_type == "DRIVER_START":
                                driver_id = fields[2]
                                with self.lock:
                                    self.current_driver = driver_id
                                    self.charging_active = True
                                    self.charge_start_time = datetime.now()
                                    self.charge_kwh_needed = 10
                                    self.charge_progress = 0
                                    self.last_progress_update = 0
                                    self.charging_complete = False
                                
                                print(f"\n╔{'═'*68}╗")
                                print(f"║{' '*68}║")
                                print(f"║  🚗  DRIVER CONNECTED - CHARGING SESSION STARTED  {'⚡':<22}║")
                                print(f"║{' '*68}║")
                                print(f"╠{'═'*68}╣")
                                print(f"║  📅 Start Time: {self.charge_start_time.strftime('%H:%M:%S'):<50}║")
                                print(f"║  ⏱️  Estimated Duration: ~14 seconds{' '*32}║")
                                print(f"║  🔋 Target: 100%{' '*52}║")
                                print(f"╚{'═'*68}╝\n")
                                
                                # Start progress monitoring thread
                                progress_thread = threading.Thread(
                                    target=self._monitor_progress,
                                    daemon=True
                                )
                                progress_thread.start()

                            # Handle charging complete notification
                            elif msg_type == "CHARGING_COMPLETE":
                                with self.lock:
                                    self.charging_complete = True
                                    self.charge_progress = 100
                                
                                print(f"\n╔{'═'*68}╗")
                                print(f"║{' '*68}║")
                                print(f"║  🎉  CHARGING COMPLETE - 100% REACHED!  {'🔋':<26}║")
                                print(f"║{' '*68}║")
                                print(f"╚{'═'*68}╝\n")

                            # Handle driver stop notification (unplugged)
                            elif msg_type == "DRIVER_STOP":
                                with self.lock:
                                    was_charging = self.charging_active or self.charging_complete
                                    final_progress = self.charge_progress
                                    self.current_driver = None
                                    self.charging_active = False
                                    self.charging_complete = False
                                    self.charge_progress = 0
                                
                                if was_charging:
                                    if final_progress >= 100:
                                        # Full charge completed
                                        print(f"\n╔{'═'*68}╗")
                                        print(f"║{' '*68}║")
                                        print(f"║  ✅  VEHICLE UNPLUGGED - SESSION COMPLETE  {'🔌':<24}║")
                                        print(f"║{' '*68}║")
                                        print(f"╠{'═'*68}╣")
                                        print(f"║  🔋 Final Charge: 100%{' '*44}║")
                                        print(f"║  💚 Battery: Full{' '*50}║")
                                        print(f"║  🎫 Ticket: Sent{' '*50}║")
                                        print(f"╚{'═'*68}╝\n")
                                    else:
                                        # Early disconnect
                                        print(f"\n╔{'═'*68}╗")
                                        print(f"║{' '*68}║")
                                        print(f"║  ⚠️   VEHICLE UNPLUGGED - EARLY DISCONNECT  {'🔌':<22}║")
                                        print(f"║{' '*68}║")
                                        print(f"╠{'═'*68}╣")
                                        print(f"║  🔋 Final Charge: {final_progress}%{' '*(45 - len(str(final_progress)))}║")
                                        print(f"║  ⚡ Status: Partial charge{' '*42}║")
                                        print(f"║  🎫 Ticket: Sent{' '*50}║")
                                        print(f"╚{'═'*68}╝\n")

                        else:
                            break

                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"[{self.cp_id} Monitor] Listener error: {e}")
                    break

        except Exception as e:
            print(f"[{self.cp_id} Monitor] CENTRAL connection lost: {e}")

    def _monitor_progress(self):
        """Monitor and display charging progress"""
        while self.running:
            # Check status
            with self.lock:
                should_stop = not self.charging_active and not self.charging_complete
                is_complete = self.charging_complete
                is_active = self.charging_active
                start_time = self.charge_start_time
            
            # Exit if no longer charging and not complete
            if should_stop:
                break
            
            # If charging complete, spam message every 2 seconds
            if is_complete:
                print(f"🔋 ⚡ 100% CHARGED - Please unplug vehicle to complete session ⚡ 🔋")
                time.sleep(2)
                continue
            
            # Calculate elapsed time
            if is_active and start_time:
                elapsed = (datetime.now() - start_time).total_seconds()
                
                # Calculate progress (14 seconds = 100%)
                progress = min(int((elapsed / 14.0) * 100), 100)
                
                # Only print every 2 seconds (approximately)
                with self.lock:
                    if progress >= self.last_progress_update + 14:  # 14% every 2 seconds
                        self.last_progress_update = progress
                        self.charge_progress = progress
                        
                        # Create progress bar with percentage
                        bar_length = 40
                        filled = int(bar_length * progress / 100)
                        bar = '█' * filled + '░' * (bar_length - filled)
                        
                        # Time remaining
                        time_remaining = max(0, 14 - int(elapsed))
                        
                        print(f"⚡ [{bar}] {progress}% | ⏱️  {time_remaining}s remaining")
                        
                        if progress >= 100:
                            self.charging_complete = True
            
            time.sleep(2)

    def health_check_loop(self):
        """Send health checks to engine every second"""
        while self.running:
            time.sleep(HEALTH_CHECK_INTERVAL)

            try:
                if not self.engine_socket:
                    continue

                # Send health check
                health_msg = Protocol.encode(
                    Protocol.build_message(MessageTypes.HEALTH_CHECK, self.cp_id)
                )
                self.engine_socket.send(health_msg)

                # Wait for response with timeout
                self.engine_socket.settimeout(2)
                try:
                    response_data = self.engine_socket.recv(4096)

                    if response_data:
                        response, is_valid = Protocol.decode(response_data)

                        if is_valid:
                            fields = Protocol.parse_message(response)

                            if fields[0] == "HEALTH_OK":
                                with self.lock:
                                    if not self.engine_healthy:
                                        print(f"\n╔{'═'*68}╗")
                                        print(f"║{' '*68}║")
                                        print(f"║  ✅  ENGINE RECOVERED - System operational  {'💚':<24}║")
                                        print(f"║{' '*68}║")
                                        print(f"╚{'═'*68}╝\n")
                                        
                                        # Send recovery to CENTRAL
                                        recovery_msg = Protocol.encode(
                                            Protocol.build_message(
                                                MessageTypes.RECOVERY, self.cp_id
                                            )
                                        )
                                        self.central_socket.send(recovery_msg)

                                    self.engine_healthy = True
                                    self.consecutive_failures = 0

                            elif fields[0] == "HEALTH_KO":
                                self._handle_engine_fault()

                        else:
                            self._handle_engine_fault()
                    else:
                        self._handle_engine_fault()

                except socket.timeout:
                    self._handle_engine_fault()

            except Exception as e:
                print(f"[{self.cp_id} Monitor] Health check error: {e}")
                self._handle_engine_fault()

    def _handle_engine_fault(self):
        """Handle engine fault detection"""
        with self.lock:
            self.consecutive_failures += 1

            if self.consecutive_failures >= self.failure_threshold:
                if self.engine_healthy:
                    self.engine_healthy = False
                    print(f"\n╔{'═'*68}╗")
                    print(f"║{' '*68}║")
                    print(f"║  ⚠️   ENGINE FAULT DETECTED - Critical failure!  {'🔴':<18}║")
                    print(f"║{' '*68}║")
                    print(f"╠{'═'*68}╣")
                    print(f"║  🔧 Status: Not responding to health checks{' '*23}║")
                    print(f"║  📡 Action: Notifying central system...{' '*27}║")
                    print(f"║  ⏳ Recovery: Monitoring for reconnection{' '*24}║")
                    print(f"╚{'═'*68}╝\n")

                    # Send fault to CENTRAL
                    try:
                        fault_msg = Protocol.encode(
                            Protocol.build_message(MessageTypes.FAULT, self.cp_id)
                        )
                        self.central_socket.send(fault_msg)
                    except Exception as e:
                        print(f"[{self.cp_id} Monitor] Failed to send fault: {e}")

    def display_menu(self):
        """Display interactive menu"""
        print(f"\n╔{'═'*68}╗")
        print(f"║{' '*68}║")
        print(f"║  {self.cp_id} MONITOR - Ready for operation  {'🖥️':<28}║")
        print(f"║{' '*68}║")
        print(f"╚{'═'*68}╝\n")
        
        while self.running:
            try:
                cmd = input(f"\n[{self.cp_id} Monitor]> ").strip().lower()

                if cmd == "help":
                    print(f"\n╔{'═'*68}╗")
                    print(f"║  {self.cp_id} MONITOR COMMANDS{' '*40}║")
                    print(f"╠{'═'*68}╣")
                    print(f"║  status  - View current status and health{' '*26}║")
                    print(f"║  help    - Show this help menu{' '*37}║")
                    print(f"║  quit    - Exit monitor{' '*44}║")
                    print(f"╚{'═'*68}╝\n")

                elif cmd == "status":
                    with self.lock:
                        engine_icon = "💚" if self.engine_healthy else "🔴"
                        engine_text = "HEALTHY" if self.engine_healthy else "FAULTY"
                        
                        if self.charging_complete:
                            status_text = "CHARGED TO 100% - WAITING FOR UNPLUG"
                            status_icon = "🔋"
                        elif self.charging_active:
                            status_text = "CHARGING IN PROGRESS"
                            status_icon = "⚡"
                        else:
                            status_text = "AVAILABLE"
                            status_icon = "🟢"
                        
                        print(f"\n╔{'═'*68}╗")
                        print(f"║  {self.cp_id} STATUS REPORT{' '*42}║")
                        print(f"╠{'═'*68}╣")
                        print(f"║  Engine Status: {engine_text} {engine_icon}{' '*(43 - len(engine_text))}║")
                        print(f"║  Health Checks: {self.consecutive_failures}/{self.failure_threshold} failures{' '*37}║")
                        print(f"║  Point Status: {status_text} {status_icon}{' '*(43 - len(status_text))}║")
                        
                        if self.charging_active or self.charging_complete:
                            if self.charge_start_time:
                                elapsed = (datetime.now() - self.charge_start_time).total_seconds()
                                print(f"╠{'═'*68}╣")
                                print(f"║  Time Elapsed: {int(elapsed)}s{' '*(51 - len(str(int(elapsed))))}║")
                                print(f"║  Progress: {self.charge_progress}%{' '*(55 - len(str(self.charge_progress)))}║")
                        
                        print(f"╚{'═'*68}╝\n")

                elif cmd == "quit":
                    print(f"\n╔{'═'*68}╗")
                    print(f"║{' '*68}║")
                    print(f"║  {self.cp_id} Monitor shutting down...  {'👋':<32}║")
                    print(f"║{' '*68}║")
                    print(f"╚{'═'*68}╝\n")
                    self.running = False
                    break

                elif cmd == "":
                    continue

                else:
                    print(f"\n❌ Unknown command: '{cmd}'. Type 'help' for available commands.\n")

            except EOFError:
                break
            except KeyboardInterrupt:
                print(f"\n\n╔{'═'*68}╗")
                print(f"║{' '*68}║")
                print(f"║  {self.cp_id} Monitor interrupted  {'⚠️':<36}║")
                print(f"║{' '*68}║")
                print(f"╚{'═'*68}╝\n")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}\n")

    def run(self):
        """Run the monitor"""
        print(f"[{self.cp_id} Monitor] Starting...")

        if not self.connect_to_engine():
            print(f"[{self.cp_id} Monitor] Cannot connect to Engine")
            return

        if not self.connect_to_central():
            print(f"[{self.cp_id} Monitor] Cannot connect to CENTRAL")
            return

        # Start CENTRAL listener
        central_thread = threading.Thread(
            target=self._listen_central,
            daemon=True
        )
        central_thread.start()

        # Start health check loop in background
        health_thread = threading.Thread(
            target=self.health_check_loop,
            daemon=True
        )
        health_thread.start()

        # Display menu
        try:
            self.display_menu()
        except KeyboardInterrupt:
            print(f"\n[{self.cp_id} Monitor] Shutting down...")
        finally:
            self.running = False
            if self.engine_socket:
                self.engine_socket.close()
            if self.central_socket:
                self.central_socket.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ev_cp_monitor.py <CP_ID> [engine_host] [engine_port] "
              "[central_host] [central_port]")
        sys.exit(1)

    cp_id = sys.argv[1]
    engine_host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
    engine_port = int(sys.argv[3]) if len(sys.argv) > 3 else None
    central_host = sys.argv[4] if len(sys.argv) > 4 else "localhost"
    central_port = int(sys.argv[5]) if len(sys.argv) > 5 else 5000

    monitor = EVCPMonitor(cp_id, engine_host, engine_port, central_host, central_port)
    monitor.run()