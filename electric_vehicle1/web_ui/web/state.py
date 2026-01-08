# web_ui/web/state.py
import threading
import time
from copy import deepcopy

class UIState:
    def __init__(self):
        self.lock = threading.Lock()
        self.charging_points = {}
        self.drivers = {}
        self.history = []
        self.logs = []               # NEW: list of log entries
        self.last_update = time.time()

    def _touch(self):
        self.last_update = time.time()

    def set_full_state(self, cps, drivers, history):
        with self.lock:
            self.charging_points = {}
            for item in cps:
                try:
                    cp_id = item[0]
                    self.charging_points[cp_id] = {
                        "state": item[1],
                        "location": [item[2], item[3]],
                        "price_per_kwh": float(item[4]),
                        "current_driver": item[5] or None,
                        "kwh_delivered": float(item[6]),
                        "amount_euro": float(item[7])
                    }
                except:
                    continue

            self.drivers = {}
            for d in drivers:
                try:
                    did = d[0]
                    self.drivers[did] = {
                        "status": d[1],
                        "current_cp": d[2] or None
                    }
                except:
                    continue

            self.history = history or []
            self._touch()

    def update_cp(self, cp_id, fields):
        with self.lock:
            cp = self.charging_points.setdefault(cp_id, {
                "state": "DISCONNECTED",
                "location": [0,0],
                "price_per_kwh": 0.0,
                "current_driver": None,
                "kwh_delivered": 0.0,
                "amount_euro": 0.0
            })
            cp.update(fields)
            self._touch()

    def update_driver(self, driver_id, fields):
        with self.lock:
            d = self.drivers.setdefault(driver_id, {"status": "IDLE", "current_cp": None})
            d.update(fields)
            self._touch()

    def add_history(self, session):
        with self.lock:
            self.history.insert(0, session)
            self.history = self.history[:100]
            self._touch()

    # NEW: add log entries for UI
    def add_log(self, entry):
        """
        entry: dict with keys {source, text, time}
        """
        with self.lock:
            try:
                # insert at beginning
                self.logs.insert(0, entry)
                # keep bounded size
                if len(self.logs) > 500:
                    self.logs = self.logs[:500]
            except Exception:
                pass
            self._touch()

    def snapshot(self):
        with self.lock:
            return {
                "charging_points": deepcopy(self.charging_points),
                "drivers": deepcopy(self.drivers),
                "history": deepcopy(self.history),
                "logs": deepcopy(self.logs),   # NEW
                "timestamp": self.last_update
            }
