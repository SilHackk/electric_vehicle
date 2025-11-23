# web_ui/web/state.py
import threading
import time
from copy import deepcopy

class UIState:
    def __init__(self):
        self.lock = threading.Lock()
        self.charging_points = {}  # cp_id -> dict
        self.drivers = {}          # driver_id -> dict
        self.history = []          # list of sessions
        self.last_update = time.time()

    def set_full_state(self, cps, drivers, history):
        with self.lock:
            # cps expected as: list of [cp_id, state, lat, lon, price, current_driver, kwh, amount]
            self.charging_points = {}
            for item in cps:
                try:
                    cp_id = item[0]
                    self.charging_points[cp_id] = {
                        "state": item[1],
                        "location": [item[2], item[3]],
                        "price_per_kwh": float(item[4]),
                        "current_driver": item[5] if item[5] else None,
                        "kwh_delivered": float(item[6]),
                        "amount_euro": float(item[7])
                    }
                except Exception:
                    # if parsing fails, skip
                    continue

            # drivers expected as list of [driver_id, status, current_cp]
            self.drivers = {}
            for d in drivers:
                try:
                    did = d[0]
                    self.drivers[did] = {
                        "status": d[1],
                        "current_cp": d[2] if d[2] else None
                    }
                except Exception:
                    continue

            # history: list of session dicts (already parsed by central)
            self.history = history or []
            self.last_update = time.time()

    def update_cp(self, cp_id, fields: dict):
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
            self.last_update = time.time()

    def update_driver(self, driver_id, fields: dict):
        with self.lock:
            d = self.drivers.setdefault(driver_id, {"status": "IDLE", "current_cp": None})
            d.update(fields)
            self.last_update = time.time()

    def add_history(self, session):
        with self.lock:
            self.history.insert(0, session)
            # keep only recent 100
            self.history = self.history[:100]
            self.last_update = time.time()

    def snapshot(self):
        with self.lock:
            return {
                "charging_points": deepcopy(self.charging_points),
                "drivers": deepcopy(self.drivers),
                "history": deepcopy(self.history),
                "timestamp": self.last_update
            }
