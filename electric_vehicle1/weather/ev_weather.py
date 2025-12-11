import requests
import threading
import time
from config import WEATHER_API_KEY, WEATHER_UPDATE_INTERVAL, WEATHER_ALERT_THRESHOLD
from datetime import datetime

WEATHER_API_KEY = "99e654a32d0b0ae64255762e50a81239"
WEATHER_UPDATE_INTERVAL = 4  # seconds
WEATHER_SERVICE_URL = os.getenv("WEATHER_SERVICE_URL", "http://localhost:5002")
WEATHER_ALERT_THRESHOLD = 0  # degrees Celsius - below this = alert

class EVWeather:
    def __init__(self, central_api_url="http://central:5003"):  # FIX: Port 5003
        self.central_api_url = central_api_url
        self.locations = {}
        self.running = True
        self.lock = threading.Lock()
        print("[EV_Weather] Initializing...")
    
    def add_location(self, cp_id, city):
        with self.lock:
            self.locations[cp_id] = city
        print(f"[EV_Weather] ✅ Added {cp_id} -> {city}")
    
    def get_temperature(self, city):
        try:
            url = f"https://api.openweathermap``.org``/data/2.5/weather?q={city}&appid={WEATHER_API_KEY}&units=metric"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return response.json()['main']['temp']
            return None
        except Exception as e:
            print(f"[EV_Weather] ❌ Error: {e}")
            return None
    
    def notify_central(self, cp_id, alert_status, temperature):
        try:
            payload = {
                "cp_id": cp_id,
                "alert": alert_status,
                "temperature": temperature,
                "timestamp": datetime.now().isoformat()
            }
            response = requests.post(
                f"{self.central_api_url}/api/weather_alert",
                json=payload,
                timeout=5
            )
            print(f"[EV_Weather] 📤 {alert_status} for {cp_id} ({temperature}°C)")
        except Exception as e:
            print(f"[EV_Weather] ⚠️ Failed: {e}")
    
    def monitor_loop(self):
        cp_states = {}
        while self.running:
            time.sleep(WEATHER_UPDATE_INTERVAL)
            with self.lock:
                for cp_id, city in self.locations.items():
                    temp = self.get_temperature(city)
                    if temp is None:
                        continue
                    is_alert = temp < WEATHER_ALERT_THRESHOLD
                    prev_state = cp_states.get(cp_id, None)
                    if is_alert != prev_state:
                        status = "ALERT_COLD" if is_alert else "WEATHER_OK"
                        self.notify_central(cp_id, status, temp)
                        cp_states[cp_id] = is_alert
    
    def auto_discover_cps(self):
        try:
            # FIX: Use registry:5001
            response = requests.get("http://registry:5001/list", timeout=5)
            if response.status_code == 200:
                for cp in response.json().get('charging_points', []):
                    self.add_location(cp['cp_id'], "Madrid")  # Default
        except Exception as e:
            print(f"[EV_Weather] Auto-discover failed: {e}")
    
    def menu_loop(self):
        while self.running:
            try:
                print("\n[EV_Weather Menu]")
                print("1. Add location")
                print("2. View locations")
                print("3. Exit")
                choice = input("Choice: ").strip()
                if choice == "1":
                    cp_id = input("CP ID: ").strip()
                    city = input("City: ").strip()
                    self.add_location(cp_id, city)
                elif choice == "2":
                    with self.lock:
                        for cp_id, city in self.locations.items():
                            print(f"  {cp_id} -> {city}")
                elif choice == "3":
                    self.running = False
                    break
            except Exception as e:
                print(f"Error: {e}")
    
    def run(self):
        print("[EV_Weather] Starting...")
        time.sleep(3)  # Wait for services
        self.auto_discover_cps()
        threading.Thread(target=self.monitor_loop, daemon=True).start()
        try:
            self.menu_loop()
        except KeyboardInterrupt:
            print("\n[EV_Weather] Shutting down...")
        finally:
            self.running = False

if __name__ == "__main__":
    weather = EVWeather()
    weather.run()