import requests
import threading
import time
import json
from config import WEATHER_API_KEY, WEATHER_UPDATE_INTERVAL, WEATHER_ALERT_THRESHOLD
from datetime import datetime

class EVWeather:
    def __init__(self, central_api_url="http://localhost:5000"):
        self.central_api_url = central_api_url
        self.locations = {}  # {cp_id: city_name}
        self.running = True
        self.lock = threading.Lock()
        print("[EV_Weather] Initializing...")
    
    def add_location(self, cp_id, city):
        """Add CP location to monitor"""
        with self.lock:
            self.locations[cp_id] = city
        print(f"[EV_Weather] ✅ Added {cp_id} -> {city}")
    
    def get_temperature(self, city):
        """Fetch temperature from OpenWeather"""
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={WEATHER_API_KEY}&units=metric"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                return data['main']['temp']
            return None
        except Exception as e:
            print(f"[EV_Weather] ❌ Error fetching weather: {e}")
            return None
    
    def notify_central(self, cp_id, alert_status, temperature):
        """Send alert to Central via REST"""
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
            print(f"[EV_Weather] 📤 Alert sent for {cp_id}: {alert_status} ({temperature}°C)")
        except Exception as e:
            print(f"[EV_Weather] ⚠️ Failed to notify Central: {e}")
    
    def monitor_loop(self):
        """Continuously monitor weather"""
        cp_states = {}  # Track previous state
        
        while self.running:
            time.sleep(WEATHER_UPDATE_INTERVAL)
            
            with self.lock:
                for cp_id, city in self.locations.items():
                    temp = self.get_temperature(city)
                    
                    if temp is None:
                        continue
                    
                    is_alert = temp < WEATHER_ALERT_THRESHOLD
                    prev_state = cp_states.get(cp_id, None)
                    
                    # Only notify on state change
                    if is_alert != prev_state:
                        status = "ALERT_COLD" if is_alert else "WEATHER_OK"
                        self.notify_central(cp_id, status, temp)
                        cp_states[cp_id] = is_alert
    
    def menu_loop(self):
        """Interactive menu"""
        while self.running:
            try:
                print("\n[EV_Weather Menu]")
                print("1. Add location")
                print("2. View locations")
                print("3. Exit")
                choice = input("Choice: ").strip()
                
                if choice == "1":
                    cp_id = input("CP ID: ").strip()
                    city = input("City name: ").strip()
                    self.add_location(cp_id, city)
                
                elif choice == "2":
                    with self.lock:
                        print("\nMonitored locations:")
                        for cp_id, city in self.locations.items():
                            print(f"  {cp_id} -> {city}")
                
                elif choice == "3":
                    self.running = False
                    break
            except Exception as e:
                print(f"Error: {e}")
    
    def auto_discover_cps(self):
        """Fetch CPs from registry and add default locations"""
        try:
            response = requests.get(f"{self.central_api_url.replace(':5000', ':5001')}/list", timeout=5)
            if response.status_code == 200:
                data = response.json()
                for cp in data.get('charging_points', []):
                    cp_id = cp['cp_id']
                    # Default to Madrid for demo (can parse lat/lon for real cities)
                    city = "Madrid"
                    self.add_location(cp_id, city)
        except Exception as e:
            print(f"[EV_Weather] Auto-discover failed: {e}")

    def run(self):
        """Start weather service"""
        # Auto-discover CPs on startup
        print("[EV_Weather] Auto-discovering charging points...")
        time.sleep(2)  # Wait for registry
        self.auto_discover_cps()
        
        monitor_thread = threading.Thread(target=self.monitor_loop, daemon=True)
        monitor_thread.start()
        
        try:
            self.menu_loop()
        except KeyboardInterrupt:
            print("\n[EV_Weather] Shutting down...")
        finally:
            self.running = False

if __name__ == "__main__":
    weather = EVWeather()
    weather.run()