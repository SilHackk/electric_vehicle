# ============================================================================
# EV_Weather - Orų monitoringo modulis
# Tikrina miestų temperatūrą ir praneša Central apie žemas temperatūras
# ============================================================================

import requests
import threading
import time
import os
from datetime import datetime



# TAVO API KEY (iš pradinio pranešimo)
WEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Konfigūracija
WEATHER_UPDATE_INTERVAL = 4  # sekundės tarp tikrinimų
WEATHER_ALERT_THRESHOLD = 0  # laipsniai Celsijaus - žemiau šio = alert
CENTRAL_API_URL = "http://central:5000"  # Central REST API adresas
REGISTRY_URL = "http://registry:5001"    # Registry API adresas

class EVWeather:
    def __init__(self, central_api_url=CENTRAL_API_URL):
        """
        Inicializuoja orų monitoringo modulį
        
        Args:
            central_api_url: Central sistemos REST API adresas
        """
        self.central_api_url = central_api_url
        self.locations = {}  # cp_id -> city_name
        self.running = True
        self.lock = threading.Lock()
        
        print("[EV_Weather] Initializing...")
        print(f"[EV_Weather] API Key: {WEATHER_API_KEY[:8]}...")
        print(f"[EV_Weather] Central API: {central_api_url}")
        print(f"[EV_Weather] Alert Threshold: {WEATHER_ALERT_THRESHOLD}°C")
    
    def add_location(self, cp_id, city):
        """Pridėti naują CP vietą stebėjimui"""
        with self.lock:
            self.locations[cp_id] = city
        print(f"[EV_Weather] ✅ Added location: {cp_id} -> {city}")
    
    def remove_location(self, cp_id):
        """Pašalinti CP vietą iš stebėjimo"""
        with self.lock:
            if cp_id in self.locations:
                city = self.locations.pop(cp_id)
                print(f"[EV_Weather] ❌ Removed location: {cp_id} ({city})")
    
    def get_temperature(self, city):
        """
        Gauti miesto temperatūrą iš OpenWeatherMap API
        
        Returns:
            float: Temperatūra Celsijaus laipsniais arba None
        """
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather"
            params = {
                "q": city,
                "appid": WEATHER_API_KEY,
                "units": "metric"  # Celsijus
            }
            
            response = requests.get(url, params=params, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                temp = data['main']['temp']
                print(f"[EV_Weather] 🌡️  {city}: {temp}°C")
                return temp
            else:
                print(f"[EV_Weather] ⚠️  API Error {response.status_code} for {city}")
                return None
        
        except Exception as e:
            print(f"[EV_Weather] ❌ Error getting weather for {city}: {e}")
            return None
    
    def notify_central(self, cp_id, alert_status, temperature):
        """
        Pranešti Central apie orų būklę
        
        Args:
            cp_id: CP identifikatorius
            alert_status: "ALERT_COLD" arba "WEATHER_OK"
            temperature: Temperatūra
        """
        try:
            payload = {
                "cp_id": cp_id,
                "alert": alert_status,
                "temperature": temperature,
                "timestamp": datetime.now().isoformat()
            }
            
            
            if response.status_code == 200:
                icon = "❄️" if alert_status == "ALERT_COLD" else "✅"
                print(f"[EV_Weather] {icon} Sent {alert_status} for {cp_id} ({temperature}°C)")
            else:
                print(f"[EV_Weather] ⚠️  Central response: {response.status_code}")
        
        except Exception as e:
            print(f"[EV_Weather] ⚠️  Failed to notify Central: {e}")
    def notify_central_tcp(self, cp_id, alert):
        """
        Send weather alert to Central via TCP protocol (NOT HTTP).
        """
        try:
            msg = Protocol.build_message(
                "WEATHER_ALERT",
                cp_id,
                alert
            )
            self.central_socket.send(Protocol.encode(msg))
            print(f"[EV_Weather] 📤 Sent {alert} for {cp_id} via TCP")
        except Exception as e:
            print(f"[EV_Weather] ❌ TCP notify failed: {e}")
    def monitor_loop(self):
        """
        Pagrindinis stebėjimo ciklas
        Tikrina visas vietas kas WEATHER_UPDATE_INTERVAL sekundžių
        """
        cp_states = {}  # Saugoti praeitus įspėjimus, kad nesiųstų dublikatų
        
        print("[EV_Weather] 🔄 Starting monitoring loop...")
        
        while self.running:
            time.sleep(WEATHER_UPDATE_INTERVAL)
            
            with self.lock:
                current_locations = list(self.locations.items())
            
            for cp_id, city in current_locations:
                # Gauti temperatūrą
                temp = self.get_temperature(city)
                
                if temp is None:
                    continue  # Praleisti, jei nepavyko gauti
                
                # Patikrinti, ar žemiau slenksčio
                is_alert = temp < WEATHER_ALERT_THRESHOLD
                
                # Siųsti tik jei būsena pasikeitė
                prev_state = cp_states.get(cp_id, None)
                
                if is_alert != prev_state:
                    status = "ALERT_COLD" if is_alert else "WEATHER_OK"
                    self.notify_central(cp_id, status, temp)
                    cp_states[cp_id] = is_alert
    
    def auto_discover_cps(self):
        """
        Automatiškai gauti CP sąrašą iš Registry
        Prideda visus CP su numatytu miestu
        """
        print("[EV_Weather] 🔍 Auto-discovering CPs from Registry...")
        
        try:
            response = requests.get(f"{REGISTRY_URL}/list", timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                cps = data.get('charging_points', [])
                
                for cp in cps:
                    cp_id = cp['cp_id']
                    # Numatytai naudoti Madrid (gali būti pakeista vėliau)
                    self.add_location(cp_id, "Madrid")
                
                print(f"[EV_Weather] ✅ Auto-discovered {len(cps)} CPs")
            else:
                print(f"[EV_Weather] ⚠️  Registry response: {response.status_code}")
        
        except Exception as e:
            print(f"[EV_Weather] ❌ Auto-discover failed: {e}")
    
    def menu_loop(self):
        """Interaktyvus meniu CP vietų valdymui"""
        while self.running:
            try:
                print("\n" + "="*60)
                print("[EV_Weather MENU]")
                print("="*60)
                print("1. Add location (CP + City)")
                print("2. View all locations")
                print("3. Remove location")
                print("4. Refresh from Registry")
                print("5. Exit")
                print("="*60)
                
                choice = input("Choice (1-5): ").strip()
                
                if choice == "1":
                    cp_id = input("Enter CP ID (e.g., CP-001): ").strip()
                    city = input("Enter city name (e.g., Madrid): ").strip()
                    
                    if cp_id and city:
                        self.add_location(cp_id, city)
                    else:
                        print("❌ Invalid input")
                
                elif choice == "2":
                    print("\n📍 Current Locations:")
                    print("-" * 40)
                    with self.lock:
                        if not self.locations:
                            print("  (none)")
                        for cp_id, city in self.locations.items():
                            print(f"  {cp_id} -> {city}")
                    print()
                
                elif choice == "3":
                    cp_id = input("Enter CP ID to remove: ").strip()
                    self.remove_location(cp_id)
                
                elif choice == "4":
                    self.auto_discover_cps()
                
                elif choice == "5":
                    print("\n[EV_Weather] Shutting down...")
                    self.running = False
                    break
                
                else:
                    print("❌ Invalid choice (1-5)")
            
            except KeyboardInterrupt:
                print("\n[EV_Weather] Interrupted")
                self.running = False
                break
            
            except Exception as e:
                print(f"❌ Error: {e}")
    
    def run(self):
        """Paleisti EV_Weather modulį"""
        print("\n" + "="*60)
        print("🌦️  EV_Weather Service Starting")
        print("="*60)
        
        # Palaukti, kol kitos paslaugos pasiruošia
        print("[EV_Weather] Waiting 3 seconds for services to start...")
        time.sleep(3)
        
        # Automatiškai aptikti CP iš Registry
        self.auto_discover_cps()
        
        # Paleisti monitoringo gija fone
        monitor_thread = threading.Thread(target=self.monitor_loop, daemon=True)
        monitor_thread.start()
        print("[EV_Weather] ✅ Monitoring thread started")
        
        # Rodyti meniu
        try:
            self.menu_loop()
        except KeyboardInterrupt:
            print("\n[EV_Weather] Shutting down...")
        finally:
            self.running = False
        
        print("[EV_Weather] 👋 Goodbye")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Galima perduoti Custom Central API URL
    central_url = os.getenv("CENTRAL_API_URL", "http://central:5003")
    
    weather = EVWeather(central_api_url=central_url)
    weather.run()