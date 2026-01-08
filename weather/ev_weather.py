# weather/ev_weather.py
# ============================================================
# EV_W – Weather Control Office (Release 2 compliant)
# Notifies EV_Central via REST API
# ============================================================

import os
import time
import requests

# Environment variables (from docker-compose)
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://registry:5001")
CENTRAL_API_URL = os.getenv("CENTRAL_API_URL", "http://central:5003")

WEATHER_INTERVAL = 4  # seconds


def get_registered_cps():
    """
    Get all registered CPs from Registry.
    Always returns: list of {cp_id, city}
    """
    try:
        r = requests.get(f"{REGISTRY_URL}/list", timeout=5)
        r.raise_for_status()
        data = r.json()

        cps = []

        # Case 1: list of dicts (CORRECT / EXPECTED)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    cp_id = item.get("cp_id")
                    city = item.get("city")
                    if cp_id and city:
                        cps.append({
                            "cp_id": cp_id,
                            "city": city
                        })

        # Case 2: dict keyed by cp_id (OLDER VERSION)
        elif isinstance(data, dict):
            for cp_id, item in data.items():
                if isinstance(item, dict):
                    city = item.get("city")
                    if city:
                        cps.append({
                            "cp_id": cp_id,
                            "city": city
                        })

        return cps

    except Exception as e:
        print(f"[WEATHER] ERROR contacting Registry: {e}")
        return []


def get_temperature(city):
    """
    Get current temperature from OpenWeatherMap
    """
    try:
        url = (
            "https://api.openweathermap.org/data/2.5/weather"
            f"?q={city}&appid={OPENWEATHER_API_KEY}&units=metric"
        )
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        data = r.json()
        return data["main"]["temp"]
    except Exception as e:
        print(f"[WEATHER] ERROR getting weather for {city}: {e}")
        return None


def notify_central(cp_id, temperature):
    """
    Notify Central via REST API
    """
    if temperature < 0:
        alert = "ALERT_COLD"
    else:
        alert = "WEATHER_OK"

    payload = {
        "cp_id": cp_id,
        "alert": alert,
        "temperature": temperature
    }

    try:
        r = requests.post(
            f"{CENTRAL_API_URL}/api/weather_alert",
            json=payload,
            timeout=5
        )
        r.raise_for_status()
        print(f"[WEATHER] Sent {alert} for {cp_id} (temp={temperature}°C)")
    except Exception as e:
        print(f"[WEATHER] ERROR notifying Central for {cp_id}: {e}")


def main():
    print("[WEATHER] Weather Control Office started")

    while True:
        cps = get_registered_cps()

        for cp in cps:
            cp_id = cp.get("cp_id")
            city = cp.get("city")

            if not cp_id or not city:
                continue

            temperature = get_temperature(city)
            if temperature is None:
                continue

            notify_central(cp_id, temperature)

        time.sleep(WEATHER_INTERVAL)


if __name__ == "__main__":
    main()
