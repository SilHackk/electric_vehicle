#!/usr/bin/env python3
"""
CP Manager - Interactive Control Panel for Managing Charging Points
Run inside Docker container with access to Docker socket
"""

import requests
import subprocess
import time
import json
import sys

REGISTRY_URL = "http://registry:5001"
CENTRAL_URL = "http://central:5000"

def print_header(text):
    print(f"\n╔{'═'*68}╗")
    print(f"║{text.center(68)}║")
    print(f"╚{'═'*68}╝\n")

def print_menu():
    print(f"\n╔{'═'*68}╗")
    print(f"║{'🔧 CP MANAGER - Control Panel'.center(68)}║")
    print(f"╠{'═'*68}╣")
    print(f"║  1. Create new CP{' '*51}║")
    print(f"║  2. Delete CP{' '*55}║")
    print(f"║  3. List all CPs{' '*52}║")
    print(f"║  4. View CP status{' '*50}║")
    print(f"║  5. Exit{' '*60}║")
    print(f"╚{'═'*68}╝\n")

def create_cp():
    """Create new charging point"""
    print_header("CREATE NEW CHARGING POINT")
    
    cp_id = input("Enter CP ID (e.g., CP-010): ").strip()
    if not cp_id:
        print("❌ CP ID cannot be empty")
        return
    
    latitude = input("Enter latitude (default 40.5): ").strip() or "40.5"
    longitude = input("Enter longitude (default -3.1): ").strip() or "-3.1"
    price = input("Enter price €/kWh (default 0.30): ").strip() or "0.30"
    
    try:
        price = float(price)
    except ValueError:
        print("❌ Invalid price")
        return
    
    # Extract CP number
    try:
        cp_num = int(cp_id.split('-')[1])
    except (IndexError, ValueError):
        print("❌ Invalid CP_ID format. Use: CP-001, CP-010, etc.")
        return
    
    print(f"\n📝 Step 1/3: Registering {cp_id} in Registry...")
    
    # Register in Registry
    try:
        response = requests.post(
            f"{REGISTRY_URL}/register",
            json={
                "cp_id": cp_id,
                "latitude": latitude,
                "longitude": longitude,
                "price_per_kwh": price
            },
            timeout=10
        )
        
        if response.status_code != 201:
            print(f"❌ Registration failed: {response.text}")
            return
        
        result = response.json()
        print(f"✅ Registered successfully")
        print(f"   Username: {result.get('username')}")
        print(f"   Password: {result.get('password')}")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return
    
    # Wait for Central to detect
    print(f"\n⏳ Step 2/3: Waiting for Central to detect (15 seconds)...")
    time.sleep(15)
    
    # Check if image exists, if not, show error
    print(f"\n🚀 Step 3/3: Launching CP containers...")
    result = subprocess.run(
        ["docker", "images", "-q", "evcharging-cp"],
        capture_output=True,
        text=True
    )
    
    if not result.stdout.strip():
        print(f"❌ Image 'evcharging-cp' not found!")
        print(f"\n⚠️  Please build the image first:")
        print(f"   docker build -t evcharging-cp -f Dockerfile.cp .")
        return
    
    cp_port = 6000 + cp_num
    network = "electric_vehicle_evcharging_net"
    
    # Launch Engine
    print(f"   🔧 Starting CP Engine...")
    engine_cmd = [
        "docker", "run", "-d",
        "--name", f"evcharging_cp_engine_{cp_num}",
        "--network", network,
        "-p", f"{cp_port}:{cp_port}",
        "-e", "KAFKA_BROKER=kafka:9092",
        "-it",
        "evcharging-cp",
        "python", "charging_point/ev_cp_engine.py",
        cp_id, str(latitude), str(longitude), str(price),
        "central", "5000"
    ]
    
    result = subprocess.run(engine_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Failed to start engine: {result.stderr}")
        return
    
    print(f"   ✅ Engine started")
    time.sleep(2)
    
    # Launch Monitor
    print(f"   🔍 Starting CP Monitor...")
    monitor_cmd = [
        "docker", "run", "-d",
        "--name", f"evcharging_cp_monitor_{cp_num}",
        "--network", network,
        "-e", "KAFKA_BROKER=kafka:9092",
        "-it",
        "evcharging-cp",
        "python", "charging_point/ev_cp_monitor.py",
        cp_id, f"evcharging_cp_engine_{cp_num}", str(cp_port),
        "central", "5000"
    ]
    
    result = subprocess.run(monitor_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Failed to start monitor: {result.stderr}")
        return
    
    print(f"   ✅ Monitor started")
    
    print_header(f"✅ {cp_id} READY FOR CHARGING!")
    print(f"📊 Check logs: docker logs evcharging_cp_engine_{cp_num}")

def delete_cp():
    """Delete charging point"""
    print_header("DELETE CHARGING POINT")
    
    cp_id = input("Enter CP ID to delete (e.g., CP-010): ").strip()
    if not cp_id:
        print("❌ CP ID cannot be empty")
        return
    
    confirm = input(f"⚠️  Delete {cp_id}? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("❌ Cancelled")
        return
    
    try:
        cp_num = int(cp_id.split('-')[1])
    except (IndexError, ValueError):
        print("❌ Invalid CP_ID format")
        return
    
    print(f"\n🛑 Stopping containers...")
    
    # Stop and remove containers
    subprocess.run(["docker", "stop", f"evcharging_cp_engine_{cp_num}"], 
                   capture_output=True)
    subprocess.run(["docker", "stop", f"evcharging_cp_monitor_{cp_num}"], 
                   capture_output=True)
    subprocess.run(["docker", "rm", f"evcharging_cp_engine_{cp_num}"], 
                   capture_output=True)
    subprocess.run(["docker", "rm", f"evcharging_cp_monitor_{cp_num}"], 
                   capture_output=True)
    
    print(f"✅ Containers stopped and removed")
    
    # Unregister from Registry
    print(f"\n📝 Unregistering from Registry...")
    try:
        response = requests.delete(f"{REGISTRY_URL}/unregister/{cp_id}", timeout=10)
        if response.status_code == 200:
            print(f"✅ Unregistered from Registry")
        else:
            print(f"⚠️  Registry response: {response.text}")
    except Exception as e:
        print(f"⚠️  Registry error: {e}")
    
    print_header(f"✅ {cp_id} DELETED")

def list_cps():
    """List all charging points"""
    print_header("ALL CHARGING POINTS")
    
    try:
        # Get from Registry
        response = requests.get(f"{REGISTRY_URL}/list", timeout=10)
        if response.status_code != 200:
            print(f"❌ Failed to get list: {response.text}")
            return
        
        data = response.json()
        cps = data.get("charging_points", [])
        
        if not cps:
            print("No charging points registered\n")
            return
        
        print(f"{'CP ID':<12} {'Location':<20} {'Registered At':<25}")
        print("─" * 70)
        
        for cp in cps:
            cp_id = cp.get('cp_id', 'N/A')
            lat = cp.get('latitude', '?')
            lon = cp.get('longitude', '?')
            location = f"({lat}, {lon})"
            registered = cp.get('registered_at', 'N/A')[:19]
            
            print(f"{cp_id:<12} {location:<20} {registered:<25}")
        
        print()
    
    except Exception as e:
        print(f"❌ Error: {e}\n")

def view_status():
    """View running CP containers status"""
    print_header("CP CONTAINERS STATUS")
    
    result = subprocess.run(
        ["docker", "ps", "--filter", "name=evcharging_cp_", "--format", 
         "{{.Names}}\t{{.Status}}"],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print("❌ Failed to get container status")
        return
    
    lines = result.stdout.strip().split('\n')
    if not lines or lines[0] == '':
        print("No CP containers running\n")
        return
    
    print(f"{'Container Name':<35} {'Status':<30}")
    print("─" * 70)
    
    for line in lines:
        if '\t' in line:
            name, status = line.split('\t', 1)
            print(f"{name:<35} {status:<30}")
    
    print()

def main():
    print_header("🔧 CP MANAGER STARTED")
    print("Connected to Registry and Central")
    
    # Check if evcharging-cp image exists
    result = subprocess.run(
        ["docker", "images", "-q", "evcharging-cp"],
        capture_output=True,
        text=True
    )
    
    if not result.stdout.strip():
        print("\n⚠️  WARNING: Image 'evcharging-cp' not found!")
        print("Please build it first:")
        print("  docker build -t evcharging-cp -f Dockerfile.cp .")
        print("\nContinuing anyway, but CP creation will fail...\n")
    else:
        print("✅ Image 'evcharging-cp' found")
        print("Ready to manage charging points\n")
    
    while True:
        try:
            print_menu()
            choice = input("Choice (1-5): ").strip()
            
            if choice == "1":
                create_cp()
            elif choice == "2":
                delete_cp()
            elif choice == "3":
                list_cps()
            elif choice == "4":
                view_status()
            elif choice == "5":
                print("\n👋 Exiting CP Manager...\n")
                break
            else:
                print("\n❌ Invalid choice. Please enter 1-5\n")
        
        except KeyboardInterrupt:
            print("\n\n👋 Exiting CP Manager...\n")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}\n")

if __name__ == "__main__":
    main()