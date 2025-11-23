"""
Setup Verification Script for EVCharging System
Run this before starting the system to check everything is ready!
"""

import os
import sys

def check_file(filepath, description):
    """Check if a file exists"""
    if os.path.exists(filepath):
        print(f"✅ {description}: {filepath}")
        return True
    else:
        print(f"❌ MISSING {description}: {filepath}")
        return False

def check_directory(dirpath, description):
    """Check if a directory exists"""
    if os.path.isdir(dirpath):
        print(f"✅ {description}: {dirpath}")
        return True
    else:
        print(f"❌ MISSING {description}: {dirpath}")
        return False

def main():
    print("="*70)
    print("🔍 EVCharging System Setup Checker")
    print("="*70)
    
    all_good = True
    
    print("\n📁 Checking Directory Structure...")
    all_good &= check_directory("central", "Central directory")
    all_good &= check_directory("charging_point", "Charging Point directory")
    all_good &= check_directory("driver", "Driver directory")
    all_good &= check_directory("shared", "Shared directory")
    all_good &= check_directory("data", "Data directory")
    
    print("\n📄 Checking Core Files...")
    all_good &= check_file("config.py", "Configuration file")
    all_good &= check_file("requirements.txt", "Requirements file")
    all_good &= check_file("docker-compose.yml", "Docker Compose file")
    
    print("\n🐳 Checking Dockerfiles...")
    all_good &= check_file("Dockerfile.central", "Central Dockerfile")
    all_good &= check_file("Dockerfile.cp", "CP Dockerfile")
    all_good &= check_file("Dockerfile.driver", "Driver Dockerfile")
    
    print("\n🖥️ Checking Python Modules...")
    all_good &= check_file("central/ev_central.py", "Central module")
    all_good &= check_file("charging_point/ev_cp_engine.py", "CP Engine module")
    all_good &= check_file("charging_point/ev_cp_monitor.py", "CP Monitor module")
    all_good &= check_file("driver/ev_driver.py", "Driver module")
    
    print("\n🔧 Checking Shared Utilities...")
    all_good &= check_file("shared/__init__.py", "Shared package init")
    all_good &= check_file("shared/protocol.py", "Protocol module")
    all_good &= check_file("shared/kafka_client.py", "Kafka client module")
    
    print("\n📊 Checking Data Files...")
    all_good &= check_file("data/charging_requests.txt", "Sample requests file")
    
    print("\n" + "="*70)
    if all_good:
        print("✅ ALL CHECKS PASSED! You're ready to run the system!")
        print("\nNext steps:")
        print("1. Make sure Docker Desktop is running")
        print("2. Run: docker-compose build")
        print("3. Run: docker-compose up")
    else:
        print("❌ SOME FILES ARE MISSING!")
        print("\nPlease create the missing files before continuing.")
        print("Check the deployment guide for details.")
    print("="*70)
    
    return 0 if all_good else 1

if __name__ == "__main__":
    sys.exit(main())