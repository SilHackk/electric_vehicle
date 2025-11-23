# ============================================================================
# EVCharging System - Shared Configuration
# ============================================================================
import os

# CENTRAL Configuration
CENTRAL_HOST = "0.0.0.0"
CENTRAL_PORT = 5000
CENTRAL_DB_FILE = "central_db.txt"

# KAFKA Configuration - reads from environment variable or defaults to docker network
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

KAFKA_TOPICS = {
    "system_events": "evcharging_system_events",
    "charging_logs": "evcharging_charging_logs",
    "health_checks": "evcharging_health_checks"
}

# CP Configuration Base
CP_BASE_PORT = 6000  # CP1: 6001, CP2: 6002, etc.

# Protocol Configuration
STX = b'\x02'  # Start of text
ETX = b'\x03'  # End of text
DELIMITER = b'#'

# States
CP_STATES = {
    "ACTIVATED": "ACTIVATED",
    "SUPPLYING": "SUPPLYING",
    "STOPPED": "STOPPED",
    "OUT_OF_ORDER": "OUT_OF_ORDER",
    "DISCONNECTED": "DISCONNECTED"
}

# Status Colors (for reference)
COLORS = {
    "ACTIVATED": "GREEN",
    "SUPPLYING": "GREEN",
    "STOPPED": "ORANGE",
    "OUT_OF_ORDER": "RED",
    "DISCONNECTED": "GRAY"
}

# Timing (seconds)
HEALTH_CHECK_INTERVAL = 1
SUPPLY_UPDATE_INTERVAL = 1
WAIT_BETWEEN_REQUESTS = 4