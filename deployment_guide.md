# EV Charging System - Distributed Deployment Guide

**Host PC IP: 172.21.42.11**

---

## 📋 Pre-Deployment Setup

### On ALL 3 Computers:

1. **Install Docker & Docker Compose**
   ```bash
   # Verify installation
   docker --version
   docker-compose --version
   ```

2. **Copy Entire Project Folder**
   - Transfer the complete project to all 3 computers
   - Make sure you have: `Dockerfile.central`, `Dockerfile.cp`, `Dockerfile.driver`, `shared/`, `config.py`, etc.

3. **Open Firewall Ports (Computer 1 ONLY)**
   ```bash
   # Linux/Mac:
   sudo ufw allow 9092/tcp   # Kafka
   sudo ufw allow 5000/tcp   # Central
   
   # Windows:
   # Open Windows Defender Firewall → Advanced Settings → Inbound Rules
   # New Rule → Port → TCP → 9092, 5000 → Allow
   ```

4. **Test Network Connectivity**
   ```bash
   # From Computer 2 & 3, test if Computer 1 is reachable:
   ping 172.21.42.11
   ```

---

## 🚀 Deployment Steps

### STEP 1: Computer 1 (Kafka + Central)

**Navigate to project folder:**
```bash
cd /path/to/your/evcharging-project
```

**Use the YAML file: `docker-compose-kafka-central.yml`**
(Already configured with IP 172.21.42.11)

**Build images:**
```bash
docker-compose -f docker-compose-kafka-central.yml build
```

**Start services:**
```bash
docker-compose -f docker-compose-kafka-central.yml up
```

**Wait for Kafka to fully start!**
Look for log message: `"Kafka Server started"`
This takes ~30-60 seconds.

**To access Central admin console (in another terminal):**
```bash
docker attach distributed_central
```

---

### STEP 2: Computer 2 (Charging Point)

**Navigate to project folder:**
```bash
cd /path/to/your/evcharging-project
```

**Use the YAML file: `docker-compose-chargepoints.yml`**
(Already configured with IP 172.21.42.11)

**Build images:**
```bash
docker-compose -f docker-compose-chargepoints.yml build
```

**Start services:**
```bash
docker-compose -f docker-compose-chargepoints.yml up
```

**To interact with CP engine menu (in another terminal):**
```bash
docker attach chargepoint_engine_1
```

---

### STEP 3: Computer 3 (Driver)

**Navigate to project folder:**
```bash
cd /path/to/your/evcharging-project
```

**Use the YAML file: `docker-compose-manual-drivers.yml`**
(Already configured with IP 172.21.42.11)

**Build images:**
```bash
docker-compose -f docker-compose-manual-drivers.yml build
```

**Start services:**
```bash
docker-compose -f docker-compose-manual-drivers.yml up
```

**To interact with drivers menu (in another terminal):**
```bash
docker attach manual_driver_1
```

---

## ✅ Verification

### Check if everything is connected:

**On Computer 1:**
```bash
docker logs distributed_central
```
Should see:
- `✅ CP Registered: CP-001 at (40.5, -3.1)`
- `✅ Driver Registered: DRIVER-001`

**On Computer 2:**
```bash
docker logs chargepoint_engine_1
```
Should see:
- `Connected to CENTRAL`
- `Registered with CENTRAL`

**On Computer 3:**
```bash
docker logs manual_driver_1
```
Should see:
- `✓ Connected to CENTRAL`
- `Ready for manual commands`

---

## 🎮 Using the System

### On Computer 3 (Driver):
```bash
# Attach to driver
docker attach manual_driver_1

# In the menu:
# 1. Request charge
# Enter CP ID: CP-001
# Enter kWh: 15

# 2. View status
# 3. View available CPs
# 4. Finish charging (unplug)
# 5. Exit
```

### On Computer 1 (Central Admin):
```bash
# Attach to central
docker attach distributed_central

# Commands:
list           # List all CPs and their status
stop CP-001    # Stop a charging point
resume CP-001  # Resume a charging point
history        # View recent charging sessions
quit           # Shutdown system
```

---

## 🛑 Stopping the System

**Stop in reverse order:**

**Computer 3:**
```bash
docker-compose -f docker-compose-manual-driver.yml down
```

**Computer 2:**
```bash
docker-compose -f docker-compose-chargepoint.yml down
```

**Computer 1:**
```bash
docker-compose -f docker-compose-kafka-central.yml down
```

**To remove all data and start fresh:**
```bash
docker-compose down -v
```

---

## ⚠️ Troubleshooting

### "Connection refused to Kafka"
- Check Computer 1 firewall allows port 9092
- Verify Kafka is running: `docker ps | grep kafka`
- Test from Computer 2/3: `telnet 172.21.42.11 9092`

### "Cannot connect to Central"
- Check Computer 1 firewall allows port 5000
- Verify Central is running: `docker ps | grep central`
- Test from Computer 2/3: `telnet 172.21.42.11 5000`

### "Network not found" error
- Make sure you're in the project root folder when running docker-compose
- Check YAML file has `networks: distributed_net: driver: bridge`

### Container won't start
```bash
# View detailed logs
docker logs <container_name>

# Example:
docker logs distributed_kafka
docker logs chargepoint_engine_1
```

---

## 📁 File Checklist

**Make sure these files exist on ALL computers:**

```
evcharging-project/
├── Dockerfile.central
├── Dockerfile.cp
├── Dockerfile.driver
├── requirements.txt
├── config.py
├── central/
│   └── ev_central.py
├── charging_point/
│   ├── ev_cp_engine.py
│   └── ev_cp_monitor.py
├── driver/
│   └── ev_driver.py
├── shared/
│   ├── __init__.py
│   ├── protocol.py
│   ├── kafka_client.py
│   └── file_storage.py
└── data/
    └── charging_requests.txt
```

---

## 🔧 Quick Command Reference

| Action | Command |
|--------|---------|
| Build images | `docker-compose -f <file.yml> build` |
| Start system | `docker-compose -f <file.yml> up` |
| Stop system | `docker-compose -f <file.yml> down` |
| View logs | `docker logs <container_name>` |
| Attach to container | `docker attach <container_name>` |
| Detach from container | Press `Ctrl+P` then `Ctrl+Q` |
| List running containers | `docker ps` |

---

## 📌 Important Notes

1. **Always start Computer 1 first** and wait for Kafka to fully start
2. **"context: ." means current folder** - run commands from project root
3. **IP 172.21.42.11 is hardcoded** in all YAML files - no need to change anything
4. **config.py does NOT need changes** - environment variables override it
5. **Data persistence** - Computer 1 saves data to `./data` folder automatically

---

## 🆘 Getting Help

If something doesn't work:

1. Check logs: `docker logs <container_name>`
2. Verify network connectivity: `ping 172.21.42.11`
3. Check ports are open: `telnet 172.21.42.11 9092`
4. Make sure Kafka fully started before starting other computers

---

**System Ready!** 🎉

Start Computer 1 → Wait 30s → Start Computer 2 → Start Computer 3
