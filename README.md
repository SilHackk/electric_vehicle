# 🚗⚡ EVCharging – Distributed EV Charging System 

## 📌 Overview

The project simulates a **real-world EV charging infrastructure** using a **microservices architecture**, with secure communication, fault tolerance, and real-time monitoring.

All components are **fully dockerized** and can be deployed on **multiple machines**.

---

## 🧩 System Architecture

The system consists of independent services communicating via **TCP sockets**, **REST APIs**, and **Apache Kafka**.

### Core Components

- **Registry**
  - Charging Point (CP) registration
  - Credential generation (username + password)
  - CP verification API (`/verify`)
  - Registered CP listing (`/list`)

- **Central (EV_Central)**
  - Core control center
  - TCP server for CPs, Drivers, and Monitors
  - Charging authorization
  - State management
  - Audit logging
  - REST API for weather integration
  - Kafka event publishing

- **Charging Point Engine (CP Engine)**
  - Connects to Central via TCP
  - Authenticates using Registry credentials
  - Uses **symmetric encryption**
  - Sends HEARTBEAT, SUPPLY_UPDATE, and SUPPLY_END messages

- **Charging Point Monitor**
  - Performs health checks
  - Simulates faults
  - Sends recovery notifications

- **Drivers**
  - Manual driver (CLI)
  - Automated driver with fault simulation

- **Web UI**
  - Real-time monitoring dashboard
  - Charging statistics and history
  - CP registration interface
  - Live Central state visualization

- **Weather Service**
  - Uses OpenWeather API
  - Sends weather alerts to Central
  - Automatically disables/enables CPs based on conditions

- **Kafka**
  - Event streaming for logs and charging events

---

## 🔐 Security Features (Release 2)

- CP authentication via Registry
- Unique credentials per CP
- Symmetric encryption for Central ↔ CP communication
- No hardcoded secrets
- Audit logs for authentication, charging, and faults
- Controlled access to Registry APIs

---

## 🔄 Charging Workflow

1. CP registers in the Registry
2. CP connects to Central and authenticates
3. Central generates a symmetric encryption key
4. Driver requests charging
5. Central authorizes charging
6. CP sends real-time charging updates
7. Charging session ends
8. Ticket is generated and stored

---

## ⚠️ Fault Tolerance

- CP Monitors continuously check CP health
- Faults immediately stop charging safely
- CP state changes to `OUT_OF_ORDER`
- Recovery restores CP automatically
- Weather-based faults supported

---

## 🌦 Weather Integration

- Uses OpenWeather API
- Sends alerts such as:
  - `ALERT_COLD`
  - `WEATHER_OK`
- Central automatically:
  - Disables unsafe CPs
  - Restores them when conditions improve

---

## 📊 Monitoring & Web UI

The Web UI provides:
- Charging Point states
- Active drivers
- Ongoing charging sessions
- Total energy delivered
- Revenue statistics
- Charging history

The UI updates **in real time** based on Central events.

---

## ⚙️ Deployment

The system is deployed using **Docker Compose**.

### Build and Run

```bash
docker compose build
docker compose up
