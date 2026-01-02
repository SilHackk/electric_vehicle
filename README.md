The system is designed using a microservices architecture, where independent components communicate via TCP sockets, REST APIs, and Apache Kafka.
All components are fully dockerized and can be deployed on multiple machines.

The system supports:

Real EV charging workflows

Secure Charging Point (CP) authentication via Registry

Symmetric encryption for CP–Central communication

Fault detection and recovery

Real-time monitoring via Web UI

System Architecture
Core Components

Registry

Charging Point registration

Credential generation (username/password)

/verify API for CP authentication

/list API for registered CPs

Central (EV_Central)

TCP server for CPs, Drivers, and Monitors

CP state management (DISCONNECTED / ACTIVATED / SUPPLYING / OUT_OF_ORDER)

Driver authorization

Audit logging and charging history

REST API for weather integration

Kafka event publishing

Charging Point Engine (CP Engine)

Connects to Central via TCP

Authenticates using Registry credentials

Uses symmetric encryption

Sends HEARTBEAT, SUPPLY_UPDATE, SUPPLY_END messages

Charging Point Monitor

Health checks

Fault simulation

Recovery signaling

Drivers

Manual driver (CLI)

Automated driver with fault simulation

Web UI

Real-time dashboard

CP, driver, charging, revenue, and history views

CP registration via REST

Live Central state visualization

Weather Service

OpenWeather API integration

Weather alerts (ALERT_COLD / WEATHER_OK)

Automatic CP OUT_OF_ORDER / RESTORED handling

🔐 Security (Release 2 Compliance)

✔ CP authentication via Registry
✔ Encrypted Central ↔ CP communication (symmetric encryption)
✔ Unique encryption key per CP
✔ No hardcoded secrets
✔ Audit logging (authentication, charging, faults)
✔ Controlled access to Registry APIs

⚙️ Deployment

The system is deployed using Docker Compose:

docker compose build
docker compose up


Services include:

Kafka + Zookeeper

Registry

Central

CP Engines (CP-001, CP-002)

CP Monitors

Drivers (manual and automated)

Web UI

Weather Service

Deployment supports:

Single machine (Docker)

Virtual machines

Multiple physical computers in the same network
