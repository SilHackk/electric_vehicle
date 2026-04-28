# EVCharging

EVCharging is a Dockerized microservice project for EV charging simulation.

## Active services

- `central` (TCP control center + API for CP registration)
- `charging_point` engines and monitors
- `driver` clients
- `web_ui` dashboard
- `kafka` + `zookeeper`
- `model_service` (AI prediction API)
- AI recommendation page integrated into `web_ui` at `/ai`

## Removed from current setup

- Registry service
- Weather service

## Run the project

Use one-click launcher on Windows:

- `run_all.bat`

Or run manually:

```bash
docker compose -f docker-compose.yml up --build
