@echo off
setlocal

echo ==========================================
echo EVCharging one-click startup
echo ==========================================
echo.

where docker >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Docker is not installed or not in PATH.
  pause
  exit /b 1
)

docker info >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Docker Desktop is not running.
  echo Start Docker Desktop and run this file again.
  pause
  exit /b 1
)

echo [INFO] Starting all services (core + AI stack)...
docker compose -f docker-compose.yml down --remove-orphans
docker compose -f docker-compose.yml up --build

echo.
echo [INFO] Compose command finished.
pause
