$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "EVCharging one-click startup (PowerShell)" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "[ERROR] Docker is not installed or not in PATH." -ForegroundColor Red
    exit 1
}

try {
    docker info | Out-Null
} catch {
    Write-Host "[ERROR] Docker Desktop is not running." -ForegroundColor Red
    Write-Host "Start Docker Desktop and run this script again." -ForegroundColor Yellow
    exit 1
}

Write-Host "[INFO] Starting all services (core + AI stack)..." -ForegroundColor Green
docker compose -f docker-compose.yml down --remove-orphans
docker compose -f docker-compose.yml up --build
