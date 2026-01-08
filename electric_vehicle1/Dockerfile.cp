# ============================================================================
# Dockerfile.cp - CP Engine and Monitor (separate containers)
# ============================================================================
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY shared/ shared/
COPY charging_point/ charging_point/

ENV PYTHONUNBUFFERED=1

# Default to engine, can be overridden
CMD ["python", "charging_point/ev_cp_engine.py", "CP-001", "40.5", "-3.1", "0.30", "central", "5000"]