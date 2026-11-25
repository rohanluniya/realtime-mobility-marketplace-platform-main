#!/usr/bin/env bash
set -euo pipefail

PROFILE="${MARKETPLACE_SIM_PROFILE:-local}"
HOST="${MARKETPLACE_API_HOST:-0.0.0.0}"
PORT="${MARKETPLACE_API_PORT:-8010}"

if [ ! -f "/app/.local_data/iceberg_catalog.db" ]; then
  echo "[standalone] Bootstrapping marketplace pipeline (profile=${PROFILE})..."
  python /app/services/state-materializer/run_pipeline.py --profile "${PROFILE}" --compact
else
  echo "[standalone] Existing Iceberg warehouse detected in /app/.local_data; skipping bootstrap."
fi

echo "[standalone] Launching Marketplace Control Tower API on http://${HOST}:${PORT}"
exec uvicorn main:app --app-dir /app/apps/marketplace-api --host "${HOST}" --port "${PORT}"
