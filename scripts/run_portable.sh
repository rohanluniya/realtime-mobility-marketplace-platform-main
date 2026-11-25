#!/usr/bin/env bash
# Portable zero-Docker launcher for macOS and Linux (requires Python 3.12+ or uv).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROFILE="${1:-local}"
PORT="${MARKETPLACE_API_PORT:-8010}"
HOST="${MARKETPLACE_API_HOST:-127.0.0.1}"

cd "${REPO_ROOT}"

echo "=================================================================="
echo " Real-Time Mobility Marketplace & Dynamic Pricing Platform"
echo " Portable Launcher | Profile: ${PROFILE} | Port: ${PORT}"
echo "=================================================================="

if command -v uv >/dev/null 2>&1; then
  echo "[1/3] Syncing pinned Python environment via uv..."
  uv sync --extra dev
  PYTHON_BIN=".venv/bin/python"
  UVICORN_BIN=".venv/bin/uvicorn"
else
  echo "[1/3] uv not found; provisioning local .venv via python3..."
  python3 -m venv .venv
  .venv/bin/pip install --upgrade pip
  .venv/bin/pip install -e ".[dev]"
  PYTHON_BIN=".venv/bin/python"
  UVICORN_BIN=".venv/bin/uvicorn"
fi

echo "[2/3] Running end-to-end marketplace pipeline (simulator -> stream state -> KV -> Iceberg V2 -> DQ)..."
"${PYTHON_BIN}" services/state-materializer/run_pipeline.py --profile "${PROFILE}" --compact

echo "[3/3] Starting Marketplace Control Tower on http://${HOST}:${PORT} ..."
echo "      Press Ctrl+C to stop."
exec "${UVICORN_BIN}" main:app --app-dir apps/marketplace-api --host "${HOST}" --port "${PORT}"
