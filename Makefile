.PHONY: help env setup pipeline serve portable docker-standalone docker-core docker-full test chaos bench lint typecheck security sbom clean

PYTHON := .venv/bin/python
UVICORN := .venv/bin/uvicorn
PYTEST := .venv/bin/pytest
RUFF := .venv/bin/ruff
MYPY := .venv/bin/mypy

help:
	@echo "Real-Time Mobility Marketplace & Dynamic Pricing Platform"
	@echo "  make env               - Detect host OS/arch/Docker/Python capabilities"
	@echo "  make setup             - Install locked Python dependencies via uv"
	@echo "  make pipeline          - Run full local E2E pipeline (sim -> stream -> KV -> Iceberg -> DQ)"
	@echo "  make serve             - Launch FastAPI API & Control Tower on http://127.0.0.1:8010"
	@echo "  make portable          - Zero-Docker one-command bootstrap + live dashboard"
	@echo "  make docker-standalone - One-command self-contained Docker container on :8010"
	@echo "  make docker-core       - Launch distributed Kafka/Valkey/Lakekeeper/PostGIS/API stack"
	@echo "  make docker-full       - Launch full distributed stack (Flink, Spark, Trino, Airflow, Grafana)"
	@echo "  make test              - Run full unit, contract, integration, and chaos test suites"
	@echo "  make chaos             - Run 9-scenario chaos & fault-injection suite"
	@echo "  make bench             - Run load profiles, H3 multi-resolution, and Iceberg benchmarks"
	@echo "  make lint              - Run ruff linter"
	@echo "  make typecheck         - Run mypy static type checker"
	@echo "  make security          - Audit dependency licenses and generate CycloneDX SBOM"

env:
	$(PYTHON) scripts/detect_env.py

setup:
	uv sync --extra dev

pipeline:
	$(PYTHON) services/state-materializer/run_pipeline.py --profile local --compact

serve:
	$(UVICORN) main:app --app-dir apps/marketplace-api --host 127.0.0.1 --port 8010

portable:
	./scripts/run_portable.sh local

docker-standalone:
	docker compose -f docker-compose.standalone.yml up --build

docker-core:
	docker compose --profile core up -d

docker-full:
	docker compose --profile full up -d

test:
	$(PYTEST) -v

chaos:
	$(PYTEST) -v tests/chaos/test_chaos_fault_injection.py

bench:
	PYTHONPATH=. $(PYTHON) benchmarks/run_load_and_h3_benchmarks.py

lint:
	$(RUFF) check .

typecheck:
	$(MYPY) marketplace_platform apps/marketplace-api/security.py

security:
	$(PYTHON) scripts/check_licenses.py
	$(PYTHON) scripts/generate_sbom.py

clean:
	rm -rf .local_data .pytest_cache .mypy_cache .ruff_cache
