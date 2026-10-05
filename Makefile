# =============================================================================
# TidaLine Lakehouse — Project Commands
# =============================================================================
# Usage:
#   make up          Start the full stack
#   make down        Stop all containers
#   make reset       Destroy everything and start fresh
#   make status      Show running containers
#   make logs        Tail all container logs
#   make debezium    Register the CDC connector
#   make spark-batch Run Bronze → Silver Spark job
#   make dbt         Run dbt Gold transformations
#   make dbt-test    Run dbt data quality tests
#   make dbt-docs    Generate dbt documentation site
#   make pipeline    Run the full transform pipeline (debezium + spark + dbt)
# =============================================================================

.PHONY: up down reset status logs debezium spark-batch spark-stream dbt dbt-test dbt-docs pipeline

# --- Stack Management ---

up:
	docker compose up -d
	@echo ""
	@echo "✅ Stack startup initiated. Run 'docker compose ps' to check health status."
	@echo ""
	@echo "  Airflow:    http://localhost:8085"
	@echo "  Spark:      http://localhost:8080"
	@echo "  Trino:      http://localhost:8084"
	@echo "  Superset:   http://localhost:8088"
	@echo "  Grafana:    http://localhost:3000"
	@echo "  MinIO:      http://localhost:9001"
	@echo "  Redpanda:   http://localhost:9644"
	@echo "  Prometheus: http://localhost:9090"

down:
	docker compose down

reset:
	@echo "⚠️  This will destroy ALL data volumes. Press Ctrl+C to cancel."
	@sleep 3
	docker compose down -v
	@echo "✅ All containers and volumes removed."

status:
	docker compose ps

logs:
	docker compose logs -f --tail=50

# --- Data Pipeline ---

debezium:
	@echo "Registering Debezium CDC connector..."
	@curl -sf -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @infrastructure/debezium/debezium_postgres_source.json \
		&& echo "\n✅ Connector registered." \
		|| echo "\n⚠️  Already registered or Debezium not ready."

spark-batch:
	@echo "Running Bronze → Silver Spark batch job..."
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master local[2] \
		--driver-memory 512m \
		/opt/spark/jobs/batch/bronze_to_silver.py

dbt:
	@echo "Running dbt transformations..."
	. venv/bin/activate && cd transform && dbt run --profiles-dir .

dbt-test:
	@echo "Running dbt data quality tests..."
	. venv/bin/activate && cd transform && dbt test --profiles-dir .

dbt-docs:
	@echo "Generating dbt docs..."
	. venv/bin/activate && cd transform && dbt docs generate --profiles-dir .
	@echo "Run '. venv/bin/activate && cd transform && dbt docs serve' locally to view the docs."

pipeline:
	@echo "Checking if Airflow is ready to accept commands..."
	@until curl -sf http://localhost:8085/health > /dev/null; do \
		echo "⏳ Airflow webserver is still booting (this can take 3-4 minutes on first run). Waiting 10s..."; \
		sleep 10; \
	done
	@echo "\n✅ Airflow is ready! Triggering Pipeline (lakehouse_pipeline)..."
	@docker exec airflow airflow dags trigger lakehouse_pipeline > /dev/null 2>&1 \
		&& echo "\n✅ Pipeline triggered successfully! View progress at http://localhost:8085" \
		|| echo "\n⚠️  Failed to trigger pipeline."
