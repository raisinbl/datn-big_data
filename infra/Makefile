# File: Makefile

# Phony targets (không liên kết với file cụ thể)
.PHONY: postgres airflow

# Target: postgres
postgres:
	docker compose -f postgres-docker-compose.yaml up -d

# Target: airflow
airflow:
	docker compose -f postgres-docker-compose.yaml -f airflow-docker-compose.yaml up -d

down:
	docker compose -f postgres-docker-compose.yaml -f airflow-docker-compose.yaml down -v --remove-orphans