# File: Makefile

# Phony targets (không liên kết với file cụ thể)
.PHONY: postgres airflow nifi

postgres:
	docker compose -f postgres-docker-compose.yaml up -d

airflow:
	docker compose -f postgres-docker-compose.yaml -f airflow-docker-compose.yaml up -d

nifi:
	docker compose -f nifi-docker-compose.yaml up -d

down-postgres:
	docker compose -f postgres-docker-compose.yaml down -v

down-airflow:
	docker compose -f airflow-docker-compose.yaml -f postgres-docker-compose.yaml down -v

down-nifi:
	docker compose -f nifi-docker-compose.yaml down -v
