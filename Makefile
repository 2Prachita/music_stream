.PHONY: help install kafka-up kafka-down kafka-topics kafka-verify \
        produce consume-raw consume-spark \
        snowflake-setup dbt-deps dbt-run dbt-test \
        pipeline-start pipeline-stop clean

help:
	@echo ""
	@echo "  🎵 Music Stream Pipeline"
	@echo "  ─────────────────────────────────────────────────"
	@echo "  make install          Install Python dependencies"
	@echo "  make kafka-up         Start Kafka + Zookeeper (Docker)"
	@echo "  make kafka-down       Stop Kafka + Zookeeper"
	@echo "  make kafka-topics     List Kafka topics"
	@echo "  make kafka-verify     Print last 5 events from play-events topic"
	@echo "  make produce          Start event producer (5 events/sec)"
	@echo "  make produce-fast     Start event producer (20 events/sec)"
	@echo "  make consume-raw      Start raw consumer → Snowflake RAW"
	@echo "  make consume-spark    Start PySpark streaming job"
	@echo "  make snowflake-setup  Run setup.sql in Snowflake"
	@echo "  make dbt-deps         Install dbt packages"
	@echo "  make dbt-run          Run all dbt models"
	@echo "  make dbt-test         Run data quality tests"
	@echo "  make clean            Remove generated files"
	@echo ""

install:
	pip install -r requirements.txt

# ── Kafka ─────────────────────────────────────────────────────────────
kafka-up:
	docker compose up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 10
	@docker compose ps

kafka-down:
	docker compose down

kafka-topics:
	docker exec kafka /usr/bin/kafka-topics --list --bootstrap-server localhost:9092

kafka-verify:
	docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic play-events \
		--from-beginning \
		--max-messages 5

# ── Producer ──────────────────────────────────────────────────────────
produce:
	python -m producer.kafka_producer --rate 5

produce-fast:
	python -m producer.kafka_producer --rate 20

produce-batch:
	python -m producer.kafka_producer --batch 500

# ── Consumers ─────────────────────────────────────────────────────────
consume-raw:
	python -m consumer.raw_consumer

consume-spark:
	python -m consumer.spark_streaming

# ── Snowflake ─────────────────────────────────────────────────────────
snowflake-setup:
	@echo "Run snowflake/setup.sql manually in your Snowflake worksheet"
	@echo "File: $(PWD)/snowflake/setup.sql"

# ── dbt ───────────────────────────────────────────────────────────────
DBT_FLAGS = --profiles-dir . --project-dir dbt_project

dbt-deps:
	dbt deps $(DBT_FLAGS)

dbt-run:
	dbt run $(DBT_FLAGS)

dbt-test:
	dbt test $(DBT_FLAGS)

dbt-docs:
	dbt docs generate $(DBT_FLAGS)
	dbt docs serve $(DBT_FLAGS)

# ── Clean ─────────────────────────────────────────────────────────────
clean:
	docker compose down -v
	rm -rf /tmp/checkpoints dbt_project/target dbt_project/dbt_packages
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true