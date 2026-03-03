PYTHON ?= $(shell command -v python3 || command -v python)

.PHONY: up down logs build spark spark-logs spark-restart \
        api api-logs api-restart \
        simulate simulate-small simulate-dry ingest \
        create-topic consume consume-live \
        install install-all test lint analyze benchmark

# ── Infrastructure ─────────────────────────────────────────────────────────────
up:
	docker compose up -d
	@echo "Waiting for Kafka to be ready…"
	@until docker compose exec kafka \
	    /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 \
	    >/dev/null 2>&1; do sleep 2; done
	@echo "Creating Kafka topics (ensures partition leaders are elected before Spark starts)…"
	@docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server localhost:9092 \
	    --create --if-not-exists \
	    --topic adsb.raw --partitions 6 --replication-factor 1
	@docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server localhost:9092 \
	    --create --if-not-exists \
	    --topic adsb.dlq --partitions 1 --replication-factor 1
	@docker compose restart spark
	@echo "✓ Stack is up — topics ready, Spark restarted"

## Build all custom images (Spark) before first `make up`
build:
	docker compose build

down:
	docker compose down

logs:
	docker compose logs -f

# ── Spark ──────────────────────────────────────────────────────────────────────

## Start (or restart) only the Spark service
spark:
	docker compose up -d spark

## Tail Spark streaming job logs
spark-logs:
	docker compose logs -f spark

## Restart Spark job (e.g. after a code change)
spark-restart:
	docker compose restart spark

## Wipe Spark checkpoints so the job resumes from Kafka "latest" on next start.
## Run this when you see "This server does not host this topic-partition" and
## a simple restart didn't fix it (usually after docker compose down -v).
spark-clean:
	docker compose run --rm --no-deps spark \
	    bash -c "rm -rf /app/data/checkpoints/* && echo 'Checkpoints cleared'"

## Run Spark job locally (outside Docker) — requires PySpark installed
spark-local:
	KAFKA_BOOTSTRAP_SERVERS=$${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092} \
	PYTHONPATH=src \
	spark-submit \
	    --master local[2] \
	    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1,org.apache.commons:commons-pool2:2.11.1 \
	    --conf spark.jars.ivy=/tmp/.ivy2 \
	    --conf spark.driver.memory=1g \
	    src/processing/spark_streaming_job.py

# ── API ────────────────────────────────────────────────────────────────────────

## Start (or restart) only the API service
api:
	docker compose up -d api

## Tail API logs
api-logs:
	docker compose logs -f api

## Restart API (e.g. after a code change — src/ is volume-mounted)
api-restart:
	docker compose restart api

# ── Ingestion ─────────────────────────────────────────────────────────────────

## Full simulator: 500 aircraft, 1% near-miss rate → Kafka (host external port)
simulate:
	$(PYTHON) src/ingestion/adsb_simulator.py \
	    --aircraft 500 \
	    --near-miss-rate 0.01 \
	    --bootstrap $${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}

## Lighter run for dev laptops — guaranteed near-miss every 10 s for quick demos
simulate-small:
	$(PYTHON) src/ingestion/adsb_simulator.py \
	    --aircraft 50 \
	    --near-miss-rate 1.0 \
	    --bootstrap $${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092} \
	    --log-interval 5

## Dry-run: no Kafka needed — prints messages to stdout
simulate-dry:
	$(PYTHON) src/ingestion/adsb_simulator.py \
	    --aircraft 20 \
	    --near-miss-rate 0.50 \
	    --dry-run \
	    --log-interval 3 \
	    --verbose

## Live OpenSky feed (requires OPENSKY_USERNAME / OPENSKY_PASSWORD in env or .env)
ingest:
	KAFKA_BOOTSTRAP_SERVERS=$${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092} \
	$(PYTHON) src/ingestion/opensky_producer.py

# ── Kafka admin helpers ────────────────────────────────────────────────────────

## Create the adsb.raw topic explicitly (auto-create is already enabled)
create-topic:
	docker compose exec kafka \
	    /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server localhost:9092 \
	    --create --if-not-exists \
	    --topic adsb.raw \
	    --partitions 6 \
	    --replication-factor 1

## Tail the adsb.raw topic from the beginning
consume:
	docker compose exec kafka \
	    /opt/kafka/bin/kafka-console-consumer.sh \
	    --bootstrap-server localhost:9092 \
	    --topic adsb.raw \
	    --from-beginning \
	    --max-messages 20

## Continuous consumer (Ctrl-C to stop)
consume-live:
	docker compose exec kafka \
	    /opt/kafka/bin/kafka-console-consumer.sh \
	    --bootstrap-server localhost:9092 \
	    --topic adsb.raw

# ── Development ───────────────────────────────────────────────────────────────
install:
	$(PYTHON) -m pip install -r requirements.txt

install-all:
	$(PYTHON) -m pip install poetry && poetry install

test:
	$(PYTHON) -m pytest tests/ -v

lint:
	$(PYTHON) -m ruff check src/ tests/

analyze:
	$(PYTHON) src/analysis/hotspot_clustering.py --min-cluster-size 3 --min-samples 2

benchmark:
	$(PYTHON) benchmarks/throughput_test.py
	$(PYTHON) benchmarks/latency_test.py
