.PHONY: up down logs simulate simulate-small ingest test lint install

# ── Infrastructure ─────────────────────────────────────────────────────────────
up:
	docker compose up -d
	@echo "Waiting for Kafka to be ready…"
	@until docker compose exec kafka \
	    /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 \
	    >/dev/null 2>&1; do sleep 2; done
	@echo "✓ Stack is up"

down:
	docker compose down

logs:
	docker compose logs -f

# ── Ingestion ─────────────────────────────────────────────────────────────────

## Full simulator: 500 aircraft, 1% near-miss rate → Kafka at kafka:9092
simulate:
	KAFKA_BOOTSTRAP_SERVERS=$${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
	python src/ingestion/adsb_simulator.py \
	    --aircraft 500 \
	    --near-miss-rate 0.01 \
	    --bootstrap $${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

## Lighter run for dev laptops
simulate-small:
	python src/ingestion/adsb_simulator.py \
	    --aircraft 50 \
	    --near-miss-rate 0.10 \
	    --bootstrap $${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
	    --log-interval 5

## Dry-run: no Kafka needed — prints messages to stdout
simulate-dry:
	python src/ingestion/adsb_simulator.py \
	    --aircraft 20 \
	    --near-miss-rate 0.50 \
	    --dry-run \
	    --log-interval 3 \
	    --verbose

## Live OpenSky feed (requires OPENSKY_USERNAME / OPENSKY_PASSWORD in env or .env)
ingest:
	KAFKA_BOOTSTRAP_SERVERS=$${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
	python src/ingestion/opensky_producer.py

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
	pip install confluent-kafka kafka-python python-geohash pydantic

install-all:
	pip install poetry && poetry install

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/

analyze:
	python src/analysis/hotspot_clustering.py

benchmark:
	python benchmarks/throughput_test.py
	python benchmarks/latency_test.py
