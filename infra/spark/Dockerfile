# ── Spark worker image for ADS-B near-miss detector ──────────────────────────
#
# Base: python:3.11-slim  (Debian bookworm — same family as other project images)
# Java: default-jre-headless (OpenJDK 17 on Debian 12)
# Spark: installed via `pip install pyspark` (avoids bitnami/apache registry deps)
#
# The Kafka SQL connector JAR is resolved by spark-submit --packages at first
# startup and cached in the ivy-cache volume for fast subsequent restarts.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# Java runtime (required by Spark) + procps (pgrep for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

# default-jre-headless creates this symlink regardless of architecture
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Python deps needed by the streaming job's foreachBatch handler
RUN pip install --no-cache-dir \
        pyspark==3.5.1 \
        psycopg2-binary==2.9.9 \
        redis==5.0.1 \
        pandas==2.2.1 \
        pyarrow==15.0.2 \
        confluent-kafka==2.3.0

# Working directories (spark-submit writes checkpoints + Parquet here)
RUN mkdir -p /app/data/events /app/data/checkpoints /tmp/.ivy2

WORKDIR /app
