# ── Spark worker image for ADS-B near-miss detector ──────────────────────────
#
# Base: bitnami/spark:3.5.1  (ships with PySpark 3.5, Java 17, Python 3.11)
# Adds: Python packages needed by the streaming job's foreachBatch handler
#       (psycopg2, redis, pandas, pyarrow – none of these are in the base image)
#
# The Kafka SQL connector JAR is downloaded by spark-submit --packages at
# first startup and cached in the ivy2 volume to avoid re-downloading.
# ─────────────────────────────────────────────────────────────────────────────
FROM bitnami/spark:3.5.1

USER root

RUN pip install --no-cache-dir \
        psycopg2-binary==2.9.9 \
        redis==5.0.1 \
        pandas==2.2.1 \
        pyarrow==15.0.2 \
        confluent-kafka==2.3.0

# Writable directories for Parquet output, checkpoints, and Ivy JAR cache
RUN mkdir -p /app/data/events /app/data/checkpoints /tmp/.ivy2 \
 && chown -R 1001:1001 /app /tmp/.ivy2

USER 1001

WORKDIR /app
