# Spark with the connectors this project needs baked in, so jobs start without
# downloading anything. Versions must match: the Iceberg runtime is built per Spark minor.
FROM apache/spark:4.1.4-java21-python3

ARG SPARK_VERSION=4.1.4
ARG ICEBERG_VERSION=1.12.0
ARG PACKAGES=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION},org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION},org.apache.spark:spark-avro_2.13:${SPARK_VERSION}

USER root
# Resolve the packages and their dependencies once with Spark's own resolver, then
# copy the jars onto Spark's classpath. The resolver names files group_artifact-version.jar;
# the group is stripped so libraries Spark already ships (Hadoop, Snappy...) are skipped.
RUN echo "pass" > /tmp/noop.py \
    && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --packages "${PACKAGES}" /tmp/noop.py \
    && for jar in /tmp/ivy/jars/*.jar; do \
         name=$(basename "$jar"); \
         name=${name#*_}; \
         base=$(echo "$name" | sed -E 's/-[0-9][^-]*\.jar$//'); \
         ls /opt/spark/jars/ | grep -qE "^${base}-[0-9]" || cp "$jar" "/opt/spark/jars/${name}"; \
       done \
    && rm -rf /tmp/ivy /tmp/noop.py \
    && mkdir -p /checkpoints && chown spark:spark /checkpoints
# The jobs' Prometheus metrics (src/flight_lakehouse/streaming/metrics.py). Keep in step
# with the version in uv.lock.
RUN pip3 install --no-cache-dir prometheus-client==0.26.0

USER spark
ENV PYTHONPATH=/app/src
WORKDIR /app
