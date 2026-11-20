# Airflow with what its tasks run: dbt (gold models) and Spark (Iceberg maintenance).
# Java and Spark, with the Iceberg jars, come from the project's Spark image (make
# spark-build), so both run the same versions; compose passes it in as the `spark` build
# context. dbt gets its own venv, because its dependencies clash with Airflow's.
FROM apache/airflow:3.3.2-python3.12

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /bin/uv
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark /opt/spark

USER root
# Keep these in step with uv.lock.
RUN uv venv /opt/dbt --python /usr/python/bin/python3.12 \
    && uv pip install --python /opt/dbt/bin/python --no-cache dbt-core==1.12.7 dbt-trino==1.12.0

USER airflow
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/usr/python/bin/python3.12 \
    PATH="/opt/dbt/bin:${PATH}"
