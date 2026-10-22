# C. Pipeline: Spark 3.5.6 (local mode, PySpark) with the ClickHouse and PostgreSQL clients.
FROM apache/spark:3.5.6-python3

USER root

# The base image ships Python 3.8, so library versions are pinned to the last
# releases that support it.
RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir \
      clickhouse-connect==0.8.18 \
      psycopg2-binary==2.9.9 \
      pandas==2.0.3 \
      pyarrow==17.0.0

# PYTHONPATH lets plain `python3` import pyspark (spark-submit sets it on its own)
ENV PATH="/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"

USER spark
WORKDIR /opt/jobs
