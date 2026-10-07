# Spark 3.5.6 (local mode) with the Kafka connector and Python client libraries.
# Used for batch ETL, Structured Streaming, and the Kafka producer.
FROM apache/spark:3.5.6-python3

USER root

# The base image ships Python 3.8, so library versions are pinned to the last
# releases that support it.
RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir \
      confluent-kafka==2.6.1 \
      clickhouse-connect==0.8.18 \
      pandas==2.0.3 \
      pyarrow==17.0.0 \
      psycopg2-binary==2.9.9

# Kafka source/sink for Spark SQL and Structured Streaming (matches Spark 3.5.6 / Scala 2.12)
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.6/spark-token-provider-kafka-0-10_2.12-3.5.6.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

# PYTHONPATH lets plain `python3` import pyspark (spark-submit sets it on its own)
ENV PATH="/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"

USER spark
WORKDIR /opt/jobs
