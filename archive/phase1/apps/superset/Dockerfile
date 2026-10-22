# Apache Superset with the ClickHouse driver (clickhouse-connect SQLAlchemy dialect)
FROM apache/superset:4.1.3
USER root
RUN pip install --no-cache-dir clickhouse-connect==0.8.18
COPY superset_config.py /app/pythonpath/superset_config.py
USER superset
