"""Shared settings for the Spark jobs."""
import os

from pyspark.sql import SparkSession

SOURCE = "/data/source/dwh"
LAKE = "/lake"
RAW, STAGING, CORE = f"{LAKE}/raw", f"{LAKE}/staging", f"{LAKE}/core"
WAREHOUSE = "/opt/warehouse"
SEEDS = f"{WAREHOUSE}/seeds"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "transactions"

# End of the observation period: reference date for ages, recency, and DOB checks
REFERENCE_DATE = "2016-10-21"


def spark_session(app):
    spark = (SparkSession.builder
             .master("local[*]")
             .appName(app)
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ClickHouse connection: local container by default, any managed service (e.g. Aiven) via .env
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_LOCAL = CH_HOST == "clickhouse"          # local container: reads /lake directly via file()
CH_DATABASES = ("dwh", "mart", "dq")


def clickhouse():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "bank"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "bank"),
        secure=os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true")


def ensure_databases(ch, names=CH_DATABASES):
    """Create the databases if missing. Aiven only accepts ENGINE = Replicated
    (set CLICKHOUSE_DATABASE_ENGINE=Replicated); the local server uses its default."""
    engine = os.environ.get("CLICKHOUSE_DATABASE_ENGINE", "")
    for name in names:
        ch.command(f"CREATE DATABASE IF NOT EXISTS {name}"
                   + (f" ENGINE = {engine}" if engine else ""))


def run_sql_file(ch, path):
    """Execute every statement of a .sql file (comments stripped, split on ';')."""
    import re
    sql = re.sub(r"--[^\n]*", "", open(path).read())
    for stmt in filter(None, (part.strip() for part in sql.split(";"))):
        ch.command(stmt)
