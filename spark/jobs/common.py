"""Shared settings for the Phase 2 pipeline (Berka)."""
import os

# A. Source and B. Data Lake (paths inside the spark container)
SOURCE = "/data/source"                  # data/berka on the host
SOURCE_RAW = f"{SOURCE}/raw"             # Berka CSV files (PKDD'99, ';'-separated)
LAKE = "/lake"
BATCHES = f"{LAKE}/batches"              # one folder per batch: input snapshot (sheet: B)
STAGING = f"{LAKE}/staging"              # intermediate tables, e.g. stg_customer
WAREHOUSE = "/opt/warehouse"

# D. ClickHouse: local container by default, any managed service (e.g. Aiven) via .env
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_LOCAL = CH_HOST == "clickhouse"       # local container: reads /lake directly via file()
CH_DATABASES = ("dw",)


def spark_session(app):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .master("local[*]")
             .appName(app)
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


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
    """Execute every statement of a .sql file on ClickHouse (comments stripped, split on ';')."""
    import re
    sql = re.sub(r"--[^\n]*", "", open(path).read())
    for stmt in filter(None, (part.strip() for part in sql.split(";"))):
        ch.command(stmt)


# E. PostgreSQL operational store
def postgres():
    import psycopg2
    conn = psycopg2.connect(host=os.environ.get("POSTGRES_HOST", "postgres"),
                            dbname=os.environ.get("POSTGRES_DB", "bankops"),
                            user=os.environ.get("POSTGRES_USER", "bank"),
                            password=os.environ.get("POSTGRES_PASSWORD", "bank"))
    conn.autocommit = True
    return conn
