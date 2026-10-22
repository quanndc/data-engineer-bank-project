"""P2-1: check that every block of the architecture answers (run inside the spark container)."""
import os

from common import CH_HOST, SOURCE_RAW, clickhouse, ensure_databases, postgres, spark_session

ch = clickhouse()
ensure_databases(ch)
print("D ClickHouse :", CH_HOST, ch.query("SELECT version()").result_rows[0][0],
      "| databases:", [r[0] for r in ch.query("SHOW DATABASES").result_rows if r[0] == "dw"],
      "| backup disk:", [r[0] for r in ch.query("SELECT name FROM system.disks").result_rows if r[0] == "backups"])
cur = postgres().cursor()
cur.execute("SELECT version(), (SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'ops')")
v, ops = cur.fetchone()
print("E PostgreSQL :", v.split(",")[0], "| schema ops:", bool(ops))
spark = spark_session("smoke")
print("C Spark      :", spark.version, "| rows in trans.csv:",
      spark.read.option("header", True).option("sep", ";").csv(f"{SOURCE_RAW}/trans.csv").count())
spark.stop()
print("A Source     :", sorted(f for f in os.listdir(SOURCE_RAW)),
      "| tier file:", os.path.exists("/data/source/customer_profile_simulated.csv"))
print("B Data Lake  :", "writable" if os.access("/lake", os.W_OK) else "NOT writable")
