"""B1 / A1: build the One Big Table rows with Spark from the core (star schema) layer.
Output: /lake/bench/obt/{obt_transactions,obt_balance_snapshot} (Parquet, small row groups)."""
import sys
import time

sys.path.insert(0, "/opt/jobs")
from pyspark.sql import functions as F  # noqa: E402

from common import CORE, LAKE, spark_session  # noqa: E402

spark = spark_session("bench_build_obt")
start = time.time()
d = spark.read.parquet(f"{CORE}/dim_date")
c = (spark.read.parquet(f"{CORE}/dim_customer")
     .withColumnRenamed("dq_flags", "customer_dq_flags"))
l = spark.read.parquet(f"{CORE}/dim_location")
t = spark.read.parquet(f"{CORE}/dim_transaction_type")
f = spark.read.parquet(f"{CORE}/fact_transactions")
s = spark.read.parquet(f"{CORE}/fact_customer_balance_snapshot")

obt = (f.join(F.broadcast(d), "date_key").join(c, "customer_key")
        .join(F.broadcast(l), "location_key").join(F.broadcast(t), "txn_type_key"))
obt.write.mode("overwrite").option("parquet.block.size", 16 * 2**20) \
   .parquet(f"{LAKE}/bench/obt/obt_transactions")
snap = s.join(c, "customer_key")
snap.write.mode("overwrite").option("parquet.block.size", 16 * 2**20) \
    .parquet(f"{LAKE}/bench/obt/obt_balance_snapshot")
print(f"spark_build_s={time.time() - start:.2f}")
spark.stop()
