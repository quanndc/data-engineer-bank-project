"""P4 -- BR7: near-real-time detection of unusual transactions.

Kafka topic `transactions` -> Spark Structured Streaming -> dwh.fact_transaction_alert.

Rules (sized on the historical data: together they flag about 1.8% of transactions):
  HIGH_AMOUNT         amount > 20,000 INR (the historical 99th percentile)
  HIGH_BALANCE_RATIO  amount >= 5,000 INR and amount > account balance

Source keys are mapped to the conformed surrogate keys of dim_customer / dim_location
with a stream-static join, so alerts join the star schema directly.

    spark-submit p4_stream_alerts.py              # from new events only (latest)
    spark-submit p4_stream_alerts.py --earliest   # replay everything retained in Kafka
"""
import sys

from pyspark.sql import functions as F

from common import KAFKA_BOOTSTRAP, LAKE, STAGING, TOPIC, clickhouse, spark_session
from p1_ingest_raw import TXN_SCHEMA

TAU = 20000          # HIGH_AMOUNT threshold (INR)
MIN_RATIO_AMOUNT = 5000
CHECKPOINT = f"{LAKE}/checkpoints/alerts"
COLUMNS = ["transaction_key", "date_key", "customer_key", "location_key",
           "transaction_amount", "account_balance", "rule", "produced_at", "detected_at"]


def write_batch(batch_df, batch_id):
    rows = batch_df.select(*COLUMNS).collect()
    if not rows:
        return
    ch = clickhouse()
    ch.insert("dwh.fact_transaction_alert", [list(r) for r in rows], column_names=COLUMNS)
    print(f"batch {batch_id}: {len(rows)} alerts", flush=True)


def main():
    spark = spark_session("p4_stream_alerts")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    starting = "earliest" if "--earliest" in sys.argv else "latest"

    # Static lookups (source key -> conformed surrogate key), loaded once and broadcast
    cust_map = (spark.read.parquet(f"{STAGING}/customer_key_map")
                .select("source_customer_key", "customer_key").cache())
    loc_map = (spark.read.parquet(f"{STAGING}/location_key_map")
               .select("source_location_key", "location_key").cache())
    # Materialize the caches now: otherwise the first micro-batch pays for loading
    # ~1M customer keys and its alerts arrive several seconds late (cold start).
    print(f"lookups cached: {cust_map.count():,} customers, {loc_map.count():,} locations",
          flush=True)
    cust_map, loc_map = F.broadcast(cust_map), F.broadcast(loc_map)

    events = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
              .option("subscribe", TOPIC)
              .option("startingOffsets", starting)
              .option("maxOffsetsPerTrigger", 200000)
              .load()
              .select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("t"))
              .select("t.*"))

    amount = F.col("transaction_amount").cast("decimal(18,2)")
    balance = F.col("account_balance").cast("decimal(18,2)")
    alerts = (events
              .withColumn("transaction_amount", amount)
              .withColumn("account_balance", balance)
              .withColumn("rule",
                          F.when(amount > TAU, "HIGH_AMOUNT")
                           .when((amount >= MIN_RATIO_AMOUNT) & (amount > balance),
                                 "HIGH_BALANCE_RATIO"))
              .filter(F.col("rule").isNotNull())
              .withColumn("produced_at", (F.col("produced_at_ms") / 1000).cast("timestamp"))
              .withColumn("detected_at", F.current_timestamp())
              .withColumnRenamed("customer_key", "source_customer_key")
              .withColumnRenamed("location_key", "source_location_key")
              .join(cust_map, "source_customer_key", "left")
              .join(loc_map, "source_location_key", "left")
              .withColumn("customer_key", F.coalesce("customer_key", F.lit(0)))
              .withColumn("location_key", F.coalesce("location_key", F.lit(0))))

    query = (alerts.writeStream
             .foreachBatch(write_batch)
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime="2 seconds")
             .start())
    print(f"streaming from '{TOPIC}' ({starting}), checkpoint {CHECKPOINT}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
