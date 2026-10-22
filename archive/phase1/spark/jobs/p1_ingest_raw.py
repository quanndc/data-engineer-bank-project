"""P1 -- Raw layer.

Transactions are read from the Kafka topic (batch read of everything retained);
reference data (customers, locations) is read directly from the source CSV files.
Everything is written unchanged, with explicit types, to Parquet under /lake/raw.
"""
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import KAFKA_BOOTSTRAP, RAW, SOURCE, TOPIC, spark_session

TXN_SCHEMA = T.StructType([
    T.StructField("transaction_key", T.LongType()),
    T.StructField("customer_key", T.LongType()),
    T.StructField("location_key", T.LongType()),
    T.StructField("date_key", T.IntegerType()),
    T.StructField("transaction_amount", T.StringType()),
    T.StructField("account_balance", T.StringType()),
    T.StructField("transaction_type", T.StringType()),
    T.StructField("produced_at_ms", T.LongType()),
])

CUSTOMER_SCHEMA = T.StructType([
    T.StructField("customer_key", T.LongType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("customer_dob", T.DateType()),
    T.StructField("customer_gender", T.StringType()),
    T.StructField("household_id", T.LongType()),
    T.StructField("marital_status_derived", T.StringType()),
    T.StructField("account_status", T.StringType()),
])

LOCATION_SCHEMA = T.StructType([
    T.StructField("location_key", T.LongType()),
    T.StructField("customer_location", T.StringType()),
])


def main():
    spark = spark_session("p1_ingest_raw")

    kafka = (spark.read.format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", TOPIC)
             .option("startingOffsets", "earliest")
             .option("endingOffsets", "latest")
             .load())
    txn = (kafka
           .select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("t"),
                   F.col("partition").alias("kafka_partition"),
                   F.col("offset").alias("kafka_offset"))
           .select("t.*", "kafka_partition", "kafka_offset")
           .withColumn("transaction_amount", F.col("transaction_amount").cast("decimal(18,2)"))
           .withColumn("account_balance", F.col("account_balance").cast("decimal(18,2)")))
    txn.write.mode("overwrite").parquet(f"{RAW}/transactions")

    csv = spark.read.option("header", True).option("dateFormat", "yyyy-MM-dd")
    csv.schema(CUSTOMER_SCHEMA).csv(f"{SOURCE}/dim_customer.csv") \
       .write.mode("overwrite").parquet(f"{RAW}/customers")
    csv.schema(LOCATION_SCHEMA).csv(f"{SOURCE}/dim_location.csv") \
       .write.mode("overwrite").parquet(f"{RAW}/locations")

    for name in ("transactions", "customers", "locations"):
        n = spark.read.parquet(f"{RAW}/{name}").count()
        print(f"raw/{name}: {n:,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
