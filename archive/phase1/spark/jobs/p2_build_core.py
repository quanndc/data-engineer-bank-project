"""P2 -- Staging (cleaning) and core (Kimball star schema) layers.

staging/ : cleaned rows with data quality flags, still on source keys
core/    : dimensions and facts with surrogate keys, ready for ClickHouse

Design decisions (see CHANGELOG):
  * customer natural key = (customer_id, original DOB, original gender): unique per individual
  * DOB: 1800-01-01 placeholder -> NULL; dates after the reference date -> minus 100 years
    (two-digit-year error); anything outside age 0..100 afterwards -> NULL
  * locations: ~55 largest cities normalised from seeds/city_mapping.csv, the rest -> 'Other'
  * balance snapshot: one row per customer per month end (semi-additive measure)
"""
import datetime as dt

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import CORE, RAW, REFERENCE_DATE, SEEDS, STAGING, spark_session

# Column expressions need an active SparkSession, so they are built by functions
def ref_date():
    return F.to_date(F.lit(REFERENCE_DATE))


def min_dob():
    return F.to_date(F.lit("1916-10-22"))            # age <= 100 at the reference date

# Data quality flags (bit mask)
DQ_DOB_PLACEHOLDER = 1      # DOB was 1800-01-01
DQ_DOB_CENTURY_FIXED = 2    # DOB was in the future, shifted back 100 years
DQ_DOB_MISSING = 4          # DOB was NULL in the source
DQ_DOB_OUT_OF_RANGE = 8     # DOB outside age 0..100 after correction -> NULL
DQ_GENDER_INVALID = 16      # gender not M/F -> 'Unknown'
DQ_SHARED_ID = 32           # customer_id shared with other individuals
DQ_ZERO_AMOUNT = 1          # (fact) transaction amount is 0

TXN_TYPES = [(1, "Purchase"), (2, "Transfer"), (3, "Bill Payment"), (4, "Loan Payment")]

# Indian public holidays: fixed national days every year + festivals in the 2016 data window
HOLIDAYS_2016 = {
    "2016-08-18": "Raksha Bandhan", "2016-08-25": "Janmashtami",
    "2016-09-05": "Ganesh Chaturthi", "2016-09-13": "Eid al-Adha",
    "2016-10-11": "Dussehra", "2016-10-12": "Muharram", "2016-10-30": "Diwali",
}
NATIONAL_DAYS = {"01-26": "Republic Day", "08-15": "Independence Day", "10-02": "Gandhi Jayanti"}


def flag(cond, bit):
    return F.when(cond, F.lit(bit)).otherwise(F.lit(0))


# ---------------------------------------------------------------- staging
def stage_customers(spark):
    c = spark.read.parquet(f"{RAW}/customers")
    dob, gender = F.col("customer_dob"), F.col("customer_gender")
    corrected = (F.when(dob == F.to_date(F.lit("1800-01-01")), None)
                  .when(dob > ref_date(), F.add_months(dob, -1200))
                  .otherwise(dob))
    in_range = (corrected >= min_dob()) & (corrected <= ref_date())
    shared = F.count("*").over(Window.partitionBy("customer_id")) > 1

    c = (c
         .withColumn("customer_nk", F.sha2(F.concat_ws(
             "|", "customer_id",
             F.coalesce(dob.cast("string"), F.lit("NA")),
             F.coalesce(gender, F.lit("NA"))), 256))
         .withColumn("dob", F.when(in_range, corrected))
         .withColumn("age", F.floor(F.months_between(ref_date(), F.col("dob")) / 12).cast("int"))
         .withColumn("gender", F.when(gender.isin("M", "F"), gender).otherwise("Unknown"))
         .withColumn("dq_flags",
                     flag(dob == F.to_date(F.lit("1800-01-01")), DQ_DOB_PLACEHOLDER)
                     + flag(dob > ref_date(), DQ_DOB_CENTURY_FIXED)
                     + flag(dob.isNull(), DQ_DOB_MISSING)
                     + flag(corrected.isNotNull() & ~in_range, DQ_DOB_OUT_OF_RANGE)
                     + flag(~gender.isin("M", "F") | gender.isNull(), DQ_GENDER_INVALID)
                     + flag(shared, DQ_SHARED_ID)))
    age = F.col("age")
    c = c.withColumn("age_group",
                     F.when(age.isNull(), "Unknown")
                      .when(age < 18, "0-17").when(age < 25, "18-24").when(age < 35, "25-34")
                      .when(age < 45, "35-44").when(age < 55, "45-54").when(age < 65, "55-64")
                      .otherwise("65+"))
    c.write.mode("overwrite").parquet(f"{STAGING}/customers")
    return spark.read.parquet(f"{STAGING}/customers")


def stage_locations(spark):
    loc = (spark.read.parquet(f"{RAW}/locations")
           .withColumn("location_name",
                       F.regexp_replace(F.upper(F.trim("customer_location")), r"\s+", " ")))
    seeds = (spark.read.option("header", True).csv(f"{SEEDS}/city_mapping.csv")
             .withColumn("kw_len", F.length("keyword")))
    # a location matches a keyword when it IS the keyword or ENDS WITH " keyword";
    # the longest keyword wins (e.g. NAVI MUMBAI before MUMBAI, NEW DELHI before DELHI)
    matched = (loc.join(F.broadcast(seeds),
                        (F.col("location_name") == F.col("keyword"))
                        | F.col("location_name").endswith(F.concat(F.lit(" "), F.col("keyword"))),
                        "left")
               .withColumn("rk", F.row_number().over(
                   Window.partitionBy("location_key").orderBy(F.col("kw_len").desc_nulls_last())))
               .filter("rk = 1")
               .select("location_key", "location_name",
                       F.coalesce("city", F.lit("Other")).alias("city"),
                       F.coalesce("state", F.lit("Other")).alias("state"),
                       F.col("city").isNotNull().cast("int").alias("is_top_city")))
    matched.write.mode("overwrite").parquet(f"{STAGING}/locations")
    return spark.read.parquet(f"{STAGING}/locations")


def stage_transactions(spark):
    t = spark.read.parquet(f"{RAW}/transactions")
    # Kafka delivery is at-least-once: keep one row per transaction (earliest offset)
    first = Window.partitionBy("transaction_key").orderBy("kafka_partition", "kafka_offset")
    t = (t.withColumn("rn", F.row_number().over(first)).filter("rn = 1").drop("rn")
          .withColumn("dq_flags", flag(F.col("transaction_amount") == 0, DQ_ZERO_AMOUNT)))
    t.write.mode("overwrite").parquet(f"{STAGING}/transactions")
    return spark.read.parquet(f"{STAGING}/transactions")


# ---------------------------------------------------------------- core
def build_dim_date(spark):
    start, end = dt.date(2015, 1, 1), dt.date(2017, 12, 31)
    rows = []
    d = start
    while d <= end:
        name = HOLIDAYS_2016.get(d.isoformat()) or NATIONAL_DAYS.get(d.strftime("%m-%d"))
        iso = d.isocalendar()
        rows.append((int(d.strftime("%Y%m%d")), d, d.year, (d.month - 1) // 3 + 1, d.month,
                     d.strftime("%B"), iso[1], d.day, iso[2], d.strftime("%A"),
                     int(iso[2] >= 6), int(name is not None), name or ""))
        d += dt.timedelta(days=1)
    cols = ["date_key", "full_date", "year", "quarter", "month", "month_name", "week_of_year",
            "day_of_month", "day_of_week", "day_name", "is_weekend", "is_holiday", "holiday_name"]
    spark.createDataFrame(rows, cols).coalesce(1).write.mode("overwrite").parquet(f"{CORE}/dim_date")


def build_dim_customer(spark, cust):
    sk = F.row_number().over(Window.orderBy("customer_id", "customer_dob", "customer_gender"))
    dim = cust.select(
        sk.alias("customer_key"), "customer_nk", "customer_id", "dob", "age", "age_group",
        "gender", F.col("marital_status_derived").alias("marital_status"), "account_status",
        "household_id", "dq_flags",
        F.col("customer_key").alias("source_customer_key"))
    dim.write.mode("overwrite").parquet(f"{STAGING}/customer_key_map")
    dim = spark.read.parquet(f"{STAGING}/customer_key_map")
    unknown = spark.createDataFrame(
        [(0, "UNKNOWN", "UNKNOWN", None, None, "Unknown", "Unknown", "Unknown", "Unknown", 0, 0)],
        "customer_key int, customer_nk string, customer_id string, dob date, age int, "
        "age_group string, gender string, marital_status string, account_status string, "
        "household_id long, dq_flags int")
    (dim.drop("source_customer_key").unionByName(unknown)
        .coalesce(4).write.mode("overwrite").parquet(f"{CORE}/dim_customer"))
    return dim.select("source_customer_key", "customer_key")


def build_dim_location(spark, loc):
    sk = F.row_number().over(Window.orderBy("location_name", "location_key"))
    dim = loc.select(sk.alias("location_key"), "location_name", "city", "state", "is_top_city",
                     F.col("location_key").alias("source_location_key"))
    dim.write.mode("overwrite").parquet(f"{STAGING}/location_key_map")
    dim = spark.read.parquet(f"{STAGING}/location_key_map")
    unknown = spark.createDataFrame([(0, "UNKNOWN", "Unknown", "Unknown", 0)],
                                    "location_key int, location_name string, city string, "
                                    "state string, is_top_city int")
    (dim.drop("source_location_key").unionByName(unknown)
        .coalesce(1).write.mode("overwrite").parquet(f"{CORE}/dim_location"))
    return dim.select("source_location_key", "location_key")


def build_dim_transaction_type(spark):
    rows = [(k, n, 1) for k, n in TXN_TYPES] + [(0, "Unknown", 0)]
    (spark.createDataFrame(rows, "txn_type_key int, type_name string, is_rule_inferred int")
        .coalesce(1).write.mode("overwrite").parquet(f"{CORE}/dim_transaction_type"))


def build_facts(spark, txn, cust_map, loc_map):
    types = spark.createDataFrame(TXN_TYPES, "txn_type_key int, transaction_type string")
    src = (txn.withColumnRenamed("customer_key", "source_customer_key")
              .withColumnRenamed("location_key", "source_location_key"))
    fact = (src
            .join(F.broadcast(loc_map), "source_location_key", "left")
            .join(cust_map, "source_customer_key", "left")
            .join(F.broadcast(types), "transaction_type", "left")
            .select("transaction_key", "date_key",
                    F.coalesce("customer_key", F.lit(0)).alias("customer_key"),
                    F.coalesce("location_key", F.lit(0)).alias("location_key"),
                    F.coalesce("txn_type_key", F.lit(0)).alias("txn_type_key"),
                    "transaction_amount", "account_balance", "dq_flags"))
    fact.repartition(4, "date_key").write.mode("overwrite").parquet(f"{CORE}/fact_transactions")
    fact = spark.read.parquet(f"{CORE}/fact_transactions")

    # Periodic snapshot: last known balance per customer at each month end
    d = F.to_date(F.col("date_key").cast("string"), "yyyyMMdd")
    t = fact.select("customer_key", "transaction_key", "account_balance", "date_key",
                    d.alias("txn_date"), F.last_day(d).alias("txn_month_end"))
    month_ends = t.select(F.col("txn_month_end").alias("snapshot_date")).distinct()
    last_in_month = (t.withColumn("rn", F.row_number().over(
                         Window.partitionBy("customer_key", "txn_month_end")
                               .orderBy(F.col("txn_date").desc(), F.col("transaction_key").desc())))
                      .filter("rn = 1").drop("rn"))
    snap = (last_in_month.join(F.broadcast(month_ends),
                               F.col("snapshot_date") >= F.col("txn_month_end"))
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("customer_key", "snapshot_date")
                      .orderBy(F.col("txn_month_end").desc())))
            .filter("rn = 1")
            .select(F.date_format("snapshot_date", "yyyyMMdd").cast("int").alias("snapshot_date_key"),
                    "customer_key",
                    F.col("account_balance").alias("balance"),
                    F.col("date_key").alias("last_txn_date_key"),
                    F.datediff(F.least(F.col("snapshot_date"), ref_date()), "txn_date")
                     .alias("days_since_last_txn"),
                    (F.col("snapshot_date") > ref_date()).cast("int").alias("is_partial_period")))
    snap.coalesce(4).write.mode("overwrite").parquet(f"{CORE}/fact_customer_balance_snapshot")


def main():
    spark = spark_session("p2_build_core")
    cust = stage_customers(spark)
    loc = stage_locations(spark)
    txn = stage_transactions(spark)

    build_dim_date(spark)
    cust_map = build_dim_customer(spark, cust)
    loc_map = build_dim_location(spark, loc)
    build_dim_transaction_type(spark)
    build_facts(spark, txn, cust_map, loc_map)

    for name in ("dim_date", "dim_customer", "dim_location", "dim_transaction_type",
                 "fact_transactions", "fact_customer_balance_snapshot"):
        print(f"core/{name}: {spark.read.parquet(f'{CORE}/{name}').count():,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
