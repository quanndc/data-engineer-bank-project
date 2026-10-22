"""C. Batch pipeline (PySpark) -- flows L01 (ingest), L02 (process and load), L05 (re-run).

    spark-submit pipeline.py ref                         # REF_INITIAL: reference data -> dimensions
    spark-submit pipeline.py history 1993-01 1998-12     # HIST_YYYY_MM batches, one per month
    spark-submit pipeline.py rerun HIST_1995_03          # re-run a batch from its Data Lake snapshot
    spark-submit pipeline.py reconcile                   # whole source vs. warehouse
    spark-submit pipeline.py scenario KB05               # scenario batch from data/berka/scenarios/KB05
    spark-submit pipeline.py alerts all                  # back-fill the alert rules on loaded data

Every run and step is logged in PostgreSQL (ops.pipeline_run / ops.pipeline_step, BR06); rejected
records go to ops.rejected_record (BR05). Rules follow the group sheet (Data Model E01-E05, P01-P05,
C01-C03) and docs/PLAN_PHASE2.md.
"""
import datetime as dt
import os
import sys
from decimal import Decimal

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import BATCHES, SOURCE, SOURCE_RAW, STAGING, clickhouse, spark_session
import alerts
from ops_log import RunLog

OPEN_END = dt.date(2299, 12, 31)          # valid_to of the current SCD2 version (decision Q-J)
DATE_RANGE = (dt.date(1993, 1, 1), dt.date(2000, 12, 31))

FREQUENCY_NAMES = {"POPLATEK MESICNE": "Hàng tháng", "POPLATEK TYDNE": "Hàng tuần",
                   "POPLATEK PO OBRATU": "Sau giao dịch"}
TYPE_NAMES = {"PRIJEM": "Tiền vào", "VYDAJ": "Tiền ra", "VYBER": "Rút tiền"}
DIRECTION = {"PRIJEM": "IN", "VYDAJ": "OUT", "VYBER": "OUT"}
OPERATION_NAMES = {"VKLAD": "Nộp tiền mặt", "PREVOD Z UCTU": "Nhận chuyển khoản từ ngân hàng khác",
                   "VYBER": "Rút tiền mặt", "PREVOD NA UCET": "Chuyển khoản sang ngân hàng khác",
                   "VYBER KARTOU": "Rút tiền bằng thẻ", "NOT_RECORDED": "Không ghi nhận"}
PURPOSE_NAMES = {"POJISTNE": "Phí bảo hiểm", "SLUZBY": "Phí sao kê / dịch vụ",
                 "UROK": "Lãi tiền gửi", "SANKC. UROK": "Lãi phạt (số dư âm)",
                 "SIPO": "Chi tiêu gia đình", "DUCHOD": "Lương hưu", "UVER": "Trả nợ vay",
                 "NOT_RECORDED": "Không ghi nhận"}


def now():
    return dt.datetime.utcnow().replace(microsecond=0)


# ---------------------------------------------------------------- ClickHouse helpers
CH = None


def ch():
    global CH
    if CH is None:
        CH = clickhouse()
    return CH


def ch_df(sql, **params):
    """Query result as a DataFrame of Python objects (date, Decimal). query_df is not used: pandas
    timestamps end in 2262 and cannot hold valid_to = 2299-12-31."""
    r = ch().query(sql, parameters=params or None)
    names = r.column_names
    if not names:  # empty result: clickhouse-connect returns no column names
        names = [row[0] for row in ch().query(f"DESCRIBE TABLE ({sql})", parameters=params or None).result_rows]
    return pd.DataFrame(r.result_rows, columns=names)


def ch_insert(table, pdf):
    if len(pdf):
        ch().insert_df(f"dw.{table}", pdf.reset_index(drop=True))
    return len(pdf)


# ---------------------------------------------------------------- source and Data Lake (A -> B)
def read_source(spark, name):
    """Berka CSV (PKDD'99): ';'-separated, everything read as string."""
    return (spark.read.option("header", True).option("sep", ";").option("quote", '"')
            .csv(f"{SOURCE_RAW}/{name}.csv"))


def snapshot(df, batch_id, name):
    """L01: keep the input of the batch in the Data Lake, then read it back from there."""
    path = f"{BATCHES}/{batch_id}/{name}.parquet"
    df.coalesce(1).write.mode("overwrite").parquet(path)
    return df.sparkSession.read.parquet(path)


def yymmdd(col):
    return F.to_date(F.concat(F.lit("19"), col), "yyyyMMdd")


# ---------------------------------------------------------------- dimensions (Type 1)
def upsert_type1(table, sk_col, nk_col, new, attrs):
    """Type 1: new natural keys get a new surrogate key; changed attributes are written as a new
    row with the same key and a later updated_at (ReplacingMergeTree keeps the latest)."""
    cols = ", ".join([sk_col, nk_col] + attrs + ["created_at"])
    old = ch_df(f"SELECT {cols} FROM dw.{table} FINAL")
    ts = now()
    merged = new.merge(old, on=nk_col, how="left", suffixes=("", "_old"))
    is_new = merged[sk_col].isna()
    changed = ~is_new & (merged[attrs].astype(str).values
                         != merged[[a + "_old" for a in attrs]].astype(str).values).any(axis=1)
    start = int(old[sk_col].max()) if len(old) else 0
    merged.loc[is_new, sk_col] = range(start + 1, start + 1 + int(is_new.sum()))
    merged.loc[is_new, "created_at"] = ts
    out = merged[is_new | changed].copy()
    out["updated_at"] = ts
    out[sk_col] = out[sk_col].astype("int64")
    ch_insert(table, out[[sk_col, nk_col] + attrs + ["created_at", "updated_at"]])
    return int(is_new.sum()), int(changed.sum())


def load_dim_date():
    if ch_df("SELECT count() AS n FROM dw.dim_date")["n"][0]:
        return 0
    days = pd.date_range(*DATE_RANGE, freq="D")
    pdf = pd.DataFrame({
        "date_key": days.strftime("%Y%m%d").astype(int),
        "full_date": days.date,
        "day_of_month": days.day,
        "day_of_week": days.dayofweek + 1,
        "day_name": days.day_name(),
        "month_number": days.month,
        "month_name": days.month_name(),
        "quarter_number": days.quarter,
        "year_number": days.year,
        "year_month": days.strftime("%Y-%m"),
        "is_weekend": (days.dayofweek >= 5).astype(int),
    })
    return ch_insert("dim_date", pdf)


def district_map():
    m = ch_df("SELECT district_id, district_sk FROM dw.dim_district FINAL")
    return dict(zip(m.district_id, m.district_sk))


def account_map():
    m = ch_df("SELECT account_id, account_sk FROM dw.dim_account FINAL")
    return dict(zip(m.account_id, m.account_sk))


# ---------------------------------------------------------------- dim_customer (SCD Type 2)
def apply_customer_versions(incoming):
    """C01-C03: one version per tier. A new tier closes the current version (valid_to = new
    tier_effective_from, is_current = 0) and opens a new one with a new customer_sk.
    incoming: client_id, gender, birth_date, customer_district_sk, membership_tier,
              tier_effective_from."""
    cur = ch_df("SELECT * FROM dw.dim_customer FINAL WHERE is_current = 1")
    current = {int(r.client_id): r for r in cur.itertuples()}
    next_sk = int(ch_df("SELECT max(customer_sk) AS m FROM dw.dim_customer")["m"][0] or 0) + 1
    ts, rows, opened, closed, unchanged, refused = now(), [], 0, 0, 0, []
    for r in incoming.itertuples():
        c = current.get(int(r.client_id))
        if c is not None and c.membership_tier == r.membership_tier:
            unchanged += 1
            continue
        if c is not None:
            if r.tier_effective_from <= c.valid_from:
                refused.append((int(r.client_id), "tier_effective_from is not after the current version"))
                continue
            rows.append(dict(customer_sk=c.customer_sk, client_id=c.client_id, gender=c.gender,
                             birth_date=c.birth_date, customer_district_sk=c.customer_district_sk,
                             membership_tier=c.membership_tier, valid_from=c.valid_from,
                             valid_to=r.tier_effective_from, is_current=0,
                             created_at=c.created_at, updated_at=ts))
            closed += 1
        rows.append(dict(customer_sk=next_sk, client_id=int(r.client_id), gender=r.gender,
                         birth_date=r.birth_date, customer_district_sk=int(r.customer_district_sk),
                         membership_tier=r.membership_tier, valid_from=r.tier_effective_from,
                         valid_to=OPEN_END, is_current=1, created_at=ts, updated_at=ts))
        next_sk += 1
        opened += 1
    ch_insert("dim_customer", pd.DataFrame(rows))
    return opened, closed, unchanged, refused


# ---------------------------------------------------------------- REF_INITIAL batch
def run_ref(spark):
    batch_id = "REF_INITIAL"
    log = RunLog(batch_id, "HISTORICAL", "Dữ liệu tham chiếu: district, account, client, disp, hạng mô phỏng",
                 lake_path=f"{BATCHES}/{batch_id}")
    try:
        with log.step("ingest") as s:
            src = {n: snapshot(read_source(spark, n), batch_id, n)
                   for n in ("district", "account", "client", "disp")}
            prof = (spark.read.option("header", True).csv(f"{SOURCE}/customer_profile_simulated.csv"))
            src["customer_profile_simulated"] = snapshot(prof, batch_id, "customer_profile_simulated")
            log.rows_received = sum(df.count() for df in src.values())
            s["rows_in"] = s["rows_out"] = log.rows_received

        with log.step("validate") as s:
            acc_ids = src["account"].select("account_id").distinct()
            owners = src["disp"].filter(F.trim("type") == "OWNER")
            per_account = owners.groupBy("account_id").count()
            problems = {
                "accounts without exactly one OWNER":
                    acc_ids.join(per_account, "account_id", "left").filter("count IS NULL OR count <> 1").count(),
                "OWNER clients without a tier":
                    owners.join(src["customer_profile_simulated"], "client_id", "left_anti").count(),
                "clients with an unknown district":
                    src["client"].join(src["district"].withColumnRenamed("A1", "district_id"),
                                       "district_id", "left_anti").count(),
            }
            s["rows_in"] = log.rows_received
            bad = {k: v for k, v in problems.items() if v}
            if bad:
                raise ValueError(f"reference data check failed: {bad}")
            s["rows_out"] = log.rows_received

        with log.step("load_dimensions") as s:
            n_date = load_dim_date()
            d = src["district"].toPandas()
            new_d, ch_d = upsert_type1(
                "dim_district", "district_sk", "district_id",
                pd.DataFrame({"district_id": d["A1"].astype(int), "district_name": d["A2"].str.strip(),
                              "region_name": d["A3"].str.strip()}),
                ["district_name", "region_name"])
            dmap = district_map()
            a = src["account"].toPandas()
            new_a, ch_a = upsert_type1(
                "dim_account", "account_sk", "account_id",
                pd.DataFrame({
                    "account_id": a["account_id"].astype(int),
                    "account_open_date": pd.to_datetime("19" + a["date"], format="%Y%m%d").dt.date,
                    "statement_frequency_code": a["frequency"].str.strip(),
                    "statement_frequency_name": a["frequency"].str.strip().map(FREQUENCY_NAMES)
                                                 .fillna("Không xác định"),
                    "account_district_sk": a["district_id"].astype(int).map(dmap).astype(int)}),
                ["account_open_date", "statement_frequency_code", "statement_frequency_name",
                 "account_district_sk"])
            s["rows_out"] = n_date + new_d + ch_d + new_a + ch_a
            print(f"  dim_date +{n_date} | dim_district +{new_d} new, {ch_d} changed | "
                  f"dim_account +{new_a} new, {ch_a} changed")

        with log.step("build_customer") as s:
            # E01-E05: client + disp(OWNER) + district + simulated tier -> one customer table
            client = (src["client"]
                      .withColumn("yy", F.substring("birth_number", 1, 2).cast("int"))
                      .withColumn("mm_raw", F.substring("birth_number", 3, 2).cast("int"))
                      .withColumn("dd", F.substring("birth_number", 5, 2).cast("int"))
                      .withColumn("gender", F.when(F.col("mm_raw") > 50, "F").otherwise("M"))
                      .withColumn("mm", F.when(F.col("mm_raw") > 50, F.col("mm_raw") - 50)
                                  .otherwise(F.col("mm_raw")))
                      .withColumn("birth_date", F.make_date(F.col("yy") + 1900, "mm", "dd")))
            owner = src["disp"].filter(F.trim("type") == "OWNER").select("client_id", "account_id")
            prof = src["customer_profile_simulated"]
            stg_customer = (client.join(owner, "client_id")
                            .join(prof.select("client_id", "membership_tier",
                                              F.to_date("tier_effective_from").alias("tier_effective_from")),
                                  "client_id")
                            .select(F.col("client_id").cast("int"), "gender", "birth_date",
                                    F.col("district_id").cast("int"), "membership_tier",
                                    "tier_effective_from"))
            stg_customer.write.mode("overwrite").parquet(f"{STAGING}/stg_customer.parquet")
            owner.select(F.col("account_id").cast("int"), F.col("client_id").cast("int")) \
                 .write.mode("overwrite").parquet(f"{STAGING}/stg_account_owner.parquet")
            n_clients, n_owners = stg_customer.count(), owner.count()
            s["rows_in"], s["rows_out"] = src["client"].count(), n_clients
            if n_clients != n_owners or stg_customer.select("client_id").distinct().count() != n_clients:
                raise ValueError(f"customer join lost or duplicated rows: {n_clients} vs {n_owners} owners")
            print(f"  stg_customer {n_clients} rows (1 per OWNER client), stg_account_owner {n_owners} rows")

        with log.step("load_customer_scd2") as s:
            inc = spark.read.parquet(f"{STAGING}/stg_customer.parquet").toPandas()
            inc["customer_district_sk"] = inc["district_id"].map(district_map())
            opened, closed, unchanged, refused = apply_customer_versions(inc)
            s["rows_in"], s["rows_out"] = len(inc), opened + closed
            log.rows_loaded = opened
            if refused:
                raise ValueError(f"{len(refused)} tier changes refused, e.g. {refused[:3]}")
            print(f"  dim_customer: {opened} versions opened, {closed} closed, {unchanged} unchanged")
        print(f"{batch_id}: {log.finish()}")
    except Exception:
        print(f"{batch_id}: FAILED (see ops.pipeline_step)")
        raise


# ---------------------------------------------------------------- transaction batches
VALID_TYPES = ("PRIJEM", "VYDAJ", "VYBER")


def standardize_codes(df):
    """P01-P05: trim/upper case; NULL or blank -> NOT_RECORDED."""
    def code(c):
        return F.coalesce(F.when(F.trim(F.col(c)) != "", F.upper(F.trim(F.col(c)))), F.lit("NOT_RECORDED"))
    return (df.withColumn("type_code", F.upper(F.trim("type")))
              .withColumn("operation_code", code("operation"))
              .withColumn("purpose_code", code("k_symbol")))


def validate(spark, log, t):
    """BR05: V1-V6. Returns accepted rows; rejected rows are written to ops.rejected_record.
    Transactions already loaded by this same batch are skipped (re-run), not rejected."""
    amap = account_map()
    accounts = spark.createDataFrame(pd.DataFrame({"account_id": [str(k) for k in amap]}))
    t = t.withColumn("txn_date", yymmdd(F.col("date")))
    dates = ch_df("SELECT min(date_key) AS lo, max(date_key) AS hi FROM dw.dim_date")
    w = Window.partitionBy("trans_id").orderBy(F.monotonically_increasing_id())
    t = (t.withColumn("dup_rank", F.row_number().over(w))
          .join(accounts.withColumn("acc_ok", F.lit(True)), "account_id", "left"))
    loaded = ch_df("SELECT toString(transaction_id) AS trans_id, load_batch_id FROM dw.fact_transaction "
                   "FINAL WHERE date_key BETWEEN %(lo)s AND %(hi)s",
                   lo=int(t.agg(F.min(F.date_format("txn_date", "yyyyMMdd"))).first()[0] or 0),
                   hi=int(t.agg(F.max(F.date_format("txn_date", "yyyyMMdd"))).first()[0] or 0))
    if len(loaded):
        t = t.join(spark.createDataFrame(loaded), "trans_id", "left")
    else:
        t = t.withColumn("load_batch_id", F.lit(None).cast("string"))
    amount_ok = F.col("amount").rlike(r"^\s*\d+(\.\d+)?\s*$")
    rule = (F.when(F.col("trans_id").isNull() | F.col("account_id").isNull() | F.col("date").isNull()
                   | F.col("type").isNull() | F.col("amount").isNull(), F.lit("V1"))
             .when(F.col("acc_ok").isNull(), F.lit("V2"))
             .when(~amount_ok, F.lit("V3"))
             .when(F.col("txn_date").isNull()
                   | (F.date_format("txn_date", "yyyyMMdd").cast("int") < int(dates.lo[0]))
                   | (F.date_format("txn_date", "yyyyMMdd").cast("int") > int(dates.hi[0])), F.lit("V4"))
             .when(~F.upper(F.trim("type")).isin(*VALID_TYPES), F.lit("V5"))
             .when((F.col("dup_rank") > 1)
                   | (F.col("load_batch_id").isNotNull() & (F.col("load_batch_id") != log.batch_id)),
                   F.lit("V6")))
    t = t.withColumn("rule", rule).cache()
    reasons = {"V1": "thiếu trường bắt buộc", "V2": "tài khoản không tồn tại trong kho",
               "V3": "số tiền âm hoặc không phải số", "V4": "ngày không hợp lệ hoặc ngoài lịch",
               "V5": "mã loại giao dịch lạ", "V6": "trùng transaction_id (trong lô hoặc đã nạp bởi lô khác)"}
    src_cols = ["trans_id", "account_id", "date", "type", "operation", "amount", "balance",
                "k_symbol", "bank", "account"]
    bad = t.filter(F.col("rule").isNotNull()).select(*src_cols, "rule").toPandas()
    log.reject(("trans", r.trans_id, r.rule, reasons[r.rule], {c: getattr(r, c) for c in src_cols})
               for r in bad.itertuples())
    already = t.filter(F.col("rule").isNull() & (F.col("load_batch_id") == log.batch_id))
    accepted = t.filter(F.col("rule").isNull())
    return accepted, len(bad), already.count()


def load_profiles(spark, t):
    """Add combinations that appear for the first time; labels are Type 1.
    Returns (combinations in the batch, new profiles)."""
    combos = t.select("type_code", "operation_code", "purpose_code").distinct().toPandas()
    combos = combos.assign(
        combo=combos.type_code + "|" + combos.operation_code + "|" + combos.purpose_code,
        type_name=combos.type_code.map(TYPE_NAMES).fillna(combos.type_code),
        operation_name=combos.operation_code.map(OPERATION_NAMES).fillna(combos.operation_code),
        purpose_name=combos.purpose_code.map(PURPOSE_NAMES).fillna(combos.purpose_code),
        cash_flow_direction=combos.type_code.map(DIRECTION))
    old = ch_df("SELECT transaction_profile_sk, type_code || '|' || operation_code || '|' || purpose_code AS combo "
                "FROM dw.dim_transaction_profile FINAL")
    new = combos[~combos.combo.isin(old.combo)].copy()
    start = int(old.transaction_profile_sk.max()) if len(old) else 0
    new["transaction_profile_sk"] = range(start + 1, start + 1 + len(new))
    new["created_at"] = new["updated_at"] = now()
    ch_insert("dim_transaction_profile", new[[
        "transaction_profile_sk", "type_code", "type_name", "operation_code", "operation_name",
        "purpose_code", "purpose_name", "cash_flow_direction", "created_at", "updated_at"]])
    return len(combos), len(new)


def load_fact(spark, log, t):
    """E01-E05: account_sk, customer_sk (OWNER version valid on the transaction date),
    transaction_profile_sk; transactions already loaded by this batch are skipped."""
    amap = ch_df("SELECT toString(account_id) AS account_id, account_sk FROM dw.dim_account FINAL")
    pmap = ch_df("SELECT type_code, operation_code, purpose_code, transaction_profile_sk "
                 "FROM dw.dim_transaction_profile FINAL")
    cust = ch_df("SELECT client_id, customer_sk, valid_from, valid_to FROM dw.dim_customer FINAL")
    owner = spark.read.parquet(f"{STAGING}/stg_account_owner.parquet") \
                 .withColumn("account_id", F.col("account_id").cast("string"))
    versions = spark.createDataFrame(pd.DataFrame({
        "v_client_id": cust.client_id.astype(int), "customer_sk": cust.customer_sk.astype(int),
        "valid_from": cust.valid_from, "valid_to": cust.valid_to}))
    new_rows = t.filter(F.col("load_batch_id").isNull())
    # the date condition is part of the join: a transaction outside every version keeps
    # customer_sk = NULL and is rejected (V7) instead of silently disappearing
    in_version = ((F.col("client_id") == F.col("v_client_id"))
                  & (F.col("txn_date") >= F.col("valid_from")) & (F.col("txn_date") < F.col("valid_to")))
    f = (new_rows.join(F.broadcast(spark.createDataFrame(amap)), "account_id")
         .join(F.broadcast(spark.createDataFrame(pmap)), ["type_code", "operation_code", "purpose_code"])
         .join(F.broadcast(owner), "account_id", "left")
         .join(F.broadcast(versions), in_version, "left"))
    no_version = f.filter(F.col("customer_sk").isNull()).select("trans_id", "account_id", "date").toPandas()
    log.reject(("trans", r.trans_id, "V7", "không có phiên bản chủ sở hữu hiệu lực tại ngày giao dịch",
                {"trans_id": r.trans_id, "account_id": r.account_id, "date": r.date})
               for r in no_version.itertuples())
    blank = lambda c: F.when(F.trim(F.col(c)) != "", F.trim(F.col(c)))  # noqa: E731
    out = (f.filter(F.col("customer_sk").isNotNull())
           .select(F.col("trans_id").cast("long").alias("transaction_id"),
                   F.date_format("txn_date", "yyyyMMdd").cast("int").alias("date_key"),
                   F.col("account_sk").cast("long"), F.col("customer_sk").cast("long"),
                   F.col("transaction_profile_sk").cast("int"),
                   F.col("amount").cast("decimal(18,2)").alias("amount"),
                   F.col("balance").cast("decimal(18,2)").alias("balance_after"),
                   blank("bank").alias("counterparty_bank_code"),
                   blank("account").alias("counterparty_account_ref"))
           .toPandas())
    out["load_batch_id"] = log.batch_id
    out["loaded_at"] = dt.datetime.utcnow()
    return ch_insert("fact_transaction", out), len(no_version)


def reconcile_batch(log, accepted, rejected_v7):
    """Accepted input rows (minus V7) must equal the fact rows of this batch: count and money."""
    exp = accepted.select(
        F.count("*").alias("n"),
        F.sum(F.when(F.upper(F.trim("type")) == "PRIJEM", F.col("amount").cast("decimal(18,2)"))).alias("m_in"),
        F.sum(F.when(F.upper(F.trim("type")) != "PRIJEM", F.col("amount").cast("decimal(18,2)"))).alias("m_out")
    ).first()
    got = ch_df("""SELECT count() AS n,
                          sumIf(f.amount, p.cash_flow_direction = 'IN') AS m_in,
                          sumIf(f.amount, p.cash_flow_direction = 'OUT') AS m_out
                   FROM dw.fact_transaction AS f FINAL
                   JOIN (SELECT * FROM dw.dim_transaction_profile FINAL) AS p USING (transaction_profile_sk)
                   WHERE f.load_batch_id = %(b)s""", b=log.batch_id).iloc[0]
    expected_n = exp["n"] - rejected_v7
    ok = int(got.n) == expected_n
    if rejected_v7 == 0:  # money can be compared exactly only when no accepted row was dropped
        ok &= Decimal(str(got.m_in)) == (exp["m_in"] or Decimal(0)) and \
              Decimal(str(got.m_out)) == (exp["m_out"] or Decimal(0))
    detail = (f"expected {expected_n} rows / in {exp['m_in']} / out {exp['m_out']}; "
              f"warehouse {int(got.n)} rows / in {got.m_in} / out {got.m_out}")
    if not ok:
        raise ValueError("reconciliation failed: " + detail)
    return int(got.n)


def load_tier_changes(log, changes):
    """C01-C03 for a change file: client_id, membership_tier, tier_effective_from, updated_at.
    Unchanged attributes (gender, birth date, district) come from the client's current version."""
    pdf = changes.toPandas()
    pdf["client_id"] = pdf["client_id"].astype(int)
    pdf["tier_effective_from"] = pd.to_datetime(pdf["tier_effective_from"]).dt.date
    cur = ch_df("SELECT client_id, gender, birth_date, customer_district_sk, "
                "membership_tier AS current_tier FROM dw.dim_customer FINAL WHERE is_current = 1")
    inc = pdf.merge(cur.assign(client_id=cur.client_id.astype(int)), on="client_id", how="left")
    same = inc.membership_tier == inc.current_tier          # already on this tier: nothing to do
    # a change that arrives after transactions dated on/after its effective date were loaded would
    # leave those facts on the old version (late-arriving change): refuse it (sheet C01-C03)
    late = ch_df("""SELECT c.client_id AS client_id, max(f.date_key) AS last_txn
                    FROM dw.fact_transaction AS f FINAL
                    JOIN (SELECT customer_sk, client_id FROM dw.dim_customer FINAL WHERE is_current = 1) AS c
                      USING (customer_sk)
                    WHERE c.client_id IN %(ids)s GROUP BY c.client_id""",
                 ids=tuple(int(x) for x in pdf.client_id) or (0,))
    last = dict(zip(late.client_id.astype(int), late.last_txn.astype(int)))
    eff_key = inc.tier_effective_from.map(lambda d: int(d.strftime("%Y%m%d")))
    too_late = inc[~same & (inc.client_id.map(lambda c: last.get(int(c), 0)) >= eff_key)]
    log.reject(("customer_profile_changes", str(r.client_id), "V4",
                "đã có giao dịch từ ngày hiệu lực trở đi được nạp với hạng cũ",
                {"client_id": r.client_id, "tier_effective_from": r.tier_effective_from})
               for r in too_late.itertuples())
    inc = inc[~inc.client_id.isin(too_late.client_id)]
    unknown = inc[inc.gender.isna()]
    log.reject(("customer_profile_changes", str(r.client_id), "V2", "khách hàng không tồn tại trong kho",
                {"client_id": r.client_id, "membership_tier": r.membership_tier}) for r in unknown.itertuples())
    opened, closed, unchanged, refused = apply_customer_versions(inc[inc.gender.notna()].drop(columns="current_tier"))
    log.reject(("customer_profile_changes", str(cid), "V4", why, {"client_id": cid}) for cid, why in refused)
    return len(pdf), opened, closed, unchanged


def run_transactions(spark, batch_id, df, description, period=(None, None), batch_type="HISTORICAL",
                     scenario_code=None, from_snapshot=False, tier_changes=None):
    log = RunLog(batch_id, batch_type, description, period[0], period[1],
                 lake_path=f"{BATCHES}/{batch_id}", scenario_code=scenario_code)
    with log.step("ingest") as s:
        t = (spark.read.parquet(f"{BATCHES}/{batch_id}/trans.parquet") if from_snapshot
             else snapshot(df, batch_id, "trans"))
        if tier_changes is not None:
            tier_changes = snapshot(tier_changes, batch_id, "customer_profile_changes")
        log.rows_received = s["rows_in"] = s["rows_out"] = t.count()
    if tier_changes is not None:
        with log.step("load_customer_scd2") as s:
            n, opened, closed, unchanged = load_tier_changes(log, tier_changes)
            s["rows_in"], s["rows_out"] = n, opened + closed
            print(f"  dim_customer: {opened} versions opened, {closed} closed, {unchanged} unchanged")
    with log.step("validate") as s:
        accepted, n_bad, n_already = validate(spark, log, t)
        s["rows_in"], s["rows_rejected"] = log.rows_received, n_bad
        s["rows_out"] = log.rows_received - n_bad
    with log.step("standardize") as s:
        accepted = standardize_codes(accepted).cache()
        s["rows_in"] = s["rows_out"] = accepted.count()
    with log.step("load_dimensions") as s:
        s["rows_in"], s["rows_out"] = load_profiles(spark, accepted)
    with log.step("load_fact") as s:
        n_new, n_v7 = load_fact(spark, log, accepted)
        s["rows_in"], s["rows_out"], s["rows_rejected"] = accepted.count() - n_already, n_new, n_v7
    with log.step("reconcile") as s:
        log.rows_loaded = reconcile_batch(log, accepted, n_v7)
        s["rows_in"] = s["rows_out"] = log.rows_loaded
    with log.step("detect_alerts") as s:   # L03: after the load, rules read the warehouse
        new, found = alerts.detect(batch_id, log.run_id)
        s["rows_in"], s["rows_out"] = found, sum(new.values())
        if found:
            print(f"  alerts: {found} found, {sum(new.values())} new {new}")
    status = log.finish()
    print(f"{batch_id}: {status} | received {log.rows_received} | loaded {n_new} new "
          f"({n_already} already loaded) | rejected {log.rows_rejected}")
    return status


SCENARIOS = {  # scenario code -> (batch_id, description); input files in data/berka/scenarios/<code>/
    "KB03": ("KB03_FRAUD", "KB03: giao dịch thỏa quy tắc cảnh báo gian lận"),
    "KB05": ("KB05_DATA_ERRORS", "KB05: giao dịch có lỗi chủ đích (V1-V6) lẫn giao dịch hợp lệ"),
    "KB07": ("KB07_AFTER_BACKUP", "KB07: lô nạp sau khi sao lưu (bị xoá khi khôi phục)"),
    "KB08": ("KB08_TIER_CHANGE", "KB08: đổi hạng thành viên + giao dịch kỳ sau"),
}


def run_scenario(spark, code):
    batch_id, description = SCENARIOS[code]
    folder = f"{SOURCE}/scenarios/{code}"
    trans = (spark.read.option("header", True).option("sep", ";").option("quote", '"')
             .csv(f"{folder}/trans.csv"))
    changes_path = f"{folder}/customer_profile_changes.csv"
    changes = (spark.read.option("header", True).csv(changes_path)
               if os.path.exists(changes_path) else None)
    return run_transactions(spark, batch_id, trans, description, batch_type="SCENARIO",
                            scenario_code=code, tier_changes=changes)


def run_history(spark, first, last):
    trans = read_source(spark, "trans").cache()
    months = pd.period_range(first, last, freq="M")
    for m in months:
        yymm = m.strftime("%y%m")
        run_transactions(spark, f"HIST_{m.strftime('%Y_%m')}",
                         trans.filter(F.substring("date", 1, 4) == yymm),
                         f"Giao dịch Berka tháng {m.strftime('%m/%Y')}",
                         (m.start_time.date(), m.end_time.date()))


def reconcile_all(spark):
    """Whole source (accepted by the rules) vs. warehouse."""
    src = read_source(spark, "trans").select(
        F.count("*").alias("n"),
        F.sum(F.when(F.trim("type") == "PRIJEM", F.col("amount").cast("decimal(18,2)"))).alias("m_in"),
        F.sum(F.when(F.trim("type") != "PRIJEM", F.col("amount").cast("decimal(18,2)"))).alias("m_out")).first()
    dw_ = ch_df("""SELECT count() AS n, uniqExact(transaction_id) AS ids,
                          sumIf(f.amount, p.cash_flow_direction = 'IN') AS m_in,
                          sumIf(f.amount, p.cash_flow_direction = 'OUT') AS m_out
                   FROM dw.fact_transaction AS f FINAL
                   JOIN (SELECT * FROM dw.dim_transaction_profile FINAL) AS p USING (transaction_profile_sk)
                   WHERE f.load_batch_id LIKE 'HIST_%'""").iloc[0]
    print(f"source    : {src['n']:,} transactions | in {src['m_in']} | out {src['m_out']}")
    print(f"warehouse : {int(dw_.n):,} rows ({int(dw_.ids):,} distinct ids) | in {dw_.m_in} | out {dw_.m_out}")
    same = (int(dw_.n) == src["n"] == int(dw_.ids) and Decimal(str(dw_.m_in)) == src["m_in"]
            and Decimal(str(dw_.m_out)) == src["m_out"])
    print("RECONCILED" if same else "DIFFERENT")
    return same


def main():
    cmd = sys.argv[1]
    spark = spark_session(f"pipeline_{cmd}")
    os.makedirs(BATCHES, exist_ok=True)
    if cmd == "ref":
        run_ref(spark)
    elif cmd == "history":
        run_history(spark, sys.argv[2], sys.argv[3])
    elif cmd == "rerun":
        run_transactions(spark, sys.argv[2], None, "Chạy lại từ bản chụp trong Data Lake",
                         from_snapshot=True)
    elif cmd == "scenario":
        run_scenario(spark, sys.argv[2])
    elif cmd == "alerts":  # back-fill: evaluate the rules on batches loaded before this step existed
        target = None if sys.argv[2] == "all" else sys.argv[2]
        new, found = alerts.detect(target)
        print(f"alerts on {sys.argv[2]}: {found} found, {sum(new.values())} new {new}")
    elif cmd == "reconcile":
        sys.exit(0 if reconcile_all(spark) else 1)
    spark.stop()


if __name__ == "__main__":
    main()
