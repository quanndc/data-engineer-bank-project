"""BR7 -- Risk management: near-real-time alert page.

Reads dwh.fact_transaction_alert (written by the Spark Structured Streaming job)
and the conformed dimensions, refreshing itself every two seconds.
"""
import os

import altair as alt
import clickhouse_connect
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Risk Alerts - Bank DWH", page_icon="🚨", layout="wide")


@st.cache_resource
def client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        username=os.environ.get("CLICKHOUSE_USER", "bank"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "bank"))


RULE_LABELS = {"HIGH_AMOUNT": "High amount (> 20,000 INR)",
               "HIGH_BALANCE_RATIO": "Amount > balance (>= 5,000 INR)"}

st.title("🚨 Unusual transaction alerts")
st.caption("BR7 · Risk management · Kafka → Spark Structured Streaming → ClickHouse · "
           "refreshes every 2 seconds")

with st.sidebar:
    st.header("Filters")
    rules = st.multiselect("Rule", list(RULE_LABELS), default=list(RULE_LABELS),
                           format_func=RULE_LABELS.get)
    window = st.select_slider("Time window", ["5 min", "15 min", "1 hour", "24 hours", "All"],
                              value="1 hour")
    min_amount = st.number_input("Minimum amount (INR)", min_value=0, value=0, step=1000)

WINDOW_SQL = {"5 min": "INTERVAL 5 MINUTE", "15 min": "INTERVAL 15 MINUTE",
              "1 hour": "INTERVAL 1 HOUR", "24 hours": "INTERVAL 24 HOUR"}


@st.fragment(run_every="2s")
def live_view():
    if not rules:
        st.info("Select at least one rule.")
        return
    where = ["a.rule IN %(rules)s", "a.transaction_amount >= %(min_amount)s"]
    if window != "All":
        where.append(f"a.detected_at >= now64(3) - {WINDOW_SQL[window]}")
    params = {"rules": tuple(rules), "min_amount": min_amount}
    cond = " AND ".join(where)

    kpi = client().query(f"""
        SELECT count(), sum(a.transaction_amount),
               quantile(0.95)(dateDiff('millisecond', a.produced_at, a.inserted_at)),
               max(a.detected_at)
        FROM dwh.fact_transaction_alert AS a WHERE {cond}""", parameters=params).result_rows[0]
    alerts, value, p95_ms, last = kpi

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Alerts", f"{alerts:,}")
    c2.metric("Flagged value (INR)", f"{(value or 0):,.0f}")
    c3.metric("End-to-end latency p95", f"{(p95_ms or 0) / 1000:.2f} s" if alerts else "–")
    c4.metric("Last alert", last.strftime("%H:%M:%S") if alerts else "–")

    if not alerts:
        st.info("No alerts in this window. Start the stream (`make stream`) and replay "
                "transactions (`make produce-live`).")
        return

    per_10s = client().query_df(f"""
        SELECT toStartOfInterval(a.detected_at, INTERVAL 10 SECOND) AS t, a.rule, count() AS alerts
        FROM dwh.fact_transaction_alert AS a WHERE {cond}
        GROUP BY t, a.rule ORDER BY t""", parameters=params)
    per_10s["rule"] = per_10s["rule"].map(RULE_LABELS)
    chart = (alt.Chart(per_10s).mark_bar()
             .encode(x=alt.X("t:T", title="Detected at"),
                     y=alt.Y("alerts:Q", title="Alerts per 10 s"),
                     color=alt.Color("rule:N", title="Rule",
                                     legend=alt.Legend(orient="bottom")))
             .properties(height=220))
    st.altair_chart(chart, use_container_width=True)

    latest = client().query_df(f"""
        SELECT a.detected_at AS detected_at,
               a.transaction_key AS transaction,
               c.customer_id AS customer,
               l.city AS city,
               a.transaction_amount AS amount_inr,
               a.account_balance AS balance_inr,
               a.rule AS rule,
               dateDiff('millisecond', a.produced_at, a.inserted_at) AS latency_ms
        FROM dwh.fact_transaction_alert AS a
        LEFT JOIN dwh.dim_customer AS c ON a.customer_key = c.customer_key
        LEFT JOIN dwh.dim_location AS l ON a.location_key = l.location_key
        WHERE {cond}
        ORDER BY a.detected_at DESC, a.transaction_key DESC
        LIMIT 50""", parameters=params)
    latest["rule"] = latest["rule"].map(RULE_LABELS)
    latest["amount_inr"] = pd.to_numeric(latest["amount_inr"])
    latest["balance_inr"] = pd.to_numeric(latest["balance_inr"])
    st.subheader("Latest 50 alerts")
    st.dataframe(latest, use_container_width=True, hide_index=True,
                 column_config={
                     "detected_at": st.column_config.DatetimeColumn("Detected at", format="HH:mm:ss.SSS"),
                     "amount_inr": st.column_config.NumberColumn("Amount (INR)", format="%.2f"),
                     "balance_inr": st.column_config.NumberColumn("Balance (INR)", format="%.2f"),
                     "latency_ms": st.column_config.NumberColumn("Latency (ms)", format="%d"),
                 })


live_view()
