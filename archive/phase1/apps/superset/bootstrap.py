"""Create the Superset content for the banking DWH: one dashboard per user group.

Idempotent: objects created by a previous run (same names / slugs) are deleted first.
Runs inside the superset container:  make app-bootstrap

  database  : "Bank DWH (ClickHouse)"
  datasets  : one virtual dataset per BR query (warehouse/queries/clickhouse) + helpers
  dashboards: Executive, Regional, Marketing, Product, Wealth, Risk, Data quality
  roles     : one role per user group, attached to its dashboard (DASHBOARD_RBAC)
"""
import json
import os
import re
import secrets
import time
from urllib.parse import quote

import requests

BASE = "http://localhost:8088"
QUERIES = "/opt/queries/clickhouse"
DB_NAME = "Bank DWH (ClickHouse)"
# Same ClickHouse settings as the Spark jobs (.env); local container by default
DB_URI = "clickhousedb://{user}:{pw}@{host}:{port}/dwh{tls}".format(
    user=quote(os.environ.get("CLICKHOUSE_USER", "bank"), safe=""),
    pw=quote(os.environ.get("CLICKHOUSE_PASSWORD", "bank"), safe=""),
    host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
    port=os.environ.get("CLICKHOUSE_PORT", "8123"),
    tls="?secure=true" if os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true" else "")


# ---------------------------------------------------------------- API helpers
class Api:
    def __init__(self):
        self.s = requests.Session()
        r = self.s.post(f"{BASE}/api/v1/security/login", json={
            "username": os.environ["SUPERSET_ADMIN_USER"],
            "password": os.environ["SUPERSET_ADMIN_PASSWORD"],
            "provider": "db", "refresh": True})
        r.raise_for_status()
        self.s.headers["Authorization"] = "Bearer " + r.json()["access_token"]
        csrf = self.s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"]
        self.s.headers.update({"X-CSRFToken": csrf, "Referer": BASE})

    def call(self, method, path, **kw):
        for attempt in range(6):            # Superset rate-limits the API (50 requests/s)
            r = self.s.request(method, f"{BASE}/api/v1/{path}", **kw)
            if r.status_code != 429:
                break
            time.sleep(0.5 * (attempt + 1))
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
        return r.json() if r.text else {}

    def all(self, resource):
        return self.call("GET", f"{resource}/", params={"q": "(page_size:1000)"})["result"]


def metric(column, agg="SUM", label=None):
    return {"expressionType": "SIMPLE", "column": {"column_name": column},
            "aggregate": agg, "label": label or f"{agg}({column})"}


def sql_metric(expr, label):
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}


def sql_filter(expr):
    return {"expressionType": "SQL", "sqlExpression": expr, "clause": "WHERE"}


def query_sql(name):
    """BR query text without comments, usable as a virtual dataset / sub-query."""
    sql = open(f"{QUERIES}/{name}.sql").read()
    return re.sub(r"--[^\n]*", "", sql).strip()


# ---------------------------------------------------------------- datasets
DATASETS = {name: query_sql(name) for name in [
    "br01_daily_kpis", "br02_weekday_weekend", "br03_city_ranking", "br04_rfm_segments",
    "br05_age_gender", "br06_type_mix", "br07_alerts", "br08_high_balance",
    "br09_data_quality", "br10_amount_bands"]}
# helper datasets (dashboard-specific shapes of the same BR data)
DATASETS["br02_weekday_labels"] = (
    "SELECT concat(toString(day_of_week), ' ', substring(day_name, 1, 3)) AS day_label, * "
    f"FROM ({DATASETS['br02_weekday_weekend']})")
DATASETS["br06_top_cities"] = (
    f"SELECT * FROM ({DATASETS['br06_type_mix']}) WHERE city IN ("
    f"SELECT city FROM ({DATASETS['br03_city_ranking']}) WHERE city != 'Other' "
    "ORDER BY value_rank LIMIT 10)")
DATASETS["br04_campaign_targets"] = (
    "SELECT r.customer_key, c.customer_id, r.segment, r.rfm_code, r.recency, r.frequency, "
    "r.monetary, c.age_group, c.gender "
    "FROM mart.rfm AS r INNER JOIN dwh.dim_customer AS c ON r.customer_key = c.customer_key "
    "WHERE r.segment != 'Hibernating'")
TEMPORAL = {"br01_daily_kpis": "full_date", "br07_alerts": "full_date"}


# ---------------------------------------------------------------- charts per dashboard
def big(name, ds, m):
    return dict(name=name, ds=ds, viz="big_number_total", w=4, h=30,
                params={"metric": m, "y_axis_format": "SMART_NUMBER", "subheader": ""})


def bar(name, ds, x, metrics, w=6, h=55, groupby=(), horizontal=False, sort=None, asc=True,
        stack=None, filters=()):
    p = {"x_axis": x, "metrics": metrics, "groupby": list(groupby),
         "orientation": "horizontal" if horizontal else "vertical",
         "x_axis_sort": sort or x, "x_axis_sort_asc": asc, "row_limit": 10000,
         "show_legend": bool(groupby), "y_axis_format": "SMART_NUMBER",
         "adhoc_filters": list(filters), "truncateXAxis": True, "rich_tooltip": True}
    if stack:
        p["stack"] = stack
    return dict(name=name, ds=ds, viz="echarts_timeseries_bar", w=w, h=h, params=p)


def line(name, ds, x, metrics, w=12, h=50):
    return dict(name=name, ds=ds, viz="echarts_timeseries_line", w=w, h=h, params={
        "x_axis": x, "time_grain_sqla": "P1D", "metrics": metrics, "groupby": [],
        "row_limit": 10000, "show_legend": True, "rich_tooltip": True,
        "y_axis_format": "SMART_NUMBER", "x_axis_time_format": "smart_date",
        "markerEnabled": True, "adhoc_filters": []})


def pie(name, ds, groupby, m, w=4, h=55):
    return dict(name=name, ds=ds, viz="pie", w=w, h=h, params={
        "groupby": [groupby], "metric": m, "row_limit": 20, "donut": True,
        "show_labels": True, "label_type": "key_percent", "labels_outside": True,
        "show_legend": True, "number_format": "SMART_NUMBER", "adhoc_filters": []})


def table(name, ds, columns, w=12, h=60, order=None, filters=()):
    return dict(name=name, ds=ds, viz="table", w=w, h=h, params={
        "query_mode": "raw", "all_columns": columns,
        "order_by_cols": [json.dumps([order[0], order[1]])] if order else [],
        "row_limit": 10000, "server_pagination": False, "include_search": True,
        "adhoc_filters": list(filters), "table_timestamp_format": "smart_date"})


def pivot(name, ds, rows, cols, m, w=6, h=60):
    return dict(name=name, ds=ds, viz="pivot_table_v2", w=w, h=h, params={
        "groupbyRows": rows, "groupbyColumns": cols, "metrics": [m],
        "aggregateFunction": "Sum", "rowOrder": "key_a_to_z", "colOrder": "key_a_to_z",
        "valueFormat": "SMART_NUMBER", "metricsLayout": "COLUMNS", "row_limit": 10000,
        "adhoc_filters": []})


def heatmap(name, ds, x, y, m, w=6, h=55):
    return dict(name=name, ds=ds, viz="heatmap_v2", w=w, h=h, params={
        "x_axis": x, "groupby": y, "metric": m, "row_limit": 1000,
        "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc",
        "linear_color_scheme": "blue_white_yellow", "normalize_across": "heatmap",
        "show_values": True, "show_legend": True, "legend_type": "continuous",
        "y_axis_format": "SMART_NUMBER", "value_bounds": [None, None],
        "left_margin": "auto", "bottom_margin": "auto", "adhoc_filters": []})


TXN, VAL = metric("transactions", label="Transactions"), metric("transaction_value", label="Transaction value (INR)")

DASHBOARDS = [
    dict(slug="executive", title="Executive Overview", role="Executive Board",
         intro="**Executive board** · BR1 daily performance · BR2 weekday vs. weekend",
         rows=[
             [big("[BR1] Transactions", "br01_daily_kpis", TXN),
              big("[BR1] Transaction value (INR)", "br01_daily_kpis", VAL),
              big("[BR1] Avg daily active customers", "br01_daily_kpis",
                  metric("active_customers", "AVG", "Avg daily active customers"))],
             [line("[BR1] Daily transactions", "br01_daily_kpis", "full_date", [TXN])],
             [bar("[BR2] Avg transactions per day, by weekday", "br02_weekday_labels", "day_label",
                  [metric("avg_transactions_per_day", "MAX", "Avg transactions per day")]),
              bar("[BR2] Avg value per day (INR), by weekday", "br02_weekday_labels", "day_label",
                  [metric("avg_value_per_day", "MAX", "Avg value per day (INR)")])],
         ]),
    dict(slug="regional", title="Regional Performance", role="Regional Management",
         intro="**Regional management** · BR3 city ranking · BR10 amount bands · use the *City* filter",
         filters=[("City", "br03_city_ranking", "city")],
         rows=[
             [bar("[BR3] Top 15 cities by transaction value", "br03_city_ranking", "city", [VAL],
                  horizontal=True, sort="Transaction value (INR)", asc=True,
                  filters=[sql_filter("city != 'Other' AND value_rank <= 16")]),
              table("[BR3] City ranking", "br03_city_ranking",
                    ["value_rank", "city", "state", "transactions", "transaction_value",
                     "customers", "avg_transaction_amount"], w=6, h=55,
                    order=("value_rank", True))],
             [bar("[BR10] Transactions by amount band", "br10_amount_bands", "amount_band", [TXN]),
              pivot("[BR10] Transactions by city and amount band", "br10_amount_bands",
                    ["city"], ["amount_band"], TXN)],
         ]),
    dict(slug="marketing", title="Marketing & CRM", role="Marketing",
         intro="**Marketing / CRM** · BR4 RFM segments and campaign targets · BR5 age × gender",
         filters=[("Segment", "br04_campaign_targets", "segment")],
         rows=[
             [pie("[BR4] Customers by RFM segment", "br04_rfm_segments", "segment",
                  metric("customers", label="Customers")),
              table("[BR4] RFM segment profile", "br04_rfm_segments",
                    ["segment", "customers", "pct_customers", "avg_recency_days",
                     "avg_frequency", "avg_monetary", "total_monetary"], w=8, h=55)],
             [heatmap("[BR5] Avg transaction amount by age group and gender", "br05_age_gender",
                      "age_group", "gender",
                      metric("avg_transaction_amount", "MAX", "Avg transaction amount (INR)")),
              bar("[BR5] Transaction value by age group and gender", "br05_age_gender",
                  "age_group", [VAL], groupby=["gender"])],
             [table("[BR4] Campaign target list (top 10,000 non-hibernating customers by value)", "br04_campaign_targets",
                    ["customer_id", "segment", "rfm_code", "recency", "frequency", "monetary",
                     "age_group", "gender"], order=("monetary", False))],
         ]),
    dict(slug="product", title="Product Usage", role="Product Development",
         intro="**Product development** · BR6 transaction type mix (labels are rule-inferred)",
         rows=[
             [pie("[BR6] Transaction type share", "br06_type_mix", "type_name", TXN),
              bar("[BR6] Type mix in the top 10 cities (100%)", "br06_top_cities", "city", [TXN],
                  groupby=["type_name"], horizontal=True, stack="Expand", w=8)],
             [pivot("[BR6] Transactions by city and type", "br06_type_mix",
                    ["city"], ["type_name"], TXN, w=12)],
         ]),
    dict(slug="wealth", title="Wealth Management", role="Wealth Management",
         intro="**Wealth management** · BR8 customers in the top 1% of balances at the latest "
               "month end (balance is semi-additive: read from the month-end snapshot)",
         rows=[
             [big("[BR8] High-balance customers", "br08_high_balance",
                  sql_metric("count()", "High-balance customers")),
              big("[BR8] Top-1% balance threshold (INR)", "br08_high_balance",
                  metric("top1pct_threshold", "MAX", "Threshold (INR)")),
              big("[BR8] Balance held by these customers (INR)", "br08_high_balance",
                  metric("balance", label="Total balance (INR)"))],
             [bar("[BR8] High-balance customers by age group", "br08_high_balance", "age_group",
                  [sql_metric("count()", "Customers")], groupby=["gender"], w=5),
              table("[BR8] High-balance customer list", "br08_high_balance",
                    ["customer_id", "age_group", "gender", "balance", "days_since_last_txn"],
                    w=7, h=55, order=("balance", False))],
         ]),
    dict(slug="risk", title="Risk Alerts (summary)", role="Risk Management",
         intro="**Risk management** · BR7 · the live alert feed refreshing every 2 s is the "
               "Streamlit page at http://localhost:8501; this dashboard summarizes alerts per day",
         rows=[
             [big("[BR7] Alerts", "br07_alerts", metric("alerts", label="Alerts")),
              big("[BR7] Flagged value (INR)", "br07_alerts", metric("flagged_value", label="Flagged value (INR)")),
              big("[BR7] Avg end-to-end latency (ms)", "br07_alerts",
                  metric("avg_latency_ms", "AVG", "Avg latency (ms)"))],
             [table("[BR7] Alerts per day and rule", "br07_alerts",
                    ["full_date", "rule", "alerts", "flagged_value", "avg_latency_ms"])],
         ]),
    dict(slug="data-quality", title="Data Quality", role="Data Team",
         intro="**Data team** · BR9 results of the latest data correctness run (ISO/IEC 25012)",
         rows=[
             [big("[BR9] Checks passed", "br09_data_quality",
                  sql_metric("countIf(status = 'PASS')", "Checks passed")),
              big("[BR9] Checks failed", "br09_data_quality",
                  sql_metric("countIf(status = 'FAIL')", "Checks failed")),
              big("[BR9] Data quality flags raised", "br09_data_quality",
                  sql_metric("sumIf(toUInt64OrZero(observed), status = 'INFO')", "Flags raised"))],
             [table("[BR9] Correctness checks", "br09_data_quality",
                    ["check_id", "characteristic", "metric", "expected", "observed", "status"],
                    w=7, h=60, filters=[sql_filter("status != 'INFO'")]),
              table("[BR9] Records corrected or flagged, by rule", "br09_data_quality",
                    ["check_id", "metric", "observed"], w=5, h=60,
                    filters=[sql_filter("status = 'INFO'")])],
         ]),
]


# ---------------------------------------------------------------- build
def cleanup(api):
    slugs = {d["slug"] for d in DASHBOARDS}
    for d in api.all("dashboard"):
        if d.get("slug") in slugs:
            api.call("DELETE", f"dashboard/{d['id']}")
    for c in api.all("chart"):
        if c["slice_name"].startswith("[BR"):
            api.call("DELETE", f"chart/{c['id']}")
    for ds in api.all("dataset"):
        if ds["table_name"] in DATASETS:
            api.call("DELETE", f"dataset/{ds['id']}")


def ensure_database(api):
    for db in api.all("database"):
        if db["database_name"] == DB_NAME:
            # point the existing connection at the ClickHouse currently configured in .env
            api.call("PUT", f"database/{db['id']}", json={"sqlalchemy_uri": DB_URI})
            return db["id"]
    return api.call("POST", "database/", json={
        "database_name": DB_NAME, "sqlalchemy_uri": DB_URI,
        "expose_in_sqllab": True, "allow_run_async": False})["id"]


COLUMN_FIELDS = {"id", "column_name", "verbose_name", "description", "expression", "filterable",
                 "groupby", "is_active", "is_dttm", "python_date_format", "type",
                 "advanced_data_type", "extra"}


def create_datasets(api, db_id):
    ids = {}
    for name, sql in DATASETS.items():
        ds_id = api.call("POST", "dataset/", json={
            "database": db_id, "schema": "dwh", "table_name": name, "sql": sql})["id"]
        api.call("PUT", f"dataset/{ds_id}/refresh")
        if name in TEMPORAL:
            cols = api.call("GET", f"dataset/{ds_id}")["result"]["columns"]
            cols = [{k: v for k, v in c.items() if k in COLUMN_FIELDS} for c in cols]
            for c in cols:
                c["is_dttm"] = c["column_name"] == TEMPORAL[name]
            api.call("PUT", f"dataset/{ds_id}", json={"main_dttm_col": TEMPORAL[name], "columns": cols})
        ids[name] = ds_id
    return ids


def native_filter(name, ds_id, column):
    return {"id": f"NATIVE_FILTER-{secrets.token_hex(5)}", "name": name,
            "filterType": "filter_select", "type": "NATIVE_FILTER", "description": "",
            "targets": [{"datasetId": ds_id, "column": {"name": column}}],
            "controlValues": {"multiSelect": True, "enableEmptyFilter": False,
                              "defaultToFirstItem": False, "searchAllOptions": True},
            "defaultDataMask": {"filterState": {}, "extraFormData": {}},
            "cascadeParentIds": [], "scope": {"rootPath": ["ROOT_ID"], "excluded": []}}


def build_dashboard(api, spec, ds_ids, role_id):
    dash_id = api.call("POST", "dashboard/", json={
        "dashboard_title": spec["title"], "slug": spec["slug"], "published": True})["id"]
    pos = {"DASHBOARD_VERSION_KEY": "v2",
           "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
           "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": spec["title"]}},
           "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]}}
    # intro row (markdown) telling which user group and BRs the dashboard serves
    pos["ROW-intro"] = {"type": "ROW", "id": "ROW-intro", "children": ["MARKDOWN-intro"],
                        "parents": ["ROOT_ID", "GRID_ID"],
                        "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    pos["MARKDOWN-intro"] = {"type": "MARKDOWN", "id": "MARKDOWN-intro", "children": [],
                             "parents": ["ROOT_ID", "GRID_ID", "ROW-intro"],
                             "meta": {"width": 12, "height": 12, "code": spec["intro"]}}
    pos["GRID_ID"]["children"].append("ROW-intro")

    n = 0
    for r, row in enumerate(spec["rows"]):
        row_id = f"ROW-{r}"
        pos[row_id] = {"type": "ROW", "id": row_id, "children": [],
                       "parents": ["ROOT_ID", "GRID_ID"],
                       "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        pos["GRID_ID"]["children"].append(row_id)
        for chart in row:
            ds_id = ds_ids[chart["ds"]]
            params = dict(chart["params"], viz_type=chart["viz"], datasource=f"{ds_id}__table")
            chart_id = api.call("POST", "chart/", json={
                "slice_name": chart["name"], "viz_type": chart["viz"],
                "datasource_id": ds_id, "datasource_type": "table",
                "params": json.dumps(params), "dashboards": [dash_id]})["id"]
            n += 1
            cid = f"CHART-{n}"
            pos[cid] = {"type": "CHART", "id": cid, "children": [],
                        "parents": ["ROOT_ID", "GRID_ID", row_id],
                        "meta": {"width": chart["w"], "height": chart["h"],
                                 "chartId": chart_id, "sliceName": chart["name"]}}
            pos[row_id]["children"].append(cid)

    meta = {"native_filter_configuration": [native_filter(name, ds_ids[ds], col)
                                            for name, ds, col in spec.get("filters", [])],
            "color_scheme": "supersetColors", "refresh_frequency": 0,
            "timed_refresh_immune_slices": [], "expanded_slices": {}, "label_colors": {},
            "cross_filters_enabled": False}
    api.call("PUT", f"dashboard/{dash_id}", json={
        "position_json": json.dumps(pos), "json_metadata": json.dumps(meta),
        "roles": [role_id] if role_id else []})
    return dash_id, n


def ensure_role(api, name):
    try:
        for role in api.call("GET", "security/roles/", params={"q": "(page_size:100)"})["result"]:
            if role["name"] == name:
                return role["id"]
        return api.call("POST", "security/roles/", json={"name": name})["id"]
    except RuntimeError as e:               # roles API disabled -> dashboards stay unrestricted
        print(f"  (role '{name}' not created: {e})")
        return None


def main():
    api = Api()
    cleanup(api)
    db_id = ensure_database(api)
    ds_ids = create_datasets(api, db_id)
    print(f"database {db_id}; {len(ds_ids)} datasets")
    for spec in DASHBOARDS:
        role_id = ensure_role(api, spec["role"])
        dash_id, n = build_dashboard(api, spec, ds_ids, role_id)
        print(f"dashboard '{spec['title']}' (/superset/dashboard/{spec['slug']}/): "
              f"{n} charts, role '{spec['role']}'")


if __name__ == "__main__":
    main()
