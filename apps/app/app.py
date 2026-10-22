"""F. Application -- BankDW Phase 2 (Berka).

One app, three tabs, as agreed in the group sheet:
  Tổng quan  (U01 Quản lý kinh doanh)   BR01
  Phân tích  (U02 Chuyên viên phân tích) BR02, BR03, BR04
  Vận hành   (U03 Kỹ sư dữ liệu)         BR05, BR06, BR07, BR08
P2-1 skeleton: shows that D (ClickHouse) and E (PostgreSQL) are reachable. The BR views are
added in P2-6.
"""
import os

import clickhouse_connect
import psycopg2
import streamlit as st

st.set_page_config(page_title="BankDW", page_icon="🏦", layout="wide")


@st.cache_resource
def clickhouse():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "bank"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "bank"),
        secure=os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true")


def postgres():
    return psycopg2.connect(host=os.environ.get("POSTGRES_HOST", "postgres"),
                            dbname=os.environ.get("POSTGRES_DB", "bankops"),
                            user=os.environ.get("POSTGRES_USER", "bank"),
                            password=os.environ.get("POSTGRES_PASSWORD", "bank"))


st.title("🏦 BankDW — Dòng tiền và hoạt động tài khoản")
st.caption("Kho dữ liệu Kimball trên ClickHouse · kho vận hành PostgreSQL · dữ liệu Berka (1993–1998)")

with st.sidebar:
    st.subheader("Trạng thái hệ thống")
    try:
        st.success(f"Data Warehouse (ClickHouse {clickhouse().query('SELECT version()').result_rows[0][0]})")
    except Exception as e:  # noqa: BLE001 - shown to the user
        st.error(f"Data Warehouse: {e}")
    try:
        with postgres() as conn:
            conn.cursor().execute("SELECT 1")
        st.success("Kho vận hành (PostgreSQL)")
    except Exception as e:  # noqa: BLE001
        st.error(f"Kho vận hành: {e}")

overview, analysis, operations = st.tabs(["Tổng quan", "Phân tích", "Vận hành"])
with overview:
    st.info("U01 Quản lý kinh doanh · BR01 Xem tổng quan dòng tiền — sẽ có ở P2-6.")
with analysis:
    st.info("U02 Chuyên viên phân tích · BR02 Tra cứu tài khoản · BR03 Cảnh báo gian lận · "
            "BR04 Theo dõi xử lý cảnh báo — sẽ có ở P2-6.")
with operations:
    st.info("U03 Kỹ sư dữ liệu · BR05 Kiểm soát dữ liệu · BR06 Giám sát pipeline · "
            "BR07 Sao lưu, khôi phục · BR08 Báo cáo lịch sử — sẽ có ở P2-6.")
