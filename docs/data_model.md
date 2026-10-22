# Mô hình dữ liệu Phase 2 — vật lý

Cột, grain và quy tắc lấy từ sheet *Data Dictionary* (chốt 18/10/2026). File này ghi phần sheet còn để mở: **kiểu vật lý** và cách hiện thực trên ClickHouse / PostgreSQL.
DDL: [warehouse/clickhouse/ddl/dw.sql](../warehouse/clickhouse/ddl/dw.sql), [warehouse/postgres/ddl/ops.sql](../warehouse/postgres/ddl/ops.sql). Tạo và kiểm tra: `make schema`.

## 1. ERD — Data Warehouse (ClickHouse, database `dw`)

```mermaid
erDiagram
    dim_date ||--o{ fact_transaction : "R01 date_key"
    dim_account ||--o{ fact_transaction : "R02 account_sk"
    dim_customer ||--o{ fact_transaction : "R03 customer_sk (OWNER, valid on txn date)"
    dim_transaction_profile ||--o{ fact_transaction : "R04 transaction_profile_sk"
    dim_district ||--o{ dim_account : "R05 account_district_sk (branch)"
    dim_district ||--o{ dim_customer : "R06 customer_district_sk (residence)"
    customer_profile_simulated }o..o{ dim_customer : "ETL01 client_id (not a FK)"

    fact_transaction {
        UInt64 transaction_id
        UInt32 date_key FK
        UInt32 account_sk FK
        UInt32 customer_sk FK
        UInt16 transaction_profile_sk FK
        Decimal amount
        Decimal balance_after
        String counterparty_bank_code
        String counterparty_account_ref
        String load_batch_id
        DateTime64 loaded_at
    }
    dim_customer {
        UInt32 customer_sk PK
        UInt32 client_id
        String gender
        Date32 birth_date
        UInt16 customer_district_sk FK
        String membership_tier
        Date32 valid_from
        Date32 valid_to
        UInt8 is_current
    }
    dim_account {
        UInt32 account_sk PK
        UInt32 account_id
        Date account_open_date
        String statement_frequency_code
        String statement_frequency_name
        UInt16 account_district_sk FK
    }
    dim_district {
        UInt16 district_sk PK
        UInt16 district_id
        String district_name
        String region_name
    }
    dim_transaction_profile {
        UInt16 transaction_profile_sk PK
        String type_code
        String operation_code
        String purpose_code
        String cash_flow_direction
    }
    dim_date {
        UInt32 date_key PK
        Date full_date
        String year_month
        UInt8 is_weekend
    }
    customer_profile_simulated {
        UInt32 client_id
        String membership_tier
        Date tier_effective_from
        DateTime updated_at
    }
```

`dim_district` dùng **2 vai trò** (khu vực chi nhánh qua `dim_account`, khu vực cư trú qua `dim_customer`), không nối thẳng fact; hai khu vực có thể khác nhau (409 tài khoản).

## 2. Quyết định vật lý (ClickHouse)

| Vấn đề | Cách làm | Lý do |
|---|---|---|
| PK/FK | Chỉ là quan hệ logic; pipeline kiểm tra khóa trước khi công bố | ClickHouse không ép ràng buộc (sheet, ghi chú ERD) |
| Sửa Type 1, đóng phiên bản SCD2 | `ReplacingMergeTree(updated_at)`: ghi **dòng mới** cùng khóa, `updated_at` mới hơn; đọc bằng `FINAL` | ClickHouse không có UPDATE tại chỗ; dimension nhỏ nên `FINAL` rẻ |
| Mốc "hiện tại" của SCD2 | `valid_to = 2299-12-31` (`Date32`) | Sheet ghi mốc logic 9999-12-31, nhưng giới hạn của `Date32` là 2299-12-31 (Q-J) |
| Ngày sinh trước 1970 | `birth_date Date32` | `Date` chỉ từ 1970 |
| Chạy lại cùng lô không nhân đôi | Pipeline bỏ qua `transaction_id` đã có; thêm `ReplacingMergeTree(loaded_at)` làm lưới an toàn | Sheet L05, N05 |
| Sắp xếp và phân vùng fact | `ORDER BY (account_sk, date_key, transaction_id)`, `PARTITION BY` năm | BR02 lọc theo tài khoản + kỳ; BR01 lọc theo kỳ |
| Số tiền | `Decimal(18, 2)` | Không sai số khi đối chiếu tiền vào/ra với nguồn |

## 3. Kho vận hành (PostgreSQL, schema `ops`)

| Bảng | BR | Nội dung |
|---|---|---|
| `batch` | BR06 | Lô đầu vào (lịch sử theo tháng hoặc kịch bản KB), đường dẫn bản chụp trong Data Lake |
| `pipeline_run`, `pipeline_step` | BR06 | Mỗi lần chạy / chạy lại và từng bước: trạng thái, thời gian, số nhận / đạt / lỗi, bước thất bại, nguyên nhân |
| `dq_rule`, `rejected_record` | BR05 | Quy tắc V1–V6; bản ghi bị tách kèm mã quy tắc, lý do và nội dung gốc (`jsonb`) |
| `alert_rule`, `alert` | BR03 | Quy tắc R1, R2; cảnh báo kèm lý do, căn cứ (`evidence`), giao dịch liên quan; **duy nhất theo (tài khoản, quy tắc, kỳ)** để chống trùng |
| `alert_action` | BR04 | Lịch sử chuyển trạng thái `NEW` (chưa xem) → `IN_REVIEW` (đang xem xét) → `RESOLVED` (đã xử lý) và ghi chú |
| `backup_catalog`, `backup_operation` | BR07 | Danh mục bản sao lưu trên ổ `backups` của ClickHouse, số liệu đối chiếu, kết quả từng lần sao lưu / khôi phục |

PostgreSQL có PK, FK, CHECK thật. Theo sheet, PostgreSQL **nằm ngoài phạm vi sao lưu BR07** và không chứa dữ liệu kho.
