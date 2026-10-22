# Benchmark P6 — mức dữ liệu ×1

Ngày chạy: 2026-10-06. Dữ liệu gốc: 1.418.764 giao dịch, 1.046.782 khách hàng, 2.820.640 dòng snapshot số dư.
Kết quả thô: `lake/bench/results/` (`query_runs.csv`, `query_summary.csv`, `result_check.csv`, `load_*.csv`).

---

## 1. Ba giải pháp được so sánh

| Mã trong file kết quả | Giải pháp | Mô hình dữ liệu | Công nghệ lưu trữ / truy vấn | Vai trò |
|---|---|---|---|---|
| `ch_star` | Star schema trên ClickHouse | Kimball star schema (4 dimension + 2 fact) | ClickHouse 25.3 (lưu theo **cột**) | **Đề xuất** |
| `ch_obt` | One Big Table trên ClickHouse | One Big Table: mọi thuộc tính dimension chép vào từng dòng fact, **không cần join** | ClickHouse 25.3 | Đối chứng A1 |
| `pg_star` | Star schema trên PostgreSQL | Cùng star schema, có PK, FK, index | PostgreSQL 16.4 (lưu theo **dòng**) | Đối chứng A2 |

Hai phép so sánh, mỗi phép **chỉ đổi 1 yếu tố**:
- **B1 = Star schema trên ClickHouse vs One Big Table trên ClickHouse**: cùng công nghệ (ClickHouse), khác mô hình → *Kimball có đáng không?*
- **B2 = Star schema trên ClickHouse vs Star schema trên PostgreSQL**: cùng mô hình (star schema), khác công nghệ → *lưu theo cột có lợi cho phân tích không?*

---

## 2. Bộ truy vấn (workload) — 8 BR

BR7 (streaming) và BR9 (đọc bảng audit nhỏ) không đưa vào benchmark truy vấn.

| BR | Câu hỏi nghiệp vụ | Truy vấn làm gì | Star schema (ClickHouse và PostgreSQL): đọc bảng nào / join | One Big Table (ClickHouse): đọc bảng nào | Phép tính nặng | Số dòng kết quả |
|---|---|---|---|---|---|---:|
| BR1 | Giao dịch, giá trị, số khách hoạt động mỗi ngày | Gom theo ngày | `fact_transactions` ⋈ `dim_date` (1 join) | `obt_transactions` | `count`, `sum`, **đếm khách khác nhau** (distinct) | 55 |
| BR2 | Hành vi theo thứ trong tuần | Gom theo thứ, chia trung bình theo số ngày | fact ⋈ `dim_date` (1 join) | `obt_transactions` | distinct số ngày, `avg` | 7 |
| BR3 | Xếp hạng thành phố | Gom theo thành phố + **xếp hạng** | fact ⋈ `dim_location` (1 join) | `obt_transactions` | distinct khách, **window `rank()`** | 55 |
| BR4 | Phân khúc RFM | **Tính lại RFM từ fact** (không đọc mart): gom theo từng khách → chấm điểm → gom theo phân khúc | fact ⋈ `dim_date` (1 join) | `obt_transactions` | gom 1 triệu nhóm (mỗi khách 1 nhóm), 3 tầng truy vấn con | 5 |
| BR5 | Chi tiêu theo tuổi × giới tính | Gom theo nhóm tuổi, giới tính | fact ⋈ **`dim_customer` (1 triệu dòng)** | `obt_transactions` | join với dimension **lớn nhất**, distinct khách | 19 |
| BR6 | Cơ cấu loại giao dịch theo thành phố | Gom theo thành phố × loại, tính % trong thành phố | fact ⋈ `dim_location` ⋈ `dim_transaction_type` (2 join) | `obt_transactions` | **window `sum() over (partition by city)`** | 216 |
| BR8 | Khách top 1% số dư | Lấy snapshot cuối cùng, tính ngưỡng p99, lọc khách ≥ ngưỡng | `fact_customer_balance_snapshot` ⋈ `dim_customer` (1 join) | `obt_balance_snapshot` | **tính phân vị chính xác**, trả về **10.482 dòng** | 10.482 |
| BR10 | Phân bố theo dải tiền × thành phố | Gán dải tiền, gom theo thành phố × dải | fact ⋈ `dim_location` (1 join) | `obt_transactions` | `CASE`/`multiIf` | 328 |

---

## 3. Cách đo

| Hạng mục | Cách làm |
|---|---|
| Số lần chạy | Mỗi truy vấn **6 lần** liên tiếp |
| Cold | Lần chạy **thứ 1**. ClickHouse: xoá mark cache và uncompressed cache trước khi chạy. Không xoá được page cache của hệ điều hành từ trong container → "cold" = *lần chạy đầu*, không phải đọc hoàn toàn từ đĩa |
| Warm | Lần **2–6**: báo **median** và **p95** |
| Đo cái gì | Thời gian phía client (wall-clock), **gồm cả truyền kết quả** về client |
| Client | Python trong container Spark (`clickhouse-connect` / `psycopg2`), cùng mạng Docker |
| Tài nguyên | ClickHouse ≤ 2,5 GB RAM; PostgreSQL ≤ 1 GB RAM (`shared_buffers` 256 MB, `work_mem` 64 MB, tối đa 4 worker song song, `jit=off`); Superset/Streamlit/stream đã tắt |
| Kiểm tra đúng | Kết quả `ch_obt` và `pg_star` được so **từng dòng, từng ô** với `ch_star` (số: sai lệch ≤ 0,011) |

---

## 4. Kết quả thời gian truy vấn (ms)

| BR | Star schema / ClickHouse — cold | Star schema / ClickHouse — warm median | Star schema / ClickHouse — p95 | One Big Table / ClickHouse — cold | One Big Table / ClickHouse — warm median | One Big Table / ClickHouse — p95 | Star schema / PostgreSQL — cold | Star schema / PostgreSQL — warm median | Star schema / PostgreSQL — p95 | **One Big Table nhanh hơn star schema (cùng ClickHouse)** | **PostgreSQL chậm hơn ClickHouse (cùng star schema)** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| BR1 | 98,7 | 75,0 | 144,3 | 56,8 | 51,0 | 82,7 | 2.882,6 | 2.146,8 | 2.515,0 | 1,47× | 28,6× |
| BR2 | 37,4 | 23,3 | 31,7 | 17,5 | 16,6 | 16,7 | 820,6 | 844,1 | 1.085,5 | 1,40× | 36,2× |
| BR3 | 141,3 | 63,7 | 69,8 | 54,5 | 47,5 | 50,3 | 2.155,8 | 2.109,2 | 2.713,2 | 1,34× | 33,1× |
| BR4 | 145,8 | 66,2 | 85,5 | 64,7 | 41,0 | 50,7 | 1.321,6 | 1.297,0 | 1.482,8 | 1,61× | 19,6× |
| BR5 | 221,2 | 87,1 | 120,2 | 61,4 | 47,4 | 57,0 | 3.120,4 | 2.473,5 | 2.510,9 | 1,84× | 28,4× |
| BR6 | 58,3 | 39,1 | 40,1 | 23,5 | 17,8 | 20,0 | 106,4 | 92,6 | 96,9 | 2,20× | 2,4× |
| BR8 | 128,2 | 62,1 | 108,9 | 51,7 | 37,3 | 57,1 | 776,6 | 508,5 | 531,3 | 1,66× | 8,2× |
| BR10 | 33,9 | 26,5 | 29,6 | 19,4 | 21,0 | 23,5 | 377,3 | 370,2 | 414,8 | 1,26× | 14,0× |

Tỉ lệ tính trên **warm median**.

---

## 5. Dung lượng và thời gian nạp

| Giải pháp | Bảng | Dòng | Trên đĩa |
|---|---|---:|---:|
| Star schema / ClickHouse | `dim_customer` | 1.046.783 | 84,2 MiB |
| | `fact_customer_balance_snapshot` | 2.820.640 | 35,8 MiB |
| | `fact_transactions` | 1.418.764 | 22,9 MiB |
| | `dim_location`, `dim_date`, `dim_transaction_type` | 10.456 | 0,2 MiB |
| | **Tổng** | | **143,1 MiB** |
| One Big Table / ClickHouse | `obt_balance_snapshot` | 2.820.640 | 255,6 MiB |
| | `obt_transactions` | 1.418.764 | 138,3 MiB |
| | **Tổng** | | **393,9 MiB (2,75× star schema / ClickHouse)** |
| Star schema / PostgreSQL | `fact_customer_balance_snapshot` | 2.820.640 | 223,0 MiB |
| | `dim_customer` | 1.046.783 | 180,1 MiB |
| | `fact_transactions` | 1.418.764 | 170,3 MiB |
| | 3 dimension nhỏ | 10.456 | 1,5 MiB |
| | **Tổng (gồm index)** | | **574,9 MiB (4,0× star schema / ClickHouse)** |

| Giải pháp | Nạp dữ liệu | Ghi chú |
|---|---:|---|
| Star schema / ClickHouse | ~1,5 s | Đọc Parquet core (đo trong pipeline, 1 lần chạy) |
| One Big Table / ClickHouse | 13,2 s (Apache Spark join) + 3,9 s (nạp) = **17,1 s** | Phải join fact với mọi dimension trước khi nạp |
| Star schema / PostgreSQL | 8,9 s (`COPY`) + 7,5 s (PK, FK, index, `ANALYZE`) = **16,4 s** | |

---

## 6. Độ đúng giữa các giải pháp (dòng X1 trong report)

| BR | One Big Table / ClickHouse = Star schema / ClickHouse | Star schema / PostgreSQL = Star schema / ClickHouse |
|---|:-:|:-:|
| BR1, BR2, BR3, BR4, BR5, BR6, BR8, BR10 | ✅ 8/8 | ✅ 8/8 |

**16/16 cặp so sánh khớp** từng dòng, từng ô.

---

## 7. Đối chiếu giả thuyết

| Giả thuyết | Kết quả ×1 |
|---|---|
| **H1** One Big Table nhanh hơn một chút; star schema tốn ít chỗ hơn và cập nhật rẻ hơn (cùng ClickHouse) | Tốc độ: **đúng** (One Big Table nhanh hơn 1,26–2,20×, chênh lệch tuyệt đối chỉ 5–40 ms). Dung lượng: **đúng** (One Big Table 2,75×). Cập nhật: **chưa đo** |
| **H2** ClickHouse nhanh hơn PostgreSQL ở mọi BR | **Đúng** ở 8/8 BR: nhanh hơn **2,4–36,2×**. Chênh ít nhất ở BR6 (2,4×) |
| **H3** ClickHouse Kafka table engine trễ thấp hơn Apache Spark Structured Streaming | Chưa đo (B3) |
| **H4** Apache Spark ETL tăng tuyến tính theo dữ liệu, giảm khi thêm lõi | Chưa đo (B4); chỉ còn đo được phần "thêm lõi" vì chỉ chạy ×1 |

---

## 8. Chưa đo / giới hạn

| Hạng mục | Trạng thái |
|---|---|
| B1 — chi phí cập nhật (đổi tên 1 thành phố, đổi 1 thuộc tính khách) | Chưa làm |
| B2 — số truy vấn/giây khi nhiều người dùng cùng lúc | Chưa làm |
| B3 — Apache Spark Structured Streaming vs ClickHouse Kafka table engine | Chưa làm |
| B4 — Apache Spark ETL với 2 / 4 / 8 lõi | Chưa làm |
| Mức ×10, ×50 | **Không làm** (quyết định của nhóm: chỉ chạy ×1, do giới hạn đĩa/RAM) → report phải ghi rõ, và không kết luận được xu hướng khi dữ liệu lớn dần |
| Cold thực sự (đọc từ đĩa) | Không làm được trong container (không xoá được page cache) |
