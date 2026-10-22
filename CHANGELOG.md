# Changelog — Banking Data Warehouse (Kimball)

Ghi lại các phát hiện, quyết định thiết kế và thay đổi của dự án.


---

## [2026-10-22] Phase 2 — P2-0: dữ liệu Berka và Membership tier mô phỏng

- Bắt đầu Phase 2 theo [docs/PLAN_PHASE2.md](docs/PLAN_PHASE2.md) (căn cứ: sheet nhóm chốt 18/10). Tag **`phase1-kaggle`** (`4c554aa`) đánh dấu hết Phase 1.
- ClickHouse chuyển về **container trên máy** (comment các dòng Aiven trong `.env`) để benchmark công bằng và làm được BR07.
- Tải Berka (bản PKDD'99 trên Kaggle) → `data/berka/raw/`. Script [tools/profile_berka.py](tools/profile_berka.py): **14/15 số liệu khảo sát khớp sheet**; VYBER 9.597 chưa khớp cách đếm nào (16.666 giao dịch VYBER, 0 làm tăng số dư) → cần nhóm xác nhận. Chi tiết: [docs/berka_profile.md](docs/berka_profile.md).
- Script [tools/simulate_membership_tier.py](tools/simulate_membership_tier.py) → `data/berka/customer_profile_simulated.csv`: 4.500 dòng; hạng theo tiền vào năm đầu (Standard 46,1% / Silver 32,0% / Gold 15,5% / Platinum 6,4%); đạt mọi kiểm tra; chạy lại ra cùng file.

---

## [2026-10-07] ClickHouse chạy được cả local lẫn cloud (Aiven), chọn bằng `.env`

- Các biến mới trong `.env` (mẫu ở [.env.example](.env.example)): `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_SECURE`, `CLICKHOUSE_DATABASE_ENGINE`. Không khai báo gì = local như trước.
- Spark jobs, Streamlit, Superset đọc cùng các biến này. `make app-bootstrap` cập nhật lại kết nối Superset theo `.env`.
- [docker-compose.yml](docker-compose.yml): service `clickhouse` vào profile `local-ch`; Makefile chỉ bật profile này khi `CLICKHOUSE_HOST` là `clickhouse` (local).
- [p2_load_clickhouse.py](spark/jobs/p2_load_clickhouse.py): local vẫn dùng `file()` (ClickHouse tự đọc Parquet); cloud gửi từng file Parquet lên qua HTTPS (`INSERT … FORMAT Parquet`). Đã thử đường cloud trên ClickHouse local: 6/6 bảng giống hệt (số dòng và checksum).
- Aiven chỉ cho tạo database với `ENGINE = Replicated` → `ensure_databases()` tạo `dwh`, `mart`, `dq` theo `CLICKHOUSE_DATABASE_ENGINE`.
- Lệnh mới `make sql Q="..."` ([ch_sql.py](spark/jobs/ch_sql.py)); `make smoke` và `make stream-reset` không còn gọi `clickhouse-client` trong container nên chạy được với cả cloud.
- Kiểm tra lại ở chế độ local: `make load`, `make dq` (12/12 PASS), `make marts`, `make app-bootstrap` đều chạy đúng.
- **Chạy thật trên Aiven** (ClickHouse 26.3): `make pipeline` nạp đủ 6 bảng (số dòng khớp local), 12/12 DQ PASS, 10 truy vấn BR trả cùng số dòng như local, mọi bảng tự thành `ReplicatedMergeTree`. Nạp qua mạng chậm hơn local: ~106 s (dim_customer 73 s) so với ~1,5 s. Superset và Streamlit đã trỏ sang Aiven.
- Gặp lại lỗi Kafka không ghi vào volume: sau `docker compose down`, topic `transactions` bị tạo mới, phải `make produce` lại.
- Benchmark vẫn chỉ chạy local (dùng `file()`, `SYSTEM DROP … CACHE` và `clickhouse-client` trong container).
- Lần đầu chạy `make app-bootstrap` ngay sau `make load` gặp lỗi ClickHouse hết bộ nhớ (giới hạn 2,5 GB); chạy lại thì qua. Không liên quan đến thay đổi này.

---

## [2026-10-07] Tài liệu kiến trúc cho team + các điểm cần làm rõ

### Tài liệu
- [docs/architecture.html](docs/architecture.html): sơ đồ tổng quát 6 chặng (Nguồn → Kafka → Data Lake → Data Warehouse → Phân tích → UI) và nhánh realtime BR7, kèm 3 phần giải thích:
  - Giải thích từng chặng của sơ đồ.
  - Dữ liệu nằm ở đâu: thô hay đã xử lý, UI đọc từ đâu, dùng công nghệ gì.
  - Bên trong Data Lake: raw (chép) → staging (sửa lỗi) → core (dựng star schema).

### Thống nhất cách gọi
- **Data warehouse = ClickHouse database `dwh`**. Đây là bản chính thức. `lake/` chỉ là khu xử lý trung gian, dựng lại được bất cứ lúc nào.
- **Data mart (theo Kimball) = 1 fact + các dimension của nó**, nằm trong `dwh`. Có 3 mart: giao dịch, số dư cuối tháng, cảnh báo. Ba mart dùng chung `dim_date` và `dim_customer`.
- `mart.rfm` **không phải data mart** theo Kimball mà là bảng tổng hợp. Tên database `mart` và chữ "data marts" trong report, `00_databases.sql`, `marts.sql` dễ gây nhầm → **chưa sửa**, nhóm cần chọn: chỉ đổi chữ, hay đổi tên database.

### Phát hiện cần xử lý
- **Kafka không ghi vào volume** (`bankdw_kafka-data` = 0 B). Nghi do thiếu `KAFKA_LOG_DIRS: /var/lib/kafka/data` (chưa kiểm tra trong container). Hệ quả: `make down` làm mất dữ liệu Kafka, phải `make produce` lại trước `make ingest`. **Chưa sửa.**
- **Dữ liệu nhập tay chưa có nguồn**: danh sách 10 ngày lễ Ấn Độ (`p2_build_core.py`) và [warehouse/seeds/city_mapping.csv](warehouse/seeds/city_mapping.csv) (thành phố → bang). Cần đối chiếu với nguồn chính thức và trích dẫn trong report.
- Nguồn Kaggle có **8 file CSV, nhóm dùng 3** (giao dịch, khách hàng, địa điểm). Không dùng `dim_date.csv` (chỉ 55 ngày) và 4 bảng tổng hợp có sẵn (tính sai).

### Git
- Repo chưa có remote → chưa push được.

---

## [2026-10-07] P6 — Benchmark ×1 (B1, B2) + dọn .gitignore

- Benchmark ở mức ×1 cho B1 (star schema vs One Big Table, cùng ClickHouse) và B2 (ClickHouse vs PostgreSQL, cùng star schema): 8 BR × 6 lần, 16/16 cặp kết quả khớp. Chi tiết và số liệu: [docs/BENCHMARK.md](docs/BENCHMARK.md).
- Code: [benchmark/](benchmark/), [warehouse/postgres/](warehouse/postgres/), [warehouse/clickhouse/ddl/obt.sql](warehouse/clickhouse/ddl/obt.sql); service `postgres` (profile `bench`, cổng 5433) trong [docker-compose.yml](docker-compose.yml); thêm `psycopg2-binary` vào image Spark.
- Còn lại của P6: chi phí cập nhật, truy vấn/giây nhiều người dùng, B3, B4.
- `.gitignore`: thêm file build LaTeX, `*.pyc`, `.venv/`, `.vscode/`, `.idea/`; bỏ theo dõi folder template `report/LaTeX_Template_for_Technical_Report/` (giữ ở máy, `main.tex` không dùng).

---

## [2026-10-06] Demo chạy thử + sửa độ trễ khởi động của stream (BR7)

- Chạy demo toàn bộ (Superset + Streamlit + stream) trong trình duyệt; phát 60.000 giao dịch mới (`SKIP=100000`, 1.000/s): Streamlit hiện **p95 5,42 s**, vượt mục tiêu 5 s.
- Phân tích theo khoảng 10 s: chỉ **~15 s đầu** chậm (p50 5,6 s, max 6,3 s); sau đó ổn định (p50 ~2 s, max ~3,7 s). Nguyên nhân: bảng tra cứu khoá (~1 triệu khách) được `cache()` **lười** → lô đầu tiên phải nạp.
- Sửa [spark/jobs/p4_stream_alerts.py](spark/jobs/p4_stream_alerts.py): nạp sẵn cache bằng `count()` khi khởi động stream.
- Đo lại (60.000 giao dịch mới, `SKIP=200000`, 1.000/s): 992 cảnh báo / 992 giao dịch; **10 s đầu p50 2,6 s, max 3,9 s; cả lần chạy p50 1,96 s, p95 3,00 s, max 3,92 s**.
- Ghi chú cho report: số liệu p95 3,18 s trong report (40.000 giao dịch, 2.000/s) vẫn đúng; có thể bổ sung chi tiết "cold start" vào phần thảo luận B3.

---

## [2026-10-06] Report: bổ sung kết quả P1–P5

| Phần | Bổ sung |
|---|---|
| II.F Data Characteristics | Đoạn *Temporal coverage*: Aug 31 ngày, Sep 22 ngày, Oct chỉ 2 ngày; dữ liệu thực chất đủ từ 1/8 đến ~21/9 |
| II.G Data Quality Defects | 142.360 ID dùng chung bao phủ 304.877 bản ghi khách (29,1%) |
| IV Criteria (bảng tiêu chí) | Đổi chỉ số "null foreign key" → "trỏ tới Unknown member (key 0)" cho khớp kiểm tra thật |
| V.A Environment | Mô tả đúng môi trường: Docker VM **8 GB / 11 vCPU** (máy 18 GB), dừng workload khác trừ 1 container rảnh ~64 MB; bảng mới `tab:env` (phiên bản + giới hạn RAM; PostgreSQL còn TBD) |
| VI.A Data Correctness | Viết kết quả: 12/12 PASS, đối soát 0,00 INR, chạy lại với 40k sự kiện trùng vẫn đúng; streaming 642/642 khớp batch, p50 2,10 s, p95 3,18 s. Bảng `tab:correctness-results` viết lại đủ 15 dòng (C1–C3, U1–U3, A1–A3, K1–K3, S1, S2, X1); chỉ X1 (so sánh giữa các giải pháp) còn TBD |
| VI.B Performance | Bảng mới `tab:pipeline`: thời gian từng bước ở ×1 (**ghi rõ là 1 lần chạy**, chỉ để tham khảo); bảng XII benchmark vẫn TBD |
| VI.C Effectiveness | Bảng mới `tab:br-results` (2 cột): 10/10 BR demo thành công + 1 kết quả cụ thể cho mỗi BR; thời gian phản hồi ~0,75 s/biểu đồ, ~1,1 s/trang. Còn TODO: số thao tác, thời gian trả lời, SUS, ảnh chụp |
| VI.E Threats to Validity | Thêm: dữ liệu thực chất chỉ đến 21/9; nhiều khách có số dư trùng nhau đến từng đồng |
| VII Limitations | Ghi rõ chỉ 2 ngày thuộc tháng 10 |
| Appendix A | Thay lệnh cũ (`make etl`, `make bench` không tồn tại) bằng lệnh thật |

- Đã sửa 1 nhận định sai trước khi ghi: số tiền trung bình **không tăng đều theo tuổi** (nhóm 0–17 cao hơn 18–24; 65+ nữ thấp hơn 55–64 nữ) → viết lại đúng theo số liệu.
- Còn lại trong report: 11 `TODO`, 12 `TBD` — chủ yếu chờ P6 (benchmark) và ảnh chụp.

---

## [2026-10-06] P5 — Ứng dụng: Superset + Streamlit ✅ (Mốc M2)

### Hạ tầng
- Service mới trong [docker-compose.yml](docker-compose.yml), profile `app` (không chạy khi `make up`): `bankdw-superset` (Superset 4.1.3 + clickhouse-connect, cổng **8088**, giới hạn 1,5 GB) và `bankdw-streamlit` (Streamlit 1.40, cổng **8501**, 512 MB).
- [apps/superset/superset_config.py](apps/superset/superset_config.py): cache kết quả 300 s, `DASHBOARD_RBAC`, API quản lý role.
- Tài khoản admin + secret key sinh ngẫu nhiên trong `.env` (không commit); mẫu ở [.env.example](.env.example).
- Makefile: `make app-up`, `make app-down`, `make app-bootstrap`.

### Superset — [apps/superset/bootstrap.py](apps/superset/bootstrap.py) (idempotent, qua REST API)
- 1 kết nối ClickHouse, **13 virtual dataset** (10 BR + 3 dataset phụ), **7 dashboard / 32 biểu đồ**, **7 role** (mỗi nhóm người dùng 1 role, gắn vào dashboard).

| Dashboard | Nhóm người dùng | BR | Biểu đồ | Bộ lọc |
|---|---|---|---:|---|
| Executive Overview | Ban điều hành | BR1, BR2 | 6 | — |
| Regional Performance | Quản lý khu vực | BR3, BR10 | 4 | City |
| Marketing & CRM | Marketing | BR4, BR5 | 5 | Segment |
| Product Usage | Phát triển sản phẩm | BR6 | 3 | — |
| Wealth Management | Quản lý tài sản | BR8 | 5 | — |
| Risk Alerts (summary) | Quản trị rủi ro | BR7 | 4 | — |
| Data Quality | Nhóm dữ liệu | BR9 | 5 | — |

### Streamlit — [apps/streamlit/app.py](apps/streamlit/app.py) (BR7)
- Tự làm mới mỗi 2 s: 4 chỉ số (số cảnh báo, giá trị bị gắn cờ, độ trễ p95, cảnh báo gần nhất), biểu đồ cảnh báo theo 10 s, bảng 50 cảnh báo mới nhất (nối `dim_customer`, `dim_location`); bộ lọc quy tắc, khung thời gian, số tiền tối thiểu.

### Kiểm tra bằng trình duyệt
- Cả 7 dashboard và trang Streamlit hiển thị đúng dữ liệu. Demo trực tiếp: số cảnh báo trên Streamlit tăng theo thời gian thực khi phát giao dịch.
- Thời gian phản hồi mỗi biểu đồ (đo trong trình duyệt): **~0,75 s**; trang tải ~1,1 s. (Lúc kiểm tra thấy chờ >10 s là do khung trình duyệt bị ẩn nên bị hạ tốc độ, không phải do hệ thống.)
- RAM khi chạy toàn bộ: ClickHouse ~1 GB, Kafka ~455 MB, Superset ~310 MB, Streamlit ~117 MB, Spark (rảnh) ~12 MB.

### Lỗi gặp và sửa
- Mount 1 file đơn lẻ vào container không thấy file mới → mount cả thư mục `apps/superset`.
- Superset giới hạn API 50 yêu cầu/giây (HTTP 429) → tự chờ và thử lại.
- Streamlit: định dạng số `%,.2f` không hợp lệ → `%.2f`.
- Nhóm tuổi `<18` xếp sau `65+` → đổi nhãn thành `0-17`.
- Ô "Records with a data quality flag" (485k) gây hiểu nhầm (cộng dồn số cờ) → đổi thành "Data quality flags raised".
- Bảng Campaign target list giới hạn 10.000 dòng → ghi rõ "top 10,000 … by value" trong tiêu đề.
- Phát lại giao dịch từ đầu sinh **cảnh báo trùng** (1.144 dòng / 642 giao dịch) → thêm `--skip` cho producer (`make produce-live SKIP=…`) và `make stream-reset` (xoá bảng cảnh báo + checkpoint). Chạy lại demo sạch: 642 cảnh báo / 642 giao dịch, p50 2,1 s, p95 3,2 s.

### Phát hiện về dữ liệu (đưa vào report)
- **Tháng 10 chỉ có 2 ngày dữ liệu** (4.987 giao dịch); tháng 9 có 22 ngày; từ 22/9 mỗi ngày < 10.000 giao dịch. Dữ liệu thực chất đầy đủ từ 1/8 đến ~21/9 → ảnh hưởng cách diễn giải recency (RFM) và biểu đồ xu hướng BR1.
- Bảng Campaign target list cho thấy trực quan lỗi trùng `customer_id` (ví dụ `C8217728` xuất hiện 2 lần với 2 người khác nhau).

### Ghi chú cho đánh giá hiệu quả (tiêu chí 3)
- Ảnh chụp cho report nên chụp lại ở độ phân giải cao (khung trình duyệt hiện chỉ cho ảnh ~800 px).

---

## [2026-10-06] P4 — Streaming phát hiện giao dịch bất thường (BR7) ✅

### Quy tắc cảnh báo (đã định cỡ trên dữ liệu lịch sử)
| Quy tắc | Điều kiện | Giao dịch bị gắn cờ |
|---|---|---:|
| `HIGH_AMOUNT` | số tiền > 20.000 INR (p99) | 13.734 (0,97%) |
| `HIGH_BALANCE_RATIO` | số tiền ≥ 5.000 INR **và** > số dư | — |
| Cả hai | | **25.838 (1,82%)** |

- Đã thử quy tắc "số tiền > ρ × số dư" không có ngưỡng tối thiểu: gắn cờ **8–13%** giao dịch (quá nhiều, vì nhiều tài khoản số dư nhỏ) → thêm ngưỡng 5.000 INR.

### Code
- [spark/jobs/p4_stream_alerts.py](spark/jobs/p4_stream_alerts.py): Kafka → Structured Streaming (micro-batch 2 s, `maxOffsetsPerTrigger` 200k) → stream-static join với `customer_key_map` / `location_key_map` (khoá chuẩn của star schema) → `foreachBatch` ghi ClickHouse; checkpoint `lake/checkpoints/alerts`.
- Makefile: `make stream`, `make stream-stop`, `make stream-logs`, `make produce-live RATE=… LIMIT=…`.

### Kết quả (40.000 giao dịch, 2.000 giao dịch/giây)
| Chỉ số | Giá trị |
|---|---:|
| Cảnh báo | 642 (1,6%): HIGH_AMOUNT 358, HIGH_BALANCE_RATIO 284 |
| Độ trễ đầu-cuối (producer → hiện trong ClickHouse) p50 / p95 / max | **1,96 s / 2,88 s / 3,33 s** (mục tiêu p95 ≤ 5 s: đạt) |
| Đối chiếu với batch (cùng quy tắc trên 40.000 giao dịch đầu trong DWH) | **642/642 khớp** (giao dịch, quy tắc, customer_key, location_key); 0 trùng |

- Kiểm tra khử trùng: Kafka chứa 1.458.764 sự kiện (40.000 bị phát lại) → `fact_transactions` vẫn 1.418.764; DQ 12/12 PASS.

### Report đã cập nhật
- Mô tả BR7 trong bảng BR; đoạn quy tắc cảnh báo (kèm lý do thêm ngưỡng 5.000 INR); Listing 3 thay bằng code thật (rút gọn từ `p4_stream_alerts.py`).

---

## [2026-10-06] Git + P3 — Mart RFM và 10 truy vấn BR ✅

### Git
- Khởi tạo repo (nhánh `main`), commit đầu `f105593` (P0–P2 + report). `data/`, `lake/` không đưa vào repo.

### Code
- [warehouse/clickhouse/ddl/marts.sql](warehouse/clickhouse/ddl/marts.sql): `mart.rfm`, ngưỡng cố định — R (≤7/21/42/63 ngày), F = min(số giao dịch, 5), M (<500/2.000/10.000/50.000 INR); 5 phân khúc: Champions, Loyal, Recent, At Risk (High Value), Hibernating.
- [warehouse/clickhouse/ddl/streaming.sql](warehouse/clickhouse/ddl/streaming.sql): `dwh.fact_transaction_alert` cho BR7 (không bị xoá khi nạp lại batch); có `produced_at`, `detected_at`, `inserted_at` để đo độ trễ.
- [warehouse/queries/clickhouse/](warehouse/queries/clickhouse/): 10 file `br01`–`br10`, mỗi file có `ORDER BY` xác định để so sánh kết quả giữa các giải pháp.
- [spark/jobs/p3_marts_and_queries.py](spark/jobs/p3_marts_and_queries.py): dựng mart, chạy 10 BR, lưu CSV vào `lake/results/clickhouse_star/`.
- `make marts`; `make pipeline` giờ = ingest → core → load → dq → marts.
- Đổi mount: `./warehouse` → `/opt/warehouse` (DDL, truy vấn, seed); thêm `run_sql_file()` dùng chung.

### Kết quả (ClickHouse, star schema, ×1, lần chạy thứ hai)
| BR | Dòng | Thời gian |
|---|---:|---:|
| br01 daily KPIs | 55 | 37 ms |
| br02 weekday/weekend | 7 | 24 ms |
| br03 city ranking | 55 | 59 ms |
| br04 RFM segments | 5 | 13 ms |
| br05 age × gender | 19 | 90 ms |
| br06 type mix | 216 | 36 ms |
| br07 alerts | 0 (chờ P4) | 4 ms |
| br08 high balance (top 1%) | 10.482 | 55 ms |
| br09 data quality | 19 | 6 ms |
| br10 amount bands | 328 | 32 ms |

- Chạy lại toàn bộ pipeline (core → load → dq → marts): **38 s**, 12/12 DQ PASS.
- *Lưu ý:* đây mới là số đo sơ bộ (1 lần chạy), chưa phải benchmark theo phương pháp (6 lần, median/p95) của P6.

### Phát hiện cho report
- RFM: **91,7% Hibernating** (hệ quả của 71% khách chỉ có 1 giao dịch); **At Risk (High Value)** 22.988 khách nhưng **601 triệu INR** → nhóm mục tiêu cho chiến dịch.
- BR2: Thứ Bảy trung bình cao nhất (33.491 giao dịch/ngày), Thứ Sáu thấp nhất (20.114).
- BR8: ngưỡng top 1% = 1.587.419 INR; nhiều khách có **số dư trùng nhau đến từng đồng** (115.035.495,10) → dấu hiệu dữ liệu synthetic, ghi vào phần giới hạn.

---

## [2026-10-06] P1–P2 — Ingestion, staging, star schema, data quality ✅ (Mốc M1)

### Quyết định thiết kế đã chốt (4 điểm)
1. **Snapshot số dư theo cuối tháng** (không theo ngày): 2.820.640 dòng thay vì ~1,1 tỷ.
2. **RFM chấm theo ngưỡng cố định** (không `ntile`): R theo số ngày (≤7/21/42/63), F = min(số giao dịch, 5), M theo dải INR (500/2.000/10.000/50.000). Sẽ hiện thực ở P3.
3. **`dim_location`**: seed [warehouse/seeds/city_mapping.csv](warehouse/seeds/city_mapping.csv) chuẩn hoá **54 thành phố** (khớp khi tên = từ khoá hoặc kết thúc bằng " từ khoá", ưu tiên từ khoá dài nhất) → phủ **77,6%** giao dịch; còn lại = "Other".
4. **Ngày sinh**: 1800-01-01 → NULL; sau 2016-10-21 → trừ 100 năm; sau khi sửa ngoài tuổi 0–100 → NULL. Tiêu chí kiểm tra dùng **cùng khoảng [1916-10-22, 2016-10-21]**.

### Khảo sát thêm trước khi code
- Natural key `(customer_id, DOB gốc, giới tính gốc)` **duy nhất 100%** (1.046.782 / 1.046.782).
- Ngày sinh sau sửa phần lớn rơi vào 1971–1975 → giả thuyết lỗi năm 2 chữ số là hợp lý.

### Code
- [ingestion/producer.py](ingestion/producer.py): phát lại giao dịch theo thứ tự ngày, key = customer, idempotent, `--rate` để giả lập streaming, gắn `produced_at_ms` để đo độ trễ (B3).
- [spark/jobs/p1_ingest_raw.py](spark/jobs/p1_ingest_raw.py): Kafka (giao dịch) + CSV (khách hàng, địa điểm) → `lake/raw`.
- [spark/jobs/p2_build_core.py](spark/jobs/p2_build_core.py): `lake/staging` (làm sạch + cờ DQ, khử trùng theo `transaction_key`) → `lake/core` (4 dimension + 2 fact, surrogate key, Unknown member = 0).
- [warehouse/clickhouse/ddl/dwh.sql](warehouse/clickhouse/ddl/dwh.sql) + [spark/jobs/p2_load_clickhouse.py](spark/jobs/p2_load_clickhouse.py): ClickHouse tự đọc Parquet bằng `file()`.
- [spark/jobs/p2_dq_checks.py](spark/jobs/p2_dq_checks.py): 12 kiểm tra (ISO/IEC 25012) + 7 chỉ số thông tin → `dq.audit`.
- Makefile: `make produce`, `make pipeline` (= ingest → core → load → dq).

### Kết quả chạy
| Bước | Kết quả | Thời gian |
|---|---|---|
| produce | 1.418.764 sự kiện vào Kafka, undelivered = 0 | 5,2 s (~273k/s) |
| ingest | raw: 1.418.764 giao dịch, 1.046.782 khách, 9.354 địa điểm | ~17 s |
| core | dim_date 1.096; dim_customer 1.046.783; dim_location 9.355; dim_transaction_type 5; fact_transactions 1.418.764; snapshot 2.820.640 | ~32 s |
| load | 6 bảng vào ClickHouse | ~1,5 s |
| dq | **12/12 PASS** (C1–C3, U1–U3, A1–A3, K1–K3); chênh lệch tổng tiền = 0,00 INR | — |

Chỉ số thông tin (khớp profiling): DOB placeholder 56.782; DOB sửa lùi 100 năm 117.460; DOB thiếu 3.385; DOB ngoài 0–100 sau sửa 0; giới tính không hợp lệ 1.101; customer_id dùng chung 304.877 bản ghi; giao dịch 0 đồng 1.102.

### Lỗi gặp và sửa
- Biểu thức Spark khai báo ở cấp module (trước khi có SparkSession) → `AssertionError` → chuyển thành hàm.
- Smoke test: Kafka không tự tạo topic (`auto.create = false`) → phải tạo topic trước khi produce.

### Report đã cập nhật cho khớp
- `dim_location` (54 thành phố, 77,6%), snapshot cuối tháng (2,8 triệu dòng), RFM ngưỡng cố định (Listing 5), khoảng DOB [1916-10-22, 2016-10-21] ở bảng tiêu chí và bảng lỗi DQ, Listing 2 thêm bước lọc tuổi 0–100.

---

## [2026-10-06] P0 — Hạ tầng (Docker Compose) ✅

### Đã làm
- [docker-compose.yml](docker-compose.yml) (project `bankdw`), 3 service:

| Service | Image | Cổng host | RAM giới hạn | Ghi chú |
|---|---|---|---|---|
| `bankdw-kafka` | `apache/kafka:3.9.1` (KRaft, 1 node) | 9094 | 1 GB | Không tự tạo topic; topic `transactions` 3 partition |
| `bankdw-clickhouse` | `clickhouse/clickhouse-server:25.3` (25.3.14.14) | 8123, 9000 | 2,5 GB | User `bank`; database `dwh`, `mart`, `dq`; đọc Parquet ở `./lake` |
| `bankdw-spark` | build từ `apache/spark:3.5.6-python3` | — | 3 GB | Local mode; có jar Kafka connector + confluent-kafka 2.6.1, clickhouse-connect 0.8.18, pandas 2.0.3, pyarrow 17.0.0 |

- [spark/Dockerfile](spark/Dockerfile), [warehouse/clickhouse/init/00_databases.sql](warehouse/clickhouse/init/00_databases.sql), [Makefile](Makefile) (`make up / down / ps / logs / topic / smoke / clean`), [.gitignore](.gitignore).
- Thư mục: `spark/jobs/`, `ingestion/`, `lake/` (Parquet, không commit).

### Kiểm tra
- `make smoke`: Kafka, ClickHouse, Spark, thư viện Python và dữ liệu nguồn đều OK.
- Kiểm tra đầu-cuối: đẩy 3 message vào Kafka → Spark đọc (batch Kafka source) → ghi vào ClickHouse → đếm đủ 3 dòng. Đã xoá topic/table thử.
- RAM lúc chạy không tải: Kafka ~400 MB, ClickHouse ~390 MB, Spark ~3 MB.

### Vấn đề gặp và cách xử lý
- Image Spark 3.5.6 dùng **Python 3.8** → pandas 2.2 không cài được → hạ xuống pandas 2.0.3, pyarrow 17.0.0.
- `python3` không import được `pyspark` → thêm `PYTHONPATH` trong Dockerfile.
- ClickHouse không khởi động khi mount `./lake` ở chế độ `:ro` (entrypoint cần `chown`) → mount read-write.

### Lưu ý
- Docker Desktop: **8 GB RAM**. Container `octiq-local-kb` (dự án khác) có `restart=unless-stopped` nên đã tự chạy lại; hiện chỉ dùng ~64 MB.
- Cổng 5432 để trống cho Postgres của dự án khác; Postgres đối chứng (P6) sẽ dùng cổng khác.

---

## [2026-10-06] Report: bổ sung A2 (nguồn dữ liệu dự kiến)

- Thêm mục **II.D Expected Data Sources** (trước *Data Source*) và bảng `tab:expected-sources`: 4 hệ thống nguồn của ngân hàng thật (core banking, card/payment switch, customer master/KYC/CRM, danh mục chi nhánh/ATM/POS) → đặc điểm → file Kaggle tương ứng.
- Ghi rõ dự án **chỉ dùng bộ Kaggle**; các hệ thống ngân hàng là nguồn *dự kiến* khi triển khai thật, không phải hệ thống nhóm đã kết nối.
- Giải thích lý do kiến trúc tách 2 đường: giao dịch qua Kafka, dữ liệu tham chiếu nạp batch từ CSV.

---

## [2026-10-06] Report: bổ sung A1, A4, A5

- **A1 – Mục tiêu kinh doanh:** thêm bảng `tab:objectives` (phần II.C), 7 mục tiêu O1–O7, mỗi mục tiêu gắn với BR và một ví dụ quyết định kinh doanh. Mỗi BR phục vụ đúng một mục tiêu.
- **A4 – Công nghệ cho ứng dụng (chỉ Solution 1):** thêm mục *Technology for the Application* — Superset (dataset, native filter, role/caching), Streamlit (trang cảnh báo tự refresh) kèm **Listing 6** (Streamlit, BR7), và bảng `tab:techroles` (công nghệ → vai trò → BR). PostgreSQL / One Big Table / Kafka engine giữ vai trò đối chứng, không mô tả thêm.
- **A5 – Bối cảnh Big Data:** thêm mục *Big Data Considerations* theo 5V (Volume, Velocity, Variety, Veracity, Value); số liệu: ngày cao điểm 36.798 giao dịch ≈ 0,43 giao dịch/giây.
- Thêm 2 tài liệu tham khảo: Apache Superset, Streamlit.
- A2, A3 và nhóm C vẫn để lại.

---

## [2026-10-06] Report: sửa các điểm chưa chuẩn (nhóm B)

- **B1 – Sơ đồ kiến trúc khớp thiết kế:** thêm mũi tên CSV → Spark batch ("reference data: customers, locations"); mô tả Ingestion/Batch ghi rõ chỉ giao dịch đi qua Kafka, dữ liệu khách hàng và địa điểm đọc trực tiếp từ CSV.
- **B2 – Thì quá khứ cho Research Methodology** (theo hướng dẫn template "describe what you did"). ⚠️ Phần này giờ mô tả như đã làm xong → phải thực hiện đúng như vậy trước khi nộp.
- **B3 – Code minh họa:** thêm comment `% NOTE: reference design ...` trước 5 listing; phải thay bằng code thật khi có repo.
- **B4 – Ngày:** `\date{\today}` → `\date{\todo{submission date}}` (điền ngày nộp).
- **B5 – Hedging:** thêm comment hướng dẫn của template ở đầu phần Analysis and Interpretation.
- **B6 – Bỏ Index Terms** (template không dùng; chế độ `peerreview` thường không dùng keywords).
- Nhóm A (bổ sung nội dung) và C (chờ kết quả) **để lại**, chưa làm.

---

## [2026-10-06] Viết lại report theo template IEEE technical report

- Template nhóm cung cấp: `report/LaTeX_Template_for_Technical_Report/` (IEEEtran **v1.8**, chế độ **`peerreview`**). Copy `IEEEtran.cls` ra `report/` để build.
- [report/main.tex](report/main.tex) viết lại theo template:
  - Định dạng: 1 cột, trang tiêu đề + mục lục + danh sách hình + danh sách bảng, `\IEEEpeerreviewmaketitle`, phụ lục **trước** tài liệu tham khảo, bibliography viết tay.
  - **Cấu trúc 7 phần theo template:**

| # | Phần (template) | Nội dung của nhóm |
|---|---|---|
| I | Introduction | Bối cảnh, mục tiêu, đóng góp |
| II | Problem Definition | Problem statement, user groups, 10 BR, dữ liệu nguồn, đặc điểm dữ liệu, lỗi chất lượng (ISO/IEC 25012), phạm vi |
| III | Proposed Solutions | Giải pháp 1: Kimball trên ClickHouse (kiến trúc, mô hình, công nghệ, 5 listing, ứng dụng); Giải pháp 2: One Big Table; Giải pháp 3: PostgreSQL; thay thế bổ sung (Kafka engine, lakehouse); Related Work |
| IV | Criteria for Assessing Solutions | 3 tiêu chí (độ đúng, hiệu năng, hiệu quả khai thác) + giả thuyết H1–H4 |
| V | Research Methodology | Môi trường, profiling, kiểm tra độ đúng, nhân bản dữ liệu, thiết kế benchmark B1–B5, đánh giá hiệu quả |
| VI | Analysis and Interpretation | Bảng kết quả (TBD), diễn giải, threats to validity |
| VII | Conclusions and Recommendations | Kết luận, khuyến nghị (TODO), giới hạn, hướng phát triển |

- Tên tác giả viết theo kiểu tiếng Anh (tên trước, họ sau, không dấu); thêm Group 27 và giảng viên hướng dẫn.
- **Sửa lỗi layout:** `peerreview` của IEEEtran là **2 cột** (chỉ trang bìa 1 cột), không phải 1 cột như đã hiểu nhầm → bảng/hình bị tràn, đè chữ. Đã sửa:
  - Bảng rộng và 2 sơ đồ (kiến trúc, star schema) dùng `table*`/`figure*` trải ngang 2 cột.
  - Bảng nhỏ (Source Files, Transaction Types, Transactions per Customer, Work Allocation) giữ 1 cột, chữ `\footnotesize`.
  - Code listing: dòng ≤ 52 ký tự, chữ `\scriptsize`, đặt dạng float để không bị cắt ngang 2 cột.
  - Sơ đồ kiến trúc: dời ô "Parquet lake" và nhãn "producer" để không đè ô Kafka; star schema: nới rộng ô.
  - Bỏ `fontspec` (Tectonic không tìm thấy font TeX Gyre); dùng font mặc định của class.

---

## [2026-10-06] Bản nháp Technical Report (LaTeX)

- Tạo [report/main.tex](report/main.tex): **1 file duy nhất**, IEEEtran, tiếng Anh, bibliography viết inline (`thebibliography`, không cần file `.bib`).
- Đã viết: Abstract, I–IV đầy đủ (bối cảnh, BR, profiling + ISO/IEC 25012, mô hình Kimball, kiến trúc, 5 listing code minh họa, bảng chọn công nghệ, giải pháp thay thế, related work), V (nguyên tắc UI + ánh xạ BR → màn hình), VI (tiêu chí đánh giá, phương pháp benchmark, giả thuyết H1–H4), VII (giới hạn, hướng phát triển), phụ lục.
- Còn `TODO`/`TBD` (màu đỏ): tên thành viên, group ID, ảnh dashboard, version phần mềm, toàn bộ số liệu kết quả, phần Discussion, link repo.
- Code trong các listing là **thiết kế tham chiếu**; phải cập nhật theo code thật khi hiện thực.

### Phát hiện thêm khi viết report
- `customer_dob`: **56.782** dòng là placeholder `1800-01-01`; **117.460** dòng có ngày sinh **sau 2016-10-21** (đến 2075). Có vẻ do lỗi parse năm 2 chữ số ("75" → 2075). Thiết kế: sửa lùi 100 năm và gắn cờ `dq_dob_fixed`.
- 13.734 giao dịch (0,97%) vượt ngưỡng p99 = 20.000 INR → ước lượng số cảnh báo BR7 nếu chỉ dùng ngưỡng số tiền.
- Số giao dịch theo ngày: thấp nhất 3, cao nhất 36.798, trung bình 25.796.

---

## [2026-10-06] Chốt chuẩn viết Technical Report: IEEE

### Bối cảnh
- PDF yêu cầu **không quy định chuẩn** (không template, không citation style, không ngôn ngữ, không số trang). Chỉ có ràng buộc: PDF, tên file theo group ID, 4–7 phần, > 3 ví dụ minh họa công nghệ, thiếu < 10% nội dung, > 85% tiêu chí đánh giá rõ ràng.
- Người viết là học viên thạc sĩ Computer Science → chọn chuẩn học thuật của ngành.

### Quyết định

| Khía cạnh | Chuẩn |
|---|---|
| Template, định dạng | **IEEE (IEEEtran), LaTeX trên Overleaf** |
| Bố cục | IMRaD điều chỉnh, gói trong **7 phần** |
| Trích dẫn | **IEEE citation style** |
| Tiêu chí độ đúng dữ liệu | **ISO/IEC 25012** (accuracy, completeness, consistency, uniqueness, …) |
| Benchmark | Phương pháp **TPC-H/TPC-DS** (scale factor, công bố cấu hình và kết quả) |
| Sơ đồ kiến trúc | **C4 model** |
| Thuật ngữ Kimball | *The Data Warehouse Toolkit* (Kimball & Ross, 3rd ed., 2013) |

- Không dùng ANSI/NISO Z39.18 và arc42 (hợp tài liệu doanh nghiệp hơn report học thuật).
- Ngôn ngữ: **tiếng Anh** (đề xuất, chờ xác nhận với giảng viên).
- Độ dài dự kiến: 12–20 trang IEEE hai cột, chưa tính phụ lục.

### Bố cục report

```
Abstract + Index Terms
I.   Introduction                     – bối cảnh, vấn đề, contributions
II.  Requirements & Stakeholders      – nhóm người dùng, BR1–BR10
III. Data Sources & Characteristics   – profiling, vấn đề chất lượng dữ liệu
IV.  Proposed Solution                – kiến trúc (C4), Kimball, công nghệ, giải pháp thay thế, related work ngắn
V.   Application                      – dashboard Superset, cảnh báo realtime Streamlit
VI.  Evaluation & Discussion          – độ đúng (ISO/IEC 25012), hiệu năng (B1–B5), hiệu quả khai thác
VII. Conclusion & Future Work
References · Appendix (hướng dẫn chạy, link repo, phân công)
```

### Còn hỏi giảng viên
- Có template report của môn/trường không?
- Viết tiếng Việt hay tiếng Anh?

---

## [2026-10-06] Ghi lại yêu cầu đồ án

- Tạo [docs/requirement.md](docs/requirement.md): tóm tắt phần Project từ PDF yêu cầu môn CO5173 (mục tiêu, đề tài, nội dung bắt buộc, sản phẩm nộp, rubric, bonus). Bỏ qua phần Presentation.
- **Sửa lỗi:** mỗi BR demo thành công được **1 điểm** (4/n với n = 4, trên thang 10), không phải 0,5 điểm như ghi trước đó. 8 BR là đủ điểm tối đa 8/10 của tiêu chí Application Implementation.

---

## [2026-10-06] Lập kế hoạch thực hiện

- Tạo [docs/PLAN.md](docs/PLAN.md): 9 giai đoạn (P0–P8), 3 mốc (M1 dữ liệu vào DWH, M2 demo đủ 10 BR, M3 nộp bài), phân công 4 thành viên, khung report 7 phần, rủi ro.
- Lịch đang tính theo tuần (~8 tuần), chờ hạn nộp chính xác để đổi sang ngày.

---

## [2026-10-06] Chốt công nghệ cuối cùng & kế hoạch benchmark

### Quyết định
- **Giữ ClickHouse** làm data warehouse chính. Không có trong danh sách gợi ý của đề (list có "…" nên vẫn hợp lệ); report cần có bảng giải thích lý do chọn.

### Danh sách công nghệ

| Lớp | Công nghệ | Vai trò |
|---|---|---|
| Hạ tầng | Docker Compose | Chạy toàn bộ hệ thống trên 1 máy, giới hạn CPU/RAM mỗi container |
| Nguồn giả lập | Python producer | Đọc CSV, phát giao dịch vào Kafka theo thứ tự ngày (điều chỉnh được tốc độ) |
| Ingestion | Apache Kafka (KRaft) | Hàng đợi sự kiện giao dịch |
| Xử lý batch | Apache Spark 3.5 (PySpark) | Làm sạch, sinh surrogate key, nạp dim/fact/snapshot/marts, nhân bản dữ liệu ×k |
| Xử lý streaming | Spark Structured Streaming | Phát hiện giao dịch bất thường (BR7) |
| Lớp raw/staging | Parquet | Lưu dữ liệu thô và đã làm sạch |
| Data warehouse chính | **ClickHouse** | Kimball star schema + marts |
| DWH đối chứng | PostgreSQL | Cùng star schema, để so sánh row store vs column store |
| Data quality | Script kiểm tra SQL (Python) + bảng `audit` | Đánh giá độ đúng dữ liệu, cấp dữ liệu cho BR9 |
| Ứng dụng BI | Apache Superset | Dashboard theo nhóm người dùng (BR1–6, 8–10) |
| Ứng dụng realtime | Streamlit | Trang cảnh báo giao dịch bất thường (BR7) |
| Benchmark | Python script | Chạy truy vấn lặp lại, ghi kết quả ra CSV, vẽ biểu đồ |
| Tùy chọn (bonus) | Spark SQL + Delta Lake | Lakehouse (giống Databricks, có trong list của đề), chỉ dùng cho benchmark |

### Kế hoạch benchmark

| Mã | So sánh | Câu hỏi cần trả lời | Chỉ số đo |
|---|---|---|---|
| **B1** | Kimball star schema vs One Big Table (cùng ClickHouse) | Mô hình Kimball có đáng không? | Thời gian truy vấn từng BR, dung lượng, thời gian nạp, **chi phí thay đổi** (sửa 1 thuộc tính khách: star sửa 1 dòng dim, OBT phải ghi lại hàng triệu dòng fact) |
| **B2** | ClickHouse vs PostgreSQL (cùng star schema) | Column store có hơn row store cho phân tích không? | Thời gian truy vấn từng BR, dung lượng (tỷ lệ nén), thời gian nạp, số truy vấn/giây khi nhiều người dùng dashboard cùng lúc |
| **B3** | Spark Structured Streaming vs ClickHouse Kafka Engine + Materialized View (BR7) | Cách xử lý luồng nào hợp hơn? | Độ trễ end-to-end (từ lúc phát giao dịch đến lúc hiện cảnh báo), throughput (sự kiện/giây), độ phức tạp triển khai |
| **B4** | Khả năng mở rộng của Spark ETL | ETL có chịu được khi dữ liệu lớn lên không? | Thời gian ETL ở ×1/×10/×50, với 2/4/8 core |
| B5 *(tùy chọn)* | ClickHouse vs Spark SQL + Delta Lake | So với lakehouse (công nghệ trong list) thì sao? | Thời gian truy vấn từng BR, dung lượng |

### Phương pháp đo
- Mức dữ liệu: ×1, ×10, ×50 (dừng ở ×20 nếu máy không chịu nổi).
- Mỗi truy vấn chạy 6 lần: bỏ lần đầu (cold cache), lấy **median** và **p95** của 5 lần còn lại; ghi riêng kết quả cold.
- Mọi hệ thống chạy với **cùng giới hạn CPU/RAM** trong Docker để so sánh công bằng.
- Ghi lại version phần mềm và cấu hình máy trong report.
- Bộ truy vấn benchmark = truy vấn thật của từng BR (BR1–BR10), không dùng truy vấn tự chế.

### Liên hệ với 3 tiêu chí đánh giá của đề
- **Độ đúng dữ liệu:** đối soát số dòng, tổng tiền giữa nguồn ↔ staging ↔ DWH; kiểm tra khóa duy nhất, not null, quan hệ fact–dim; **kết quả mỗi BR phải giống nhau** giữa star, OBT, ClickHouse, PostgreSQL.
- **Hiệu năng:** B1–B5.
- **Hiệu quả khai thác qua ứng dụng:** mỗi BR được trả lời bởi dashboard nào, mất bao nhiêu thao tác/giây; có thể làm khảo sát nhỏ với người dùng thử.

---

## [2026-10-06] Chốt stack công nghệ

### Ràng buộc
- Được tự chọn công nghệ, miễn **báo cáo đầy đủ** và **quay được video demo chạy thật**.
- → Ưu tiên công nghệ chạy ổn định trên 1 laptop bằng Docker Compose, ít thành phần dễ hỏng khi demo.

### Stack đã chọn

| Vai trò | Công nghệ | Dùng cho |
|---|---|---|
| Ingestion (giả lập luồng) | **Apache Kafka** | Producer phát lại giao dịch theo thứ tự ngày |
| Xử lý dữ liệu | **Apache Spark 3.5** (batch + Structured Streaming) | Làm sạch, sinh surrogate key, nạp dim/fact; phát hiện giao dịch bất thường (BR7) |
| Lớp raw/staging | Parquet | Lưu dữ liệu thô và dữ liệu đã làm sạch |
| Data warehouse | **ClickHouse** (column store) | Kimball star schema + marts |
| Ứng dụng BI | **Apache Superset** | Dashboard theo từng nhóm người dùng (BR1–6, 8–10) |
| Ứng dụng realtime | **Streamlit** | Trang cảnh báo giao dịch bất thường (BR7) |
| Chất lượng dữ liệu | Script kiểm tra SQL + bảng audit | Đánh giá độ đúng dữ liệu, cấp dữ liệu cho BR9 |

### Giải pháp thay thế để benchmark
> Cập nhật: A1/A2 đã được mở rộng thành kế hoạch benchmark B1–B5 (xem mục "Chốt công nghệ cuối cùng & kế hoạch benchmark"). A1 → B1, A2 → B2.

- **A1 — Mô hình:** Kimball star schema vs One Big Table (bảng phẳng), cùng trên ClickHouse.
- **A2 — Công nghệ lưu trữ:** ClickHouse (column store) vs PostgreSQL (row store), cùng star schema.
- Đo ở 3 mức dữ liệu: **×1 (1,4 triệu), ×10 (14 triệu), ×50 (71 triệu dòng)**.
- Chỉ số đo: thời gian ETL, thời gian chạy truy vấn của từng BR, dung lượng lưu trữ.
- **Cách nhân bản dữ liệu:** bản sao thứ k gán cho tập khách mới (`customer_id` + hậu tố `_k`, key mới), giữ nguyên ngày và địa điểm, số tiền lệch ngẫu nhiên ±5%. Tương đương ngân hàng có gấp k lần khách trong cùng 55 ngày.
- Dữ liệu nhân bản **chỉ dùng để đo hiệu năng**. Kiểm tra độ đúng dữ liệu và demo các BR chạy trên bộ gốc ×1. Nếu máy không chịu nổi ×50 thì dừng ở ×20.

### Lý do
- Kafka + Spark Streaming nằm trong danh sách gợi ý của đề, và phục vụ trực tiếp BR7.
- ClickHouse nhanh cho truy vấn phân tích, chạy Docker gọn, Superset kết nối sẵn.
- Hai phương án thay thế giúp nhóm vừa có so sánh mô hình (Kimball có đáng không?) vừa có so sánh công nghệ → đủ điều kiện cộng bonus.

---

## [2026-10-06] Chốt số BR (nhóm 4 người)

### Quyết định
- n = 4 → tối thiểu **8 BR**, mỗi BR demo thành công = **1 điểm** (4/n, thang 10) của tiêu chí Application Implementation; 8 BR = 8/10 tối đa. *(Đã sửa: trước đó ghi nhầm 0,5 điểm.)*
- Chọn **8 BR chính + 2 BR dự phòng** (BR9, BR10) để phòng BR nào demo không đạt, đồng thời lấy bonus "thêm BR/nhóm người dùng".
- Phân công: **mỗi thành viên phụ trách 2 BR** end-to-end (mart → dashboard → demo).

### Danh sách Business Requirements

| # | Nhóm người dùng | Business requirement | Dữ liệu Kimball dùng | Demo |
|---|---|---|---|---|
| BR1 | Ban điều hành | Theo dõi số lượng, giá trị giao dịch và số khách hoạt động theo ngày/tuần/tháng | `fact_transactions` × `dim_date` | Dashboard KPI + biểu đồ xu hướng |
| BR2 | Ban điều hành | So sánh hành vi giao dịch ngày thường với cuối tuần và theo thứ trong tuần | `fact_transactions` × `dim_date` (cờ weekday/weekend) | Biểu đồ theo thứ |
| BR3 | Quản lý khu vực | Xếp hạng thành phố/khu vực theo giá trị giao dịch, số khách, giá trị trung bình | `fact_transactions` × `dim_location` | Bảng xếp hạng + bộ lọc khu vực |
| BR4 | Marketing/CRM | Phân khúc khách theo RFM (recency tính từ 2016-10-21) để chọn khách cho chiến dịch | `mart_rfm` (tính lại từ fact) | Phân bố phân khúc + xuất danh sách khách |
| BR5 | Marketing/CRM | Hành vi chi tiêu theo nhóm tuổi và giới tính | `fact_transactions` × `dim_customer` | Heatmap tuổi × giới tính |
| BR6 | Phát triển sản phẩm | Cơ cấu loại giao dịch theo khu vực và nhóm khách | `fact_transactions` × `dim_transaction_type` × `dim_customer` | Biểu đồ cơ cấu |
| BR7 | Quản trị rủi ro | Cảnh báo **gần realtime** giao dịch bất thường (vượt p99 hoặc vượt tỷ lệ so với số dư) | Luồng Kafka → Spark Streaming → `fact_transaction_alert` | Trang cảnh báo realtime |
| BR8 | Quản lý tài sản (Wealth) | Danh sách khách số dư cao (top phân vị) để cross-sell sản phẩm đầu tư/tiết kiệm | `fact_customer_balance_snapshot` (semi-additive) | Danh sách lọc theo khu vực/phân khúc |
| BR9 *(dự phòng)* | Nhóm dữ liệu | Theo dõi chất lượng dữ liệu: số bản ghi lỗi, đã sửa, tỷ lệ đối soát nguồn ↔ DWH | Kết quả data tests + bảng audit | Dashboard data quality |
| BR10 *(dự phòng)* | Quản lý khu vực | Phân bố giá trị giao dịch (dải tiền) theo khu vực để định hướng hạn mức/phí | `fact_transactions` × `dim_location` | Histogram theo khu vực |

### Lưu ý khả thi
- Không có BR nào cần lịch sử dài (churn, CLV, giao dịch định kỳ) vì dữ liệu chỉ có 55 ngày và 71% khách chỉ có 1 giao dịch.
- BR5 phụ thuộc việc làm sạch ngày sinh và giới tính. Giá trị lỗi được đưa vào nhóm "Unknown".
- BR8 dựa trên `account_balance` tại thời điểm giao dịch, không phải số dư cuối ngày thật. Cần nêu rõ giả định này trong report.

### Môi trường máy dev
- Docker 28.3, 18 GB RAM, 11 CPU, Java 17 → chạy được Kafka + Spark + lưu trữ + BI bằng Docker Compose. Cần giới hạn RAM cho từng container.

---

## [2026-10-06] Phân tích yêu cầu đồ án (CO5173 — phần Project)

### Bối cảnh
- Môn **Data Engineering (CO5173)**, HK1 2026–2027.
- Đề tài: *Build a system where your data engineering solution is proved successful (in a specific application domain)*.
- Domain: **Banking**. Giải pháp DE: **Kimball dimensional data warehouse**.

### Yêu cầu nội dung của Project
- [ ] Bối cảnh đề tài.
- [ ] Stakeholders, đặc biệt là các **nhóm người dùng (user groups)**.
- [ ] **≥ 2·n business requirements** (n = số thành viên), gắn với mục tiêu kinh doanh của ngân hàng.
- [ ] Nguồn dữ liệu và đặc điểm dữ liệu (dùng lại phần khảo sát bên dưới).
- [ ] Giải pháp DE kèm **lý do (rationale)** + **≥ 1 giải pháp thay thế** để so sánh/benchmark, đặc biệt trong bối cảnh Big Data.
- [ ] Công nghệ **quản lý dữ liệu** (ví dụ: Redshift, Hive, BigQuery, Databricks, Cassandra…) và công nghệ **xử lý dữ liệu** (ví dụ: Flink, Spark Streaming, Kafka Streams…), cùng cách áp dụng chúng vào ứng dụng.
- [ ] Phát triển **ứng dụng** khai thác dữ liệu.
- [ ] Đánh giá theo 3 tiêu chí: (1) **độ đúng của dữ liệu** sau DE, (2) **hiệu năng** của giải pháp DE, (3) **hiệu quả** hỗ trợ khai thác dữ liệu qua ứng dụng.
- [ ] Kết luận.

### Sản phẩm nộp (hạn: cuối khóa, tên file theo group ID)
- Technical report (PDF) — 10% (bắt buộc).
- Slide (PDF) — 5% (bắt buộc).
- Video thuyết trình + demo **20–30 phút** (mp4/webm) — 5% (bắt buộc).
- Product: bằng chứng ứng dụng **chạy và đánh giá được** — 15% (bắt buộc).

### Điểm chính trong rubric
- **Application Implementation (8/10)**: mỗi business requirement được demo thành công được **4/n điểm** → phải demo end-to-end **đủ cả 2n BR** mới đạt tối đa.
- **Technology Content (4/10)**: trình bày kỹ công nghệ, **> 3 ví dụ minh họa**.
- **Application Content (4/10)**: thiếu < 10% nội dung yêu cầu.
- **Evaluation of Technology (1/10)**: > 85% tiêu chí đánh giá được định nghĩa rõ.
- **Report Structure (1/10)** và **Slide structure (2/10)**: từ **4 đến 7 phần**.
- **User Interface Design (2/10)**: hợp ngữ cảnh, tính đến đặc điểm từng nhóm người dùng.
- **Bonus**: thêm giải pháp thay thế có đánh giá tốt, thêm BR/nhóm người dùng, thêm thảo luận kết quả nghiên cứu.

### Hệ quả cho thiết kế
- Kimball là **phương pháp mô hình hóa**, không phải công nghệ → phải chọn thêm công nghệ quản lý và xử lý dữ liệu cụ thể. DuckDB + dbt có thể không đủ thuyết phục trong bối cảnh "Big Data".
- 1,4 triệu dòng là nhỏ → cần **nhân bản dữ liệu (scale ×10, ×50…)** để benchmark hiệu năng.
- Các BR phải chọn sao cho **khả thi với dữ liệu** (lịch sử chỉ 55 ngày, đa số khách có 1 giao dịch — xem mục bên dưới).

### Còn chờ xác định
- ~~Số thành viên nhóm (n)~~ → n = 4, xem mục "Chốt số BR".
- ~~Stack công nghệ cuối cùng và giải pháp thay thế~~ → xem mục "Chốt stack công nghệ".

---

## [2026-10-06] Khảo sát dữ liệu nguồn & định hướng Kimball

### Đã làm
- Tải bộ dữ liệu Kaggle [Banking Data Warehouse Sample](https://www.kaggle.com/datasets/eserkaraceper/banking-data-warehouse-sample) (license CC BY-SA 4.0, dữ liệu synthetic) về thư mục `data/`.
- Profile toàn bộ 8 bảng bằng DuckDB.

### Tổng quan dữ liệu nguồn
- Giao dịch ngân hàng Ấn Độ (đơn vị INR), **2016-08-01 → 2016-10-21** (55 ngày có giao dịch).

| Lớp | File | Số dòng | Ghi chú |
|---|---|---:|---|
| dwh | `fact_transactions.csv` | 1.418.764 | amount, balance, transaction_type |
| dwh | `dim_customer.csv` | 1.046.782 | dob, gender, household, marital, account_status |
| dwh | `dim_location.csv` | 9.354 | chỉ 1 cột `customer_location` |
| dwh | `dim_date.csv` | 55 | chỉ các ngày có giao dịch |
| datamart | `mart_financial_summary.csv` | 55 | tổng hợp theo ngày |
| datamart | `mart_customer_summary.csv` | 1.268.599 | tổng hợp theo khách |
| datamart | `customer_behavior_segmentation.csv` | 1.268.599 | phân khúc hành vi |
| datamart | `mart_rfm_segmentation.csv` | 1.268.599 | phân khúc RFM |

- Toàn vẹn tham chiếu: 100% `fact_transactions.customer_key` khớp với `dim_customer`.

### Phát hiện — vấn đề dữ liệu

**Khóa & grain**
- **Một `customer_id` ứng với nhiều `customer_key`**: 884.265 `customer_id` nhưng có 1.046.782 `customer_key`.
  - 142.360 `customer_id` có nhiều hơn 1 key. Trong số đó, 141.466 ID có các bản ghi **khác ngày sinh** và 60.185 ID có bản ghi **khác giới tính**.
  - → Đây là **nhiều người khác nhau bị trùng ID**, không phải thay đổi kiểu SCD.
- Các mart bị trùng khách: 1.268.599 dòng cho 884.265 `customer_id`, có ID xuất hiện tới 20 dòng (có vẻ mỗi địa điểm sinh 1 dòng).

**Độ sâu lịch sử**
- 71% khách (740.653) chỉ có **1 giao dịch**, tối đa 6 giao dịch/khách.
- → Phân tích hành vi, giao dịch định kỳ, dự đoán churn hầu như không có ý nghĩa.

**Mart có sẵn bị lỗi**
- RFM: `recency` nằm trong khoảng 3.484–3.565 ngày → được tính từ ngày chạy script (~2026), **không phải từ cuối kỳ dữ liệu (2016-10-21)**.
- RFM: hai nhóm "At Risk" và "Potential Loyalists" có số dòng bằng hệt nhau (303.215) → cách chia nhóm máy móc.
- `activity_segment` chỉ có **1 giá trị** duy nhất ("Low Activity").
- `value_segment`: 99,3% là "Low Value".

**Thuộc tính giả lập**
- `household_id`, `marital_status_derived`, `account_status` là do tác giả tự sinh.
- 94% tài khoản ở trạng thái "Closed" → không dùng các cột này để rút ra insight.
- `transaction_type` được suy ra bằng quy tắc, phân bố rất lệch:

| Loại | Số giao dịch | Tỷ lệ |
|---|---:|---:|
| Purchase | 1.114.638 | 78,6% |
| Transfer | 298.845 | 21,1% |
| Bill Payment | 3.834 | 0,3% |
| Loan Payment | 1.447 | 0,1% |

**Dữ liệu bẩn**
- `customer_dob`: có giá trị vô lý (1800-01-01, 2075-12-31), 3.385 dòng NULL.
- `customer_gender`: ngoài M/F còn có `T` (1 dòng) và `Unknown` (1.100 dòng).
- `transaction_amount`: 1.102 giao dịch có số tiền = 0. Max là 1.560.035 INR, trong khi p99 chỉ 20.000.
- `account_balance`: 0 → 115 triệu INR, không có giá trị âm.

**Thiếu**
- Không có giờ giao dịch → không làm được `dim_time`.
- `dim_date` chỉ có 55 ngày, thiếu thuộc tính lịch.
- `dim_location` chưa chuẩn hóa (ví dụ "DELHI" và "NEW DELHI" đang tách riêng), không có cấp thành phố/bang.

### Quyết định thiết kế (Kimball)
- Chỉ dùng `fact_transactions` + `dim_customer` + `dim_location` làm **nguồn**. Không dùng lại các CSV mart có sẵn.
- **Business process:** giao dịch tài khoản khách hàng.
- **Grain:** 1 dòng = 1 giao dịch.
- **Dimensions:**
  - `dim_date`: lịch đầy đủ (2015–2017), thêm thứ, cuối tuần, tuần, quý, ngày lễ Ấn Độ.
  - `dim_customer`: natural key = `customer_id + customer_dob + customer_gender`, tạo surrogate key mới. Làm sạch ngày sinh/giới tính, thêm nhóm tuổi, thêm dòng "Unknown".
  - `dim_location`: chuẩn hóa tên, thêm cấp thành phố/bang.
  - `dim_transaction_type`: tách thành dimension riêng, kèm cờ ghi rõ nhãn được suy ra bằng quy tắc.
- **Measures:**
  - `transaction_amount`: additive.
  - `account_balance`: **semi-additive** (không cộng theo thời gian, chỉ lấy cuối kỳ hoặc trung bình).
- **Fact bổ sung:** periodic snapshot (ví dụ `fact_customer_daily_balance`).
- **Marts:** tính lại từ fact. RFM dùng mốc **2016-10-21** cho recency, gộp trùng theo surrogate key.
- **Stack đề xuất:** DuckDB (hoặc Postgres) + dbt, chia 3 lớp `staging` → `core` → `marts`. Thêm dbt test: unique, not_null, relationships.

### Giới hạn đã chấp nhận
- Không có SCD thật (không có ngày hiệu lực, chỉ 55 ngày). Nếu muốn minh họa SCD2 thì phải tự giả lập.
- Chỉ có 1 business process → không làm được bus matrix nhiều fact. Có thể giả lập thêm 1 quy trình (mở tài khoản hoặc khoản vay) để minh họa conformed dimension.
- Bộ gốc có thể tham khảo để lấy thêm giờ giao dịch: Kaggle "Bank Customer Segmentation (1M+ Transactions)".

### Việc tiếp theo
> Cập nhật: stack dbt + DuckDB đã được thay bằng Kafka + Spark + ClickHouse (xem mục "Chốt stack công nghệ").

- [ ] Khởi tạo dự án dbt + DuckDB.
- [ ] Viết lớp staging (làm sạch).
- [ ] Viết các dimension và bảng fact.
- [ ] Viết mart RFM đã sửa lỗi recency.
- [ ] Thêm dbt tests.
