# Benchmark Phase 2 — mức dữ liệu ×1

- **Ngày đo:** 22/10/2026. Lần chạy hoàn chỉnh: `make bench-all` (truy vấn lúc 20:34–20:35); đo lại tải đồng thời lúc 22:38 và B3 lúc 22:40 (lý do ở mục 8).
- **Dữ liệu:** kho sau P2-8 gồm **1.057.571 giao dịch** (1.056.320 Berka và 1.251 giao dịch kịch bản), 5.118 phiên bản khách hàng, 4.500 tài khoản, 77 khu vực, 15 hồ sơ giao dịch.
- **Số liệu gốc:** [docs/results/p2/](results/p2/) gồm các file `*.csv` và `*.json`, cùng log console của từng bước trong `logs/`.
- **Code:** [spark/jobs/benchmark.py](../spark/jobs/benchmark.py) (B1, B2), [spark/jobs/bench_ops.py](../spark/jobs/bench_ops.py) (B3), [spark/jobs/bench_spark.py](../spark/jobs/bench_spark.py) (B4).

```bash
make bench-all       # bench-load → bench → bench-concurrency → bench-update → bench-ops → bench-spark
```

Mọi phép thử ghi (B1 cập nhật, B3) chạy trên **bản sao**: database `bench` trong ClickHouse, database `dwpg` và schema `bench_ops` trong PostgreSQL. Kho `dw` và kho vận hành `ops` chỉ được đọc.

---

## 1. Các phép so sánh — mỗi phép chỉ đổi 1 yếu tố

| Mã | So sánh | Giữ nguyên | Đổi | Câu hỏi |
|---|---|---|---|---|
| **B1** | Star schema vs One Big Table | ClickHouse | Mô hình dữ liệu | Mô hình Kimball có đáng chi phí join không? |
| **B2** | ClickHouse vs PostgreSQL | Star schema | Công nghệ kho (cột / dòng) | Lưu theo cột có lợi cho các BR không? (quyết định Q-K) |
| **B3** | Kho vận hành PostgreSQL riêng vs dồn hết vào ClickHouse | Dữ liệu cảnh báo, thao tác của app và pipeline | Kiến trúc khối E | Có cần một kho vận hành riêng không? |
| **B4** | Apache Spark 1 / 2 / 4 / 8 lõi; 1 lô vs 72 lô tháng | Phép biến đổi của pipeline | Số lõi, cách chia lô | Pipeline có mở rộng theo lõi không? Chia lô tốn bao nhiêu? |

Ba giải pháp kho trong B1 và B2:

| Mã | Giải pháp | Vai trò |
|---|---|---|
| `ch_star` | Kimball star schema trên ClickHouse 25.3 (1 fact + 5 dimension, SCD2) | **Đề xuất** |
| `ch_obt` | One Big Table trên ClickHouse: mọi thuộc tính dimension (cả 2 vai trò khu vực, hạng và khoảng hiệu lực của phiên bản) chép vào từng dòng fact | Đối chứng B1 |
| `pg_star` | Cùng star schema trên PostgreSQL 16.4, có PK, FK và index | Đối chứng B2 |

Ba giải pháp được nạp từ **cùng một bản xuất CSV** của kho `dw`, nên có cùng số dòng (1.057.571 dòng fact ở cả ba).

---

## 2. Môi trường

| Hạng mục | Giá trị |
|---|---|
| Máy | Apple M3 Pro, 11 lõi CPU, 18 GB RAM, macOS |
| Docker | Docker Desktop 28.3.3, Compose 2.39.2; máy ảo 11 vCPU, 7,65 GiB RAM, nhân Linux 6.10.14 |
| Container khi đo | **chỉ 4 container BankDW** (người dùng đã tắt các tiến trình khác) |
| ClickHouse | 25.3.14.14; giới hạn 2,5 GB; `max_threads` = auto(11); mark cache 256 MiB, tắt uncompressed cache; **tắt log hệ thống chạy ngầm** (mục 8) |
| PostgreSQL | 16.4; giới hạn 1 GB; `shared_buffers` 256 MB (server); trong phiên: `work_mem` 64 MB, `max_parallel_workers_per_gather` 4, `jit` off, `effective_cache_size` 4 GB. **Giống cấu hình benchmark Phase 1** |
| Spark | 3.5.6, PySpark, Python 3.8, `local[n]`, driver 2 GB, `spark.sql.shuffle.partitions` 8; giới hạn container 3 GB |
| Client đo | Python trong container Spark (`clickhouse-connect` 0.8.18, `psycopg2` 2.9.9), cùng mạng Docker |

---

## 3. Cách đo

| Hạng mục | Cách làm |
|---|---|
| Truy vấn (B1, B2) | 9 truy vấn lấy từ app, phủ BR01, BR02, BR03, BR08. Mỗi truy vấn chạy **6 lần** trên mỗi giải pháp |
| Cold / warm | Lần 1 là **cold**: xoá mark cache và uncompressed cache của ClickHouse trước khi chạy. Không xoá được page cache của hệ điều hành từ trong container, nên "cold" chỉ là *lần chạy đầu*. Lần 2–6 là **warm**: báo median và p95 |
| Thời gian | Wall-clock phía client, **gồm cả thời gian truyền kết quả** |
| Kiểm tra đúng | Kết quả `ch_obt` và `pg_star` được so **từng dòng, từng ô** với `ch_star` (số làm tròn tới 0,01) |
| Đồng thời (B2) | 1 / 4 / 8 client chạy vòng lặp hỗn hợp **BR01 theo hạng → BR02 một tài khoản ngẫu nhiên → BR08 theo hạng** (mỗi nhóm người dùng một truy vấn) trong 15 giây; báo số truy vấn/giây, median và p95 |
| Cập nhật (B1) | Sửa Type 1 rồi sửa ngược lại: đo thời gian và số dòng phải ghi lại. ClickHouse One Big Table dùng `ALTER TABLE … UPDATE` và chờ mutation xong (`mutations_sync = 2`) |
| B3 | Mỗi thao tác gọi tuần tự, 1 client, đúng như app và pipeline đang làm: 200 lần đổi trạng thái, 200 lần ghi bước, 50 lần đọc. Thêm 5 phép thử toàn vẹn |
| B4 | Mỗi cấu hình (số lõi × cách chia lô) chạy trong **một JVM mới**; tách riêng thời gian khởi động phiên Spark |

### Bộ truy vấn

| Mã | BR | Câu hỏi | Star schema: join | One Big Table | Phép tính nặng | Dòng |
|---|---|---|---|---|---|---:|
| `br01_kpi_1998` | BR01 | KPI một kỳ (1998) | fact ⋈ profile | không join | `sumIf` | 1 |
| `br01_by_month` | BR01 | Biểu đồ theo tháng, toàn bộ lịch sử | fact ⋈ profile ⋈ date | không join | gom 75 tháng | 75 |
| `br01_by_tier_1998` | BR01 | Theo hạng thành viên, có số khách | fact ⋈ profile ⋈ customer | không join | **đếm khách khác nhau** | 4 |
| `br01_by_region` | BR01 | Theo vùng của chi nhánh | fact ⋈ profile ⋈ account ⋈ district (vai trò chi nhánh) | không join | 3 join | 8 |
| `br02_account_detail` | BR02 | Toàn bộ giao dịch của tài khoản 2378 | fact ⋈ account ⋈ profile | không join | lọc 1 tài khoản, trả 437 dòng | 437 |
| `br02_account_totals` | BR02 | Tổng của tài khoản 2378, năm 1998 | fact ⋈ account ⋈ profile | không join | lọc tài khoản + kỳ | 1 |
| `br03_r1_scan` | BR03 | Quy tắc R1 trên toàn bộ lịch sử | fact ⋈ profile | không join | **window 6 tháng** trên 4.500 tài khoản; kết quả 1.698 = đúng số cảnh báo R1 trong `ops.alert` | 1 |
| `br08_tier_old_new` | BR08 | Theo hạng, kỳ cũ và kỳ mới | fact ⋈ profile ⋈ customer (SCD2) | không join | đếm khách khác nhau | 8 |
| `br08_client_versions` | BR08 | Các phiên bản hạng của khách 24 | fact ⋈ profile ⋈ customer | không join | lọc 1 khách | 2 |

---

## 4. B1 + B2 — Thời gian truy vấn (ms)

| Truy vấn | Star / ClickHouse: cold | median | p95 | One Big Table / ClickHouse: cold | median | p95 | Star / PostgreSQL: cold | median | p95 | **One Big Table nhanh hơn star** (B1) | **So với ClickHouse star, PostgreSQL** (B2) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| br01_kpi_1998 | 24,0 | 12,8 | 21,1 | 9,1 | 8,6 | 13,6 | 62,5 | 41,7 | 51,0 | 1,5× | chậm hơn 3,3× |
| br01_by_month | 32,1 | 28,1 | 31,1 | 14,4 | 11,8 | 15,1 | 131,5 | 105,9 | 108,8 | 2,4× | chậm hơn 3,8× |
| br01_by_tier_1998 | 17,4 | 14,6 | 15,7 | 7,5 | 7,1 | 7,7 | 378,3 | 323,3 | 339,3 | 2,1× | **chậm hơn 22,1×** |
| br01_by_region | 18,3 | 14,9 | 16,3 | 7,5 | 6,8 | 7,5 | 62,0 | 51,4 | 54,0 | 2,2× | chậm hơn 3,4× |
| br03_r1_scan | 46,6 | 45,9 | 51,7 | 30,2 | 30,0 | 30,8 | 1.031,2 | 1.029,6 | 1.065,5 | 1,5× | **chậm hơn 22,4×** |
| br08_tier_old_new | 45,8 | 37,1 | 37,7 | 23,2 | 22,5 | 25,3 | 622,8 | 623,2 | 651,2 | 1,6× | **chậm hơn 16,8×** |
| br02_account_detail | 21,2 | 17,2 | 18,0 | 7,5 | 5,8 | 6,2 | 2,0 | 1,0 | 1,1 | 3,0× | **nhanh hơn 17×** |
| br02_account_totals | 7,0 | 6,2 | 6,6 | 3,6 | 3,4 | 3,4 | 0,7 | 0,3 | 0,3 | 1,8× | **nhanh hơn 21×** |
| br08_client_versions | 26,1 | 18,3 | 21,5 | 6,9 | 5,4 | 5,6 | 1,5 | 0,4 | 0,5 | 3,4× | **nhanh hơn 46×** |

Tỉ lệ tính trên warm median. **18/18 cặp so sánh cho kết quả khớp** từng dòng, từng ô ([result_check.csv](results/p2/result_check.csv)).

---

## 5. B1 + B2 — Dung lượng, thời gian nạp, chi phí cập nhật, tải đồng thời

### 5.1 Dung lượng (MB trên đĩa)

| Giải pháp | Fact | Tổng | So với star / ClickHouse |
|---|---:|---:|---:|
| Star / ClickHouse | 18,5 (54,0 chưa nén) | **18,6** | 1× |
| One Big Table / ClickHouse | 25,5 (111,5 chưa nén) | **25,5** | 1,37× |
| Star / PostgreSQL | 102,1 bảng + 58,6 index = 160,7 | **162,5** | 8,7× |

### 5.2 Thời gian nạp (từ cùng bản CSV, đo trong [load.json](results/p2/load.json))

| Giải pháp | Thời gian | Chi tiết |
|---|---:|---|
| Star / ClickHouse | **0,96 s** | INSERT CSV vào 6 bảng (fact 0,93 s) |
| One Big Table / ClickHouse | 0,96 s + **1,35 s** | Phải nạp star (hoặc join ở Spark) rồi join 6 bảng thành 1 |
| Star / PostgreSQL | **4,64 s** | COPY 2,18 s + PK, FK, index, `ANALYZE` 2,43 s |

### 5.3 Chi phí sửa một thuộc tính Type 1 ([update_cost.csv](results/p2/update_cost.csv))

| Thao tác | Star / ClickHouse | One Big Table / ClickHouse | Star / PostgreSQL |
|---|---|---|---|
| U1 sửa tên khu vực 1 (dùng ở cả 2 vai trò) | ghi **1 dòng**, 6,0 / 6,7 ms | ghi lại **150.058 dòng** (mutation), 99,3 / 59,1 ms | `UPDATE` 1 dòng, 4,1 / 0,5 ms |
| U2 sửa ngày sinh khách 24 (2 phiên bản) | ghi **2 dòng**, 8,0 / 3,8 ms | ghi lại **330 dòng**, 42,6 / 31,6 ms | `UPDATE` 2 dòng, 1,9 / 0,6 ms |

Mỗi ô có hai số: lần sửa và lần sửa ngược lại. Sau khi sửa và sửa ngược, cả ba giải pháp vẫn cho cùng kết quả `br01_by_region`.

Lưu ý: One Big Table đúng với SCD2 *theo thời điểm giao dịch* (hạng được chép vào dòng khi nạp). Nhưng mỗi lần sửa một thuộc tính Type 1 phải ghi lại mọi dòng fact liên quan, và số dòng này tăng theo dữ liệu.

### 5.4 Nhiều người dùng cùng lúc ([concurrency.csv](results/p2/concurrency.csv))

| Giải pháp | 1 client: truy vấn/giây | median / p95 (ms) | 4 client: truy vấn/giây | median / p95 | 8 client: truy vấn/giây | median / p95 |
|---|---:|---|---:|---|---:|---|
| Star / ClickHouse | 52,1 | 14,1 / 34,8 | 82,0 | 42,2 / 90,9 | 93,2 | 76,5 / 148,1 |
| One Big Table / ClickHouse | 91,3 | 6,6 / 24,1 | 215,8 | 14,1 / 38,4 | 251,2 | 25,1 / 66,2 |
| Star / PostgreSQL | 4,4 | 222,9 / 454,1 | 12,2 | 266,6 / 835,7 | 12,4 | 366,8 / 1.933,5 |

---

## 6. B3 — Kiến trúc kho vận hành: PostgreSQL riêng vs dồn vào ClickHouse

ClickHouse được thử theo hai cách:
- **versioned:** ghi một dòng phiên bản mới (ReplacingMergeTree) và đọc bằng `FINAL`; luật chuyển trạng thái phải kiểm tra ở phía client;
- **mutation:** `ALTER TABLE … UPDATE` và chờ mutation xong.

### 6.1 Thời gian ([ops_latency.csv](results/p2/ops_latency.csv), ms mỗi thao tác)

| Thao tác (như app / pipeline) | n | PostgreSQL median / p95 | ClickHouse median / p95 | PostgreSQL nhanh hơn |
|---|---:|---|---|---:|
| W1 đổi trạng thái NEW → IN_REVIEW + ghi lịch sử | 200 | 0,19 / 0,67 | 5,92 / 10,70 (versioned) | 31× |
| W1 đổi trạng thái IN_REVIEW → RESOLVED + ghi lịch sử | 200 | 0,17 / 0,31 | 11,91 / 18,17 (mutation) | 70× |
| W2 ghi 1 bước pipeline (bắt đầu + kết thúc) | 200 | 0,32 / 0,54 | 2,46 / 3,59 (versioned) | 7,7× |
| W3 danh sách 50 cảnh báo mới nhất | 50 | 0,11 / 0,18 | 2,61 / 4,58 | 24× |
| W3 một cảnh báo + lịch sử xử lý | 50 | 0,06 / 0,10 | 3,26 / 5,47 | 54× |

W2 trên ClickHouse tạo ra **400 data part** cho 400 câu INSERT nhỏ (mỗi INSERT một part). Đây chính là kiểu ghi mà ClickHouse khuyên tránh.

### 6.2 Toàn vẹn dữ liệu — kho tự bảo đảm, không cần app ([ops_integrity.csv](results/p2/ops_integrity.csv))

| Bảo đảm (BR cần) | PostgreSQL | ClickHouse |
|---|:-:|:-:|
| I1 chặn trạng thái không hợp lệ (`'DONE'`) — BR04 | ✅ CHECK | ❌ ghi được |
| I2 chặn chuyển trạng thái sai (RESOLVED → NEW) — BR04 | ✅ hàm `set_alert_status` | ❌ ghi được |
| I3 chặn cảnh báo trùng (cùng tài khoản, quy tắc, kỳ) — BR03 | ✅ UNIQUE | ❌ ghi được 2 cảnh báo |
| I4 chặn lịch sử của cảnh báo không tồn tại — BR04 | ✅ khóa ngoại | ❌ ghi được |
| I5 đổi trạng thái và ghi lịch sử trong một giao dịch: bước 2 lỗi thì bước 1 được hoàn tác — BR04 | ✅ transaction | ❌ trạng thái đã đổi, lịch sử không có |

Thêm một giới hạn đo được: mutation **không sửa được cột phiên bản** của ReplacingMergeTree (`CANNOT_UPDATE_COLUMN`), nên phải chọn một trong hai cách: ghi phiên bản mới, hoặc mutation (khi đó không cập nhật được thời điểm đổi trạng thái).

---

## 7. B4 — Pipeline Spark: số lõi và cách chia lô ([spark_scaling.csv](results/p2/spark_scaling.csv))

Phép đo dùng phép biến đổi của pipeline trên 72 bản chụp lô lịch sử: đọc Parquet → chuẩn hóa mã → tra tài khoản, hồ sơ giao dịch, chủ tài khoản và **phiên bản SCD2 hiệu lực tại ngày giao dịch** → ghi Parquet. Phép đo không ghi vào ClickHouse hay PostgreSQL. Mọi lần chạy ra **đúng 1.056.320 dòng, 0 dòng thiếu phiên bản**.

| Lõi | 1 lô (cả lịch sử): khởi động phiên | biến đổi | dòng/giây | 72 lô tháng: khởi động phiên | biến đổi | dòng/giây | 72 lô chậm hơn 1 lô |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,9 s | 7,7 s | 137.164 | 1,9 s | 30,6 s | 34.526 | 4,0× |
| 2 | 1,7 s | 6,0 s | 174.770 | 2,1 s | 32,5 s | 32.478 | 5,4× |
| 4 | 2,0 s | 5,4 s | 194.009 | 1,6 s | 31,8 s | 33.237 | 5,9× |
| 8 | 1,6 s | 5,4 s | 196.139 | 1,9 s | 29,6 s | 35.718 | 5,5× |

**Thời gian pipeline thật** (đọc từ `ops.pipeline_step`, gồm cả ghi ClickHouse, ghi log và đối chiếu): 72 lô tháng có median **3,4 s/lô**, p95 5,3 s, tổng 273,5 s, median 4.334 dòng/s. Median theo bước: ingest 0,55 · validate 0,85 · standardize 0,15 · load_dimensions 0,14 · **load_fact 1,42** · reconcile 0,15 (s) ([pipeline_steps.csv](results/p2/pipeline_steps.csv)).

---

## 8. Sự cố trong lúc đo và cách xử lý

| # | Sự cố | Nguyên nhân | Xử lý | Ảnh hưởng tới số liệu |
|---|---|---|---|---|
| 1 | Lần đo đầu (P2-8, cùng ngày) dùng PostgreSQL mặc định (`shared_buffers` 128 MB, `work_mem` 4 MB, 2 worker) | Chưa áp cấu hình Phase 1 | Đặt `shared_buffers` 256 MB trong [docker-compose.yml](../docker-compose.yml); đặt `work_mem`, worker, `jit` trong phiên benchmark | Số liệu lần đầu bị thay; kho `ops` vẫn giữ `work_mem` mặc định |
| 2 | Nạp PostgreSQL mất **42,0 s** | Có PK và FK ngay khi COPY: kiểm tra trên từng dòng | Tạo PK, FK, index **sau** COPY (giống Phase 1) | Còn 4,64 s |
| 3 | ClickHouse báo `MEMORY_LIMIT_EXCEEDED` ở `br02_account_detail` (bộ đếm tổng 2,20 GiB, RSS thật 1,30 GiB) | Các log hệ thống chạy ngầm (`trace_log` 4 triệu dòng, `text_log`, `metric_log`…) giữ bộ đệm và luồng ghi | [warehouse/clickhouse/config.d/system_logs.xml](../warehouse/clickhouse/config.d/system_logs.xml) tắt chúng (giữ `query_log`, `part_log`, `backup_log`, `error_log`); xoá các bảng log cũ | RSS nền 1,2 GB → 0,74 GB; chạy lại toàn bộ |
| 4 | `br01_by_month` lệch giữa hai hệ | Tháng không có tiền vào: PostgreSQL `sum` trả `NULL`, ClickHouse `sumIf` trả 0 | Thêm `coalesce` vào truy vấn PostgreSQL | Lỗi của truy vấn, không phải dữ liệu |
| 5 | Mutation ClickHouse lỗi `CANNOT_UPDATE_COLUMN` | Không được sửa cột phiên bản của ReplacingMergeTree | Chỉ sửa `status`; ghi lại thành phát hiện của B3 | — |
| 6 | **Máy ngủ khoảng 2 giờ** trong lúc `make bench-all` đang đo tải đồng thời: PostgreSQL bị timeout | macOS ngủ khi không dùng | Giữ máy thức; **đo lại** tải đồng thời (22:38) và B3 (22:40) | Bỏ số liệu tải đồng thời của lần bị ngắt (log của lần đo lại đã ghi đè log cũ) |
| 7 | B3 đếm 800 part thay vì 400 | `system.part_log` còn giữ part của lần chạy trước | Chỉ đếm từ lúc bắt đầu lần chạy | Đã chạy lại |

---

## 9. Đối chiếu giả thuyết

| Giả thuyết | Kết quả ×1 |
|---|---|
| **H1** ClickHouse nhanh hơn PostgreSQL ở truy vấn tổng hợp, và tốn ít chỗ hơn | **Đúng.** Nhanh hơn 3,3–22,4× ở 6 truy vấn tổng hợp; tốn ít chỗ hơn 8,7×. Khi nhiều người dùng: 93 so với 12 truy vấn/giây ở 8 client |
| **H2** Với tra cứu 1 tài khoản hoặc 1 khách, index B-tree của PostgreSQL thu hẹp hoặc đảo khoảng cách | **Đúng, đảo hẳn.** PostgreSQL nhanh hơn 17–46×, nhưng ClickHouse vẫn dưới 20 ms |
| **H3** Thời gian một lô chủ yếu là chi phí cố định, không phụ thuộc số dòng | **Đúng.** 72 lô tháng chậm hơn 1 lô 4–6×; thêm lõi gần như không giúp (1 → 8 lõi: 7,7 → 5,4 s) vì dữ liệu ×1 chỉ khoảng 1 triệu dòng |
| **H4** (B1) One Big Table nhanh hơn star một chút; star tốn ít chỗ hơn và sửa rẻ hơn | **Đúng.** One Big Table nhanh hơn 1,5–3,4× (chênh tuyệt đối 3–16 ms), nhưng tốn chỗ hơn 1,37× (gấp 2× khi chưa nén). Sửa tên 1 khu vực phải ghi lại 150.058 dòng thay vì 1 dòng |
| **H5** (B3) Kho vận hành nên tách sang PostgreSQL | **Đúng.** PostgreSQL nhanh hơn 8–70× ở mọi thao tác của app và pipeline, và tự bảo đảm 5/5 ràng buộc mà ClickHouse không bảo đảm được |

---

## 10. Chưa đo / giới hạn

| Hạng mục | Trạng thái |
|---|---|
| Mức ×10, ×50 | **Chưa đo** (theo yêu cầu: chạy ×1 trước, xem kết quả rồi mới quyết định). Ở ×1, Spark không hưởng lợi từ thêm lõi; cần dữ liệu lớn hơn mới thấy được xu hướng mở rộng |
| Cold thực sự (đọc từ đĩa) | Không làm được trong container (không xoá được page cache) |
| PostgreSQL được tinh chỉnh sâu hơn (partition theo năm, BRIN index, materialized view) | Không làm; dùng cùng cấu hình như Phase 1 để so sánh được |
| Phép chiếu (projection) theo tài khoản trên ClickHouse cho BR02 | Không làm; ghi là hướng cải thiện |
| Môi trường | 1 máy, 1 máy ảo dùng chung cho cả hai hệ; client chạy trên cùng máy |
