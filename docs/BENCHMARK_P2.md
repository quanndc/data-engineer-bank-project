# Benchmark Phase 2 — mức dữ liệu ×1 và ×10 (có giải thích)

> **Cách đọc tài liệu này.** Mục 1 cho biết benchmark trả lời câu hỏi gì. Mục 2 giải nghĩa các thuật ngữ dùng trong bảng. Từ mục 4 trở đi, mỗi phép đo có cùng một khung: **Câu hỏi → Cách đo → Đọc bảng thế nào → Kết quả → Kết luận**. Nếu chỉ cần kết luận, đọc mục 1, mục 10 (×1) và mục 12.9 (×10).

- **Ngày đo:** 22/10/2026. Lần chạy hoàn chỉnh bằng `make bench-all` (phần truy vấn chạy lúc 20:34–20:35); phần nhiều người dùng đo lại lúc 22:38, B3 đo lại lúc 22:40 (lý do ở mục 9).
- **Dữ liệu đo:** toàn bộ kho sau P2-8: **1.057.571 giao dịch** (1.056.320 của Berka và 1.251 của các kịch bản KB), 5.118 phiên bản khách hàng, 4.500 tài khoản, 77 khu vực, 15 loại giao dịch. Đây là mức **×1**, tức dữ liệu gốc, chưa nhân bản.
- **Số liệu gốc:** [docs/results/p2/](results/p2/) (file `.csv`, `.json`) và log màn hình của từng bước trong [docs/results/p2/logs/](results/p2/logs/).
- **Code:** [benchmark.py](../spark/jobs/benchmark.py) (B1, B2), [bench_ops.py](../spark/jobs/bench_ops.py) (B3), [bench_spark.py](../spark/jobs/bench_spark.py) (B4). Chạy lại tất cả: `make bench-all`.
- **An toàn dữ liệu:** mọi phép thử có ghi dữ liệu đều chạy trên **bản sao** (database `bench` trong ClickHouse; database `dwpg` và schema `bench_ops` trong PostgreSQL). Kho thật `dw` và kho vận hành `ops` chỉ được đọc.

---

## 1. Benchmark này trả lời câu hỏi gì?

Giải pháp của nhóm gồm ba lựa chọn chính: **(1)** mô hình Kimball (star schema), **(2)** đặt kho trên ClickHouse, **(3)** tách một kho vận hành riêng trên PostgreSQL. Ngoài ra pipeline chạy bằng **Spark theo lô tháng**. Benchmark kiểm tra từng lựa chọn bằng cách so nó với một phương án thay thế hợp lý.

**Nguyên tắc: mỗi phép so sánh chỉ đổi đúng một yếu tố, mọi thứ khác giữ nguyên.** Nhờ vậy khi kết quả khác nhau, ta biết chắc là do yếu tố đã đổi.

| Mã | So sánh | Giữ nguyên | Chỉ đổi | Câu hỏi bằng lời thường |
|---|---|---|---|---|
| **B1** | Star schema ↔ One Big Table | Cùng ClickHouse, cùng dữ liệu | **Cách tổ chức bảng** | Chia dữ liệu thành bảng fact + các bảng dimension (phải nối bảng khi truy vấn) có đáng không, hay gộp hết thành một bảng to cho nhanh? |
| **B2** | ClickHouse ↔ PostgreSQL | Cùng star schema, cùng dữ liệu | **Phần mềm cơ sở dữ liệu** (lưu theo cột ↔ lưu theo dòng) | Dùng ClickHouse làm kho có thật sự nhanh hơn một cơ sở dữ liệu thông thường không? |
| **B3** | Kho vận hành trên PostgreSQL ↔ trên ClickHouse | Cùng dữ liệu cảnh báo, cùng thao tác | **Nơi lưu dữ liệu vận hành** | Có cần thêm PostgreSQL không, hay để luôn cảnh báo, lịch sử xử lý, log pipeline trong ClickHouse cho gọn? |
| **B4** | Spark 1 / 2 / 4 / 8 lõi; 1 lô ↔ 72 lô | Cùng phép biến đổi dữ liệu | **Số lõi CPU, cách chia lô** | Thêm CPU có làm pipeline nhanh hơn không? Chia thành 72 lô tháng thì tốn thêm bao nhiêu? |

---

## 2. Giải nghĩa thuật ngữ

| Thuật ngữ | Nghĩa |
|---|---|
| **ms** | mili giây = 1/1000 giây. 100 ms = 0,1 giây; con người bắt đầu cảm thấy "chậm" từ khoảng 100–300 ms |
| **Truy vấn** | Một câu SQL hỏi kho, ví dụ "tổng tiền vào năm 1998 là bao nhiêu" |
| **Cold** (lần chạy lạnh) | Lần chạy **đầu tiên** của một truy vấn, sau khi xoá bộ nhớ đệm của ClickHouse. Giống lần đầu mở một ứng dụng: thường chậm hơn |
| **Warm** (lần chạy nóng) | Các lần chạy **sau** (lần 2–6), khi dữ liệu đã nằm sẵn trong bộ nhớ. Đây là tình huống thường gặp khi người dùng bấm xem nhiều lần |
| **Median** (trung vị) | Xếp 5 lần chạy warm từ nhanh đến chậm, lấy **lần ở giữa** (lần thứ 3). Cho biết thời gian "điển hình", không bị kéo lệch bởi một lần bất thường |
| **p95** (phân vị 95) | Mốc mà **95% số lần chạy nhanh hơn hoặc bằng**. Cho biết trường hợp chậm nhưng vẫn thường gặp. Với chỉ 5 lần đo, p95 gần như là lần chậm nhất |
| **Nhanh hơn N×** | Lấy median của bên chậm chia cho median của bên nhanh. Ví dụ 41,7 ms ÷ 12,8 ms = 3,3× |
| **Star schema** (`ch_star`) | Mô hình Kimball của nhóm: 1 bảng **fact** (mỗi giao dịch 1 dòng, chỉ có số tiền và các khóa) + 5 bảng **dimension** (ngày, tài khoản, khách hàng, khu vực, loại giao dịch). Muốn biết "giao dịch này của khách hạng nào" phải **join** (nối) fact với `dim_customer` |
| **One Big Table** (`ch_obt`) | Phương án gộp: chép mọi thông tin của dimension vào **từng dòng** giao dịch (tên khu vực, hạng, giới tính…). Truy vấn không cần join, nhưng thông tin bị lặp lại hàng triệu lần |
| **Join** | Nối hai bảng qua khóa chung, ví dụ nối giao dịch với khách hàng qua `customer_sk` |
| **Lưu theo cột / theo dòng** | ClickHouse lưu **từng cột** riêng: truy vấn chỉ cần cột `amount` thì chỉ đọc cột đó, và nén rất tốt. PostgreSQL lưu **từng dòng** liền nhau: đọc 1 dòng rất nhanh, nhưng tính tổng 1 cột phải đọc cả dòng |
| **Index** (chỉ mục) | "Mục lục" giúp PostgreSQL tìm ngay các dòng của 1 tài khoản mà không phải đọc cả bảng. Tốn thêm dung lượng |
| **Truy vấn/giây (q/s)** | Trong 1 giây hệ thống trả lời được bao nhiêu truy vấn khi nhiều người dùng cùng hỏi. Càng cao càng tốt |
| **Client** | Một "người dùng ảo" gửi truy vấn liên tục. 8 client ≈ 8 người cùng dùng app một lúc |
| **Mutation** (ClickHouse) | Cách ClickHouse sửa dữ liệu đã ghi: nó **ghi lại cả khối dữ liệu** chứa các dòng cần sửa, chứ không sửa tại chỗ như PostgreSQL |
| **ReplacingMergeTree, FINAL** | Cách nhóm dùng để "sửa" trong ClickHouse: ghi **thêm một dòng mới** cùng khóa, dòng mới hơn thắng. Khi đọc phải thêm `FINAL` để chỉ lấy dòng mới nhất |
| **Data part** | Mỗi lần INSERT vào ClickHouse tạo ra một "khối" dữ liệu mới trên đĩa; sau đó ClickHouse tự gộp dần. Quá nhiều INSERT nhỏ thì sinh quá nhiều khối, ClickHouse khuyên tránh |
| **Transaction** (giao dịch CSDL) | Nhóm nhiều lệnh thành một khối "hoặc làm hết, hoặc không làm gì". Ví dụ đổi trạng thái + ghi lịch sử: nếu ghi lịch sử lỗi thì trạng thái cũng được hoàn tác |
| **Ràng buộc (constraint)** | Luật do chính cơ sở dữ liệu kiểm tra: CHECK (giá trị hợp lệ), UNIQUE (không trùng), khóa ngoại (phải tham chiếu tới dòng có thật) |
| **Lõi (core)** | Số nhân CPU Spark được dùng song song |

---

## 3. Môi trường đo

| Hạng mục | Giá trị | Vì sao quan trọng |
|---|---|---|
| Máy | Apple M3 Pro, 11 lõi CPU, 18 GB RAM | Mọi con số chỉ có nghĩa trên máy này |
| Docker | Docker Desktop 28.3.3; máy ảo 11 vCPU, 7,65 GiB RAM | Tất cả chạy chung một máy ảo |
| Container chạy lúc đo | **chỉ 4 container BankDW** (các tiến trình khác đã tắt) | Không bị chương trình khác tranh CPU, RAM |
| ClickHouse | 25.3.14.14; tối đa 2,5 GB RAM; dùng đủ 11 luồng; tắt các log hệ thống chạy ngầm (mục 9) | |
| PostgreSQL | 16.4; tối đa 1 GB RAM; **cấu hình giống benchmark Phase 1**: `shared_buffers` 256 MB, `work_mem` 64 MB, tối đa 4 worker song song, tắt JIT | Để so sánh công bằng và nhất quán với Phase 1 |
| Spark | 3.5.6, chạy cục bộ, driver 2 GB | |
| Nơi chạy lệnh đo | Python trong container Spark, cùng mạng Docker | Thời gian đo là thời gian người gọi chờ, gồm cả nhận kết quả |

---

## 4. Bộ truy vấn dùng cho B1 và B2

9 truy vấn này lấy đúng từ những gì app hỏi kho khi người dùng xem các tab. Mỗi truy vấn được viết 3 lần, một lần cho mỗi giải pháp, và **phải cho ra cùng kết quả**.

| Mã | Thuộc BR | Hỏi gì (lời thường) | Nặng hay nhẹ? | Số dòng trả về |
|---|---|---|---|---:|
| `br01_kpi_1998` | BR01 | Năm 1998 có bao nhiêu giao dịch, tổng tiền vào, tổng tiền ra? | Đọc khoảng 322 nghìn giao dịch rồi cộng | 1 |
| `br01_by_month` | BR01 | Tiền vào/ra từng tháng từ 1993 đến 1999 (để vẽ biểu đồ) | Đọc toàn bộ hơn 1 triệu giao dịch, gom theo 75 tháng | 75 |
| `br01_by_tier_1998` | BR01 | Năm 1998, mỗi hạng thành viên có bao nhiêu giao dịch, tiền vào/ra, **bao nhiêu khách** | Nặng: phải nối với bảng khách hàng và đếm khách không trùng | 4 |
| `br01_by_region` | BR01 | Năm 1998, tiền vào/ra theo vùng của chi nhánh | Nối 3 bảng | 8 |
| `br02_account_detail` | BR02 | Liệt kê **mọi giao dịch của tài khoản 2378** | Nhẹ: chỉ 1 tài khoản, nhưng trả về 437 dòng | 437 |
| `br02_account_totals` | BR02 | Tổng tiền vào/ra của tài khoản 2378 năm 1998 | Rất nhẹ: 1 tài khoản, 1 năm | 1 |
| `br03_r1_scan` | BR03 | Chạy quy tắc cảnh báo R1 trên toàn bộ lịch sử: tháng nào của tài khoản nào có tiền ra > 3 lần trung bình 6 tháng trước? | Nặng nhất: tính "cửa sổ 6 tháng" cho từng tài khoản trong 4.500 tài khoản | 1 |
| `br08_tier_old_new` | BR08 | Báo cáo theo hạng cho kỳ cũ (1993–1998) và kỳ mới (01/1999) | Nặng: toàn bộ giao dịch, nối khách hàng, đếm khách | 8 |
| `br08_client_versions` | BR08 | Khách 24 có những phiên bản hạng nào, mỗi phiên bản bao nhiêu giao dịch? | Nhẹ: 1 khách | 2 |

Kiểm tra chéo: `br03_r1_scan` cho kết quả **1.698**, đúng bằng số cảnh báo R1 đang có trong `ops.alert`.

---

## 5. B1 + B2 — Tốc độ truy vấn

### Câu hỏi
- **B1:** gộp thành One Big Table thì nhanh hơn star schema bao nhiêu?
- **B2:** ClickHouse nhanh hơn hay chậm hơn PostgreSQL bao nhiêu, với cùng một star schema?

### Cách đo
1. Xuất toàn bộ kho `dw` ra file CSV **một lần**, rồi nạp cùng file đó vào 3 nơi: star schema trên ClickHouse, One Big Table trên ClickHouse, star schema trên PostgreSQL. Cả 3 đều có **đúng 1.057.571 dòng**.
2. Mỗi truy vấn chạy **6 lần liên tiếp** trên mỗi giải pháp. Lần 1 tính riêng là **cold**. Lần 2–6 là **warm**: lấy **median** và **p95**.
3. So kết quả của One Big Table và PostgreSQL với star schema **từng dòng, từng ô**. Nếu khác nhau, phép đo không có giá trị.

### Đọc bảng thế nào
Mỗi ô là số mili giây (ms), **càng nhỏ càng nhanh**. Hai cột cuối là tỉ lệ so sánh, tính trên median.

**Ví dụ đọc dòng `br01_by_tier_1998`:** trên ClickHouse star schema, truy vấn này thường mất **14,6 ms** (median), chậm nhất khoảng 15,7 ms (p95). Cùng truy vấn trên PostgreSQL mất **323,3 ms**, tức **chậm hơn 22 lần**. One Big Table chỉ mất 7,1 ms, nhanh hơn star schema 2 lần.

### Kết quả (ms)

| Truy vấn | Star / ClickHouse: cold | median | p95 | One Big Table / ClickHouse: cold | median | p95 | Star / PostgreSQL: cold | median | p95 | **B1:** One Big Table so với star | **B2:** PostgreSQL so với ClickHouse star |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| br01_kpi_1998 | 24,0 | 12,8 | 21,1 | 9,1 | 8,6 | 13,6 | 62,5 | 41,7 | 51,0 | nhanh hơn 1,5× | chậm hơn 3,3× |
| br01_by_month | 32,1 | 28,1 | 31,1 | 14,4 | 11,8 | 15,1 | 131,5 | 105,9 | 108,8 | nhanh hơn 2,4× | chậm hơn 3,8× |
| br01_by_tier_1998 | 17,4 | 14,6 | 15,7 | 7,5 | 7,1 | 7,7 | 378,3 | 323,3 | 339,3 | nhanh hơn 2,1× | **chậm hơn 22,1×** |
| br01_by_region | 18,3 | 14,9 | 16,3 | 7,5 | 6,8 | 7,5 | 62,0 | 51,4 | 54,0 | nhanh hơn 2,2× | chậm hơn 3,4× |
| br03_r1_scan | 46,6 | 45,9 | 51,7 | 30,2 | 30,0 | 30,8 | 1.031,2 | 1.029,6 | 1.065,5 | nhanh hơn 1,5× | **chậm hơn 22,4×** |
| br08_tier_old_new | 45,8 | 37,1 | 37,7 | 23,2 | 22,5 | 25,3 | 622,8 | 623,2 | 651,2 | nhanh hơn 1,6× | **chậm hơn 16,8×** |
| br02_account_detail | 21,2 | 17,2 | 18,0 | 7,5 | 5,8 | 6,2 | 2,0 | 1,0 | 1,1 | nhanh hơn 3,0× | **nhanh hơn 17×** |
| br02_account_totals | 7,0 | 6,2 | 6,6 | 3,6 | 3,4 | 3,4 | 0,7 | 0,3 | 0,3 | nhanh hơn 1,8× | **nhanh hơn 21×** |
| br08_client_versions | 26,1 | 18,3 | 21,5 | 6,9 | 5,4 | 5,6 | 1,5 | 0,4 | 0,5 | nhanh hơn 3,4× | **nhanh hơn 46×** |

**Độ đúng:** cả **18/18** cặp so sánh (2 phương án × 9 truy vấn) cho kết quả giống hệt từng ô ([result_check.csv](results/p2/result_check.csv)). Thời gian từng lần chạy có trong [query_runs.csv](results/p2/query_runs.csv).

### Kết luận
- **B2 – công nghệ:** kết quả chia làm **hai nhóm rõ rệt**.
  - **Truy vấn tổng hợp** (6 dòng đầu): đọc rất nhiều giao dịch rồi cộng hoặc đếm. Đây là việc chính của kho (BR01, BR03, BR08). **ClickHouse nhanh hơn 3–22 lần**, vì nó chỉ đọc vài cột cần thiết và đã nén. Truy vấn càng phức tạp (đếm khách, cửa sổ 6 tháng) thì khoảng cách càng lớn.
  - **Tra cứu một tài khoản hoặc một khách** (3 dòng cuối): **PostgreSQL nhanh hơn 17–46 lần**, nhờ index tìm ngay đúng vài trăm dòng. Tuy vậy ClickHouse vẫn trả lời dưới 20 ms, người dùng không cảm thấy chậm. Thời gian của ClickHouse ở đây chủ yếu là chi phí cố định (đọc các bảng dimension, chuẩn bị join), không phải do đọc nhiều dữ liệu.
- **B1 – mô hình:** One Big Table nhanh hơn star schema ở mọi truy vấn vì không phải join. Nhưng chênh lệch tuyệt đối chỉ **3–16 ms**, người dùng không nhận ra. Cái giá của One Big Table nằm ở dung lượng và chi phí sửa dữ liệu (mục 6).

---

## 6. B1 + B2 — Dung lượng, thời gian nạp, chi phí sửa dữ liệu, nhiều người dùng

### 6.1 Dung lượng trên đĩa

**Câu hỏi:** cùng 1.057.571 giao dịch thì mỗi giải pháp chiếm bao nhiêu chỗ?
**Cách đo:** đọc kích thước thật trên đĩa từ thống kê của ClickHouse (`system.parts`) và PostgreSQL (`pg_table_size`, `pg_indexes_size`).

| Giải pháp | Bảng giao dịch | Tổng | Gấp mấy lần star / ClickHouse |
|---|---:|---:|---:|
| Star / ClickHouse | 18,5 MB (54,0 MB trước khi nén) | **18,6 MB** | 1× |
| One Big Table / ClickHouse | 25,5 MB (111,5 MB trước khi nén) | **25,5 MB** | 1,37× |
| Star / PostgreSQL | 102,1 MB dữ liệu + 58,6 MB index = 160,7 MB | **162,5 MB** | 8,7× |

**Kết luận:** ClickHouse nén dữ liệu theo cột nên rất gọn. One Big Table lớn hơn vì lặp lại tên khu vực, hạng… ở mọi dòng; trước khi nén nó gấp đôi star schema. PostgreSQL lớn nhất vì không nén và cần thêm index.

### 6.2 Thời gian nạp dữ liệu

**Câu hỏi:** nạp toàn bộ dữ liệu vào mỗi giải pháp mất bao lâu?
**Cách đo:** dùng cùng một file CSV, bấm giờ lúc nạp ([load.json](results/p2/load.json)).

| Giải pháp | Thời gian | Gồm những gì |
|---|---:|---|
| Star / ClickHouse | **0,96 s** | Nạp 6 bảng (bảng giao dịch mất 0,93 s) |
| One Big Table / ClickHouse | 0,96 s + **1,35 s** | Phải có star schema trước, rồi nối 6 bảng thành 1 bảng to |
| Star / PostgreSQL | **4,64 s** | Nạp dữ liệu 2,18 s + tạo khóa chính, khóa ngoại, index và thống kê 2,43 s |

### 6.3 Chi phí sửa một thông tin sai (Type 1)

**Câu hỏi:** khi phát hiện một thông tin trong dimension bị sai (tên khu vực, ngày sinh…) và cần sửa, mỗi giải pháp phải ghi lại bao nhiêu dữ liệu?
**Cách đo:** thực hiện 2 lần sửa trên bản sao, mỗi lần sửa rồi sửa ngược lại; ghi số dòng phải ghi lại và thời gian ([update_cost.csv](results/p2/update_cost.csv)):
- **U1:** sửa tên khu vực số 1 (Praha). Khu vực này xuất hiện ở 2 vai trò: chi nhánh của tài khoản và nơi ở của khách.
- **U2:** sửa ngày sinh của khách 24 (khách này có 2 phiên bản hạng).

**Đọc bảng:** mỗi ô ghi "số dòng phải ghi lại, thời gian lần sửa / thời gian lần sửa ngược".

| Thao tác | Star / ClickHouse | One Big Table / ClickHouse | Star / PostgreSQL |
|---|---|---|---|
| U1 sửa tên khu vực 1 | **1 dòng**; 6,0 / 6,7 ms | **150.058 dòng**; 99,3 / 59,1 ms | 1 dòng; 4,1 / 0,5 ms |
| U2 sửa ngày sinh khách 24 | **2 dòng**; 8,0 / 3,8 ms | **330 dòng**; 42,6 / 31,6 ms | 2 dòng; 1,9 / 0,6 ms |

Sau khi sửa và sửa ngược, cả 3 giải pháp vẫn cho cùng kết quả báo cáo theo vùng (đã kiểm tra).

**Kết luận:** đây là lý do chính để chọn star schema. Với star schema, thông tin khu vực nằm ở **một chỗ** nên sửa 1 dòng là xong. Với One Big Table, tên khu vực bị chép vào **mọi giao dịch** của khu vực đó, nên phải ghi lại 150.058 dòng. Số dòng này **tăng theo lượng dữ liệu**: dữ liệu gấp 10 lần thì phải ghi lại khoảng 1,5 triệu dòng. (One Big Table vẫn giữ đúng hạng tại thời điểm giao dịch cho BR08, vì hạng được chép vào lúc nạp.)

### 6.4 Nhiều người dùng cùng lúc

**Câu hỏi:** khi nhiều người cùng dùng app, hệ thống trả lời được bao nhiêu truy vấn mỗi giây, và mỗi người phải chờ bao lâu?
**Cách đo:** 1, 4 rồi 8 "người dùng ảo" chạy song song trong **15 giây**. Mỗi người lặp lại 3 truy vấn, mỗi nhóm người dùng một truy vấn: BR01 theo hạng (quản lý), BR02 tra một **tài khoản ngẫu nhiên** (chuyên viên), BR08 theo hạng (kỹ sư). Đếm tổng số truy vấn trả lời được và thời gian chờ của từng truy vấn ([concurrency.csv](results/p2/concurrency.csv)).

**Đọc bảng:** "truy vấn/giây" càng **cao** càng tốt; "median / p95" là thời gian chờ của một truy vấn (ms), càng **thấp** càng tốt. Ví dụ dòng PostgreSQL, 8 người dùng: cả hệ thống chỉ trả lời được 12,4 truy vấn/giây; một truy vấn thường chờ 367 ms, nhưng 5% trường hợp phải chờ gần **2 giây**.

| Giải pháp | 1 người: truy vấn/giây | chờ median / p95 (ms) | 4 người: truy vấn/giây | chờ median / p95 | 8 người: truy vấn/giây | chờ median / p95 |
|---|---:|---|---:|---|---:|---|
| Star / ClickHouse | 52,1 | 14,1 / 34,8 | 82,0 | 42,2 / 90,9 | 93,2 | 76,5 / 148,1 |
| One Big Table / ClickHouse | 91,3 | 6,6 / 24,1 | 215,8 | 14,1 / 38,4 | 251,2 | 25,1 / 66,2 |
| Star / PostgreSQL | 4,4 | 222,9 / 454,1 | 12,2 | 266,6 / 835,7 | 12,4 | 366,8 / 1.933,5 |

**Kết luận:** với 8 người dùng, ClickHouse star schema phục vụ **93 truy vấn/giây** và mỗi người chờ dưới 0,15 giây (p95); PostgreSQL chỉ phục vụ **12 truy vấn/giây** và có lúc chờ gần 2 giây. PostgreSQL gần như không tăng thêm sau 4 người (12,2 → 12,4), tức đã dùng hết sức, vì hai truy vấn tổng hợp trong bộ trộn rất nặng với nó.

---

## 7. B3 — Kiến trúc kho vận hành: PostgreSQL riêng hay dồn hết vào ClickHouse?

### Câu hỏi
Kiến trúc của nhóm có **2 cơ sở dữ liệu**: ClickHouse (kho, khối D) và PostgreSQL (kho vận hành, khối E: cảnh báo, trạng thái xử lý, ghi chú, log pipeline, sao lưu). Có thể bỏ PostgreSQL, để luôn dữ liệu vận hành trong ClickHouse cho đơn giản không?

### Đặc điểm của dữ liệu vận hành
Khác hẳn dữ liệu kho: rất **ít dòng**, nhưng bị **sửa liên tục từng dòng một** (đổi trạng thái 1 cảnh báo, đánh dấu 1 bước pipeline đã xong), và cần **luật chặt chẽ** (không được chuyển trạng thái sai, không được trùng cảnh báo).

### Cách đo
Chép cùng 1.702 cảnh báo vào cả hai nơi (bản sao). Thực hiện đúng các thao tác mà app và pipeline đang làm, **từng lệnh một** như thật:
- **W1:** chuyên viên đổi trạng thái cảnh báo và hệ thống ghi lịch sử xử lý (BR04). Làm 200 cảnh báo × 2 lần chuyển: NEW → IN_REVIEW, rồi IN_REVIEW → RESOLVED.
- **W2:** pipeline ghi log 1 bước: lúc bắt đầu ghi "đang chạy", lúc xong ghi "thành công" (BR06). Làm 200 bước.
- **W3:** chuyên viên mở danh sách 50 cảnh báo mới nhất, và mở 1 cảnh báo kèm lịch sử xử lý (BR03, BR04). Mỗi loại 50 lần.

Vì ClickHouse không sửa tại chỗ được, đo theo hai cách sửa:
- **versioned:** ghi thêm một dòng phiên bản mới, đọc bằng `FINAL`;
- **mutation:** dùng `ALTER TABLE … UPDATE` và chờ ClickHouse ghi lại xong.

### Kết quả tốc độ ([ops_latency.csv](results/p2/ops_latency.csv)) — ms cho **mỗi thao tác**, càng nhỏ càng tốt

| Thao tác | Số lần | PostgreSQL median / p95 | ClickHouse median / p95 | PostgreSQL nhanh hơn |
|---|---:|---|---|---:|
| W1 đổi trạng thái NEW → IN_REVIEW + ghi lịch sử | 200 | 0,19 / 0,67 | 5,92 / 10,70 (versioned) | 31× |
| W1 đổi trạng thái IN_REVIEW → RESOLVED + ghi lịch sử | 200 | 0,17 / 0,31 | 11,91 / 18,17 (mutation) | 70× |
| W2 ghi 1 bước pipeline (bắt đầu + kết thúc) | 200 | 0,32 / 0,54 | 2,46 / 3,59 (versioned) | 7,7× |
| W3 mở danh sách 50 cảnh báo mới nhất | 50 | 0,11 / 0,18 | 2,61 / 4,58 | 24× |
| W3 mở 1 cảnh báo + lịch sử xử lý | 50 | 0,06 / 0,10 | 3,26 / 5,47 | 54× |

Thêm một quan sát: 200 bước của W2 = 400 lệnh INSERT nhỏ, và chúng tạo ra **400 data part** trong ClickHouse. Đây đúng là kiểu ghi ClickHouse khuyên tránh: nhiều part nhỏ làm nó phải gộp liên tục và chậm dần.

### Kết quả toàn vẹn dữ liệu ([ops_integrity.csv](results/p2/ops_integrity.csv))

**Câu hỏi:** nếu app có lỗi và gửi một lệnh sai, **chính cơ sở dữ liệu** có tự chặn lại không? (✅ = chặn được, ❌ = để lọt dữ liệu sai)

| Tình huống thử | Vì sao quan trọng | PostgreSQL | ClickHouse |
|---|---|:-:|:-:|
| I1 ghi trạng thái không tồn tại (`'DONE'`) | Trạng thái chỉ có 3 giá trị (BR04) | ✅ CHECK chặn | ❌ ghi được |
| I2 chuyển trạng thái sai (RESOLVED → NEW) | Quy trình xử lý phải đi đúng thứ tự (BR04) | ✅ hàm `set_alert_status` chặn | ❌ ghi được |
| I3 tạo cảnh báo trùng (cùng tài khoản, quy tắc, kỳ) | Chạy lại lô không được sinh cảnh báo trùng (BR03) | ✅ UNIQUE chặn | ❌ có 2 cảnh báo giống nhau |
| I4 ghi lịch sử cho cảnh báo không tồn tại | Lịch sử phải gắn với cảnh báo có thật (BR04) | ✅ khóa ngoại chặn | ❌ ghi được |
| I5 đổi trạng thái xong, ghi lịch sử bị lỗi | Không được có trạng thái đổi mà không có lịch sử (BR04) | ✅ transaction hoàn tác cả hai | ❌ trạng thái đã đổi, lịch sử không có |

Thêm một giới hạn đo được: ClickHouse **không cho mutation sửa cột phiên bản** (`CANNOT_UPDATE_COLUMN`). Vì vậy không thể vừa sửa tại chỗ vừa cập nhật thời điểm đổi trạng thái.

### Kết luận
Tách PostgreSQL làm kho vận hành là đúng. Nó **nhanh hơn 8–70 lần** cho loại công việc này (sửa từng dòng, đọc vài dòng), và quan trọng hơn là **tự bảo vệ dữ liệu**: chặn được cả 5 loại lỗi, còn ClickHouse không chặn được loại nào. Nếu dồn vào ClickHouse, mọi luật này phải viết trong app và sẽ không an toàn khi app có lỗi.

---

## 8. B4 — Pipeline Spark: thêm CPU có nhanh hơn không? chia 72 lô tốn bao nhiêu?

### Câu hỏi
1. Pipeline dùng Spark, vốn được thiết kế để chạy song song. Cho Spark dùng 1, 2, 4 hay 8 lõi CPU thì nhanh hơn bao nhiêu?
2. Pipeline nạp lịch sử thành **72 lô tháng** (giống ngân hàng nhận dữ liệu theo kỳ). So với xử lý tất cả trong **1 lô** thì tốn thêm bao nhiêu?

### Cách đo
Lấy đúng phép biến đổi mà pipeline làm với dữ liệu lịch sử: đọc 72 bản chụp lô trong Data Lake → chuẩn hóa mã → tìm tài khoản, loại giao dịch, chủ tài khoản và **phiên bản hạng đúng tại ngày giao dịch (SCD2)** → ghi kết quả ra file. Phép đo **không ghi vào kho**, nên chạy lại bao nhiêu lần cũng được.
- Chạy 8 cấu hình: {1, 2, 4, 8 lõi} × {1 lô, 72 lô}. Mỗi cấu hình khởi động một Spark mới.
- Đo riêng **thời gian khởi động Spark** và **thời gian biến đổi**.
- Mỗi lần chạy đều kiểm tra ra đúng **1.056.320 dòng**, không dòng nào thiếu phiên bản hạng. Cả 8 lần đều đúng.

### Đọc bảng
"Biến đổi" là số giây xử lý, càng nhỏ càng tốt; "dòng/giây" càng lớn càng tốt. Cột cuối: chia thành 72 lô thì chậm hơn 1 lô bao nhiêu lần.

| Lõi | 1 lô: khởi động | biến đổi | dòng/giây | 72 lô: khởi động | biến đổi | dòng/giây | 72 lô chậm hơn 1 lô |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,9 s | 7,7 s | 137.164 | 1,9 s | 30,6 s | 34.526 | 4,0× |
| 2 | 1,7 s | 6,0 s | 174.770 | 2,1 s | 32,5 s | 32.478 | 5,4× |
| 4 | 2,0 s | 5,4 s | 194.009 | 1,6 s | 31,8 s | 33.237 | 5,9× |
| 8 | 1,6 s | 5,4 s | 196.139 | 1,9 s | 29,6 s | 35.718 | 5,5× |

([spark_scaling.csv](results/p2/spark_scaling.csv))

### Kết luận
- **Thêm lõi gần như không giúp:** từ 1 lên 2 lõi nhanh hơn một chút (7,7 → 6,0 s), từ 4 lên 8 lõi không nhanh thêm. Lý do: ở mức ×1, dữ liệu chỉ khoảng 1 triệu dòng (khoảng 24 MB Parquet), nên phần lớn thời gian là **chi phí cố định** (lên kế hoạch, chia việc, mở file), không phải tính toán. Muốn thấy Spark tận dụng nhiều lõi phải có **dữ liệu lớn hơn**, tức cần đo ở mức ×10.
- **Chia 72 lô chậm hơn 4–6 lần**, cũng vì mỗi lô phải trả lại chi phí cố định đó (khoảng 0,3 s/lô). Đổi lại, xử lý theo lô cho phép ghi log từng lô, tách lỗi từng lô và chạy lại đúng một lô (BR05, BR06). Đây là đánh đổi có chủ đích.

**Thời gian pipeline thật** (lấy từ log `ops.pipeline_step`, gồm cả ghi kho, ghi log, đối chiếu): mỗi lô tháng mất median **3,4 giây** (p95 5,3 s); 72 lô mất tổng **273,5 giây** (khoảng 4,5 phút); median 4.334 dòng/giây. Bước tốn nhất là `load_fact` (ghi giao dịch vào kho) 1,42 s, sau đó là `validate` (kiểm tra 7 quy tắc) 0,85 s ([pipeline_steps.csv](results/p2/pipeline_steps.csv)).

---

## 9. Sự cố trong lúc đo và cách xử lý

| # | Chuyện gì xảy ra | Vì sao | Đã làm gì | Có ảnh hưởng số liệu không |
|---|---|---|---|---|
| 1 | Lần đo đầu (sáng 22/10) dùng PostgreSQL với cấu hình mặc định (bộ nhớ nhỏ hơn) | Chưa áp cấu hình giống Phase 1 | Đặt `shared_buffers` 256 MB trong [docker-compose.yml](../docker-compose.yml); `work_mem`, số worker, JIT đặt riêng trong phiên đo | Số liệu lần đầu đã được thay bằng lần này. Kho `ops` thật vẫn giữ cấu hình mặc định cho `work_mem` |
| 2 | Nạp vào PostgreSQL mất **42 giây** | Khóa chính, khóa ngoại có sẵn từ đầu nên PostgreSQL kiểm tra từng dòng khi nạp | Nạp trước, tạo khóa và index sau (cách làm chuẩn khi nạp lô lớn, giống Phase 1) | Còn 4,64 s |
| 3 | ClickHouse báo **hết bộ nhớ** khi chạy truy vấn BR02 | Các bảng log hệ thống của ClickHouse (ghi liên tục ở chế độ nền, có bảng tới 4 triệu dòng) chiếm bộ nhớ | Tắt các log nền không cần ([system_logs.xml](../warehouse/clickhouse/config.d/system_logs.xml)), xoá bảng log cũ | Bộ nhớ nền giảm từ 1,2 GB xuống 0,74 GB; đã chạy lại toàn bộ |
| 4 | Truy vấn biểu đồ theo tháng cho kết quả khác nhau giữa hai hệ | Tháng không có tiền vào: PostgreSQL trả "rỗng" (NULL), ClickHouse trả 0 | Thêm `coalesce` để PostgreSQL cũng trả 0 | Lỗi của câu truy vấn, dữ liệu vẫn đúng |
| 5 | ClickHouse báo lỗi khi sửa trạng thái bằng mutation | ClickHouse không cho sửa cột phiên bản | Chỉ sửa cột trạng thái; ghi lại thành một phát hiện của B3 | — |
| 6 | **Máy ngủ khoảng 2 giờ** giữa lúc đang đo nhiều người dùng; PostgreSQL bị quá thời gian chờ | macOS tự ngủ | Giữ máy thức; **đo lại** phần nhiều người dùng (22:38) và B3 (22:40) | Bỏ số liệu của lần bị ngắt (log của lần đo lại đã ghi đè log cũ) |
| 7 | B3 đếm ra 800 data part thay vì 400 | Bảng thống kê `part_log` còn giữ số của lần chạy trước | Chỉ đếm từ lúc bắt đầu lần chạy | Đã chạy lại |

---

## 10. Tổng kết: các giả thuyết có đúng không?

Bảng dưới là kết quả ×1; kết quả ×10 và so sánh hai mức ở mục 12.9.

| Giả thuyết đặt ra trước khi đo | Kết quả ở mức ×1 | Ý nghĩa với thiết kế |
|---|---|---|
| **H1** ClickHouse nhanh hơn PostgreSQL ở truy vấn tổng hợp và tốn ít chỗ hơn | **Đúng.** Nhanh hơn 3,3–22,4×, ít chỗ hơn 8,7×; 8 người dùng: 93 so với 12 truy vấn/giây | Chọn ClickHouse làm kho là đúng |
| **H2** Khi tra cứu 1 tài khoản hoặc 1 khách, index của PostgreSQL thu hẹp khoảng cách | **Đúng, còn đảo ngược hẳn.** PostgreSQL nhanh hơn 17–46×, nhưng ClickHouse vẫn dưới 20 ms | Không ảnh hưởng trải nghiệm; nếu sau này tra cứu là việc chính thì có thể thêm projection theo tài khoản trên ClickHouse |
| **H3** Thời gian một lô chủ yếu là chi phí cố định, không phụ thuộc số dòng | **Đúng.** 72 lô chậm hơn 1 lô 4–6×; thêm lõi gần như không giúp | Chia lô là đánh đổi có chủ đích để kiểm soát từng lô |
| **H4** One Big Table nhanh hơn một chút, nhưng tốn chỗ hơn và sửa đắt hơn | **Đúng.** Nhanh hơn 1,5–3,4× (chỉ 3–16 ms), tốn chỗ hơn 1,37×, sửa 1 khu vực phải ghi lại 150.058 dòng thay vì 1 | Chọn star schema là đúng |
| **H5** Kho vận hành nên tách sang PostgreSQL | **Đúng.** Nhanh hơn 8–70×, tự chặn 5/5 loại lỗi; ClickHouse 0/5 | Kiến trúc 2 cơ sở dữ liệu là đúng |

---

## 11. Chưa đo / giới hạn

| Hạng mục | Tình trạng | Ảnh hưởng tới kết luận |
|---|---|---|
| Mức ×10 | **Đã đo** (mục 12) | — |
| Mức ×50 | **Chưa đo** | Chưa biết xu hướng ở khoảng 53 triệu giao dịch; PostgreSQL sẽ cần khoảng 8 GB đĩa |
| "Cold" thật sự (đọc từ đĩa) | Không làm được trong container (không xoá được bộ nhớ đệm của hệ điều hành) | Số cold thực chất là "lần chạy đầu" |
| Tối ưu PostgreSQL sâu hơn (chia bảng theo năm, BRIN index, materialized view) | Không làm, để giữ cấu hình giống Phase 1 | PostgreSQL có thể thu hẹp khoảng cách ở truy vấn tổng hợp nếu được tối ưu riêng |
| Projection theo tài khoản trên ClickHouse cho BR02 | Không làm | Hướng cải thiện nếu cần |
| Môi trường | Một máy, một máy ảo dùng chung cho mọi hệ; người gọi chạy cùng máy | Số tuyệt đối có thể khác trên máy chủ thật; tỉ lệ so sánh đáng tin hơn số tuyệt đối |

---

## 12. Mức ×10 — khi dữ liệu lớn gấp 10 lần

### 12.1 Dữ liệu ×10 lấy ở đâu?
Berka chỉ có khoảng 1 triệu giao dịch, nên **dữ liệu ×10 là dữ liệu nhân bản**, cách các bộ benchmark chuẩn như TPC-H vẫn làm. Mỗi tài khoản, khách hàng và giao dịch của bản sao ×1 được chép thành 10 bản với **mã mới** (tài khoản 2378 ở bản thứ 2 thành 102378…). Ngày, số tiền, loại giao dịch, khu vực, hạng và lịch sử đổi hạng giữ nguyên. Kết quả giống một ngân hàng có **45.000 tài khoản** thay vì 4.500, trong cùng giai đoạn.

- **Nằm ở đâu:** database riêng `bench10` (ClickHouse) và `dwpg10` (PostgreSQL). Kho thật `dw` vẫn ×1, app vẫn hiển thị 1.057.571 giao dịch. Trước khi làm đã sao lưu `dw` thành `before_x10`.
- **Kiểm tra:** số dòng, tổng tiền, số tài khoản, số khách ở ×10 phải **đúng bằng 10 lần** ×1 → **đạt** (10.575.710 dòng; 62.856.248.203,00 so với 6.285.624.820,30; 45.000 tài khoản; 51.180 phiên bản khách).
- **Dùng được cho:** đo hiệu năng khi khối lượng tăng. **Không dùng** để rút kết luận nghiệp vụ (độ đúng KB01–KB08 vẫn đo trên dữ liệu thật ×1). Dữ liệu lặp lại có thể giúp nén tốt hơn thực tế một chút.
- **Không đo lại B3** (kho vận hành), vì dữ liệu vận hành không lớn lên theo kho.
- Chạy lại: `make bench-all SCALE=10`. Số liệu và log: [docs/results/p2/x10/](results/p2/x10/). Dọn bản sao: `make bench-clean`.

### 12.2 Dung lượng và thời gian nạp ở ×10

| Giải pháp | Dung lượng ×1 | Dung lượng ×10 | Thời gian nạp ×10 |
|---|---:|---:|---|
| Star / ClickHouse | 18,6 MB | **185,5 MB** (540 MB trước khi nén) | 3,9 s |
| One Big Table / ClickHouse | 25,5 MB | **249,4 MB** (1.115 MB trước khi nén) | + 9,6 s để dựng từ star |
| Star / PostgreSQL | 162,5 MB | **1.612,7 MB** (1.016 MB bảng + 584 MB index) | 45,0 s (COPY 18,2 s + khóa và index 26,5 s) |

Dung lượng tăng đúng khoảng 10 lần ở cả ba giải pháp; tỉ lệ giữa chúng giữ nguyên (One Big Table 1,34×, PostgreSQL 8,7× so với star / ClickHouse). Đĩa máy dùng thêm khoảng 2 GB (còn 30 GB trống), không vượt giới hạn.

### 12.3 Tốc độ truy vấn ×10 ([x10/query_summary.csv](results/p2/x10/query_summary.csv))

**Đọc bảng:** cột "×1" và "×10" là median (ms) của cùng truy vấn ở hai mức dữ liệu. Cột "**tăng**" = ×10 chia ×1: nếu dữ liệu gấp 10 mà thời gian chỉ gấp 3 thì hệ thống "co giãn tốt"; gấp gần 10 là tăng tỉ lệ thuận với dữ liệu; nhỏ hơn 1 là không phụ thuộc lượng dữ liệu. Cột cuối so PostgreSQL với ClickHouse star **ở ×10**.

| Truy vấn | Star / ClickHouse ×1 → ×10 | tăng | One Big Table ×1 → ×10 | tăng | Star / PostgreSQL ×1 → ×10 | tăng | PostgreSQL so với ClickHouse star ở ×10 |
|---|---|---:|---|---:|---|---:|---|
| br01_kpi_1998 | 12,8 → **28,8** | 2,2× | 8,6 → 9,4 | 1,1× | 41,7 → **355,6** | 8,5× | chậm hơn 12× |
| br01_by_month | 28,1 → **121,9** | 4,3× | 11,8 → 29,8 | 2,5× | 105,9 → **837,3** | 7,9× | chậm hơn 6,9× |
| br01_by_tier_1998 | 14,6 → **47,6** | 3,3× | 7,1 → 15,7 | 2,2× | 323,3 → **1.323,7** | 4,1× | **chậm hơn 28×** |
| br01_by_region | 14,9 → **48,5** | 3,3× | 6,8 → 16,6 | 2,4× | 51,4 → **495,5** | 9,6× | chậm hơn 10× |
| br03_r1_scan | 45,9 → **199,3** | 4,3× | 30,0 → 139,2 | 4,6× | 1.029,6 → **4.209,2** | 4,1× | **chậm hơn 21×** |
| br08_tier_old_new | 37,1 → **219,4** | 5,9× | 22,5 → 77,8 | 3,5× | 623,2 → **5.378,4** | 8,6× | **chậm hơn 25×** |
| br02_account_detail | 17,2 → **53,8** | 3,1× | 5,8 → 5,5 | 0,9× | 1,0 → **0,8** | 0,8× | nhanh hơn 67× |
| br02_account_totals | 6,2 → **12,3** | 2,0× | 3,4 → 2,5 | 0,7× | 0,3 → **0,2** | 0,7× | nhanh hơn 62× |
| br08_client_versions | 18,3 → **80,4** | 4,4× | 5,4 → 7,6 | 1,4× | 0,4 → **0,3** | 0,7× | nhanh hơn 268× |

**18/18** cặp kết quả vẫn khớp từng ô ở ×10.

**Kết luận:**
- **Truy vấn tổng hợp:** khi dữ liệu tăng 10 lần, ClickHouse chậm đi **2–6 lần**, còn PostgreSQL chậm đi **4–10 lần**. Vì vậy khoảng cách giữa hai hệ **nới rộng**: từ 3–22 lần ở ×1 lên **7–28 lần** ở ×10. Ở ×10, các báo cáo BR01, BR03, BR08 trên PostgreSQL mất 0,4–5,4 giây; trên ClickHouse vẫn dưới 0,25 giây.
- **Tra cứu 1 tài khoản / 1 khách:** PostgreSQL và One Big Table **không chậm đi** vì tìm thẳng tới đúng dòng (index, hoặc thứ tự sắp xếp theo tài khoản). Star schema trên ClickHouse lại chậm đi khoảng 3–4 lần (BR02 17 → 54 ms). Nguyên nhân được kiểm chứng ở 12.4.

### 12.4 Phát hiện: vì sao tra cứu tài khoản trên star schema chậm dần?

**Câu hỏi:** bảng giao dịch đã sắp xếp theo `account_sk`, vậy sao tra 1 tài khoản lại chậm đi khi dữ liệu tăng?
**Cách kiểm:** chạy cùng câu hỏi BR02 (mọi giao dịch của tài khoản 2378) theo hai cách viết, đo thời gian và **số dòng ClickHouse thực sự đọc** (từ `system.query_log`) ([x10/lookup_experiment.csv](results/p2/x10/lookup_experiment.csv)):

| Cách viết | ×1: thời gian | ×1: dòng đọc | ×10: thời gian | ×10: dòng đọc |
|---|---:|---:|---:|---:|
| Nối `dim_account` rồi lọc `account_id = 2378` (cách app đang viết) | 33,8 ms | 1.062.086 (cả bảng) | 60,3 ms | 10.620.725 (cả bảng) |
| Tìm `account_sk` trước, rồi lọc bảng giao dịch theo `account_sk` | 6,1 ms | 54.918 | **6,8 ms** | 102.924 |

**Kết luận:** khi điều kiện lọc nằm ở bảng dimension, ClickHouse không đẩy được điều kiện đó vào bảng giao dịch, nên phải **đọc cả bảng** rồi mới nối. Chỉ cần viết lại để lọc theo khóa của bảng giao dịch, ClickHouse đọc đúng vài khối dữ liệu của tài khoản đó, và thời gian **không đổi khi dữ liệu tăng** (6,1 → 6,8 ms). Đây là một cải tiến có thể áp dụng cho truy vấn BR02 của app; **chưa áp dụng**, cần nhóm quyết định.

### 12.5 Nhiều người dùng cùng lúc ở ×10 ([x10/concurrency.csv](results/p2/x10/concurrency.csv))

Cùng cách đo như mục 6.4: 1, 4, 8 người dùng ảo trong 15 giây, mỗi người lặp lại BR01 theo hạng, BR02 một tài khoản ngẫu nhiên, BR08 theo hạng.

| Giải pháp | 1 người: truy vấn/giây | chờ median / p95 (ms) | 4 người: truy vấn/giây | chờ median / p95 | 8 người: truy vấn/giây | chờ median / p95 |
|---|---:|---|---:|---|---:|---|
| Star / ClickHouse | 9,4 | 64,2 / 219,6 | 11,3 | 331,2 / 599,4 | 9,4 | 841,0 / 1.385,1 |
| One Big Table / ClickHouse | 26,5 | 18,3 / 101,9 | 30,9 | 88,9 / 315,0 | 30,7 | 202,4 / 606,5 |
| Star / PostgreSQL | 0,35 | 1.755 / 7.188 | 0,39 | 6.121 / 16.645 | 0,40 | 12.669 / 37.783 |

**Kết luận:** ở ×10, PostgreSQL gần như không phục vụ được nhiều người: **0,4 truy vấn/giây**, có lúc chờ tới **38 giây**. ClickHouse star phục vụ khoảng 9–11 truy vấn/giây và đã bão hòa từ 4 người (thêm người không tăng thêm; thời gian chờ tăng lên khoảng 0,8–1,4 s ở 8 người). Một phần lý do là truy vấn BR02 trong bộ trộn đọc cả bảng (mục 12.4); One Big Table, không có vấn đề này, phục vụ được gấp khoảng 3 lần.

### 12.6 Chi phí sửa dữ liệu ở ×10 ([x10/update_cost.csv](results/p2/x10/update_cost.csv))

| Thao tác | Star / ClickHouse | One Big Table / ClickHouse | Star / PostgreSQL |
|---|---|---|---|
| U1 sửa tên khu vực 1 | **1 dòng**; 6,1 / 5,6 ms | **1.500.580 dòng**; 241,7 / 233,4 ms | 1 dòng; 12,8 / 0,9 ms |
| U2 sửa ngày sinh khách 24 | **2 dòng**; 5,0 / 4,4 ms | **330 dòng**; 34,8 / 43,7 ms | 2 dòng; 5,1 / 0,5 ms |

**Kết luận:** đúng như dự đoán, One Big Table phải ghi lại số dòng **tăng gấp 10** (150.058 → 1.500.580) và thời gian tăng khoảng 2,4–4 lần; star schema vẫn chỉ ghi 1 dòng, thời gian không đổi. (U2 không đổi vì khách 24 ở bản gốc vẫn chỉ có 330 giao dịch.)

### 12.7 Spark ở ×10 ([x10/spark_scaling.csv](results/p2/x10/spark_scaling.csv))

Đầu vào: 72 bản chụp lô nhân 10 = **10.563.200 giao dịch**, khoảng 95 MB Parquet (chuẩn bị một lần, không tính giờ). Mọi lần chạy ra đúng 10.563.200 dòng, 0 dòng thiếu phiên bản hạng.

| Lõi | 1 lô: biến đổi ×1 | 1 lô: biến đổi ×10 | dòng/giây ×10 | 72 lô: biến đổi ×1 | 72 lô: biến đổi ×10 | dòng/giây ×10 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 7,7 s | **28,8 s** | 367.399 | 30,6 s | **51,0 s** | 207.041 |
| 2 | 6,0 s | **19,6 s** | 538.297 | 32,5 s | **52,4 s** | 201.491 |
| 4 | 5,4 s | **17,1 s** | 615.940 | 31,8 s | **50,0 s** | 211.236 |
| 8 | 5,4 s | **14,0 s** | 756.613 | 29,6 s | **57,1 s** | 184.826 |

**Kết luận:**
- **Khi dữ liệu đủ lớn, Spark bắt đầu tận dụng nhiều lõi:** 1 lô ×10 từ 1 lên 8 lõi nhanh hơn **2,1 lần** (28,8 → 14,0 s); ở ×1 chỉ 1,4 lần. Dữ liệu gấp 10 nhưng thời gian chỉ gấp 2,6–3,7 lần, tức tốc độ xử lý (dòng/giây) tăng mạnh.
- **72 lô tháng vẫn không hưởng lợi từ thêm lõi** (khoảng 50–57 s ở mọi cấu hình): mỗi lô tháng ×10 chỉ khoảng 147 nghìn dòng, vẫn bị chi phí cố định mỗi lô chi phối. Khoảng cách giữa 72 lô và 1 lô giảm từ 4–6 lần (×1) xuống **1,8–4,1 lần** (×10). Nếu dữ liệu một kỳ lớn hơn nữa, chia lô sẽ càng ít tốn.

### 12.8 Sự cố khi đo ×10

| # | Chuyện gì xảy ra | Vì sao | Đã làm gì |
|---|---|---|---|
| 8 | ClickHouse **hết bộ nhớ** khi chép 10 bản bảng giao dịch trong một lệnh | Ghi 10 triệu dòng một lúc, 4 luồng, chia vào 7 phân vùng năm | Chép **từng bản một** (1 triệu dòng mỗi lệnh), khối ghi nhỏ hơn, giải phóng bộ nhớ đệm trước khi chép |
| 9 | ClickHouse **hết bộ nhớ** khi dựng One Big Table ×10 | Nối 10 triệu dòng với 6 bảng trong một lệnh, trong lúc ClickHouse còn đang gộp các khối vừa ghi | Dựng **từng bản một**; khởi động lại ClickHouse trước khi nạp và trước khi đo truy vấn để bộ nhớ về mức nền (khoảng 0,65 GB) |
| 10 | PostgreSQL báo "No space left on device" ở bước `VACUUM ANALYZE` | **Không phải hết đĩa** (đĩa còn 29 GB): vùng bộ nhớ dùng chung `/dev/shm` của container chỉ có 64 MB (mặc định Docker), không đủ cho các worker song song với 10 triệu dòng | Đặt `shm_size: 256m` cho container PostgreSQL trong [docker-compose.yml](../docker-compose.yml) (nằm trong RAM, vẫn trong giới hạn 1 GB); nạp lại toàn bộ ×10 |

Log của các lần lỗi đã bị ghi đè bởi lần chạy thành công; số liệu ở trên là của lần chạy thành công.

### 12.9 Các giả thuyết sau khi có ×10

| Giả thuyết | ×1 | ×10 |
|---|---|---|
| **H1** ClickHouse nhanh hơn PostgreSQL ở truy vấn tổng hợp | Đúng, 3,3–22× | **Đúng hơn nữa: 7–28×**; khoảng cách nới rộng khi dữ liệu tăng |
| **H2** PostgreSQL nhanh hơn ở tra cứu 1 tài khoản | Đúng, 17–46× | Đúng, **62–268×**, vì star / ClickHouse đọc cả bảng khi lọc qua dimension. Sửa cách viết truy vấn thì ClickHouse giữ được khoảng 7 ms (mục 12.4) |
| **H3** Thời gian một lô chủ yếu là chi phí cố định | Đúng | Đúng với lô tháng; với 1 lô lớn, Spark đã tận dụng được nhiều lõi (2,1×) |
| **H4** One Big Table nhanh hơn chút, nhưng sửa đắt | Đúng | Đúng: sửa 1 khu vực ghi lại **1,5 triệu dòng** |
