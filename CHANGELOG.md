# Changelog — Banking Data Warehouse (Kimball)

Ghi lại các phát hiện, quyết định thiết kế và thay đổi của dự án (mới nhất ở trên).

Nhật ký Phase 1 (Kaggle, Kafka, Superset; đến ngày 07/10/2026) đã chuyển sang [archive/phase1/CHANGELOG.md](archive/phase1/CHANGELOG.md); code Phase 1 ở tag `phase1-kaggle` và [archive/phase1/](archive/phase1/).










---

## [2026-10-31] Benchmark B5: Spark và Flink trên cùng một phép biến đổi (chạy trên máy) ⚡

- **Flink trong docker compose** (profile `flink`, chỉ dùng cho benchmark): image chính thức `flink:1.20.1-scala_2.12-java17`, gồm 1 jobmanager và 1 taskmanager, tổng 3 GB RAM bằng container Spark; REST API mở ở `127.0.0.1:18081`. Đọc Parquet cần `flink-sql-parquet` (tải từ Maven, đã khớp SHA-1) cùng các jar Hadoop 3.3.4 client và `zstd-jni` lấy từ image Spark, để trong `flink/lib/` (không lên git). Đã bỏ cách build image PyFlink vì quá chậm trên arm64.
- [spark/jobs/bench_engines.py](spark/jobs/bench_engines.py): chuẩn bị đầu vào chung cho x1 và x10 (72 lô Parquet, 4 bảng tra cứu xuất từ kho) và phép biến đổi phía Spark. [flink/jobs/bench_flink.py](flink/jobs/bench_flink.py): cùng phép biến đổi viết bằng Flink SQL, cùng broadcast; chạy Spark và Flink xen kẽ ở 16 cấu hình, mỗi cấu hình 3 lần; đo bằng đồng hồ đơn điệu trong máy ảo và tự đo lại khi đồng hồ nhảy.
- `make bench-engines-prepare`, `make bench-engines` (tự bật Flink rồi tắt khi xong). Kết quả nằm trong [docs/results/p2/engines/](docs/results/p2/engines/).
- **Kết quả** ([BENCHMARK_P2.md mục 13](docs/BENCHMARK_P2.md)):
  - Cả 96 lần chạy, hai engine cho kết quả **giống hệt nhau**.
  - Xử lý 1 lô: Flink nhanh hơn 1,8–3,0× ở ×1; ở ×10 hai bên gần ngang nhau.
  - **72 lô tháng: Spark nhanh hơn khoảng 3×** (Flink tốn khoảng 0,8 s chi phí cố định mỗi job, Spark khoảng 0,26 s).
  - Flink khởi động mất 6–13 s, Spark khoảng 2 s.
  - Kết luận: giữ Spark cho pipeline theo lô.
- Cập nhật [EVALUATION_P2.md](docs/EVALUATION_P2.md) (bảng Performance).

---

## [2026-10-29] App: tên cột tiếng Việt ở mọi bảng 🇻🇳

- [apps/app/app.py](apps/app/app.py): bảng `COL_VI` và hàm `vi()` đổi tên cột ngay trước khi hiển thị (truy vấn SQL giữ alias ngắn); áp dụng cho **21 bảng** ở cả bản đầy đủ và bản chỉ xem, kể cả kết quả tab SQL và phần xem trước file kịch bản. Ví dụ: `hang, so_giao_dich, tien_vao, tien_ra, dong_tien_thuan` → **Hạng, Số giao dịch, Tiền vào (CZK), Tiền ra (CZK), Dòng tiền thuần (CZK)**; `run_id, batch_id, status…` → **Mã lần chạy, Lô, Trạng thái…**.
- Đã duyệt mọi tab (10 tab bản đầy đủ, 4 tab bản chỉ xem): không lỗi.
- Chụp lại 9 ảnh app trong [report/figures/](report/figures/) cho report và slide (giữ nguyên số đo thời gian đã dùng trong report); dọn ghi nhận BR04 thử; đóng gói lại `report_overleaf.zip`, `slides_overleaf.zip`.

---

## [2026-10-29] Bản chỉ xem đã chạy trên Streamlit Community Cloud ✅

- Link: <https://data-engineer-bank-project.streamlit.app/> (`APP_MODE=viewer`, đọc Aiven ClickHouse bằng tài khoản `bankdw_viewer`).
- Đã kiểm tra bằng trình duyệt: thanh bên 1.057.571 giao dịch; Tổng quan 1998 (vào 960.964.282, ra 933.819.075, thuần 27.145.207, 322.277 giao dịch) khớp bản trên máy; tra cứu tài khoản 11; Báo cáo lịch sử (5.118 phiên bản / 4.500 hiện tại / 618 đã đóng); SQL 28 dòng trong 0,67 s; `DROP TABLE` bị chặn.
- App đang **công khai** (mở được không cần đăng nhập). Muốn chỉ nhóm xem: Settings → Sharing → *Only specific people can view this app*.
- Cập nhật dữ liệu trên cloud sau khi chạy thêm ở máy: `make publish`.

---

## [2026-10-29] Đã publish kho lên Aiven ClickHouse; tài khoản chỉ đọc cho bản chỉ xem ✅

- Dịch vụ Aiven ClickHouse mới (26.3) → `make publish`: 6 bảng, **1.057.571** giao dịch, tổng tiền **6.285.624.820,30**, 5.118 phiên bản khách, 4.500 tài khoản — **khớp hoàn toàn** với kho trên máy; 24 giây.
- Tài khoản chỉ đọc `bankdw_viewer` trên Aiven (`GRANT SELECT, SHOW TABLES, SHOW COLUMNS ON dw.*`): đọc được; `INSERT`, `DROP`, đọc `system.users` bị từ chối.
- Thử bản chỉ xem kết nối thẳng Aiven bằng tài khoản này: Tổng quan (1998: tiền vào 960.964.282), tra cứu tài khoản 11, SQL (0,22 s) — chạy đúng, không lỗi.
- `streamlit_secrets.toml` (không lên git) chứa sẵn secrets để dán vào Streamlit Cloud; cập nhật [docs/DEPLOY.md](docs/DEPLOY.md).
- Nên đổi mật khẩu `avnadmin` (đã xuất hiện trong ảnh chụp gửi qua chat) rồi cập nhật `AIVEN_CLICKHOUSE_PASSWORD` trong `.env`; app không dùng tài khoản này.

---

## [2026-10-29] Bản chỉ xem cho thành viên: chế độ viewer, tab SQL, `make publish` lên Aiven 🌐

- [apps/app/app.py](apps/app/app.py): **`APP_MODE=viewer`**: 4 tab Tổng quan (BR01) · Tra cứu tài khoản (BR02) · Báo cáo lịch sử (BR08) · **SQL**; chỉ dùng ClickHouse, không PostgreSQL, không runner, không nút ghi. Chế độ đầy đủ giữ nguyên (BR02/BR08/BR01 tách thành hàm dùng chung cho hai chế độ).
- Tab SQL: chỉ một câu đọc (`SELECT`/`WITH`/`SHOW`/`DESCRIBE`/`EXPLAIN`), chạy với `readonly=1`, tối đa 30 s, 500 MB, 10.000 dòng, tải CSV. Đã thử: `DROP` và 2 câu lệnh bị app chặn; `SETTINGS readonly = 0` và `WITH … INSERT` bị ClickHouse từ chối; bảng không đổi.
- `docker-compose.yml`: service `app-viewer` (profile `viewer`, cổng 8502); `make viewer`. App cài gói từ [apps/app/requirements.txt](apps/app/requirements.txt) (dùng chung cho Docker và Streamlit Cloud).
- [spark/jobs/publish.py](spark/jobs/publish.py) + `make publish`: chép 6 bảng `dw` lên Aiven ClickHouse (tạo bảng, xoá cũ, chép, đối chiếu số dòng và tổng tiền). Biến `AIVEN_CLICKHOUSE_*` trong `.env` (chép từ các dòng `# [aiven]`), chỉ truyền vào lần chạy publish, không vào container app.
- [docs/DEPLOY.md](docs/DEPLOY.md): hướng dẫn Aiven (tài khoản chỉ đọc `bankdw_viewer`) và Streamlit Community Cloud (secrets, giới hạn email người xem).
- **Chưa publish được**: dịch vụ Aiven cũ không còn (tên miền NXDOMAIN, dịch vụ đã tắt hoặc bị xoá) → cần bật lại hoặc tạo dịch vụ mới rồi cập nhật `.env`.

---

## [2026-10-29] Report: rút gọn, tập trung ý chính (bản Anh + Việt) ✂️

- **Abstract** 297 → **212 từ**, theo template ("approximately 200 words").
- **Toàn bộ report** 17 → **11 trang**, cả [report/main.tex](report/main.tex) và [report/main_vi.tex](report/main_vi.tex); vẫn đủ 7 phần đúng tên và thứ tự của template.
  - Bỏ: bảng mục tiêu kinh doanh và bảng nguồn dữ liệu dự kiến (thành 1 câu), bảng công nghệ–BR, listing DDL, 2 ảnh app (còn Tổng quan và cảnh báo), bảng các hướng không làm (thành 1 câu trong mục Phạm vi).
  - Gộp: thời gian truy vấn ×1 giữ 1 bảng; ×1 so với ×10 + dung lượng + thông lượng + chi phí sửa thành 1 bảng; B3 + B4 thành 1 bảng. Big Data và Related Work còn mỗi phần 1 đoạn.
  - Giữ nguyên mọi số liệu chính, 8 kịch bản, sơ đồ kiến trúc, mô hình sao.
- Bản tiếng Việt dịch lại từ bản rút gọn; số liệu đối chiếu tự động với bản tiếng Anh (17 bảng khớp từng dòng). Hai bản biên dịch sạch; cập nhật `report_overleaf.zip`.
- Ghi chú: khối F giữ **chỉ Streamlit** (làm đủ 8 BR, gồm cả ghi và điều khiển); sheet không có Superset, Superset chỉ thuộc Phase 1.

---

## [2026-10-29] Report: đối chiếu với template trong `report/LaTeX_Template_for_Technical_Report` 📐

- Cấu trúc đã đúng template (`report_template.tex`, IEEEtran peerreview): title/author/date, mục lục, danh sách hình/bảng, abstract, **7 phần đúng tên và thứ tự** (Introduction · Problem Definition · Proposed Solutions · Criteria for Assessing Solutions · Research Methodology · Analysis and Interpretation · Conclusions and Recommendations), phụ lục A/B, `thebibliography` viết tay, ngôn ngữ thận trọng (hedging) ở phần Analysis.
- Sửa theo hướng dẫn của template, ở **cả bản Anh và bản Việt**:
  - Template: Analysis "don't introduce any new content (e.g., research methods or solutions)". Phép đo bổ sung (hai cách viết truy vấn tra cứu tài khoản) chuyển sang **Research Methodology**; việc truy vấn tra cứu lọc fact theo khóa trước chuyển sang **Proposed Solutions** (mục ứng dụng); Analysis chỉ còn số liệu kết quả.
  - Template: Methodology "do not include your findings". Kết quả "khớp 14/15 số liệu khảo sát" chuyển sang **Problem Definition** (mục đặc điểm dữ liệu).
- Hai bản biên dịch sạch; cập nhật `report_overleaf.zip`.

---

## [2026-10-29] Phase 2 — Report tiếng Việt + sửa lỗi biên dịch trên Overleaf 📄

- [report/main_vi.tex](report/main_vi.tex): **bản tiếng Việt** đầy đủ của report (cùng mục, bảng, hình, số liệu; số theo định dạng Việt `1.056.320`, `3,4 s`). XeLaTeX + polyglossia, font TeX Gyre Termes; 17 trang, không lỗi, không tràn lề. Mọi con số đã được đối chiếu tự động với bản tiếng Anh (24 bảng khớp từng dòng). Thống nhất thuật ngữ "khu vực" cho district như app và slide.
- **Lỗi trên Overleaf** `fontspec … requires either XeTeX or LuaTeX`: file tiếng Việt (report và slide) bị biên dịch bằng pdfLaTeX. Thêm `report/latexmkrc` và `slides/latexmkrc` (`$pdf_mode = 5;`) để Overleaf tự dùng **XeLaTeX**; `main.tex` cũng biên dịch được bằng XeLaTeX.
- Slide tìm ảnh ở cả `../report/figures/` (repo) lẫn `figures/` (khi upload riêng).
- Gói upload Overleaf (không đưa lên git): `report_overleaf.zip` (main.tex, main_vi.tex, IEEEtran.cls, latexmkrc, figures/) và `slides_overleaf.zip` (slides.tex, latexmkrc, figures/).

---

## [2026-10-29] Phase 2 — Sửa truy vấn BR02, hoàn thiện report tiếng Anh, slide thuyết trình 📄

### Ứng dụng
- [apps/app/app.py](apps/app/app.py): **BR02** lọc bảng fact theo `account_sk` trước (khóa đầu của thứ tự sắp xếp) thay vì join `dim_account` rồi lọc `account_id`. Trên kho thật, tài khoản 11 (07/1998–12/1999): **9,6 → 4,3 ms**, đọc **162.739 → 19.694 dòng**, kết quả giống hệt. **BR08** (giao dịch theo phiên bản của 1 khách) sửa tương tự: 14,6 → 7,9 ms; vẫn quét cột `customer_sk` vì cột này không nằm trong thứ tự sắp xếp (không thêm index để khỏi đổi cấu trúc kho).

### Đo hiệu quả khai thác và ảnh chụp
- [tools/ui_walkthrough.py](tools/ui_walkthrough.py): script Playwright điều khiển app trong Chrome không giao diện (1440×900, nét gấp đôi), đi đúng luồng của từng BR, đếm thao tác, đo thời gian từ thao tác cuối đến khi app chạy xong, và chụp ảnh. Kết quả: BR01–BR04 **1–3 thao tác, 0,19–0,43 s**; BR05–BR08 hiện ngay (chuyển tab không chạy lại script); chạy lại KB03 từ app **10,5 s**. Ghi nhận BR04 thử đã xoá ngay sau khi đo.
- 9 ảnh chụp ứng dụng (đã cắt bỏ sidebar) trong [report/figures/](report/figures/), số đo trong `report/figures/ui_walkthrough.json`.

### Report tiếng Anh ([report/main.tex](report/main.tex))
- Bỏ mọi nhắc tới Phase 1 (report chỉ nói Phase 2).
- Thêm mục **Directions Considered but Not Implemented** (theo ghi chú Q03 của sheet): khoản vay, thẻ, lệnh định kỳ, RFM, kinh tế vùng, phát hiện gian lận theo luồng — kèm lý do ngoài phạm vi.
- Phương pháp và kết quả **hiệu quả khai thác** thay bằng số đo thật (bảng thao tác + thời gian), thêm **4 ảnh chụp** (Tổng quan, cảnh báo, BR08, chạy kịch bản).
- Ghi lại việc áp dụng phát hiện vào app (BR02, BR08); sửa một giải thích sai ở B2 (cũ: "khóa sắp xếp đã giới hạn về 1 tài khoản" — thực tế ClickHouse đọc cả bảng khi lọc qua dimension).
- Còn TODO cho nhóm: ngày nộp, bảng phân công. Biên dịch: 17 trang, không lỗi, không tràn lề.

### Slide ([slides/slides.tex](slides/slides.tex))
- Beamer 16:9, **tiếng Việt**, XeLaTeX (font TeX Gyre Heros), **6 phần** (yêu cầu môn học: 4–7): Bài toán · Giải pháp đề xuất · Phương án thay thế và cách đánh giá · Kết quả · Demo ứng dụng · Kết luận; 40 trang cho video 20–30 phút.
- Có ví dụ minh họa (khách 24 đổi hạng, cảnh báo tài khoản 11), biểu đồ so sánh ClickHouse–PostgreSQL, bảng ×1/×10, B1–B4, phát hiện BR02, số đo thao tác, ảnh chụp app, và trình tự demo KB01→KB08 cho video.
- Đã rà từng trang: không lỗi biên dịch, không tràn lề, không thiếu ký tự.

---

## [2026-10-22] Dọn repo: tách hẳn Phase 1 ra khỏi Phase 2 🧹

Thư mục gốc giờ chỉ còn Phase 2; mọi thứ của Phase 1 được **chuyển** (không xoá) vào `archive/phase1/` bằng `git mv`, nên lịch sử file vẫn còn.
- `docs/BENCHMARK.md`, `docs/PLAN.md`, `docs/architecture.html`, `docs/DOI_CHIEU_SHEET.md` (sheet ↔ repo Phase 1) → [archive/phase1/docs/](archive/phase1/docs/).
- `README.md` cũ (Kaggle, Kafka, Superset) → [archive/phase1/docs/README.md](archive/phase1/docs/README.md); **README mới cho Phase 2**: [README.md](README.md) (Berka, 3 nhóm người dùng, 8 BR, kiến trúc A–F, kết quả, cách chạy, cấu trúc thư mục).
- Nhật ký Phase 1 (23 mục, đến 07/10/2026) tách khỏi [CHANGELOG.md](CHANGELOG.md) sang [archive/phase1/CHANGELOG.md](archive/phase1/CHANGELOG.md); dọn các dòng trống thừa ở đầu changelog.
- `.env.example`: bỏ phần cấu hình Superset (Phase 2 không dùng).
- Dữ liệu trên máy (không có trên git): `lake/raw`, `core`, `checkpoints`, `logs`, `results`, `bench/results` và 5 thư mục staging của Phase 1 → `lake/phase1/`. `lake/` giờ chỉ còn dữ liệu Phase 2: `batches`, `staging/stg_*`, `eval`, `bench/p2`.
- Sửa link trong [docs/PLAN_PHASE2.md](docs/PLAN_PHASE2.md) tới file đối chiếu đã chuyển.

---

## [2026-10-22] Phase 2 — Đối chiếu 4 sheet với những gì đã làm 📋

- [docs/DOI_CHIEU_SHEET_P2.html](docs/DOI_CHIEU_SHEET_P2.html): trang HTML (mở trực tiếp bằng trình duyệt) đối chiếu từng mục của 4 sheet "Theo dõi dự án", "Business Requirement", "Data Model", "Data Dictionary" (bản đọc lúc 18:07 ngày 22/10) với hiện trạng: 17 đã làm, 5 làm một phần hoặc khác sheet, 2 chưa làm, 2 cần nhóm xác nhận, 1 ngoài phạm vi; có bộ lọc theo trạng thái và danh sách việc còn lại.
- Chưa làm: slide + video, phần "các hướng khác" trong report (ghi chú Q03), ngày nộp và phân công (M07). Cần nhóm xác nhận: ngưỡng R1/R2, con số VYBER 9.597.

---

## [2026-10-22] Phase 2 — P2-8: EVALUATION_P2.md có đủ ×1 và ×10 📝

- [docs/EVALUATION_P2.md](docs/EVALUATION_P2.md) mục 2 viết lại thành **một bảng có cột ×1 và ×10** cho từng phép so sánh (B1–B4, pipeline, phát hiện tra cứu tài khoản), kèm cột kết luận ngắn. Trước đây phần ×10 chỉ là một đoạn văn bên dưới bảng ×1 nên dễ bị sót.
- Sửa chỗ dẫn sai: sự cố khi đo nằm ở mục 9 (×1) và mục 12.8 (×10) của BENCHMARK_P2.md.

---

## [2026-10-22] Phase 2 — P2-8: benchmark mức ×10 ✅

### Đã làm
- **Dữ liệu ×10 (nhân bản, không phải dữ liệu mới):** mỗi tài khoản, khách, giao dịch của bản sao ×1 chép thành 10 bản với mã mới; ngày, số tiền, hạng, lịch sử SCD2 giữ nguyên. Nằm trong database riêng `bench10` (ClickHouse) và `dwpg10` (PostgreSQL); **kho `dw` vẫn ×1**. Đã sao lưu `dw` thành `before_x10` trước khi làm; sau khi đo, `dw` vẫn 1.057.571 giao dịch và `make evaluate` vẫn 38/38.
- Kiểm tra: ×10 có đúng **10 lần** ×1 về số dòng (10.575.710), tổng tiền, số tài khoản, số khách.
- [spark/jobs/benchmark.py](spark/jobs/benchmark.py), [spark/jobs/bench_spark.py](spark/jobs/bench_spark.py): chạy theo mức (`BENCH_SCALE`); `make bench-all SCALE=10`; `make bench-clean` xoá mọi bản sao benchmark (không đụng `dw`, `ops`).
- [docs/BENCHMARK_P2.md](docs/BENCHMARK_P2.md) thêm **mục 12** (cùng khung giải thích: câu hỏi → cách đo → đọc bảng → kết luận); số liệu và log ở [docs/results/p2/x10/](docs/results/p2/x10/). Report thêm bảng ×1 so với ×10 và đoạn phân tích.

### Kết quả chính
- Truy vấn tổng hợp: ClickHouse chậm đi 2–6× khi dữ liệu gấp 10, PostgreSQL chậm đi 4–10× → khoảng cách **nới rộng từ 3–22× lên 7–28×**. 8 người dùng: PostgreSQL chỉ còn 0,4 truy vấn/giây (p95 38 s).
- **Phát hiện:** tra cứu 1 tài khoản trên star / ClickHouse đọc **cả bảng giao dịch** khi lọc qua `dim_account` (60 ms, 10,6 triệu dòng ở ×10); lọc theo `account_sk` trước chỉ đọc khoảng 100 nghìn dòng, **6,8 ms ở cả ×1 và ×10** ([lookup_experiment.csv](docs/results/p2/x10/lookup_experiment.csv)). Chưa sửa app.
- One Big Table: sửa tên 1 khu vực phải ghi lại 1.500.580 dòng (star: 1 dòng).
- Spark: 1 lô ×10 nhanh hơn 2,1× khi dùng 8 lõi (28,8 → 14,0 s); 72 lô tháng vẫn không hưởng lợi.

### Sự cố và cách xử lý
- ClickHouse hết bộ nhớ khi chép 10 bản bảng giao dịch, rồi khi dựng One Big Table ×10 → chép và dựng **từng bản một**, khối ghi nhỏ hơn, khởi động lại ClickHouse trước khi nạp và đo.
- PostgreSQL báo "No space left on device": **không phải hết đĩa** (đĩa còn 29 GB) mà là `/dev/shm` 64 MB của container → `shm_size: 256m` trong [docker-compose.yml](docker-compose.yml) (RAM, trong giới hạn 1 GB).
- Đĩa: dữ liệu ×10 dùng thêm khoảng 2 GB (ClickHouse 0,4 GB, PostgreSQL 1,6 GB); đĩa máy còn khoảng 30 GB, không vượt giới hạn.

---

## [2026-10-22] Phase 2 — P2-8: viết lại BENCHMARK_P2.md cho dễ hiểu 📝

- [docs/BENCHMARK_P2.md](docs/BENCHMARK_P2.md) viết lại theo yêu cầu "đọc không hiểu": thêm mục **Benchmark trả lời câu hỏi gì**, **bảng giải nghĩa thuật ngữ** (cold/warm, median, p95, star schema, One Big Table, lưu theo cột/dòng, index, mutation, data part, transaction…), và cho **mỗi phép đo** một khung cố định: câu hỏi → cách đo → đọc bảng thế nào (kèm một dòng ví dụ) → kết quả → kết luận. Thêm cột "ý nghĩa với thiết kế" ở bảng giả thuyết.
- Không thay đổi số liệu nào; chỉ sửa ước lượng chi phí cố định mỗi lô thành khoảng 0,3 s và ghi rõ dữ liệu B4 khoảng 24 MB Parquet.

---

## [2026-10-22] Phase 2 — P2-8 (mở rộng): benchmark B1–B4 ở mức ×1 + P2-9 (bắt đầu): bản nháp report 🚧

### Benchmark — so sánh mô hình, công nghệ, kiến trúc ([docs/BENCHMARK_P2.md](docs/BENCHMARK_P2.md))
Mỗi phép so sánh chỉ đổi một yếu tố; mọi phép thử ghi chạy trên bản sao (`bench`, `dwpg`, `bench_ops`), không đụng `dw` và `ops`.
- **B1 Star vs One Big Table (cùng ClickHouse)** — [spark/jobs/benchmark.py](spark/jobs/benchmark.py): One Big Table nhanh hơn 1,5–3,4× (chênh 3–16 ms), tốn chỗ hơn 1,37×; sửa tên 1 khu vực phải ghi lại 150.058 dòng (star: 1 dòng).
- **B2 ClickHouse vs PostgreSQL (cùng star; PostgreSQL cấu hình như Phase 1)**: 9 truy vấn BR01/BR02/BR03/BR08 × 6 lần; truy vấn tổng hợp ClickHouse nhanh hơn 3,3–22,4×, tra cứu 1 tài khoản PostgreSQL nhanh hơn 17–46×; dung lượng 18,6 so với 162,5 MB; 8 client: 93 so với 12 truy vấn/giây. **18/18** cặp kết quả khớp từng ô.
- **B3 Kho vận hành PostgreSQL vs ClickHouse** — [spark/jobs/bench_ops.py](spark/jobs/bench_ops.py): PostgreSQL nhanh hơn 8–70× ở mọi thao tác của app và pipeline; tự bảo đảm 5/5 ràng buộc (trạng thái, chuyển trạng thái, chống trùng, khóa ngoại, giao dịch), ClickHouse 0/5.
- **B4 Spark 1/2/4/8 lõi, 1 lô vs 72 lô** — [spark/jobs/bench_spark.py](spark/jobs/bench_spark.py): ở ×1, thêm lõi gần như không giúp (7,7 → 5,4 s); 72 lô chậm hơn 1 lô 4–6×; mọi lần chạy đúng 1.056.320 dòng.
- `make bench-all` (và từng bước `bench-load`, `bench`, `bench-concurrency`, `bench-update`, `bench-ops`, `bench-spark`): mỗi bước giữ log console; toàn bộ số liệu và log nằm ở [docs/results/p2/](docs/results/p2/).
- [docs/EVALUATION_P2.md](docs/EVALUATION_P2.md) mục 2 thay bằng tóm tắt B1–B4.

### Cấu hình thay đổi
- PostgreSQL: `shared_buffers` 256 MB ([docker-compose.yml](docker-compose.yml)); `work_mem` 64 MB, 4 worker, `jit` off chỉ đặt trong phiên benchmark.
- ClickHouse: [warehouse/clickhouse/config.d/system_logs.xml](warehouse/clickhouse/config.d/system_logs.xml) tắt log hệ thống chạy ngầm (giữ `query_log`, `part_log`, `backup_log`, `error_log`); xoá các bảng log cũ → RAM nền 1,2 GB → 0,74 GB.

### Sự cố khi đo (ghi đủ ở mục 8 của BENCHMARK_P2.md)
- ClickHouse hết bộ nhớ ở truy vấn BR02 do log hệ thống → tắt log, chạy lại toàn bộ.
- Nạp PostgreSQL 42 s vì có khóa ngoại trong lúc COPY → tạo khóa sau COPY: 4,6 s.
- Máy ngủ khoảng 2 giờ giữa `make bench-all` làm hỏng phép đo tải đồng thời → giữ máy thức, đo lại.
- Mutation ClickHouse không sửa được cột phiên bản của ReplacingMergeTree (ghi nhận thành phát hiện của B3); `part_log` đếm lẫn lần chạy cũ → chỉ đếm từ lúc bắt đầu.

### Report (P2-9, bản nháp)
- [report/main.tex](report/main.tex) viết lại cho Phase 2: Berka, 3 nhóm người dùng, 8 BR, kiến trúc A–F, mô hình sao có SCD2, 8 kịch bản, 3 giải pháp kho + phương án kiến trúc kho vận hành, giả thuyết H1–H5, kết quả B1–B4. Biên dịch không lỗi, không tràn lề. Report Phase 1 vẫn ở tag `phase1-kaggle`.
- Còn TODO: ngày nộp, ảnh chụp 3 tab, phân công Phase 2, số thao tác đo bằng đồng hồ khi quay video.

---

## [2026-10-22] Phase 2 — P2-8: đánh giá KB01–KB08 + benchmark ClickHouse vs PostgreSQL ✅

### Đã làm
- [spark/jobs/evaluate.py](spark/jobs/evaluate.py) (`make evaluate`): đối chiếu 8 KB với **nguồn** (Berka + file kịch bản, tính lại bằng pandas), `expected.json` và `ops.*`; thêm 7 kiểm tra chất lượng dữ liệu (ISO/IEC 25012 + SCD2). Chỉ đọc kho; KB04 chạy trong transaction rồi rollback.
- [spark/jobs/benchmark.py](spark/jobs/benchmark.py) (`make bench-load`, `make bench`): chép 6 bảng sang PostgreSQL (database `dwpg`, có PK/FK, index, ANALYZE), đo 7 truy vấn BR01/BR02/BR08, mỗi truy vấn 6 lần (lần đầu lạnh, median/p95 của lần 2–6), so kết quả giữa 2 hệ; thời gian pipeline lấy từ `ops.pipeline_step`.
- [docs/EVALUATION_P2.md](docs/EVALUATION_P2.md): bảng kết quả 8 KB + bảng benchmark + giới hạn của phép đo; số liệu gốc ở [docs/results/p2/](docs/results/p2/).

### Kết quả
- **8/8 KB đạt, 38/38 kiểm tra.** BR01 khớp nguồn tới từng xu cho 4 kỳ; BR02 khớp từng giao dịch của 23 tài khoản.
- **Truy vấn tổng hợp** (BR01, BR08 theo hạng): ClickHouse nhanh hơn 2,4–19 lần (ví dụ BR08 42 ms so với 802 ms). **Tra cứu một tài khoản hoặc một khách**: PostgreSQL nhanh hơn nhờ B-tree (ví dụ 1,8 ms so với 24 ms), nhưng cả hai đều dưới 25 ms. Fact trên ClickHouse 19 MB, PostgreSQL 169 MB (gồm index).
- Pipeline: median 3,4 s mỗi lô tháng (p95 5,3 s); 72 lô mất 273,5 s.

### Lỗi tìm ra khi đánh giá và đã sửa
- **`expected.json` của KB03**: trung bình tiền ra lấy theo tháng 7–12/1998, trong khi R1 cho tháng 2/1999 phải dùng tháng 8/1998–1/1999. Kho đã đúng; sửa [tools/make_scenarios.py](tools/make_scenarios.py) và sinh lại. `trans.csv` của mọi kịch bản không đổi (đã kiểm tra bằng diff).
- **Bước `load_dimensions`** không ghi `rows_in`: nay ghi số tổ hợp trong lô và số tổ hợp mới. Đã thử bằng `make rerun BATCH=HIST_1993_01`.
- Benchmark: với tháng không có tiền vào, PostgreSQL trả `NULL` còn ClickHouse `sumIf` trả 0 → thêm `coalesce` (lỗi của truy vấn, không phải của dữ liệu).

---

## [2026-10-22] Phase 2 — P2-6 hoàn tất: ứng dụng 3 tab demo đủ 8 BR ✅

### Đã kiểm tra trên trình duyệt (sau khi giới hạn bộ đệm ClickHouse)
- **Chạy kịch bản từ giao diện:** bấm "▶ Chạy KB03" → runner chạy lại lô `KB03_FRAUD`, run 89 `SUCCEEDED`, `triggered_by = app`; nhận 5, nạp mới 0 (5 đã có), cảnh báo 5 tìm thấy / 0 mới; kho vẫn 1.057.571 giao dịch, 1.702 cảnh báo. Ứng dụng không còn lỗi hết bộ nhớ (ClickHouse ~1,1 GB / 2,4 GB khi chạy).
- **BR05:** danh mục quy tắc V1–V7, số bản ghi bị tách theo lô và quy tắc, chi tiết từng bản ghi + lý do.
- **BR07:** danh mục bản sao lưu (`before_kb07`, 1.057.571 dòng fact, 5.118 phiên bản SCD), lịch sử thao tác kèm kết quả đối chiếu.
- **BR08:** báo cáo theo hạng kỳ cũ (1993–1998) và kỳ mới (01/1999); `dim_customer` 5.118 phiên bản / 4.500 hiện tại / 618 đã đóng; khách 24: Standard → Silver từ 1999-01-01.
- Cùng với các phần đã kiểm tra trước (BR01–BR04, BR06, xem trước kịch bản): **đủ 8 BR trên 1 app**.

### Kế hoạch
- Tick toàn bộ P2-6 trong [docs/PLAN_PHASE2.md](docs/PLAN_PHASE2.md); P2-5 ghi nhận KB04 có cả phần UI.

---

## [2026-10-22] Phase 2 — P2-6 (đang làm): ứng dụng 3 tab + kênh điều khiển F → C 🚧

### Đã làm
- [spark/jobs/runner.py](spark/jobs/runner.py): dịch vụ HTTP nhỏ trong container Spark (`http://spark:8000`, chỉ trong mạng Docker, không mở cổng ra máy): chạy **lệnh trong danh sách cho phép** (kịch bản KB03/05/07/08, chạy lại lô, quét cảnh báo, đối chiếu, sao lưu, khôi phục), **mỗi lúc 1 việc**, xem trước kịch bản mà không ghi gì (N01). Lần chạy do ứng dụng khởi động ghi `triggered_by = app`.
- [apps/app/app.py](apps/app/app.py): ứng dụng Streamlit 3 tab — **Tổng quan** (BR01: kỳ, tiền vào/ra/thuần, số giao dịch, theo hạng, biểu đồ ngày/tháng), **Phân tích** (BR02 tra cứu tài khoản; BR03 danh sách + lý do + căn cứ + giao dịch liên quan; BR04 form đổi trạng thái + ghi chú + lịch sử), **Vận hành** (BR06 lần chạy + bước; BR05 bản ghi bị tách; chạy kịch bản N01–N05; BR07 sao lưu / khôi phục + kết quả đối chiếu; BR08 báo cáo theo hạng kỳ cũ / kỳ mới + phiên bản SCD của khách demo).
- [warehouse/clickhouse/config.d/memory.xml](warehouse/clickhouse/config.d/memory.xml): giới hạn bộ đệm ClickHouse (mark cache 256 MiB thay vì 5 GB mặc định, tắt uncompressed cache) — nguyên nhân gốc của các lỗi hết bộ nhớ ở P2-5 → P2-7.

### Đã kiểm tra trên trình duyệt
- BR01 hiển thị đúng (năm 1998: vào 960.964.282, ra 933.819.075, thuần 27.145.207, 322.277 giao dịch).
- BR02 tài khoản 11: 30 giao dịch 7/1998 → 12/1999, thấy giao dịch rút 60.000 của KB03.
- BR03 chi tiết cảnh báo #1699 (lý do, căn cứ JSON, giao dịch liên quan); **BR04 qua giao diện ghi nhận được** NEW → IN_REVIEW kèm ghi chú (dữ liệu thử đã xoá).
- BR06 bảng lần chạy; xem trước KB03.

### Lỗi gặp và sửa
- Màu đường tiền vào/ra bị đảo (Altair gán màu theo thứ tự chữ cái) → khai báo `domain`.
- Bấm "Chạy KB03" làm ứng dụng lỗi hết bộ nhớ ClickHouse: Streamlit chạy lại toàn bộ script (cả truy vấn tab Tổng quan) và bộ đệm ClickHouse đã đầy → cache truy vấn ứng dụng 60 s, bỏ `FINAL` trên fact trong truy vấn đọc (pipeline đảm bảo không trùng, kiểm tra U3), giới hạn bộ đệm ClickHouse. **Chưa thử lại sau khi sửa.**

### Còn lại của P2-6
- Thử lại chạy kịch bản từ giao diện; kiểm tra tab BR05, BR07, BR08; tick plan và commit hoàn tất.

---

## [2026-10-22] Phase 2 — P2-7: sao lưu và khôi phục Data Warehouse (BR07, KB07) ✅

*(Làm trước P2-6 vì tab Vận hành của ứng dụng cần gọi các chức năng này.)*

### Đã làm
- [spark/jobs/backup.py](spark/jobs/backup.py) + `make backup [NAME=]`, `make restore NAME=…`, `make backups` (luồng L06):
  - **Sao lưu:** `BACKUP DATABASE dw TO Disk('backups', …)` — cấu trúc + dữ liệu đủ 6 bảng, gồm lịch sử SCD2; ghi `ops.backup_catalog` kèm **số liệu đối chiếu** (số dòng từng bảng, tiền vào/ra, báo cáo theo hạng, số phiên bản SCD, các lô ngoài lịch sử).
  - **Khôi phục:** xoá 6 bảng rồi `RESTORE` (không trộn hai trạng thái); tính lại số liệu và so với bản sao lưu → kết quả trước/sau lưu ở `ops.backup_operation.result`.
  - Chỉ chạy khi **pipeline không ghi** (không có lần chạy `RUNNING`), đúng sheet L06. PostgreSQL ngoài phạm vi sao lưu (sheet T02).
- Kịch bản **KB07** ([tools/make_scenarios.py](tools/make_scenarios.py)): 20 lần nộp 2.500 CZK ngày 10/3/1999 ở tài khoản chưa dùng trong kịch bản khác.

### Kết quả KB07
| Bước | Kết quả |
|---|---|
| Sao lưu `before_kb07` | AVAILABLE, 0,2 s, ~20 MB trên đĩa |
| Nạp KB07 | +20 giao dịch → 1.057.591 dòng |
| Khôi phục `before_kb07` | **SUCCEEDED**; số dòng, tiền vào, tiền ra, báo cáo theo hạng, phiên bản SCD **khớp 5/5** với bản sao lưu; kho về **1.057.571** dòng, 0 dòng KB07 |
| Nạp tiếp sau khôi phục | chạy lại KB07 → nạp lại đúng 20 dòng (pipeline kiểm tra trùng theo dữ liệu thật trong kho) |
| Lịch sử Berka | vẫn RECONCILED |

### Lỗi gặp và sửa
- `dim_date` là `MergeTree` thường → không dùng được `FINAL` (lỗi ClickHouse 181) khi lấy số liệu → bỏ `FINAL` riêng cho bảng này.
- Ngay sau `RESTORE`, ClickHouse gần chạm giới hạn 2,2 GB nên các truy vấn đối chiếu hết bộ nhớ (lần khôi phục đầu bị ghi FAILED dù dữ liệu đã đúng) → xả bộ đệm (`SYSTEM DROP … CACHE`, `JEMALLOC PURGE`) và giới hạn 2 luồng trước khi đối chiếu. Bản ghi lần khôi phục lỗi được giữ trong `ops.backup_operation` làm ví dụ thật.

---

## [2026-10-22] Phase 2 — P2-5: cảnh báo gian lận (BR03) và xử lý cảnh báo (BR04) ✅

### Đã làm
- [spark/jobs/alerts.py](spark/jobs/alerts.py): 2 quy tắc tính trên kho sau khi nạp (luồng L03), tham số đọc từ `ops.alert_rule`:
  - **R1 – tiền ra tăng mạnh:** tiền ra của tài khoản trong tháng > 3 × trung bình 6 tháng trước và > 50.000 CZK (cần đủ 6 tháng lịch sử).
  - **R2 – tài khoản ngủ đông hoạt động lại:** giao dịch ra sau ≥ 180 ngày không có giao dịch.
  - Mỗi cảnh báo có lý do dễ đọc, căn cứ (`evidence`) và các giao dịch liên quan; **duy nhất theo (tài khoản, quy tắc, kỳ)**.
- Pipeline thêm bước **`detect_alerts`** sau `reconcile` cho mọi lô; lệnh quét lại `make alerts [ON=all|<lô>]`.
- **KB03** ([tools/make_scenarios.py](tools/make_scenarios.py)): 3 tài khoản rút 60.000 CZK tháng 2/1999 (gấp 10–20 lần mức thường) → R1; 2 tài khoản không giao dịch từ 31/12/1998 rồi rút 5.000 CZK ngày 15/7/1999 (196 ngày) → R2. Không trùng tài khoản KB05, KB08.
- **BR04:** hàm `ops.set_alert_status(alert_id, trạng thái, ghi chú, người xử lý)` — đổi trạng thái + ghi lịch sử trong 1 giao dịch; cho phép NEW → IN_REVIEW → RESOLVED, NEW → RESOLVED, mở lại RESOLVED → IN_REVIEW; chặn bước khác. Không đụng dữ liệu kho.

### Kết quả
| Kiểm tra | Kết quả |
|---|---|
| Quét lịch sử 1993–1998 (+ KB05, KB08) | **1.697** cảnh báo: R1 1.695 (843 tài khoản, ~24/tháng), R2 2 |
| KB03 | 5 giao dịch → **5/5 cảnh báo mới** đúng tài khoản, đúng quy tắc, đúng kỳ (khớp `expected.json`) |
| Chạy lại KB03 / quét lại toàn bộ | **0 cảnh báo mới** (không trùng); tổng 1.702 |
| BR04 | NEW → IN_REVIEW → RESOLVED ghi đủ lịch sử + ghi chú; RESOLVED → NEW **bị chặn**. Dữ liệu thử đã đặt lại về NEW |

### Lỗi gặp và sửa
- Lần chạy lại KB03 thứ nhất **FAILED ở bước `detect_alerts`**: ClickHouse hết bộ nhớ (2,2 GB) vì hàm cửa sổ chạy trên toàn bộ 1 triệu giao dịch rồi mới lọc theo lô → chỉ đọc các tài khoản lô có chạm tới; giới hạn 2 luồng và cho sắp xếp tràn ra đĩa khi quét toàn bộ. Bản ghi lần chạy lỗi được **giữ lại trong `ops.pipeline_run`** làm ví dụ thật cho BR06 (bước thất bại + nguyên nhân).

---

## [2026-10-22] Phase 2 — P2-4: SCD2 đổi hạng (KB08) ✅ — hoàn tất P2-4

### Quyết định
- **Q-E chốt (theo lựa chọn của nhóm):** đổi hạng = **đánh giá lại cuối năm 1998** theo tổng tiền vào 1998, cùng ngưỡng với hạng ban đầu; hạng mới hiệu lực **1999-01-01** → **618/4.500 khách (13,7%)** đổi hạng (12 hướng, ví dụ Gold → Platinum 109, Silver → Gold 86, Gold → Silver 72).

### Đã làm
- [tools/make_scenarios.py](tools/make_scenarios.py) thêm **KB08**: `customer_profile_changes.csv` (618 dòng) + `trans.csv` kỳ sau (tháng 1/1999: 1 lần nộp + 1 lần rút cho mỗi tài khoản đổi hạng, 1.236 giao dịch, số dư nối tiếp cuối 1998) + `expected.json`. Có kiểm tra KB08 không đụng tài khoản của KB05.
- Pipeline: bước `load_customer_scd2` trong lô kịch bản; **chặn thay đổi hạng đến muộn**: nếu khách đã có giao dịch từ ngày hiệu lực trở đi được nạp với hạng cũ → từ chối (V4), không đóng/mở phiên bản. Khách đã ở đúng hạng mới → "không đổi" (không phải lỗi).

### Kết quả KB08
| Kiểm tra | Kết quả |
|---|---|
| Phiên bản `dim_customer` | đóng **618**, mở **618** → **5.118** phiên bản, **4.500** hiện tại, **0** khoảng hiệu lực chồng lấn |
| Báo cáo theo hạng kỳ cũ (1993–1998) | **giống hệt** trước và sau KB08 (Gold / Platinum / Silver / Standard: số giao dịch, tiền vào, tiền ra) |
| Kỳ mới (1/1999) theo hạng mới | khớp `expected.json`: Gold 170 khách (vào 5.735.000 / ra 2.294.000), Platinum 178 (10.148.400 / 4.059.360), Silver 162 (2.862.400 / 1.144.960), Standard 108 (987.600 / 395.040) |
| Nạp giao dịch | 1.236 / 1.236, 0 lỗi |
| Chạy lại KB08 | 0 phiên bản mới, 618 "không đổi", 0 giao dịch mới, `SUCCEEDED` |
| Kiểm tra âm tính | đổi hạng khách 1 với hiệu lực 1998-06-01 (đã có giao dịch sau đó) → **bị từ chối V4**, khách vẫn 1 phiên bản (lô thử đã xoá khỏi `ops`) |
| Lịch sử Berka sau mọi kịch bản | vẫn **RECONCILED** với nguồn |

### Lỗi gặp và sửa
- Lần chạy lại đầu tiên đánh dấu nhầm 618 thay đổi là lỗi "đến muộn" (khách đã ở hạng mới) → chỉ áp dụng kiểm tra cho khách có hạng hiện tại khác hạng mới.

---

## [2026-10-22] Phase 2 — P2-4 (một phần): lô kịch bản và KB05 kiểm soát dữ liệu ✅

### Đã làm
- `pipeline.py scenario <KB>` / `make scenario KB=…`: chạy lô kịch bản (`batch_type = SCENARIO`) từ `data/berka/scenarios/<KB>/`; hỗ trợ sẵn file đổi hạng `customer_profile_changes.csv` → bước `load_customer_scd2` trước khi nạp giao dịch (dùng cho KB08).
- [tools/make_scenarios.py](tools/make_scenarios.py) + `make scenarios`: sinh dữ liệu kịch bản (định dạng Berka) kèm `expected.json` (kết quả mong đợi cho đánh giá P2-8).
- **KB05:** 10 giao dịch hợp lệ (tháng 1/1999, 10 tài khoản thật, số dư nối tiếp số dư cuối 1998) + 10 giao dịch lỗi chủ đích: V1 thiếu số tiền / thiếu tài khoản, V2 tài khoản 99999, V3 số tiền âm / không phải số, V4 ngày 31/02 / năm 2001 ngoài lịch, V5 loại `XYZ`, V6 trùng trong lô / trùng giao dịch đã nạp.

### Kết quả KB05 (khớp `expected.json`)
| | Mong đợi | Thực tế |
|---|---:|---:|
| Nhận | 20 | 20 |
| Nạp | 10 | 10 |
| Tách vào `ops.rejected_record` | 10 (V1 2, V2 1, V3 2, V4 2, V5 1, V6 2) | 10 (V1 2, V2 1, V3 2, V4 2, V5 1, V6 2) |
| Trạng thái lần chạy | lỗi không làm hỏng cả lô | `SUCCEEDED_WITH_REJECTS`, 6/6 bước SUCCEEDED |

### Còn mở
- **KB08:** cách giả lập đổi hạng cần nhóm chốt (đánh giá lại theo tiền vào 1998 cho ra 618/4.500 khách = 13,7%, khác đề xuất ~5% trong Q-E).

---

## [2026-10-22] Phase 2 — P2-3: pipeline theo lô, nạp toàn bộ lịch sử Berka ✅

### Đã làm
- [spark/jobs/pipeline.py](spark/jobs/pipeline.py) (PySpark) — luồng L01, L02, L05 của sheet:
  - **Lô `REF_INITIAL`:** chụp `district`, `account`, `client`, `disp`, `customer_profile_simulated` vào Data Lake → kiểm tra (mỗi tài khoản đúng 1 OWNER, khách nào cũng có hạng, khu vực hợp lệ) → `dim_date`, `dim_district`, `dim_account` (Type 1) → **ETL join `client` ⋈ `disp`(OWNER) ⋈ `district` ⋈ hạng mô phỏng → `stg_customer`** (1 dòng/khách) + `stg_account_owner` (1 dòng/tài khoản) → `dim_customer` SCD2 (phiên bản đầu). Giới tính và ngày sinh tách từ `birth_number` (tháng + 50 = nữ).
  - **Lô `HIST_YYYY_MM` (72 lô):** `ingest` (chụp vào `lake/batches/<lô>/`) → `validate` (V1–V7, bản ghi lỗi vào `ops.rejected_record`) → `standardize` (NULL/chuỗi trắng → `NOT_RECORDED`, PRIJEM → IN, VYDAJ/VYBER → OUT) → `load_dimensions` (thêm tổ hợp phân loại mới) → `load_fact` (`customer_sk` = phiên bản OWNER hiệu lực tại ngày giao dịch; bỏ qua giao dịch đã nạp bởi chính lô đó) → `reconcile` (số dòng + tiền vào/ra của lô so với kho).
  - **Chạy lại** một lô từ bản chụp trong Data Lake; **đối chiếu toàn bộ** nguồn ↔ kho.
  - Mỗi lần chạy và từng bước ghi vào `ops.pipeline_run` / `ops.pipeline_step` (BR06).
- [spark/jobs/ops_log.py](spark/jobs/ops_log.py): ghi lô, lần chạy, bước, bản ghi bị tách.
- Thêm quy tắc **V7** (không có phiên bản chủ sở hữu hiệu lực tại ngày giao dịch) vào `ops.dq_rule`.
- Makefile: `make load-ref`, `make load-history [FROM= TO=]`, `make rerun BATCH=…`, `make reconcile`.

### Kết quả
| Kiểm tra | Kết quả |
|---|---|
| Lô chạy | 73 lô (1 REF + 72 HIST), **73/73 SUCCEEDED**, tổng ~285 s (mỗi lô tháng ~4 s) |
| Đối chiếu toàn bộ | **1.056.320 / 1.056.320** giao dịch, 1.056.320 ID khác nhau; tiền vào **3.227.472.424,50**, tiền ra **3.030.321.135,80** — khớp nguồn tới từng xu |
| Chạy lại `HIST_1995_03` | 0 dòng mới, 9.304 dòng "đã nạp"; kho vẫn khớp nguồn → **không nhân đôi** |
| Bản ghi lỗi trên dữ liệu gốc | 0 (V1–V7) |
| `dim_transaction_profile` | **15 tổ hợp** (4 IN, 11 OUT) — đúng sheet K04 |
| SCD2 | 4.500 khách, mỗi khách đúng 1 phiên bản hiện tại |
| Khóa ngoại của fact (date, account, customer, profile) | 0 khóa mồ côi |
| Khách của giao dịch = OWNER của tài khoản | 100% |

### Lỗi gặp và sửa
- `valid_to = 2299-12-31` vượt giới hạn ngày của pandas (2262) → đọc ClickHouse thành đối tượng Python (`date`, `Decimal`) thay vì `query_df`.
- Phép ghép giao dịch ↔ phiên bản khách hàng lọc *sau* khi ghép → giao dịch không thuộc phiên bản nào có thể mất âm thầm → đưa điều kiện ngày vào trong phép ghép, trường hợp này bị tách theo V7.
- Kết quả truy vấn rỗng (lần nạp đầu) không có tên cột → lấy tên cột bằng `DESCRIBE`.

### Lưu ý
- Thư mục `lake/` còn dữ liệu Phase 1 (`raw/`, `core/`, `staging/` cũ của Kaggle, ~0,5 GB, không commit); Phase 2 chỉ dùng `lake/batches/` (24 MB) và các file `stg_*` trong `lake/staging/`.

---

## [2026-10-22] Phase 2 — P2-2: lược đồ Data Warehouse và kho vận hành ✅

### Đã làm
- **ClickHouse `dw`** — [warehouse/clickhouse/ddl/dw.sql](warehouse/clickhouse/ddl/dw.sql): 6 bảng theo Data Dictionary của sheet (`fact_transaction`, `dim_customer` SCD2, `dim_account`, `dim_date`, `dim_district`, `dim_transaction_profile`).
  - Dimension: `ReplacingMergeTree(updated_at)` → sửa Type 1 và đóng phiên bản SCD2 bằng cách ghi dòng mới, đọc bằng `FINAL`.
  - Fact: `ReplacingMergeTree(loaded_at)`, `ORDER BY (account_sk, date_key, transaction_id)`, phân vùng theo năm; chống trùng khi chạy lại (lưới an toàn, pipeline vẫn tự bỏ ID đã có).
  - **Q-J đã chốt về vật lý:** `valid_to = 2299-12-31` (`Date32`) thay cho mốc logic 9999-12-31; `birth_date` dùng `Date32` (trước 1970).
  - Chỉ `CREATE … IF NOT EXISTS`, không bao giờ xoá dữ liệu.
- **PostgreSQL `ops`** — [warehouse/postgres/ddl/ops.sql](warehouse/postgres/ddl/ops.sql): 10 bảng có PK/FK/CHECK: `batch`, `pipeline_run`, `pipeline_step` (BR06); `dq_rule`, `rejected_record` (BR05); `alert_rule`, `alert` (duy nhất theo tài khoản + quy tắc + kỳ), `alert_action` (BR03, BR04); `backup_catalog`, `backup_operation` (BR07). Dữ liệu mẫu: quy tắc chất lượng V1–V6, quy tắc cảnh báo R1–R2 (theo đề xuất Q-G, Q-F).
- [spark/jobs/apply_schema.py](spark/jobs/apply_schema.py) + `make schema`: tạo cả hai lược đồ và **đối chiếu từng cột với Data Dictionary**.
- [docs/data_model.md](docs/data_model.md): ERD (Mermaid) của kho, bảng quyết định vật lý, mô tả kho vận hành.

### Kiểm tra
- `make schema`: **6/6 bảng ClickHouse khớp đúng thứ tự và tên cột** với Data Dictionary; PostgreSQL 10/10 bảng; chạy lần 2 không lỗi, không nhân đôi dữ liệu mẫu.

---

## [2026-10-22] Phase 2 — P2-1: hạ tầng theo kiến trúc A–F ✅

### Đã làm
- **Lưu trữ Phase 1:** chuyển 59 file không dùng ở Phase 2 vào [archive/phase1/](archive/phase1/) bằng `git mv` (Kafka producer, Spark jobs p1–p4, Superset, Streamlit cũ, benchmark ×1, DDL/truy vấn Kaggle). Bản chạy đầy đủ của Phase 1 ở tag **`phase1-kaggle`** (đã push lên GitHub).
- **[docker-compose.yml](docker-compose.yml) viết lại** theo sheet, không còn Kafka/Superset:

| Khối | Service | Ghi chú |
|---|---|---|
| A Nguồn | — | `data/berka` mount chỉ đọc vào Spark |
| B Data Lake | — | thư mục `lake/` (`batches/`, `staging/`) |
| C Pipeline | `bankdw-spark` (Spark 3.5.6, PySpark) | image gọn hơn: bỏ jar Kafka, `confluent-kafka` |
| D Data Warehouse | `bankdw-clickhouse` 25.3 | database `dw`; ổ **`backups`** (mount `./backups`) cho BR07 |
| E Kho vận hành | `bankdw-postgres` 16.4, database `bankops`, schema `ops` | volume mới `ops-data`, cổng 5433 |
| F Ứng dụng | `bankdw-app` (Streamlit), http://localhost:8501 | khung 3 tab: Tổng quan / Phân tích / Vận hành |

- [spark/jobs/common.py](spark/jobs/common.py): đường dẫn Berka/lake mới, kết nối PostgreSQL; [spark/jobs/smoke.py](spark/jobs/smoke.py) kiểm tra cả 6 khối.
- [Makefile](Makefile) viết lại: `up`, `down`, `smoke`, `sql`, `psql`, `source-check`, `tiers`, `clean`.
- README: thêm ghi chú đang ở Phase 2.

### Kiểm tra
- `make smoke`: A–F đều trả lời (ClickHouse 25.3.14.14 có database `dw` + ổ `backups`; PostgreSQL 16.4 có schema `ops`; Spark đọc đủ 1.056.320 giao dịch; app khỏe).
- Thử `BACKUP TABLE … TO Disk('backups', …)` → xoá bảng → `RESTORE` → đủ 3/3 dòng → **BR07 làm được trên ClickHouse cài trên máy**.

### Lưu ý
- Container `octiq-local-kb` (dự án khác) đang dùng **~4,5 GB** RAM của Docker (8 GB). Giới hạn của stack BankDW cộng lại 7 GB → khi chạy pipeline nặng hoặc benchmark nên tắt container đó.
- Volume cũ của Phase 1 (`bankdw_kafka-data`, `bankdw_postgres-data`, `bankdw_superset-home`) không còn dùng; có thể xoá để giải phóng đĩa.

---

## [2026-10-22] Phase 2 — P2-0: dữ liệu Berka và Membership tier mô phỏng

- Bắt đầu Phase 2 theo [docs/PLAN_PHASE2.md](docs/PLAN_PHASE2.md) (căn cứ: sheet nhóm chốt 18/10). Tag **`phase1-kaggle`** (`4c554aa`) đánh dấu hết Phase 1.
- ClickHouse chuyển về **container trên máy** (comment các dòng Aiven trong `.env`) để benchmark công bằng và làm được BR07.
- Tải Berka (bản PKDD'99 trên Kaggle) → `data/berka/raw/`. Script [tools/profile_berka.py](tools/profile_berka.py): **14/15 số liệu khảo sát khớp sheet**; VYBER 9.597 chưa khớp cách đếm nào (16.666 giao dịch VYBER, 0 làm tăng số dư) → cần nhóm xác nhận. Chi tiết: [docs/berka_profile.md](docs/berka_profile.md).
- Script [tools/simulate_membership_tier.py](tools/simulate_membership_tier.py) → `data/berka/customer_profile_simulated.csv`: 4.500 dòng; hạng theo tiền vào năm đầu (Standard 46,1% / Silver 32,0% / Gold 15,5% / Platinum 6,4%); đạt mọi kiểm tra; chạy lại ra cùng file.
