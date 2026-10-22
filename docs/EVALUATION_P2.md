# Phase 2 — Kết quả đánh giá (P2-8)

Đo ngày 22/10/2026 trên máy cục bộ (Docker Desktop, máy ảo 8 GB; ClickHouse 25.3 giới hạn 2,5 GB,
PostgreSQL 16.4 giới hạn 1 GB). Số liệu gốc: [docs/results/p2/](results/p2/). Chạy lại:

```bash
make evaluate      # KB01–KB08 + chất lượng dữ liệu
make bench-load    # chép 6 bảng dw sang PostgreSQL (database dwpg)
make bench         # BR01/BR02/BR08 trên ClickHouse và PostgreSQL + thời gian pipeline
```

---

## 1. Data correctness — 8/8 KB đạt (38/38 kiểm tra)

Mỗi KB được đối chiếu với một nguồn **độc lập với kho**:
- **nguồn:** Berka `trans.csv` + các file kịch bản. Script tính lại bằng pandas (không dùng Spark, không dùng ClickHouse); chủ tài khoản lấy từ `disp.csv`, hạng lấy từ hai file hạng;
- **`expected.json`:** do `tools/make_scenarios.py` viết ra *trước khi* nạp dữ liệu;
- **kho vận hành** (`ops.*`).

Cả bảng kết quả bên dưới đều do [spark/jobs/evaluate.py](../spark/jobs/evaluate.py) sinh ra.

| KB | BR | Kiểm tra chính | Kết quả |
|---|---|---|---|
| KB01 | BR01 | Số giao dịch, tiền vào, tiền ra **khớp tới từng xu** với nguồn cho 4 kỳ (1993–1998, năm 1998, 01/1999, toàn bộ); tách theo hạng và số khách cũng khớp | 8/8 |
| KB02 | BR02 | 23 tài khoản (11, 14, 2378 và 20 tài khoản ngẫu nhiên, seed 27): khớp **từng giao dịch** (id, số tiền, chiều); lọc theo kỳ đúng | 2/2 |
| KB03 | BR03 | Có đúng 5 cảnh báo như mong đợi (3 R1, 2 R2). Căn cứ (tiền ra trong tháng, trung bình 6 tháng, số ngày không giao dịch) khớp. Chạy 6 lần vẫn chỉ có 5 cảnh báo. Mở được mọi giao dịch liên quan | 4/4 |
| KB04 | BR04 | NEW → IN_REVIEW → RESOLVED, mỗi bước được ghi vào lịch sử; RESOLVED → NEW bị từ chối. Kiểm tra trong transaction rồi rollback. Phần giao diện đã thử tay ở P2-6 | 3/3 |
| KB05 | BR05 | Số bản ghi bị tách theo quy tắc khớp mong đợi (V1 2, V2 1, V3 2, V4 2, V5 1, V6 2). Đúng 10 id hợp lệ vào kho. Lô ghi nhận 20 / 10 / 10 với trạng thái `SUCCEEDED_WITH_REJECTS` | 3/3 |
| KB06 | BR06 | Mọi lần chạy đều có bước, thời gian và số đạt. Lần chạy lỗi (run 82) cho thấy bước `detect_alerts` thất bại kèm nguyên nhân. Có ghi nhận chạy lại (RERUN) | 4/4 |
| KB07 | BR07 | Khôi phục thành công, 5 nhóm số liệu khớp bản sao lưu; lô KB07 không còn trong kho; kho hiện tại vẫn bằng bản sao lưu | 3/3 |
| KB08 | BR08 | SCD2: 5.118 / 4.500 / 618 phiên bản; kỳ mới tính theo hạng mới khớp mong đợi; **kỳ cũ vẫn theo hạng cũ** (khớp nguồn với hạng ban đầu); 618 khách có 2 hạng | 4/4 |
| DQ | — | Theo ISO/IEC 25012: uniqueness (không trùng id), completeness (không thiếu khóa), consistency (0 khóa ngoại mồ côi trên 5 quan hệ), SCD2 (đúng 1 phiên bản hiện tại, không chồng lấn, giao dịch trỏ đúng phiên bản theo ngày) | 7/7 |

**Lỗi tìm ra khi đánh giá và đã sửa:**
1. Trong `expected.json` của KB03, trung bình tiền ra lấy theo tháng 7–12/1998. Quy tắc R1 cho tháng 2/1999 phải dùng **tháng 8/1998–1/1999**. Kho đã tính đúng; chỗ sai nằm ở generator. Sau khi sửa và sinh lại, `trans.csv` không đổi, chỉ `expected.json` thay đổi.
2. Bước `load_dimensions` không ghi số tổ hợp nhận vào. Từ nay bước này ghi `rows_in` = số tổ hợp trong lô và `rows_out` = số tổ hợp mới.

---

## 2. Performance — tóm tắt ×1 và ×10 (chi tiết: [BENCHMARK_P2.md](BENCHMARK_P2.md))

Đo ở hai mức dữ liệu: **×1** = dữ liệu thật (1.057.571 giao dịch); **×10** = dữ liệu nhân bản 10 lần (10.575.710 giao dịch, trong database riêng, kho thật vẫn ×1). Mỗi phép so sánh chỉ đổi một yếu tố; số là thời gian điển hình (median).

| Phép so sánh | ×1 | ×10 | Kết luận |
|---|---|---|---|
| **B2** ClickHouse vs PostgreSQL — báo cáo tổng hợp (BR01, BR03, BR08) | ClickHouse nhanh hơn **3–22×** | ClickHouse nhanh hơn **7–28×** (PostgreSQL 0,4–5,4 s; ClickHouse < 0,25 s) | Dữ liệu càng lớn, ClickHouse càng hơn → chọn ClickHouse làm kho là đúng |
| **B2** — tra cứu 1 tài khoản / 1 khách (BR02) | PostgreSQL nhanh hơn 17–46× (ClickHouse vẫn < 20 ms) | PostgreSQL nhanh hơn 62–268× (ClickHouse 12–80 ms) | PostgreSQL thắng nhờ index, nhưng ClickHouse vẫn đủ nhanh; xem dòng "Phát hiện" |
| **B2** — 8 người dùng cùng lúc | ClickHouse 93 / PostgreSQL 12 truy vấn/giây | ClickHouse 9,4 / PostgreSQL **0,4** truy vấn/giây (có lúc chờ 38 s) | PostgreSQL không chịu được tải khi dữ liệu lớn |
| **B2** — dung lượng | 18,6 MB / 162,5 MB | 185,5 MB / 1.612,7 MB | ClickHouse gọn hơn khoảng 9× ở cả hai mức |
| **B1** Star schema vs One Big Table — tốc độ | One Big Table nhanh hơn 1,5–3,4× (chỉ 3–16 ms) | One Big Table nhanh hơn 1,4–11× (chênh lớn nhất ở tra cứu 1 tài khoản, 1 khách) | Chênh lệch nhỏ với người dùng |
| **B1** — sửa tên 1 khu vực | Star ghi **1 dòng**, One Big Table **150.058 dòng** | Star **1 dòng**, One Big Table **1.500.580 dòng** | Một bảng to sửa rất đắt và càng đắt khi dữ liệu tăng → chọn star schema là đúng |
| **B3** Kho vận hành PostgreSQL vs ClickHouse | PostgreSQL nhanh hơn 8–70×; tự chặn **5/5** loại dữ liệu sai, ClickHouse **0/5** | Không đo lại (dữ liệu vận hành không lớn lên theo kho) | Tách PostgreSQL riêng là đúng |
| **B4** Spark: 1 → 8 lõi (1 lô) | 7,7 → 5,4 s (thêm lõi gần như không giúp) | 28,8 → 14,0 s (**nhanh hơn 2,1×**) | Spark chỉ tận dụng nhiều lõi khi dữ liệu đủ lớn |
| **B4** — 72 lô tháng so với 1 lô | Chậm hơn 4–6× | Chậm hơn 1,8–4,1× | Chia lô tốn chi phí cố định; đổi lại kiểm soát được từng lô |
| **Pipeline thật** (chỉ ×1) | 3,4 s/lô tháng, 72 lô 273,5 s; bước nặng nhất `load_fact` 1,42 s | — | |
| **Phát hiện** tra cứu 1 tài khoản trên star / ClickHouse | Cách app đang viết: 34 ms, đọc cả bảng; lọc `account_sk` trước: 6,1 ms | Cách app: 60 ms, đọc 10,6 triệu dòng; lọc `account_sk` trước: **6,8 ms** | Viết lại truy vấn BR02 thì không chậm đi khi dữ liệu tăng (chưa sửa app) |

Số liệu lần đo đầu của P2-8 (PostgreSQL cấu hình mặc định, chỉ 7 truy vấn) đã được thay bằng lần đo này. Các sự cố khi đo được ghi ở mục 9 (×1) và mục 12.8 (×10) của BENCHMARK_P2.md.

---

## 3. Hiệu quả khai thác

| KB | Thao tác trên app (đếm theo luồng đã thử ở P2-6) | Thời gian có câu trả lời |
|---|---|---|
| KB01 | Tab Tổng quan → chọn kỳ (1–2 thao tác) | < 0,1 s mỗi truy vấn (BR01) |
| KB02 | Phân tích → nhập tài khoản → chọn kỳ (3) | < 0,03 s |
| KB03 | Vận hành → Chạy kịch bản → chọn KB03 → Chạy → Phân tích → chọn cảnh báo (6) | khoảng 10 s để nạp và quét cảnh báo |
| KB04 | Phân tích → chọn cảnh báo → chọn trạng thái + ghi chú → Lưu (4) | tức thì |
| KB05 | Vận hành → Chạy kịch bản → KB05 → Chạy → BR05 (5) | khoảng 17 s |
| KB06 | Vận hành → BR06 (1–2) | tức thì |
| KB07 | BR07 Sao lưu → Chạy KB07 → BR07 Khôi phục → xem đối chiếu (khoảng 7) | khoảng 10 s mỗi bước |
| KB08 | Vận hành → Chạy kịch bản → KB08 → Chạy → BR08 (5) | khoảng 20 s |

Số thao tác được đếm theo luồng trên giao diện. Nhóm nên đo lại bằng đồng hồ khi quay video demo (P2-9).
