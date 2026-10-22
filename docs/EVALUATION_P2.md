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

## 2. Performance — tóm tắt (chi tiết: [BENCHMARK_P2.md](BENCHMARK_P2.md))

Mức ×1 (1.057.571 giao dịch). Mỗi phép so sánh chỉ đổi một yếu tố; số dưới đây là warm median.

| Phép so sánh | Kết quả chính |
|---|---|
| **B1** Star vs One Big Table (cùng ClickHouse) | One Big Table nhanh hơn 1,5–3,4× (chênh 3–16 ms) nhưng tốn chỗ hơn 1,37×. Sửa tên 1 khu vực: star ghi 1 dòng, One Big Table ghi lại 150.058 dòng |
| **B2** ClickHouse vs PostgreSQL (cùng star) | Truy vấn tổng hợp BR01/BR03/BR08: ClickHouse nhanh hơn **3,3–22,4×**; tra cứu 1 tài khoản / 1 khách: PostgreSQL nhanh hơn 17–46× (ClickHouse vẫn < 20 ms). Dung lượng: 18,6 so với 162,5 MB. 8 client: 93 so với 12 truy vấn/giây |
| **B3** Kho vận hành PostgreSQL vs ClickHouse | PostgreSQL nhanh hơn 8–70× ở mọi thao tác của app và pipeline; tự bảo đảm **5/5** ràng buộc (trạng thái, chuyển trạng thái, chống trùng, khóa ngoại, giao dịch), ClickHouse **0/5** |
| **B4** Spark 1–8 lõi; 1 lô vs 72 lô | Ở ×1, thêm lõi gần như không giúp (7,7 → 5,4 s); 72 lô tháng chậm hơn 1 lô 4–6× do chi phí cố định mỗi lô |
| Pipeline thật | Median 3,4 s/lô tháng (p95 5,3 s), 72 lô 273,5 s; bước nặng nhất là `load_fact` (1,42 s) |

Số liệu lần đo đầu của P2-8 (PostgreSQL cấu hình mặc định, chỉ 7 truy vấn) đã được thay bằng lần đo này. Các sự cố khi đo được ghi ở mục 8 của BENCHMARK_P2.md.

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
