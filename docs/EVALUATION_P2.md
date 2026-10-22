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

## 2. Performance — ClickHouse so với phương án thay thế (Q-K)

Phương án thay thế là **cùng mô hình sao trên PostgreSQL** (row store): cùng 6 bảng, cùng cột, cùng 1.057.571 dòng fact. Bản PostgreSQL có PK/FK, các index `fact(date_key)`, `fact(account_sk, date_key)`, `fact(customer_sk)`, `dim_customer(client_id)`, và đã chạy `ANALYZE`.

Mỗi truy vấn chạy 6 lần. Lần 1 là "lạnh" (cache của ClickHouse được xoá trước đó). Median và p95 lấy từ lần 2–6. Thời gian là wall-clock phía client, gồm cả thời gian nhận kết quả. **Cả 7 truy vấn cho kết quả giống hệt nhau trên hai hệ.**

| Truy vấn | Dòng | ClickHouse median (p95) ms | PostgreSQL median (p95) ms | Nhanh hơn |
|---|---:|---:|---:|---|
| BR01 KPI một kỳ (1998) | 1 | 13,5 (14,8) | 32,2 (34,0) | CH ×2,4 |
| BR01 theo tháng, toàn bộ lịch sử | 75 | 25,9 (27,6) | 127,7 (134,0) | CH ×4,9 |
| BR01 theo hạng (1998) | 4 | 18,3 (19,2) | 343,3 (364,6) | CH ×19 |
| BR08 theo hạng, kỳ cũ và kỳ mới | 8 | 42,0 (49,7) | 802,1 (842,1) | CH ×19 |
| BR02 chi tiết tài khoản 2378 | 437 | 24,3 (25,4) | 1,8 (2,2) | **PG ×13** |
| BR02 tổng tài khoản 2378, năm 1998 | 1 | 8,8 (9,2) | 0,3 (0,4) | **PG ×29** |
| BR08 các phiên bản hạng của 1 khách | 2 | 19,9 (24,2) | 0,5 (0,6) | **PG ×40** |

**Đọc kết quả:**
- **Truy vấn tổng hợp trên nhiều dòng** (BR01, báo cáo theo hạng của BR08): ClickHouse nhanh hơn 2–19 lần, và chênh lệch càng lớn khi phải join và đếm khách phân biệt. Đây là loại truy vấn chính của U01 và U03, và là lý do chọn kho cột.
- **Tra cứu một tài khoản hoặc một khách** (BR02, BR08 theo khách): PostgreSQL nhanh hơn nhờ index B-tree. Với ClickHouse, phần lớn thời gian là chi phí cố định: đọc chiều với `FINAL` và dựng bảng băm để join. Dù vậy cả hai hệ đều dưới 25 ms, người dùng không thấy khác biệt.
- **Dung lượng:** fact trên ClickHouse chiếm 19,0 MB (nén từ 54,1 MB). Trên PostgreSQL là 102,1 MB dữ liệu cộng 66,5 MB index, tức **lớn gấp khoảng 8,9 lần**.
- **Giới hạn của phép đo:** cả hai hệ chạy chung một máy ảo. PostgreSQL dùng cấu hình mặc định (`shared_buffers` 128 MB, `work_mem` 4 MB, 2 worker song song); ClickHouse dùng `max_threads` 11. Cache trang của hệ điều hành không xoá được từ trong container, nên lần "lạnh" chỉ là lạnh một phần. Dữ liệu cũng chỉ khoảng 1 triệu dòng; ở quy mô lớn hơn, khoảng cách ở nhóm truy vấn tổng hợp sẽ còn tăng.

### Thời gian pipeline (từ `ops.pipeline_step`)

| Đo | Giá trị |
|---|---|
| 72 lô lịch sử theo tháng (lần chạy đầu) | median **3,4 s**/lô, p95 5,3 s, tổng 273,5 s; median 4.334 dòng/s |
| Median từng bước của lô tháng | ingest 0,55 · validate 0,85 · standardize 0,15 · load_dimensions 0,14 · **load_fact 1,42** · reconcile 0,15 (s) |
| Lô kịch bản (chạy lại) | khoảng 7–20 s mỗi lô; với KB05, bước validate mất 13,9 s vì có 6 quy tắc và ghi bản ghi lỗi |
| Chép sang PostgreSQL (phương án thay thế) | fact 43,4 s; tạo index và ANALYZE 1,0 s |

Thời gian mỗi lô chủ yếu là chi phí khởi động Spark local và các lượt đọc và ghi qua lại với ClickHouse, không tăng theo số dòng (mỗi lô tháng chỉ khoảng 15.000 dòng).

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
