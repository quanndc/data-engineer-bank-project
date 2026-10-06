# Kế hoạch thực hiện — Banking Data Warehouse (Kimball)

Môn Data Engineering (CO5173), HK1 2026–2027. Nhóm 4 người.
Chi tiết quyết định và phát hiện: xem [CHANGELOG.md](../CHANGELOG.md).

> Lịch tính theo tuần (giả định ~8 tuần). Cập nhật thành ngày cụ thể khi có hạn nộp.

---

## 1. Các giai đoạn

| GĐ | Nội dung | Kết quả cần có | Tuần |
|---|---|---|---|
| **P0. Khởi tạo** | Repo Git, cấu trúc thư mục, Docker Compose (Kafka, Spark, ClickHouse, PostgreSQL, Superset, Streamlit), Makefile | `make up` dựng được toàn bộ hệ thống | 1 |
| **P1. Ingestion + staging** | Producer Python đọc CSV → Kafka; Spark lưu raw ra Parquet; quy tắc làm sạch (ngày sinh, giới tính, số tiền = 0, chuẩn hóa địa điểm) | Lớp raw + staging, tài liệu quy tắc làm sạch | 1–2 |
| **P2. Kimball core** | Spark tạo `dim_date`, `dim_customer` (natural key mới + surrogate key), `dim_location`, `dim_transaction_type`, `fact_transactions`, `fact_customer_balance_snapshot` → nạp ClickHouse; script kiểm tra chất lượng + bảng `audit` | **M1: dữ liệu vào DWH, qua hết kiểm tra** | 2–3 |
| **P3. Marts + truy vấn BR** | SQL cho BR1–BR10; mart RFM tính lại với mốc 2016-10-21 | Mỗi BR có 1 file SQL + kết quả đã kiểm tra | 3–4 |
| **P4. Streaming (BR7)** | Spark Structured Streaming phát hiện giao dịch bất thường → bảng cảnh báo trong ClickHouse | Cảnh báo hiện gần như ngay khi producer phát giao dịch | 3–4 |
| **P5. Ứng dụng** | Superset: mỗi nhóm người dùng 1 dashboard; Streamlit: trang cảnh báo realtime | **M2: demo được cả 10 BR end-to-end** | 4–5 |
| **P6. Phương án thay thế + benchmark** | Script nhân bản dữ liệu (×10, ×50); One Big Table, PostgreSQL, ClickHouse Kafka Engine; script benchmark → CSV + biểu đồ | Kết quả B1–B4 (B5 nếu còn thời gian) | 5–6 |
| **P7. Đánh giá** | Tổng hợp độ đúng dữ liệu, hiệu năng, hiệu quả khai thác qua ứng dụng | Bảng số liệu + biểu đồ cho report | 6–7 |
| **P8. Sản phẩm nộp** | Report PDF, slide PDF, video 20–30 phút, gói demo | **M3: nộp bài** | 7–8 |

**Phụ thuộc:** P0 → P1 → P2 bắt buộc theo thứ tự. Sau M1, P3 / P4 / P5 / P6 chạy song song.

---

## 2. Phân công

| Thành viên | Mảng kỹ thuật | BR phụ trách |
|---|---|---|
| TV1 — Nguyễn Hồ Tuấn Kiệt | Hạ tầng: Docker, Kafka, producer, streaming | BR7, BR9 |
| TV2 — Trần Minh Quân | Spark ETL, mô hình Kimball, data quality | BR4, BR8 |
| TV3 — Huỳnh Ngọc Văn | Marts ClickHouse, dashboard Superset | BR1, BR2, BR3 |
| TV4 — Hồ Ngọc Hải | Phương án thay thế, benchmark, đánh giá | BR5, BR6, BR10 |

Mỗi người chịu trách nhiệm trọn vẹn BR của mình: truy vấn → dashboard → đoạn demo trong video → phần viết trong report.

---

## 3. Business Requirements

| # | Nhóm người dùng | Business requirement |
|---|---|---|
| BR1 | Ban điều hành | Số lượng, giá trị giao dịch, số khách hoạt động theo ngày/tuần/tháng |
| BR2 | Ban điều hành | So sánh ngày thường vs cuối tuần, theo thứ trong tuần |
| BR3 | Quản lý khu vực | Xếp hạng thành phố/khu vực theo giá trị giao dịch |
| BR4 | Marketing/CRM | Phân khúc RFM (recency tính từ 2016-10-21) |
| BR5 | Marketing/CRM | Hành vi chi tiêu theo nhóm tuổi × giới tính |
| BR6 | Phát triển sản phẩm | Cơ cấu loại giao dịch theo khu vực, nhóm khách |
| BR7 | Quản trị rủi ro | Cảnh báo giao dịch bất thường gần realtime |
| BR8 | Quản lý tài sản | Danh sách khách số dư cao để cross-sell |
| BR9 *(dự phòng)* | Nhóm dữ liệu | Dashboard chất lượng dữ liệu |
| BR10 *(dự phòng)* | Quản lý khu vực | Phân bố giá trị giao dịch theo dải tiền và khu vực |

---

## 4. Benchmark

| Mã | So sánh | Chỉ số đo |
|---|---|---|
| B1 | Kimball star schema vs One Big Table (ClickHouse) | Thời gian truy vấn từng BR, dung lượng, thời gian nạp, chi phí sửa dữ liệu |
| B2 | ClickHouse vs PostgreSQL (cùng star schema) | Thời gian truy vấn, tỷ lệ nén, thời gian nạp, truy vấn/giây khi nhiều người dùng |
| B3 | Spark Structured Streaming vs ClickHouse Kafka Engine + MV (BR7) | Độ trễ end-to-end, throughput, độ phức tạp |
| B4 | Spark ETL ở ×1/×10/×50, 2/4/8 core | Thời gian ETL |
| B5 *(tùy chọn)* | ClickHouse vs Spark SQL + Delta Lake | Thời gian truy vấn, dung lượng |

Phương pháp: bộ truy vấn = truy vấn thật của BR1–BR10; mỗi truy vấn chạy 6 lần (lần đầu ghi riêng là cold, lấy median + p95 của 5 lần sau); cùng giới hạn CPU/RAM; kết quả mỗi BR phải giống nhau giữa các phương án.

---

## 5. Khung report (7 phần, chuẩn IEEE)

Template **IEEEtran (LaTeX, Overleaf)**, trích dẫn IEEE, tiếng Anh. Chi tiết chuẩn: xem CHANGELOG mục "Chốt chuẩn viết Technical Report: IEEE".

0. Abstract + Index Terms
1. **Introduction**: bối cảnh, vấn đề, contributions
2. **Requirements & Stakeholders**: nhóm người dùng, BR1–BR10
3. **Data Sources & Characteristics**: profiling, vấn đề chất lượng dữ liệu
4. **Proposed Solution**: kiến trúc (C4), Kimball, công nghệ, giải pháp thay thế, related work ngắn
5. **Application**: dashboard Superset, cảnh báo realtime Streamlit
6. **Evaluation & Discussion**: độ đúng (ISO/IEC 25012), hiệu năng (B1–B5), hiệu quả khai thác
7. **Conclusion & Future Work**
- References · Appendix (hướng dẫn chạy, link repo, phân công)

Phần III và phần thiết kế của IV có thể viết ngay (dữ liệu đã có trong CHANGELOG).

---

## 6. Rủi ro

| Rủi ro | Cách xử lý |
|---|---|
| Superset/Kafka lỗi lúc quay video | Quay từng BR ngay khi xong M2, không dồn cuối khóa |
| PostgreSQL ở ×50 chạy quá lâu | Đặt timeout mỗi truy vấn; dừng ở ×20 nếu cần |
| Thiếu RAM khi bật tất cả container | Khi benchmark chỉ bật thành phần cần dùng |

---

## 7. Checklist mốc

- [x] M1 — Dữ liệu vào DWH, qua hết kiểm tra chất lượng (2026-10-06: 12/12 PASS)
- [x] M2 — Demo được cả 10 BR end-to-end (2026-10-06: 7 dashboard Superset + trang Streamlit)
- [ ] M3 — Nộp report, slide, video, gói demo
