# Đối chiếu: Google Sheet "Banking Project — Theo dõi quyết định" ↔ repo hiện tại

- **Sheet**: quyết định nhóm chốt ngày **18/10/2026** (4 sheet: Theo dõi dự án, Business Requirement, Data Model, Data Dictionary).
- **Repo**: những gì đã xây đến commit P5 + benchmark ×1 (ngày 06/10/2026).
- Ký hiệu: ✅ giống / tái dùng được · 🟡 có một phần · ❌ khác hoặc chưa có.

---

## 1. Tóm tắt

| Mảng | Mức khác biệt | Ghi chú ngắn |
|---|---|---|
| Lĩnh vực, môn học, 3 tiêu chí đánh giá | ✅ Giống | Banking, ≥ 8 BR, correctness / performance / hiệu quả khai thác |
| Hướng sản phẩm | ❌ Khác | Sheet: **dòng tiền + hoạt động từng tài khoản + vận hành dữ liệu**. Repo: phân tích hành vi khách hàng (RFM, tuổi, thành phố…) |
| Dataset | ❌ Khác hoàn toàn | Sheet: **Berka (CTU Financial, Séc, 1993–1998)** + bảng hạng thành viên mô phỏng. Repo: Kaggle (Ấn Độ, 2016) |
| Người dùng | ❌ Khác | Sheet: 3 nhóm. Repo: 7 nhóm |
| 8 BR | ❌ Phần lớn khác | Chỉ 3/8 BR của sheet có phần tương ứng trong repo |
| Mô hình Kimball | ❌ Khác | Sheet: 1 fact + 5 dimension, **SCD Type 2**, role-playing district, junk dimension. Repo: 1 fact + 1 snapshot + 4 dimension, không SCD |
| Kiến trúc | ❌ Khác | Sheet: **batch theo lô**, có **kho vận hành PostgreSQL**, app **điều khiển** pipeline. Repo: Kafka + streaming, app chỉ đọc |
| ClickHouse làm DWH | ✅ Giống | |
| PostgreSQL | ❌ Khác vai trò | Sheet: **kho dữ liệu vận hành** (lần chạy, lỗi, cảnh báo, ghi chú, sao lưu). Repo: chỉ là **đối chứng** khi benchmark |
| Phương án thay thế | 🟡 | Sheet: **chưa chốt**. Repo: đã có One Big Table + PostgreSQL (benchmark ×1) |

---

## 2. Người dùng

| Sheet | Nhu cầu (sheet) | Tab UI (sheet) | Trong repo |
|---|---|---|---|
| U01 Quản lý kinh doanh | Tổng quan, báo cáo lịch sử, dòng tiền chung | "Tổng quan" | 🟡 Gần với *Ban điều hành* (dashboard Executive) |
| U02 Chuyên viên phân tích | Tra cứu **từng tài khoản**, xem căn cứ cảnh báo, **ghi nhận kết quả xử lý** | "Phân tích" | 🟡 Gần với *Quản trị rủi ro* (Streamlit), nhưng không tra cứu tài khoản, không ghi nhận xử lý |
| U03 Kỹ sư dữ liệu | Chất lượng dữ liệu, giám sát pipeline, sao lưu/khôi phục, đối chiếu báo cáo lịch sử | "Vận hành" | 🟡 Gần với *Nhóm dữ liệu* (dashboard Data Quality), nhưng thiếu 3/4 nhu cầu |
| — | — | — | Repo có thêm 4 nhóm không có trong sheet: Quản lý khu vực, Marketing, Phát triển sản phẩm, Quản lý tài sản |

---

## 3. Business requirements

### 3.1 BR của sheet → repo đáp ứng đến đâu

| BR (sheet) | Nội dung | Kết quả demo cần đạt (sheet) | Repo | Thiếu gì |
|---|---|---|---|---|
| **BR01** Xem tổng quan dòng tiền | Tiền vào, tiền ra, dòng tiền thuần, số giao dịch theo kỳ; **theo hạng**; biểu đồ ngày/tháng | Đối chiếu với query tại nguồn | 🟡 BR1 (giao dịch & giá trị theo ngày) | Chiều tiền **vào/ra** (Kaggle không có), dòng tiền thuần, chia theo **hạng**, chọn kỳ |
| **BR02** Tra cứu hoạt động tài khoản | Chọn mã tài khoản + khoảng thời gian → lịch sử, tổng vào/ra, số giao dịch | Đúng với tài khoản/kỳ, đối chiếu nguồn | ❌ | Không có khái niệm **tài khoản** (Kaggle chỉ có khách hàng), không có màn hình tra cứu |
| **BR03** Phát hiện gian lận và cảnh báo | **Sau mỗi lần cập nhật** kiểm tra quy tắc, tạo cảnh báo kèm lý do, căn cứ | Nạp giao dịch gian lận chuẩn bị sẵn → sinh cảnh báo | 🟡 BR7 (streaming, quy tắc HIGH_AMOUNT / HIGH_BALANCE_RATIO) | Sheet làm **batch sau nạp**, quy tắc kiểu "tiền ra tăng mạnh", "tài khoản lâu không giao dịch"; cảnh báo lưu ở **PostgreSQL**, không phải ClickHouse |
| **BR04** Theo dõi xử lý cảnh báo | Trạng thái *chưa xem / đang xem xét / đã xử lý* + ghi chú | Thấy danh sách, lý do, trạng thái trên UI | ❌ | Không có quy trình xử lý, không ghi ngược từ UI |
| **BR05** Kiểm soát dữ liệu | Kiểm tra theo quy tắc chất lượng, **tách bản ghi không đạt** | Nạp giao dịch lỗi → phát hiện → hiện số lỗi, lý do trên UI | 🟡 12 kiểm tra DQ + bảng `dq.audit` + dashboard BR9 | Repo **gắn cờ** chứ không **tách (quarantine)** bản ghi lỗi; không có kịch bản nạp lỗi chủ đích; kiểm tra theo lần chạy toàn bộ, không theo lô |
| **BR06** Giám sát pipeline | Mỗi lần chạy: bước, trạng thái, bắt đầu/kết thúc, số nhận/đạt/lỗi, nguyên nhân thất bại | Xác định được bước thất bại | ❌ | Không lưu lịch sử lần chạy (chỉ in ra console) |
| **BR07** Sao lưu, khôi phục | Sao lưu cấu trúc + dữ liệu 6 bảng DW; chọn bản để khôi phục | Đối chiếu số dòng, tiền vào/ra, báo cáo theo hạng, phiên bản SCD | ❌ | Chưa có gì |
| **BR08** Báo cáo lịch sử (SCD2) | Hạng thành viên đổi theo thời gian; giao dịch gắn đúng phiên bản hạng | Nạp thay đổi hạng + giao dịch kỳ sau → báo cáo kỳ cũ không đổi | ❌ | Repo không có SCD (đã ghi là giới hạn trong report) |

**Tổng: 0/8 đáp ứng đủ, 3/8 có một phần (BR01, BR03, BR05), 5/8 chưa có.**

### 3.2 BR của repo không có trong sheet

| BR (repo) | Nội dung | Ghi chú |
|---|---|---|
| BR2 | Ngày thường vs cuối tuần | Có thể gộp vào BR01 của sheet (biểu đồ theo kỳ) |
| BR3 | Xếp hạng thành phố | Berka có `district` (77 khu vực, 8 vùng) → có thể làm tương tự |
| BR4 | Phân khúc RFM | Ngoài phạm vi sheet |
| BR5 | Tuổi × giới tính | Berka có `birth_date`, `gender` → làm được nếu muốn |
| BR6 | Cơ cấu loại giao dịch | Gần với `dim_transaction_profile` của sheet |
| BR8 | Khách top 1% số dư | Ngoài phạm vi sheet |
| BR10 | Phân bố theo dải tiền | Ngoài phạm vi sheet |

---

## 4. Dữ liệu nguồn

| | Sheet (Berka / CTU Financial) | Repo (Kaggle) |
|---|---|---|
| Quốc gia, thời gian | Séc, 1993–1998 (~6 năm) | Ấn Độ, 2016 (55 ngày, thực chất ~52) |
| Quy mô | 4.500 tài khoản, 5.369 khách, **1.056.320** giao dịch, 77 khu vực | 1.046.782 bản ghi khách, **1.418.764** giao dịch, 9.354 địa điểm |
| Bảng dùng | `trans`, `account`, `disp` (lọc OWNER), `client`, `district` + `customer_profile_simulated` | `fact_transactions`, `dim_customer`, `dim_location` |
| Chiều tiền vào/ra | ✅ Có (`PRIJEM` → IN; `VYDAJ`/`VYBER` → OUT) | ❌ Không có |
| Tài khoản | ✅ Có (account ≠ client) | ❌ Không |
| Lịch sử dài | ✅ ~6 năm → phân tích xu hướng, SCD có ý nghĩa | ❌ 55 ngày |
| Lỗi dữ liệu đã khảo sát | `operation` trống (183.114, đều UROK), `k_symbol` trống (481.881 NULL + 53.433 chuỗi trắng), 17 → 15 tổ hợp phân loại | ID khách trùng, ngày sinh sai, mart dựng sẵn sai |
| Dữ liệu mô phỏng | Bảng hạng thành viên (để có SCD2) + giao dịch lỗi/gian lận chủ đích cho KB03, KB05, KB08 | Nhân bản ×k (đã bỏ, chỉ chạy ×1) |

---

## 5. Mô hình dữ liệu (Kimball)

| Bảng | Sheet | Repo |
|---|---|---|
| Fact | `fact_transaction`: 1 dòng/giao dịch; `amount` (không dấu), `balance_after`, `counterparty_bank_code`, `counterparty_account_ref`, **`load_batch_id`, `loaded_at`** | `fact_transactions`: 1 dòng/giao dịch; `transaction_amount`, `account_balance`, `dq_flags` |
| Fact snapshot | — | `fact_customer_balance_snapshot` (số dư cuối tháng) |
| Khách hàng | `dim_customer` **SCD Type 2** theo hạng thành viên (`valid_from`, `valid_to`, `is_current`); chỉ OWNER | `dim_customer` Type 1; khóa tự nhiên (id, ngày sinh, giới tính) để tách ID trùng |
| Tài khoản | `dim_account` (Type 1, chu kỳ sao kê, khu vực chi nhánh) | — |
| Địa lý | `dim_district` (77 khu vực, 8 vùng), **dùng 2 vai trò**: khu vực chi nhánh (qua account) và khu vực cư trú (qua customer) — snowflake/outrigger | `dim_location` (9.354 địa điểm, 54 thành phố chuẩn hóa), nối thẳng fact |
| Phân loại giao dịch | `dim_transaction_profile`: **junk dimension** tổ hợp type + operation + purpose, có `cash_flow_direction` | `dim_transaction_type` (4 loại, nhãn suy luận) |
| Ngày | `dim_date` (có `year_month`, không ngày lễ) | `dim_date` (2015–2017, có ngày lễ Ấn Độ) |
| Số đo chính | Tiền vào, tiền ra, **dòng tiền thuần**, số giao dịch | Số giao dịch, giá trị, khách hoạt động |
| Semi-additive | `balance_after` không SUM; số dư cuối kỳ **chưa chốt** | `account_balance` không SUM; đã có snapshot cuối tháng |

---

## 6. Kiến trúc và công nghệ

| Khối (sheet) | Sheet | Repo |
|---|---|---|
| A. Nguồn | Berka + hạng mô phỏng + đầu vào bổ sung theo kịch bản | CSV Kaggle |
| — Ingestion | Pipeline **đọc nguồn theo lô** | **Kafka** (producer phát lại giao dịch) |
| B. Data Lake | Bản chụp đầu vào **từng lô** để truy vết, chạy lại; công nghệ **chưa chốt** (gợi ý Parquet) | Parquet `raw / staging / core` (không theo lô) |
| C. Pipeline | Theo lô: kiểm tra, chuẩn hóa, SCD, nạp DW, tính cảnh báo, **ghi tiến trình vào E**; công nghệ chưa chốt (gợi ý PySpark) | Apache Spark batch + **Structured Streaming** |
| D. Data Warehouse | **ClickHouse** ✅ + vùng sao lưu | **ClickHouse** ✅ (không sao lưu) |
| E. Kho vận hành | **PostgreSQL**: lần chạy, lỗi, cảnh báo, trạng thái xử lý, ghi chú, thông tin sao lưu | Không có (PostgreSQL chỉ dùng benchmark) |
| F. Ứng dụng | 1 app, 3 tab; **điều khiển**: chọn/xem trước kịch bản, chạy pipeline, sao lưu/khôi phục, cập nhật trạng thái cảnh báo; chưa phân quyền | Superset (7 dashboard, chỉ đọc, có role) + Streamlit (trang cảnh báo, chỉ đọc) |
| Nhịp cập nhật | **Theo ngày**, mô phỏng nạp lịch sử | Gần thời gian thực (stream) + batch toàn bộ |
| Cảnh báo | Quy tắc dễ giải thích: **tiền ra tăng mạnh**, **tài khoản lâu không giao dịch** | Số tiền > p99; số tiền > số dư |
| Đánh giá | 8 kịch bản (KB01–KB08), mỗi BR một kịch bản, có đối chiếu nguồn | Kiểm tra DQ + benchmark ×1 (B1, B2) |

---

## 7. Phần nào của repo tái dùng được nếu chuyển theo sheet

| Tái dùng gần như nguyên | Tái dùng ý tưởng/khung, phải viết lại | Bỏ / không cần theo sheet |
|---|---|---|
| Docker Compose: ClickHouse, Spark, PostgreSQL | ETL Spark (`p1`, `p2`): đổi sang Berka, thêm SCD2, theo lô | Kafka, producer, Structured Streaming (sheet chạy batch theo ngày) — có thể giữ làm phương án thay thế |
| `common.py`, `run_sql_file`, Makefile, cách nạp Parquet → ClickHouse | Kiểm tra DQ (`p2_dq_checks`): giữ khung 12 check, thêm tách bản ghi lỗi + ghi PostgreSQL | Mart RFM, BR2–BR10 của repo |
| Khung benchmark (`benchmark/`): ClickHouse vs PostgreSQL, cách đo 6 lần, so khớp kết quả | Streamlit: thành app 3 tab có thao tác (chạy pipeline, xử lý cảnh báo, sao lưu) | Superset (sheet chỉ có 1 app; có thể giữ hoặc bỏ) |
| Report LaTeX: cấu trúc IEEE 7 phần, tiêu chí ISO/IEC 25012, phương pháp benchmark | Report phần II–VI: viết lại theo Berka, 3 nhóm người dùng, 8 BR mới | Số liệu Kaggle trong report |

---

## 8. Điểm sheet còn để mở (cần chốt trước khi code)

Từ chính sheet:
1. Ngưỡng/quy tắc cảnh báo (BR03) và cách tránh cảnh báo trùng.
2. Quy tắc chất lượng dữ liệu (BR05), tài khoản/kỳ mẫu cho demo.
3. Danh sách hạng thành viên và lịch đổi hạng (BR08).
4. Số dư cuối kỳ, chuyển tiền nội bộ.
5. Cấu hình sao lưu, cách tiếp tục nạp sau khôi phục (BR07).
6. Công nghệ cho B (Data Lake), C (Pipeline), F (Ứng dụng) và **phương án thay thế**.
7. Lịch nạp, kích thước lô, cơ chế chống trùng khi chạy lại.
8. Ngày nộp, máy tính, phân công 4 thành viên.
