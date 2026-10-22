# PLAN PHASE 2 — Triển khai theo các quyết định đã chốt trong Google Sheet

- **Căn cứ:** Google Sheet "Banking Project — Theo dõi quyết định" (chốt 18/10/2026) và bảng đối chiếu [DOI_CHIEU_SHEET.md](DOI_CHIEU_SHEET.md).
- **Nguyên tắc:** làm đúng những gì sheet đã chốt. Những điểm sheet còn để mở được ghi là **Đề xuất**, kèm phương án mặc định; nhóm cần chốt trước khi làm phần đó.
- **Phase 1** (Kaggle, Kafka, Superset, RFM…) được giữ lại ở tag `phase1-kaggle` để tham khảo và tái dùng code, **không phát triển tiếp**.

---

## 1. Phạm vi Phase 2 (theo sheet)

| Hạng mục | Đã chốt trong sheet |
|---|---|
| Hướng sản phẩm | Dòng tiền chung của ngân hàng + hoạt động từng tài khoản + vận hành dữ liệu |
| Dataset | Berka / CTU Financial (`trans`, `account`, `disp` lọc OWNER, `client`, `district`) + `customer_profile_simulated` (hạng thành viên mô phỏng) |
| Người dùng | U01 Quản lý kinh doanh · U02 Chuyên viên phân tích · U03 Kỹ sư dữ liệu |
| BR | BR01–BR08 (mục 3) |
| DW | **ClickHouse**: 1 fact + 5 dimension; `dim_customer` SCD Type 2 |
| Kho vận hành | **PostgreSQL**: lần chạy, bước, lỗi, cảnh báo, trạng thái xử lý, ghi chú, thông tin sao lưu |
| Kiến trúc | A Nguồn → B Data Lake → C Pipeline → D ClickHouse + E PostgreSQL → F Ứng dụng; xử lý **theo lô** |
| Đánh giá | 8 kịch bản KB01–KB08 (1 kịch bản / BR), đối chiếu với nguồn |

---

## 2. Quyết định cần chốt TRƯỚC khi code (sheet còn để mở)

| # | Điểm mở (theo sheet) | Đề xuất mặc định | Chặn bước nào |
|---|---|---|---|
| Q-A | Công nghệ B (Data Lake) | Thư mục **Parquet** theo lô: `lake/batches/<batch_id>/<bảng>.parquet` | P2-3 |
| Q-B | Công nghệ C (Pipeline) | **PySpark** (tái dùng image Spark Phase 1) | P2-3 |
| Q-C | Công nghệ F (Ứng dụng) | **Streamlit** 1 app, 3 tab (đúng sheet: 1 app, chưa phân quyền) | P2-6 |
| Q-D | Nhịp nạp, cách chia lô | Lô **lịch sử theo tháng** (1993-01 → 1998-12, ~72 lô) để mô phỏng nạp lịch sử; lô **kịch bản** riêng cho KB03, KB05, KB08 | P2-3 |
| Q-E | Danh sách hạng, lịch đổi hạng | ✅ **Đã chốt 22/10:** 4 hạng Standard / Silver / Gold / Platinum theo tiền vào năm đầu; đổi hạng = **đánh giá lại cuối 1998** theo tiền vào 1998 (cùng ngưỡng), hiệu lực **1999-01-01** → 618 khách (13,7%) | P2-4 |
| Q-F | Quy tắc / ngưỡng cảnh báo (BR03) | Đang dùng đề xuất (22/10): R1 "tiền ra tăng mạnh": tiền ra trong tháng > **3×** trung bình 6 tháng trước và > 50.000 CZK. R2 "tài khoản ngủ đông hoạt động lại": không giao dịch ≥ **180 ngày** rồi có giao dịch ra. Chống trùng: 1 cảnh báo / (tài khoản, quy tắc, kỳ). Trên lịch sử: R1 1.695, R2 2 cảnh báo; chỉnh ngưỡng trong `ops.alert_rule`, không cần sửa code | P2-5 |
| Q-G | Quy tắc chất lượng (BR05) | V1 thiếu khóa bắt buộc · V2 tài khoản không tồn tại · V3 `amount` < 0 hoặc không phải số · V4 ngày ngoài lịch · V5 mã `type` lạ · V6 trùng `transaction_id`. Bản ghi lỗi **tách sang PostgreSQL**, không làm hỏng cả lô | P2-4 |
| Q-H | Số dư cuối kỳ, chuyển tiền nội bộ | **Ngoài phạm vi Phase 2.** Báo cáo chỉ dùng tiền vào/ra từ `amount` + chiều tiền; `balance_after` chỉ để tra cứu (đúng sheet) | — |
| Q-I | Cấu hình sao lưu (BR07) | ✅ **Đã làm 22/10:** `BACKUP DATABASE dw TO Disk('backups', '<tên>')`; khôi phục = xoá 6 bảng rồi `RESTORE`; thư mục `./backups` mount riêng; danh mục + số liệu đối chiếu ở `ops.backup_*`; chỉ chạy khi pipeline không ghi | P2-7 |
| Q-J | Mốc `valid_to` "vô cực" | Kiểu `Date32`, mốc **2299-12-31** (giới hạn của ClickHouse; sheet ghi 9999-12-31 là mốc *logic*) | P2-2 |
| Q-K | Phương án thay thế | Tái dùng khung benchmark Phase 1: **cùng mô hình trên PostgreSQL** (row store) so với ClickHouse, đo các truy vấn BR01/BR02/BR08 và thời gian pipeline | P2-8 |
| Q-L | Ngày nộp, máy, phân công | Cần nhóm xác nhận (sheet M07) | Lịch ở mục 6 |

---

## 3. BR → kịch bản đánh giá → thành phần

| BR | Người dùng / Tab | Kịch bản đánh giá (KB) | Dữ liệu cần | Thành phần chính |
|---|---|---|---|---|
| **BR01** Tổng quan dòng tiền | U01 / Tổng quan | KB01: chọn kỳ → tiền vào, ra, thuần, số giao dịch, **theo hạng**; biểu đồ ngày/tháng; **đối chiếu query nguồn** | Berka có sẵn | Truy vấn DW, trang Tổng quan, script đối chiếu nguồn |
| **BR02** Tra cứu tài khoản | U02 / Phân tích | KB02: chọn `account_id` + kỳ → chi tiết giao dịch, tổng vào/ra, số giao dịch; đối chiếu nguồn | Berka có sẵn | Truy vấn DW, form tra cứu |
| **BR03** Phát hiện gian lận | U02 / Phân tích | KB03: nạp lô giao dịch gian lận chuẩn bị sẵn → sinh cảnh báo có lý do, căn cứ → mở giao dịch tương ứng | **Lô bổ sung** thỏa R1/R2 | Bước "cảnh báo" của pipeline → bảng `alert` (PostgreSQL) |
| **BR04** Xử lý cảnh báo | U02 / Phân tích | KB04: đổi trạng thái *chưa xem → đang xem xét → đã xử lý*, ghi chú → thấy lịch sử trên UI | Cảnh báo từ KB03 | Bảng `alert_action` (PostgreSQL), form cập nhật |
| **BR05** Kiểm soát dữ liệu | U03 / Vận hành | KB05: nạp lô có bản ghi lỗi chủ đích → phát hiện, tách, hiện số lỗi + lý do | **Lô bổ sung** vi phạm V1–V6 | Bước "validate" → `rejected_record` (PostgreSQL) |
| **BR06** Giám sát pipeline | U03 / Vận hành | KB06: chạy lô → thấy bước, trạng thái, thời gian, số nhận/đạt/lỗi; gây lỗi có chủ đích → thấy bước thất bại + nguyên nhân | Lô bất kỳ + 1 lô gây lỗi thực thi | `pipeline_run`, `pipeline_step` (PostgreSQL) |
| **BR07** Sao lưu, khôi phục | U03 / Vận hành | KB07: sao lưu → nạp thêm lô (có thể dùng lại lô KB05) → khôi phục → đối chiếu số dòng, tiền vào/ra, báo cáo theo hạng, phiên bản SCD | Lô KB05 | `BACKUP`/`RESTORE` ClickHouse, `backup_catalog` (PostgreSQL) |
| **BR08** Báo cáo lịch sử (SCD2) | U03 / Vận hành | KB08: nạp thay đổi hạng + giao dịch kỳ sau → báo cáo **kỳ cũ không đổi**, kỳ mới dùng hạng mới | **Lô bổ sung**: thay đổi hạng + giao dịch | SCD2 trong pipeline, truy vấn theo hạng |

---

## 4. Các giai đoạn

### P2-0. Chuẩn bị repo và dữ liệu
- [x] Gắn tag `phase1-kaggle` *(22/10: tag tại `4c554aa`, đã push lên GitHub)*; chuyển code Phase 1 không dùng nữa vào `archive/phase1/` *(22/10: 59 file, `git mv`)*
- [x] Tải Berka từ CTU Relational (`relational.fel.cvut.cz`, database `financial`), xuất 5 bảng ra CSV vào `data/berka/`.
  - *Thực hiện 22/10:* dùng bản PKDD'99 gốc trên Kaggle (không cần đăng nhập) → `data/berka/raw/`; số liệu khớp sheet (xem [berka_profile.md](berka_profile.md)).
- [x] **Kiểm tra lại số liệu khảo sát của sheet (K01–K05):** *(22/10: 14/15 khớp; VYBER 9.597 cần nhóm xác nhận cách tính — [berka_profile.md](berka_profile.md))* 4.500 tài khoản, 5.369 khách, 1.056.320 giao dịch, mỗi tài khoản đúng 1 OWNER, 4.091/409 tài khoản cùng/khác khu vực, 17 → 15 tổ hợp phân loại, 183.114 `operation` trống, 481.881 NULL + 53.433 chuỗi trắng ở `k_symbol`.
- [x] **Giả lập Membership tier: 1 file CSV tương ứng với dữ liệu Berka** *(22/10: 4.500 dòng, đạt mọi kiểm tra)* → `data/berka/customer_profile_simulated.csv`
  - Cột (theo Data Dictionary): `client_id`, `membership_tier`, `tier_effective_from`, `updated_at`.
  - Phạm vi: **mỗi `client_id` là OWNER của một tài khoản có đúng 1 dòng** (khớp `client` ⋈ `disp` với `type = OWNER`); `client_id` nào cũng phải tồn tại trong `client`.
  - Cách gán hạng (Q-E): dựa trên tổng tiền vào của các tài khoản khách sở hữu trong năm đầu → 4 hạng Standard / Silver / Gold / Platinum. `tier_effective_from` ≤ ngày giao dịch đầu tiên của khách, để phiên bản đầu bao phủ toàn bộ lịch sử (đúng quy tắc của `valid_from`).
  - Sinh bằng script có **seed cố định** (`tools/simulate_membership_tier.py`): chạy lại ra cùng file; ghi rõ trong report đây là dữ liệu mô phỏng.
  - File thay đổi hạng cho kịch bản KB08 (`customer_profile_changes_kb08.csv`) làm ở P2-4.
  - **Xong khi:** số dòng = số khách OWNER; 0 `client_id` lạ; 0 trùng; phân bố hạng hợp lý (in ra bảng thống kê).
- **Xong khi:** có dữ liệu Berka + file Membership tier trong repo và bảng khảo sát khớp sheet (hoặc ghi rõ chỗ lệch).

### P2-1. Hạ tầng (A–F)
- [x] Docker Compose: ClickHouse (D), PostgreSQL (E), Spark (C), Streamlit (F); thư mục `lake/` (B) và `backups/` (vùng sao lưu).
- [x] Bỏ Kafka, Superset khỏi stack chính (giữ trong archive).
- [x] Cấu hình ClickHouse `backups` disk (Q-I). *(22/10: thử `BACKUP`/`RESTORE` một bảng thật → khôi phục đủ dữ liệu)*
- **Xong khi:** `make up` dựng đủ D, E, C, F; `make smoke` xanh. ✅ *(22/10)*

### P2-2. Lược đồ dữ liệu
- [x] **ClickHouse (D)** — 6 bảng theo Data Dictionary: `fact_transaction`, `dim_customer` (SCD2), `dim_account`, `dim_date`, `dim_district`, `dim_transaction_profile`. Chốt kiểu vật lý (Q-J). *(22/10: [dw.sql](../warehouse/clickhouse/ddl/dw.sql), 6/6 bảng khớp từng cột với Data Dictionary)*
- [x] **PostgreSQL (E)** — kho vận hành: `batch`, `pipeline_run`, `pipeline_step`, `rejected_record`, `alert`, `alert_action`, `backup_catalog`, `backup_operation`. *(22/10: [ops.sql](../warehouse/postgres/ddl/ops.sql), 10 bảng + `dq_rule`, `alert_rule` có sẵn V1–V6, R1–R2)*
- [x] Viết lại ERD (sheet, dòng 88) thành sơ đồ cho report. *(22/10: [data_model.md](data_model.md), Mermaid)*
- **Xong khi:** DDL chạy được, khớp từng cột với Data Dictionary. ✅ *(22/10, `make schema`)*

### P2-3. Pipeline theo lô (C) — luồng L01, L02, L05
- [x] Bước **ingest**: đọc A → ghi bản chụp lô vào B (`lake/batches/<batch_id>/`), ghi `batch` + `pipeline_run` (E).
- [x] Bước **standardize**: chuẩn hóa chuỗi; `NULL`/chuỗi trắng → `NOT_RECORDED`; `PRIJEM` → IN, `VYDAJ`/`VYBER` → OUT; 17 → 15 tổ hợp.
- [x] Bước **build customer (ETL join → 1 bảng customer)**: từ Data Lake, join các bảng nguồn thành **một bảng customer** trước khi nạp `dim_customer`:

  | Bảng nguồn (Data Lake) | Lấy gì | Điều kiện join |
  |---|---|---|
  | `client` | `client_id`, `gender`, `birth_date`, `district_id` | bảng gốc |
  | `disp` | xác định chủ sở hữu (`account_id`) | `disp.client_id = client.client_id` **và** `disp.type = 'OWNER'` |
  | `district` | `district_id` → `customer_district_sk` | `client.district_id = district.district_id` |
  | `customer_profile_simulated` | `membership_tier`, `tier_effective_from` | `customer_profile_simulated.client_id = client.client_id` |

  - Kết quả: bảng trung gian `stg_customer` (1 dòng / khách OWNER) ở Data Lake, rồi bước SCD2 (P2-4) sinh các phiên bản `dim_customer`.
  - Kèm bảng `stg_account_owner` (1 dòng / tài khoản: `account_id` → `client_id` OWNER) để bước nạp fact tra `customer_sk` theo **tài khoản + ngày giao dịch** (sheet: "trung gian 1 dòng/tài khoản").
  - Kiểm tra sau join (đúng sheet K01–K05): mỗi tài khoản đúng 1 OWNER; không mất / không nhân đôi khách; mọi khách có hạng; khu vực khách có thể khác khu vực tài khoản (không join account ↔ client qua khu vực).
- [x] Bước **load dimensions**: `dim_date`, `dim_district` (Type 1), `dim_account` (Type 1), `dim_transaction_profile` (chỉ thêm tổ hợp mới, sửa nhãn Type 1).
- [x] Bước **load fact**: tra `customer_sk` theo OWNER **và ngày giao dịch**; chống trùng theo `transaction_id`; gắn `load_batch_id`, `loaded_at`.
- [x] Bước **reconcile**: số dòng, ID duy nhất, tổng tiền vào/ra nguồn ↔ kho.
- [x] Mỗi bước ghi `pipeline_step` (bắt đầu, kết thúc, trạng thái, số nhận/đạt/lỗi, lỗi) → nền cho BR06.
- [x] **Chạy lại cùng lô không nhân đôi** giao dịch hay tổng tiền (L05, N05).
- [x] Nạp lịch sử Berka theo lô tháng (Q-D).
- **Xong khi:** nạp đủ Berka; đối chiếu nguồn ↔ kho khớp 100%; chạy lại 1 lô vẫn khớp. ✅ *(22/10: 73 lô, 1.056.320/1.056.320 giao dịch, tiền vào/ra khớp tới từng xu; chạy lại `HIST_1995_03` nạp 0 dòng mới)*

### P2-4. SCD2, dữ liệu mô phỏng và kiểm soát dữ liệu (BR05, BR08)
- [x] Lịch đổi hạng: file `customer_profile_changes_kb08.csv` (khách đổi hạng + `tier_effective_from` mới), dùng file hạng ban đầu từ P2-0 làm trạng thái nền.
- [x] SCD Type 2 cho `dim_customer` *(22/10: 4.500 phiên bản đầu + KB08 đóng 618 / mở 618 → 5.118 phiên bản, 4.500 hiện tại, 0 chồng lấn; từ chối thay đổi đến muộn)*: đóng phiên bản cũ (`valid_to`), mở phiên bản mới (`customer_sk` mới), đúng 1 bản `is_current`, không chồng lấn.
- [x] Bước **validate** (Q-G) *(22/10: V1–V7 chạy trong mọi lô; KB05 tách đúng 10/10 bản ghi lỗi)*: tách bản ghi lỗi vào `rejected_record` kèm mã quy tắc và lý do; bản ghi đạt vẫn được nạp.
- [x] Chuẩn bị lô kịch bản **KB05** (bản ghi lỗi) và **KB08** (đổi hạng + giao dịch kỳ sau). *(22/10: KB05 nạp 10 / tách 10 đúng V1–V6; KB08 618 khách đổi hạng theo đánh giá lại cuối 1998, hiệu lực 1999-01-01)*
- **Xong khi:** KB05 và KB08 chạy ra đúng kết quả mong đợi. ✅ *(22/10: khớp `expected.json`; kỳ cũ 1993–1998 theo hạng giống hệt trước/sau KB08)*

### P2-5. Cảnh báo (BR03, BR04) — luồng L03, L04
- [x] Bước **alert** sau khi nạp: đọc D, áp quy tắc R1/R2 (Q-F), ghi `alert` (E) kèm tài khoản, lý do, thời điểm, số liệu làm căn cứ; chống trùng.
- [x] Trạng thái xử lý + ghi chú (`alert_action`) *(22/10: hàm `ops.set_alert_status`; giao diện ở P2-6)*; **không sửa số tiền hay lịch sử hạng trong DW**.
- [x] Chuẩn bị lô **KB03** (giao dịch gian lận thỏa R1/R2).
- **Xong khi:** KB03 sinh đúng cảnh báo; KB04 cập nhật trạng thái thấy trên UI. *(22/10: KB03 ✅ 5/5 cảnh báo; KB04 ✅ cả dữ liệu và UI)*

### P2-6. Ứng dụng (F) — 1 app, 3 tab
- [x] **Tổng quan (U01):** BR01: chọn kỳ, tiền vào/ra/thuần, số giao dịch, theo hạng, biểu đồ ngày/tháng.
- [x] **Phân tích (U02):** BR02 tra cứu tài khoản · BR03 danh sách cảnh báo + căn cứ + mở giao dịch · BR04 cập nhật trạng thái, ghi chú.
- [x] **Vận hành (U03):** BR05 lỗi theo lô · BR06 lần chạy + bước · BR07 sao lưu/khôi phục · BR08 báo cáo theo hạng kỳ cũ/kỳ mới.
- [x] Luồng kịch bản N01–N05: **chọn và xem trước** (chưa ghi B/D) → **chạy** → **đối chiếu mong đợi vs thực tế** → **chạy lại**.
- **Xong khi:** demo được 8 BR trên 1 app. *(22/10 ✅: kiểm tra trên trình duyệt đủ 3 tab; chạy KB03 từ giao diện qua runner → `triggered_by = app`, 0 dòng mới, 0 cảnh báo mới)*

### P2-7. Sao lưu và khôi phục (BR07) — luồng L06
- [x] Sao lưu đủ 6 bảng DW (gồm lịch sử SCD) khi pipeline dừng ghi; ghi danh mục, trạng thái, kết quả vào `backup_catalog` / `backup_operation`.
- [x] Khôi phục về mốc đã chọn; **đối chiếu** số dòng, tiền vào/ra, báo cáo theo hạng, phiên bản SCD trước/sau.
- [x] Quy trình tiếp tục nạp sau khôi phục (sheet còn mở). *(22/10: chạy lại lô đã bị khôi phục mất → nạp lại đúng các dòng đó, vì pipeline kiểm tra trùng theo dữ liệu thật trong kho)*
- **Xong khi:** KB07 chạy đúng (sao lưu → nạp lô KB05 → khôi phục → số liệu trở về mốc cũ). ✅ *(22/10: dùng lô KB07 riêng; 5/5 nhóm số liệu khớp bản sao lưu)*

### P2-8. Đánh giá (3 tiêu chí)
- [x] **Data correctness:** mỗi KB có truy vấn **đối chiếu tại nguồn**; tái dùng khung kiểm tra ISO/IEC 25012 của Phase 1 (completeness, uniqueness, accuracy, consistency) + kiểm tra SCD (không chồng lấn, đúng 1 bản hiện tại).
- [x] **Performance:** phương án thay thế (Q-K); đo truy vấn BR01/BR02/BR08 và thời gian pipeline mỗi lô; cách đo 6 lần, median/p95 như Phase 1.
- [x] **Hiệu quả khai thác:** 8/8 KB đạt; số thao tác và thời gian để có câu trả lời trên app.
- **Xong khi:** bảng kết quả 8 KB + bảng benchmark. *(22/10 ✅: 8/8 KB, 38/38 kiểm tra; 7 truy vấn BR01/BR02/BR08 cùng kết quả trên 2 hệ; xem [EVALUATION_P2.md](EVALUATION_P2.md). Số thao tác trên app cần đo lại bằng đồng hồ khi quay video)*

### P2-9. Report và sản phẩm nộp
- [ ] Viết lại report phần II–VII theo Berka, 3 nhóm người dùng, 8 BR, kiến trúc A–F; giữ cấu trúc IEEE và phương pháp đánh giá.
- [ ] Slide, video 20–30 phút (đi theo KB01 → KB08), gói demo; tên file theo Group 27.

---

## 5. Tái dùng từ Phase 1

| Tái dùng gần như nguyên | Tái dùng khung, sửa nội dung | Bỏ |
|---|---|---|
| Image Spark, ClickHouse, PostgreSQL trong Docker Compose | ETL Spark (đọc → chuẩn hóa → dimension → fact → nạp ClickHouse) | Kafka + producer + Structured Streaming |
| `common.py`, `run_sql_file`, Makefile, cách nạp Parquet → ClickHouse | Kiểm tra DQ + bảng audit → `rejected_record`, `pipeline_step` | Superset và 7 dashboard |
| Khung benchmark (`benchmark/`): đo 6 lần, median/p95, so khớp kết quả | Streamlit → app 3 tab có thao tác | Mart RFM, BR Kaggle, dữ liệu Kaggle |
| Report: cấu trúc IEEE, tiêu chí ISO/IEC 25012, phương pháp benchmark | Nội dung report II–VII | Số liệu Kaggle trong report |

---

## 6. Lịch đề xuất (cần nhóm xác nhận ngày nộp — Q-L)

| Tuần | Giai đoạn | Mốc |
|---|---|---|
| 1 | P2-0, P2-1, P2-2 | Có Berka, hạ tầng, lược đồ |
| 2 | P2-3 | **M1:** nạp đủ lịch sử Berka, đối chiếu nguồn khớp |
| 3 | P2-4, P2-5 | SCD2, kiểm soát dữ liệu, cảnh báo (KB03, KB05, KB08 chạy được) |
| 4 | P2-6, P2-7 | **M2:** app 3 tab demo đủ 8 BR, sao lưu/khôi phục |
| 5 | P2-8 | **M3:** đánh giá 8 KB + benchmark |
| 6 | P2-9 | **M4:** report, slide, video |

### Phân công đề xuất (theo tab, mỗi người 2 BR)
| Thành viên | Phần chính | BR / KB |
|---|---|---|
| Kiet Ho Tuan Nguyen | Pipeline theo lô, giám sát pipeline | BR05, BR06 |
| Quan Minh Tran | Mô hình DW, SCD2, sao lưu/khôi phục | BR07, BR08 |
| Van Ngoc Huynh | App tab Tổng quan + tra cứu | BR01, BR02 |
| Hai Ngoc Ho | Cảnh báo, xử lý cảnh báo, benchmark | BR03, BR04 |

---

## 7. Rủi ro

| Rủi ro | Cách xử lý |
|---|---|
| CTU Relational không truy cập được | Dùng bản Berka trên Kaggle (cùng 8 bảng CSV); kiểm tra lại số liệu khảo sát |
| SCD2 trên ClickHouse (không có UPDATE thật) | Pipeline tính lại phiên bản ở Spark rồi ghi đè `dim_customer` theo lô (ReplacingMergeTree hoặc nạp lại toàn bảng — chốt ở P2-2) |
| Ứng dụng có thao tác điều khiển (chạy pipeline, sao lưu) dễ lỗi khi quay video | Mỗi kịch bản có nút "đặt lại trạng thái nền"; quay từng KB riêng |
| RAM Docker 8 GB | Berka nhỏ hơn Kaggle (1,06 triệu giao dịch); không bật Kafka/Superset |
| Report phải viết lại nhiều | Giữ nguyên khung và phương pháp; viết song song từ tuần 2 |
