<div align="center">

# 🏦 BankDW — Banking Data Warehouse (Phase 2)

**Kho dữ liệu Kimball cho dòng tiền và hoạt động tài khoản ngân hàng, kèm kho vận hành và ứng dụng điều khiển pipeline**

Đồ án môn Data Engineering (CO5173) · HCMUT · HK1 2026–2027 · Nhóm 27

![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-25.3-FFCC01?logo=clickhouse&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.40-FF4B4B?logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker%20Compose-2496ED?logo=docker&logoColor=white)

</div>

---

## 📌 Giới thiệu

Dữ liệu **Berka** (ngân hàng Séc, PKDD'99): 1.056.320 giao dịch của 4.500 tài khoản, 1993–1998, cộng **hạng thành viên mô phỏng** để có lịch sử thay đổi (SCD Type 2). Hệ thống phục vụ **3 nhóm người dùng** và **8 yêu cầu nghiệp vụ** theo các quyết định nhóm chốt ngày 18/10/2026 ([docs/PLAN_PHASE2.md](docs/PLAN_PHASE2.md)).

| Người dùng | Tab trên app | BR |
|---|---|---|
| U01 Quản lý kinh doanh | Tổng quan | BR01 tổng quan dòng tiền |
| U02 Chuyên viên phân tích | Phân tích | BR02 tra cứu tài khoản · BR03 cảnh báo gian lận · BR04 xử lý cảnh báo |
| U03 Kỹ sư dữ liệu | Vận hành | BR05 kiểm soát dữ liệu · BR06 giám sát pipeline · BR07 sao lưu/khôi phục · BR08 báo cáo lịch sử (SCD2) |

## 🏗️ Kiến trúc A–F

```
A. Nguồn (Berka CSV, hạng mô phỏng, file kịch bản)
      │ theo lô
      ▼
C. Pipeline (PySpark) ──── chụp đầu vào ───▶ B. Data Lake (Parquet theo lô)
      │ nạp                        │ ghi tiến trình, lỗi, cảnh báo
      ▼                            ▼
D. ClickHouse (star schema dw   E. PostgreSQL (ops: lần chạy, bản ghi lỗi,
   + vùng sao lưu)                  cảnh báo, trạng thái xử lý, sao lưu)
      │ đọc                        │ đọc / ghi xử lý
      └──────────────┬─────────────┘
                     ▼
F. Ứng dụng Streamlit 3 tab ── lệnh chạy / chạy lại / sao lưu / khôi phục ──▶ C
```

- **Mô hình:** 1 fact (`fact_transaction`, 1 dòng/giao dịch) + 5 dimension; `dim_customer` SCD Type 2 theo hạng; `dim_district` 2 vai trò (chi nhánh, nơi ở); `dim_transaction_profile` là junk dimension. Chi tiết: [docs/data_model.md](docs/data_model.md).
- **Pipeline:** 1 lô tham chiếu + 72 lô tháng; mỗi lô được kiểm tra theo 7 quy tắc (V1–V7), bản ghi lỗi bị tách riêng, lô được đối chiếu với đầu vào, chạy lại không tạo trùng.

## ✅ Kết quả

- **Đúng dữ liệu:** 8/8 kịch bản KB01–KB08 đạt, 38/38 kiểm tra; tiền vào/ra khớp nguồn tới từng xu. → [docs/EVALUATION_P2.md](docs/EVALUATION_P2.md)
- **Hiệu năng** (×1 và ×10): báo cáo tổng hợp trên ClickHouse nhanh hơn PostgreSQL 3–22 lần (7–28 lần ở ×10); star schema sửa dữ liệu rẻ hơn One Big Table hàng trăm nghìn lần số dòng; kho vận hành trên PostgreSQL nhanh hơn 8–70 lần và tự chặn dữ liệu sai. → [docs/BENCHMARK_P2.md](docs/BENCHMARK_P2.md)
- **Đối chiếu với sheet của nhóm:** [docs/DOI_CHIEU_SHEET_P2.html](docs/DOI_CHIEU_SHEET_P2.html)
- **Report (IEEE, LaTeX):** [report/main.tex](report/main.tex)

## 🚀 Chạy

Yêu cầu: Docker Desktop (khoảng 7 GB RAM cho máy ảo). Tải Berka (bản PKDD'99 trên Kaggle) vào `data/berka/raw/`.

```bash
make up              # ClickHouse, PostgreSQL, Spark, app (http://localhost:8501)
make schema          # tạo dw (ClickHouse) và ops (PostgreSQL), kiểm tra cột với Data Dictionary
make load-ref        # lô tham chiếu: khu vực, tài khoản, khách, hạng ban đầu
make load-history    # 72 lô tháng 1993-01 → 1998-12
make alerts          # quét cảnh báo trên lịch sử
make scenarios       # sinh file kịch bản KB03, KB05, KB07, KB08 (cũng chạy được từ tab Vận hành)
make evaluate        # đánh giá KB01–KB08
make bench-all       # benchmark B1–B4 (thêm SCALE=10 để đo ở ×10)
```

Danh sách lệnh đầy đủ: [Makefile](Makefile) (mỗi lệnh có chú thích). Nhật ký thay đổi: [CHANGELOG.md](CHANGELOG.md).

## 📁 Cấu trúc

| Thư mục | Nội dung |
|---|---|
| `spark/jobs/` | Pipeline (`pipeline.py`), cảnh báo, sao lưu, runner cho app, đánh giá, benchmark |
| `apps/app/` | Ứng dụng Streamlit 3 tab |
| `warehouse/` | DDL ClickHouse (`dw`) và PostgreSQL (`ops`), cấu hình ClickHouse |
| `tools/` | Khảo sát Berka, sinh hạng mô phỏng, sinh dữ liệu kịch bản |
| `docs/` | Kế hoạch, mô hình dữ liệu, đánh giá, benchmark; số liệu gốc trong `docs/results/p2/` |
| `report/` | Technical report (IEEE): `main.tex` (tiếng Anh), `main_vi.tex` (tiếng Việt), ảnh chụp ứng dụng trong `figures/` |
| `slides/` | Slide thuyết trình (Beamer, tiếng Việt): `slides.tex`, biên dịch bằng XeLaTeX / `tectonic slides.tex` |
| `archive/phase1/` | **Phase 1** (dữ liệu Kaggle, Kafka, Superset): code, tài liệu, changelog, README cũ. Chỉ để tham khảo; cũng có ở tag `phase1-kaggle` |
