# Yêu cầu đồ án — Data Engineering (CO5173)

Nguồn: *DE_Presentation and Project Descriptions and Requirements — Semester 1, 2026–2027* (Assoc. Prof. Dr. Võ Thị Ngọc Châu, HCMUT).
File này chỉ tóm tắt **phần Project**, bỏ qua phần Presentation.

---

## 1. Mục tiêu

- Củng cố kiến thức và kỹ năng theo chuẩn đầu ra của môn.
- Tự học và làm việc nhóm về các chủ đề data engineering.
- Phát triển một **ứng dụng phản ánh được hiệu quả của kết quả data engineering**.

---

## 2. Miền ứng dụng (Application domain)

- Mỗi nhóm làm **một miền ứng dụng duy nhất** trong lớp (của nhóm: **Banking**).
- Phải xác định:
  - Các **nhóm người dùng chính (key user groups)** mà nhóm muốn hỗ trợ.
  - Các **business requirements cần dữ liệu được chuẩn bị bởi data engineering**, ví dụ phục vụ phân tích dữ liệu, khoa học dữ liệu, khai phá dữ liệu, BI, hệ hỗ trợ ra quyết định…

---

## 3. Đề tài

**Build a system where your data engineering solution is proved successful (in a specific application domain)**

Làm data engineering sao cho dữ liệu đầu ra **dùng được cho người dùng trong miền ứng dụng**. Sản phẩm gồm 3 phần chính:

1. Nghiệp vụ và dữ liệu liên quan trong miền ứng dụng.
2. Data engineering đáp ứng các business requirements.
3. Phát triển ứng dụng để đưa kết quả vào thực tế.

Trọng tâm của môn là **quản lý dữ liệu (data management)** và **xử lý dữ liệu (data processing)**. Nhóm phải làm rõ và sử dụng một số công nghệ cho từng mảng. Ví dụ đề đưa ra (không bắt buộc, danh sách kết thúc bằng "…"):

| Mảng | Ví dụ công nghệ |
|---|---|
| Data management | Redshift, Hive, Synapse Analytics, Cassandra, BigQuery (BigLake), CouchBase, Databricks, Neo4J, RavenDB, Dynamo, Oracle data lakehouse, … |
| Data processing | Flink, Spark Streaming, Hadoop Map-Reduce, Storm, Kafka Stream, … |

### Ràng buộc
- Tối thiểu **2·n business requirements** (n = số thành viên) sử dụng dữ liệu từ data engineering.
  - Nhóm có **4 người → tối thiểu 8 BR**.
- Các BR phải thể hiện việc hỗ trợ **mục tiêu kinh doanh** của doanh nghiệp trong miền ứng dụng của nhóm.
- Phải nêu **lý do (rationale)** giải pháp data engineering hỗ trợ được các BR đó.
- Phải **đánh giá các công nghệ đã chọn**.
- Có thể dùng **giải pháp thay thế** để benchmark công nghệ đã chọn từ nhiều góc độ, **đặc biệt trong bối cảnh Big Data**.

---

## 4. Nội dung bắt buộc của Project

- [ ] Giới thiệu bối cảnh đề tài.
- [ ] Giới thiệu stakeholders (đặc biệt là người dùng), cùng business requirements và nhu cầu sử dụng dữ liệu, để định hình một ứng dụng hướng dữ liệu (data-driven) cho miền ứng dụng.
- [ ] Trình bày các nguồn dữ liệu dự kiến và đặc điểm dữ liệu cần lưu ý khi làm data engineering.
- [ ] Trình bày giải pháp data engineering và **ít nhất một giải pháp thay thế** để so sánh.
- [ ] Trình bày công nghệ quản lý dữ liệu, công nghệ xử lý dữ liệu, và cách tận dụng chúng cho ứng dụng.
- [ ] Phát triển ứng dụng cho đề tài.
- [ ] Đánh giá giải pháp data engineering và ứng dụng theo 3 tiêu chí:
  - [ ] **Độ đúng của dữ liệu** sau khi làm data engineering.
  - [ ] **Hiệu năng** của giải pháp data engineering.
  - [ ] **Hiệu quả hỗ trợ khai thác dữ liệu** thông qua ứng dụng.
- [ ] Kết luận.

---

## 5. Sản phẩm nộp

- **Hạn:** cuối khóa.
- **Tất cả file nộp phải đặt tên theo group ID.**

| Sản phẩm | Định dạng | Trọng số (trên tổng điểm môn) | Ghi chú |
|---|---|---|---|
| Technical report | PDF | 10% | Bắt buộc để được chấm phần Project |
| Slide | PDF | 5% | Bắt buộc. Trình bày giải pháp data engineering và ứng dụng chứng minh hiệu quả của giải pháp trong miền ứng dụng |
| Video | mp4 hoặc webm | 5% | Bắt buộc. Thuyết trình + demo ứng dụng, **20–30 phút** |
| Product | Ứng dụng chạy được | 15% | Bắt buộc. Bằng chứng phát triển ứng dụng: demo ứng dụng **thực thi và đánh giá được** |

---

## 6. Tiêu chí chấm Project

Thang mỗi tiêu chí: 0–<4, 4–<6,5, 6,5–<8,5, 8,5–10.

### Report

| Tiêu chí | Trọng số | Để đạt mức cao nhất (8,5–10) |
|---|---|---|
| Report Structure | 1/10 | Tổ chức tốt, mọi phần rõ ràng và logic. Mức thấp hơn nếu **> 7 phần** (quá chi tiết) hoặc **< 4 phần** (quá chung) |
| Technology Content | 4/10 | Trình bày công nghệ đầy đủ cho ứng dụng của nhóm, với **> 3 ví dụ minh họa** |
| Application Content | 4/10 | Thiếu **< 10%** nội dung bắt buộc (mục 4) |
| Evaluation of Technology Used | 1/10 | Đánh giá có hệ thống, **> 85% tiêu chí đánh giá được định nghĩa rõ** |

### Video

| Tiêu chí | Trọng số | Để đạt mức cao nhất |
|---|---|---|
| Video content | 5/10 | Khớp nội dung project, nhất quán với **85–100% slide** |
| Video presentation | 4/10 | Tự tin, mạch lạc, phối hợp tốt, **có tương tác phù hợp** |
| Video duration | 1/10 | Gần đúng thời lượng yêu cầu (lệch 2–5 phút bị trừ, lệch > 10 phút về mức thấp nhất) |

### Slide

| Tiêu chí | Trọng số | Để đạt mức cao nhất |
|---|---|---|
| Slide structure | 2/10 | Tổ chức tổng thể và cấu trúc chi tiết hợp lý. Mức thấp hơn nếu **> 7 phần** hoặc **< 4 phần** |
| Slide content | 6/10 | Chi tiết, bám nội dung, thảo luận có lập luận, **ví dụ minh họa phù hợp** |
| Slide presentation format | 2/10 | Gần như **không lỗi** chữ, màu, bảng, hình, hiệu ứng, chính tả (2–5 lỗi đã bị trừ) |

### Product

| Tiêu chí | Trọng số | Để đạt mức cao nhất |
|---|---|---|
| User Interface Design | 2/10 | Giao diện hợp ngữ cảnh, **tính đến đặc điểm người dùng**, trình bày và tương tác hài hòa |
| Application Implementation | 8/10 | Mỗi lần **demo thành công 1 business requirement** cùng kết quả khai thác dữ liệu tương ứng được **4/n điểm** (n = số thành viên) |

> **Với n = 4:** mỗi BR demo thành công = 4/4 = **1 điểm** (trên thang 10). Tiêu chí tối đa 8/10 → cần **8 BR demo thành công** (= 2·n) để đạt điểm tối đa.

---

## 7. Bonus

Cộng thẳng vào điểm cuối của Project:
- **Cá nhân:** thảo luận, hỏi/đáp xuất sắc trong các buổi trình bày.
- **Nhóm:**
  - Thêm **giải pháp thay thế** với kết quả đánh giá xuất sắc.
  - Thêm **business requirements thú vị hơn, nhiều nhóm người dùng hơn**.
  - Thêm **thảo luận kèm kết quả nghiên cứu**.

