# Khảo sát nguồn Berka — đối chiếu với sheet (K01–K05)

- Nguồn file: bản PKDD'99 gốc trên Kaggle (`marceloventura/the-berka-dataset`, 8 file CSV, phân cách `;`), lưu ở `data/berka/raw/` (không commit).
- Script: `python3 tools/profile_berka.py` (DuckDB, `tools/requirements.txt`). Ngày chạy: 2026-10-22.

| ID | Chỉ số | Sheet | Đo được | |
|---|---|---:|---:|:-:|
| K01 | Tài khoản | 4.500 | 4.500 | ✅ |
| K01 | Khách hàng | 5.369 | 5.369 | ✅ |
| K01 | Giao dịch | 1.056.320 | 1.056.320 | ✅ |
| K01 | Khu vực | 77 | 77 | ✅ |
| K02 | Tài khoản có đúng 1 OWNER | 4.500 | 4.500 | ✅ |
| K03 | Tài khoản cùng / khác khu vực với chủ | 4.091 / 409 | 4.091 / 409 | ✅ |
| K04 | Tổ hợp (type, operation, k_symbol) gốc → sau chuẩn hóa | 17 → 15 | 17 → 15 | ✅ |
| K05 | `operation` trống, và tất cả đều `k_symbol = UROK` | 183.114 | 183.114 | ✅ |
| K05 | `k_symbol` NULL + chuỗi trắng | 481.881 + 53.433 | 481.881 + 53.433 | ✅ |
| K05 | VYBER "đã đối chiếu chiều tiền trên 9.597 trường hợp" | 9.597 | xem dưới | ❓ |

**VYBER:** có 16.666 giao dịch `type = VYBER`. Không giao dịch nào làm **tăng** số dư (0 trường hợp `balance = trước + amount`), 12.092 trường hợp khớp chính xác `balance = trước − amount` → chiều **OUT** đúng như sheet. Con số 9.597 không khớp cách đếm nào ở trên → **cần nhóm xác nhận cách tính**.

Thông tin thêm: 0 giao dịch âm, 14 giao dịch bằng 0, giao dịch từ 1993-01-01 đến 1998-12-31; 4.500 khách là OWNER, mỗi khách sở hữu đúng 1 tài khoản.

## Membership tier mô phỏng (`data/berka/customer_profile_simulated.csv`)

Sinh bởi `python3 tools/simulate_membership_tier.py` (quy tắc xác định, chạy lại ra cùng file).

| Hạng | Tiền vào năm đầu (CZK) | Khách | % |
|---|---|---:|---:|
| Standard | < 150.000 | 2.075 | 46,1 |
| Silver | 150.000 – < 300.000 | 1.438 | 32,0 |
| Gold | 300.000 – < 550.000 | 698 | 15,5 |
| Platinum | ≥ 550.000 | 289 | 6,4 |

Kiểm tra: 4.500 dòng = 4.500 khách OWNER; 0 trùng; 0 `client_id` lạ; 0 giao dịch trước `tier_effective_from` (= ngày mở tài khoản); `updated_at` = 1999-01-04 08:00 (khác ngày hiệu lực).
