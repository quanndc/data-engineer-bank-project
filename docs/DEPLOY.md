# Deploy bản chỉ xem cho thành viên (Aiven + Streamlit Community Cloud)

Hệ thống đầy đủ (pipeline, kho vận hành PostgreSQL, chạy kịch bản, sao lưu) **vẫn chạy trên máy** để demo và nộp bài. Bản cho thành viên là **bản chỉ xem**:

| Có | Không có |
|---|---|
| Tổng quan (BR01), Tra cứu tài khoản (BR02), Báo cáo lịch sử theo hạng (BR08), tab **SQL** truy vấn tự do trên kho | Cảnh báo, lần chạy, bản ghi lỗi, sao lưu (cần PostgreSQL); mọi nút ghi hay điều khiển |

```
máy bạn: ClickHouse (dw) ──make publish──▶ Aiven ClickHouse ◀── Streamlit Cloud (APP_MODE=viewer) ◀── thành viên
```

Dữ liệu trên Aiven là **bản chụp** tại lúc publish; chạy thêm gì trên máy thì `make publish` lại.

---

## 1. Aiven ClickHouse (bạn làm, một lần)

1. Vào [console.aiven.io](https://console.aiven.io) → tạo (hoặc bật lại, *Power on*) một dịch vụ **ClickHouse**. Ghi nhận ở tab *Overview → Connection information*, mục **ClickHouse HTTPS & JDBC**: host, port, user `avnadmin`, password.
2. Điền vào `.env` trên máy (file này không lên git; không gửi mật khẩu qua chat):
   ```
   AIVEN_CLICKHOUSE_HOST=<host>
   AIVEN_CLICKHOUSE_PORT=<port>
   AIVEN_CLICKHOUSE_USER=avnadmin
   AIVEN_CLICKHOUSE_PASSWORD=<password>
   AIVEN_CLICKHOUSE_SECURE=true
   AIVEN_CLICKHOUSE_DATABASE_ENGINE=Replicated
   ```
3. **Tài khoản chỉ đọc** cho app: `bankdw_viewer` (đã tạo ngày 29/10/2026, mật khẩu ở `AIVEN_VIEWER_PASSWORD` trong `.env`). Tạo lại nếu cần:
   ```sql
   CREATE USER IF NOT EXISTS bankdw_viewer IDENTIFIED BY '<mật khẩu riêng>';
   GRANT SELECT, SHOW TABLES, SHOW COLUMNS ON dw.* TO bankdw_viewer;
   ```
   Đã kiểm tra: đọc được `dw`; `INSERT`, `DROP`, đọc `system.users` đều bị từ chối.

## 2. Đẩy dữ liệu lên Aiven

```bash
make publish
```

Tạo 6 bảng nếu chưa có, xoá dữ liệu cũ, chép lại (bảng fact chép theo từng năm), rồi so số giao dịch, tổng tiền, số phiên bản khách, số tài khoản giữa máy và Aiven. Kết thúc bằng `EQUAL` là đúng.

## 3. Thử bản chỉ xem trên máy

```bash
make viewer        # http://localhost:8502
```

## 4. Streamlit Community Cloud (bạn làm, một lần)

1. Vào [share.streamlit.io](https://share.streamlit.io) → đăng nhập bằng GitHub có quyền vào repo `quanndc/data-engineer-bank-project`.
2. *Create app* → *Deploy a public app from GitHub*:
   - Repository: `quanndc/data-engineer-bank-project`, branch `main`
   - Main file path: `apps/app/app.py` (Streamlit Cloud tự cài `apps/app/requirements.txt`)
   - *Advanced settings* → Python **3.11**, và **Secrets**: dán nguyên nội dung file `streamlit_secrets.toml` ở thư mục gốc repo trên máy (đã điền sẵn từ `.env`, không lên git). Dạng:
     ```toml
     APP_MODE = "viewer"
     CLICKHOUSE_HOST = "<host Aiven>"
     CLICKHOUSE_PORT = "<port>"
     CLICKHOUSE_USER = "bankdw_viewer"
     CLICKHOUSE_PASSWORD = "<mật khẩu của bankdw_viewer>"
     CLICKHOUSE_SECURE = "true"
     ```
3. Sau khi deploy: *Settings → Sharing* → chọn **Only specific people can view this app** và thêm email của các thành viên.
4. Gửi link `https://….streamlit.app` cho nhóm.

## Lưu ý

- App trên Streamlit Cloud "ngủ" khi lâu không ai mở; lần mở sau chờ khoảng 30 giây.
- Kiểm tra gói Aiven còn hạn không; dịch vụ bị tắt thì tên miền biến mất và app báo lỗi kết nối.
- Tab SQL chỉ chạy câu đọc, mỗi câu tối đa 30 giây, 500 MB bộ nhớ, 10.000 dòng; ClickHouse cũng từ chối mọi lệnh ghi (đã thử `DROP`, `INSERT`, đổi `readonly`).
