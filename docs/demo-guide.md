# Kịch bản demo nghiệm thu

Kịch bản cho buổi demo với các anh lead, bám theo đề `intern-messaging-grpc-kafka-assignment.md`: mục IV (UI demo), mục VI Ngày 5 (kịch bản ngoại lệ) và mục VII (5 câu hỏi nghiệm thu). Mọi bước đều có lệnh sẵn trong `scripts/demo.ps1`.

Thời lượng: khoảng 25–30 phút demo, cộng phần hỏi đáp.

## 1. Cách dùng script

Mở **Windows PowerShell**, đứng ở thư mục gốc của repo:

```powershell
. .\scripts\demo.ps1     # nạp script một lần; dấu chấm ở đầu là bắt buộc
demo                     # xem danh sách bước
demo 3                   # chạy bước 3
demo all                 # tập dượt: chạy lần lượt bước 0–9 và 12, dừng chờ Enter sau mỗi bước (q để dừng)
demo all -IncludeOptional  # tập dượt thêm bước 10 (tắt Redis) và 11 (đo tải)
```

Mỗi bước in ra 4 loại dòng:
- `NÓI` (xanh dương): ý cần nói.
- `TRÊN UI` (tím): thao tác trên trình duyệt.
- `PS>` (xám): lệnh đang chạy, để người xem thấy lệnh thật.
- `MONG ĐỢI` (xanh lá): kết quả đúng. Nếu kết quả khác, xem mục 4.

Khi demo thật thì chạy **từng bước** (`demo 3`, `demo 4`, …) để có thể dừng giải thích hoặc quay lại khi được hỏi. Các bước tắt service (7, 10) tự bật lại service trong `finally`, kể cả khi có lỗi giữa chừng.

## 2. Trước giờ demo (khoảng 30 phút trước)

1. Đóng ứng dụng nặng (Cursor, bớt tab Chrome). Máy có khoảng 8 GB RAM; khi thiếu RAM, MySQL có lúc khựng vài giây (`target\slow-failure-report.md`).
2. Khởi động lại Docker Desktop, chờ biểu tượng xanh.
3. `demo 0` để bật sandbox, kiểm healthy và readiness, gửi 2 đơn mồi, mở UI và RabbitMQ UI.
   - `demo 0 -Reset` để bắt đầu với DB trống (xóa đơn cũ). Chỉ dùng khi muốn danh sách "Đơn gần đây" sạch.
   - `demo 0 -Build` nếu code vừa thay đổi (build lại image, khoảng 2–2,5 phút).
   - `demo 0 -NoBrowser` nếu không muốn script tự mở trình duyệt (đỡ tốn RAM).
4. Trên UI bấm **Ctrl+F5** một lần, để trình duyệt không dùng `app.js` cũ trong cache.
5. (Tùy chọn) Kafka UI: `docker compose --profile tools up -d kafka-ui`, rồi mở http://localhost:8090.

## 3. Các bước demo

| Bước | Lệnh | Nội dung | Mục đề | Thời gian |
|---|---|---|---|---|
| 1 | `demo 1` | Mục tiêu, kiến trúc, 2 luồng, các điểm khác đề có chủ đích | I, II | 3' |
| 2 | `demo 2` | 1 lệnh khởi động, mọi service healthy | VIII.1, Ngày 1 | 1' |
| 3 | `demo 3` + UI | Luồng 1: `200 ISSUED`, timeline via RabbitMQ, Correlation ID, `CACHE MISS` rồi F5 ra `CACHE HIT` | III.1, IV, V.1 | 3' |
| 4 | `demo 4` + UI | Luồng 2: `202`, polling, `ISSUED`, via gRPC / Kafka | III.2, IV | 3' |
| 5 | `demo 5` + UI | Chặn trùng: `409 DUPLICATE_ORDER`, `409 DUPLICATE_ORDER_MISMATCH`, 6 request đồng thời | IV.3, V.1, D21 | 3' |
| 6 | `demo 6` | Truy vết log theo `correlation_id`, key Redis và TTL | V.1, V.2 | 2' |
| 7 | `demo 7` | Tắt Payment: timeout 3 s, DLQ, bật lại, đơn mồi | Ngày 5, câu 2 | 3' |
| 8 | `demo 8` | Message hỏng vào `payment.rpc.request.dlq` | Ngày 5 (DLQ) | 1' |
| 9 | `demo 9` | Kafka: gửi lại event 3 lần vẫn 1 hợp đồng; message hỏng vào DLT | III.2, câu 4 | 3' |
| 10 | `demo 10` | (Tùy chọn) Tắt Redis: vẫn tạo đơn, vẫn chặn trùng | V.1 (D12) | 2' |
| 11 | `demo 11` | (Phụ lục, chỉ khi được hỏi) Đo tải 20 và 100 đơn. Luồng 1 chậm rõ và có thể timeout (3 lần đo 100 đơn: 51/100, 0/100, 0/100 timeout, tùy máy), Luồng 2 phát hành đủ | câu 1–2 | 2' |
| 12 | `demo 12` | 5 câu hỏi nghiệm thu: ý trả lời và bằng chứng | VII | hỏi đáp |

### Bước 3: Luồng 1 (RabbitMQ RPC)
- **Trên UI:** chọn "Luồng 1: RabbitMQ RPC", bấm [Tạo Đơn & Phát Hành].
- **Mong đợi:** `POST → 200 OK`, `ISSUED`, số hợp đồng `ACBI-2026-…`; 4 bước: [Tạo đơn] via HTTP, [Thanh toán] và [Phát hành e-GCN] via RabbitMQ, [Thông báo] "SMS sent (simulated)"; `CACHE MISS (DB)`.
- Bấm **F5**: URL giữ `#order=…`, nhãn chuyển thành `CACHE HIT (REDIS)`.
- **Nói:** thread HTTP chờ reply của từng bước; `reply_to` (Direct Reply-to) và `correlation_id` ghép reply với request; timeout mỗi bước 3 s.

### Bước 4: Luồng 2 (gRPC + Kafka)
- **Trên UI:** chọn "Luồng 2: gRPC + Kafka", bấm tạo.
- **Mong đợi:** `POST → 202 Accepted`, "Đang chờ Kafka…", rồi `ISSUED` sau khoảng 1 s; [Thanh toán] via gRPC, [Phát hành e-GCN] via Kafka.
- **Nói:** gRPC vì Order cần biết ngay đã thu tiền hay chưa; phát hành không cần chờ nên đi qua event.

### Bước 5: Chặn request trùng (D21, yêu cầu của lead)
- **Trên UI:** bấm [Gửi lại Request trùng]: `POST → 409 Conflict`, nhãn "Request trùng, đã chặn (409 DUPLICATE_ORDER)", bên dưới là đơn gốc.
- **Script:** thêm trường hợp cùng mã nhưng khác số tiền (`409 DUPLICATE_ORDER_MISMATCH`) và 6 request giống hệt cùng lúc (1 thành công, 5 × `409`: `ORDER_IN_PROGRESS` khi đơn gốc chưa ghi xong, `DUPLICATE_ORDER` nếu đến sau khi đã ghi). SQL xác nhận mỗi mã chỉ có 1 đơn và 1 thanh toán.
- **Nói:** request hợp lệ đầu tiên được giữ; so cả 5 trường; hai lớp chặn: Redis `SET NX` rồi `UNIQUE(partner_order_id)` trong MySQL.
- **Nếu được hỏi "đề ghi trả về kết quả cũ?":** lead đã đổi yêu cầu. Response `DUPLICATE_ORDER` vẫn có `order_id` và `order_status` của đơn gốc, UI vẫn hiển thị đơn gốc (D21).

### Bước 6: Truy vết và Redis
- **Mong đợi:** một `correlation_id` có mặt ở order, payment và policy, qua các transport HTTP, gRPC (hoặc RabbitMQ) và Kafka; TTL key chống trùng khoảng 86400 s, key cache khoảng 600 s.
- Trên UI có sẵn lệnh ở mục "Truy vết log của đơn này qua 3 service".

### Bước 7: Payment tắt (timeout và DLQ)
- **Mong đợi:**
  - Luồng 1: `200 PROCESSING_FAILED`, `PAYMENT_TIMEOUT` sau khoảng 3 s; `payment.rpc.request.dlq` tăng 1.
  - Luồng 2: `PAYMENT_SERVICE_UNAVAILABLE` (nhanh) **hoặc** `PAYMENT_TIMEOUT` (khoảng 3 s). Cả hai đều đúng.
  - Payment được bật lại, đơn mồi `ISSUED`.
- **Nói:** HTTP không treo; request quá 3 s trong queue vào DLQ; giới hạn "đã trừ tiền nhưng đơn lỗi" (câu 2).

### Bước 8: Message hỏng vào DLQ
- **Mong đợi:** `routed = True`, `payment.rpc.request.dlq` tăng 1, log payment-service WARN "Fatal message conversion error; message will be reject"; xem payload trên RabbitMQ UI > Queues > `payment.rpc.request.dlq` > Get messages.

### Bước 9: Kafka gửi lại và DLT
- **Mong đợi:** `duplicate_event_ignored` 3 lần ở policy-service và 3 lần ở order-service; `policies = 1`; timeline 4 bước; `payment.recorded.DLT` tăng 1.

## 4. Khi có sự cố

| Hiện tượng | Xử lý |
|---|---|
| `demo 0` báo Docker chưa chạy | Mở Docker Desktop, chờ xanh, chạy lại `demo 0` |
| Một service không `healthy` | `docker compose up -d --wait`; RabbitMQ lỗi `.erlang.cookie` thì chạy `docker compose up -d --force-recreate --wait rabbitmq` (README) |
| Một đơn chậm bất thường (vài giây) | Máy thiếu RAM làm MySQL khựng, không phải lỗi luồng xử lý. Dùng `demo 6` để chỉ ra gRPC dừng đúng deadline |
| UI vẫn hiện nhãn cũ "trả về kết quả cũ" | Trình duyệt dùng `app.js` cũ: Ctrl+F5 |
| Request đầu sau khi bật lại một service mất gần 3 s | Gửi 1 đơn mồi (các bước 7 và 10 đã tự làm) |
| Bước 7 hoặc 10 bị ngắt giữa chừng | Chạy `docker compose up -d --wait` để bật lại mọi service |

## 5. Sau buổi demo

- `docker compose down` để dừng (thêm `-v` nếu muốn xóa dữ liệu).
- Dữ liệu demo có tiền tố `DEMO-`, `WARM-`, `LOAD…`; không ảnh hưởng lần chạy sau.
