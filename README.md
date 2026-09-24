# Insurance Sandbox: RabbitMQ RPC vs gRPC + Kafka

Bài tập thực tập 5 ngày: mô phỏng luồng phát hành bảo hiểm **Tạo đơn → Thanh toán → Phát hành hợp đồng → Thông báo → Cập nhật UI** trên 3 microservice. Mục đích là so sánh hai cách giao tiếp:

| | Luồng 1: `RABBITMQ_RPC` | Luồng 2: `GRPC_KAFKA` |
|---|---|---|
| Order → Payment | RabbitMQ RPC (`reply_to` + `correlation_id`) | gRPC `RecordPayment` (HTTP/2 + Protobuf) |
| Payment → Policy | Order gọi RPC sang Policy | Event Kafka `payment.recorded` |
| Policy → Order | Reply RPC | Event Kafka `policy.issued` |
| Phản hồi HTTP | `200 OK`, chờ tới khi `ISSUED` (đồng bộ) | `202 Accepted` với `PAYMENT_RECORDED`, UI polling tới `ISSUED` (bất đồng bộ) |

Kèm theo: Redis (idempotency và cache), log JSON có `correlation_id` xuyên suốt, DLQ, chống trùng khi consume Kafka.

> **Đề bài gốc:** [`intern-messaging-grpc-kafka-assignment.md`](intern-messaging-grpc-kafka-assignment.md).
> **Thay đổi stack:** đề ghi .NET 10 / ASP.NET Core. Dự án dùng **Java 21 + Spring Boot 4.1** và **MySQL 8** thay cho Postgres/SQLite, **đã được anh lead đồng ý**.

---

## Trạng thái

- **Ngày 1 đã xong:** hạ tầng và khung 3 service chạy được bằng một lệnh, mọi container `healthy`.
- **Ngày 2 đã xong: Luồng 1 (`RABBITMQ_RPC`) chạy thông qua 3 service:**
  - RPC qua `payment.rpc.request` và `policy.rpc.request`, dùng Direct Reply-to.
  - Timeout 3 giây: trả `PROCESSING_FAILED`, HTTP không treo.
  - Queue có TTL 3 giây, message hết hạn hoặc bị lỗi sẽ vào DLQ.
  - `correlation_id` được truyền qua header `x-correlation-id` và có trong log của cả 3 service.
- **Chưa làm:** Luồng 2 (`GRPC_KAFKA` hiện trả `501`), Redis (idempotency và cache), Web UI.

Chi tiết tiến độ xem [`current-state.md`](current-state.md).

---

## Chạy dự án

**Yêu cầu:** Docker Desktop (đang chạy). Để build ngoài Docker cần thêm JDK 21 và Maven 3.9.

```bash
docker compose up --build -d     # lần đầu mất vài phút (Maven tải dependency)
docker compose ps                # mọi service (trừ ui) phải ở trạng thái (healthy)
```

Dừng: `docker compose down` (thêm `-v` để xóa dữ liệu MySQL).
Kafka UI (tùy chọn): `docker compose --profile tools up -d kafka-ui`.

### Địa chỉ

| Thành phần | URL / cổng |
|---|---|
| Web UI | http://localhost:3000 (hiện là trang placeholder, UI thật làm ở Ngày 4) |
| Order Service | http://localhost:8080 (health: `/actuator/health`) |
| Payment Service | http://localhost:8081, gRPC `:9090` (Ngày 3) |
| Policy Service | http://localhost:8082 |
| RabbitMQ Management | http://localhost:15672 (user/pass trong `.env`) |
| Kafka UI (profile `tools`) | http://localhost:8090 |
| MySQL | `localhost:3307` |
| Redis | `localhost:6379` |
| Kafka (từ máy host) | `localhost:9094` |

### Build và test không cần Docker

```bash
mvn -B package                   # build 4 module + chạy test
mvn -B -pl order-service -am test -Dtest=SandboxJsonLogFormatterTests -Dsurefire.failIfNoSpecifiedTests=false
```

---

## API (Order Service)

| Method | Path | Kết quả |
|---|---|---|
| `POST` | `/api/v1/orders` | Tạo đơn và chạy Luồng 1. Xem bảng mã trả về bên dưới |
| `GET` | `/api/v1/orders/{id}` | Chi tiết đơn và timeline, đọc thẳng DB; `404` nếu không có. `cache_status` có từ Ngày 4 (Redis) |
| `GET` | `/api/v1/orders` | 100 đơn mới nhất, mới nhất trước (không kèm timeline) |

Body của `POST` (JSON dùng snake_case, `amount` là số nguyên VND):

```json
{"partner_order_id": "P-1001", "customer_name": "Nguyen Van A", "phone": "0901234567", "amount": 500000, "mode": "RABBITMQ_RPC"}
```

| Mã | Khi nào |
|---|---|
| `200` | Luồng 1 chạy xong: `status` là `ISSUED` (có `policy_number`) hoặc `PROCESSING_FAILED` (có `failure_reason`: `PAYMENT_TIMEOUT`, `POLICY_TIMEOUT`, lý do từ Payment như `INVALID_AMOUNT`, hoặc `BROKER_UNAVAILABLE`). Chờ tối đa khoảng 6 giây (2 × 3 giây) |
| `200` + `idempotent_replay: true` | `partner_order_id` đã tồn tại: trả nguyên đơn cũ, không gọi RPC lần nữa |
| `400` | Body sai; `errors[]` liệt kê field lỗi (tên snake_case), kể cả `mode` không hợp lệ |
| `409` | Hai request trùng đến cùng lúc; request thua trả 409, gửi lại sau |
| `501` | `mode = GRPC_KAFKA` (Luồng 2, làm ở Ngày 3) |

Response gồm `order_id` (dạng `ORD-yyyyMMdd-XXXXXXXX`), `status`, `correlation_id` (UUID), `policy_number`, `failure_reason` và `timeline[]`. Mỗi bước timeline có `step`, `service`, `transport`, `status`, `duration_ms`, `detail`. Các bước: `ORDER_CREATED` → `PAYMENT` → `POLICY_ISSUANCE` → `NOTIFICATION`, hoặc `PROCESSING_FAILED`.

Thử nhanh (PowerShell):

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/v1/orders -ContentType 'application/json' -Body '{"partner_order_id":"P-1001","customer_name":"Nguyen Van A","phone":"0901234567","amount":500000,"mode":"RABBITMQ_RPC"}'
```

Muốn thấy timeout: chạy `docker compose stop payment-service` rồi gửi đơn mới. Sau khoảng 3 giây sẽ nhận `PROCESSING_FAILED`, và request hết hạn nằm trong `payment.rpc.request.dlq` trên RabbitMQ UI (`:15672`).

---

## Kiến trúc

```text
Web UI (nginx :3000) ──/api──► Order Service :8080 ──┬── Luồng 1: RabbitMQ RPC ──► Payment :8081 / Policy :8082
                                    │                └── Luồng 2: gRPC ──► Payment :9090 ──Kafka──► Policy ──Kafka──► Order
                                    ├── Redis (idempotency + cache)
                                    └── MySQL (order_db | payment_db | policy_db, mỗi service một schema)
```

- **Sơ đồ sequence chi tiết** (gồm cả nhánh lỗi: timeout, trùng request, gửi lại event, DLQ): [`docs/sequence-diagrams.md`](docs/sequence-diagrams.md)
- **Các trường hợp xấu và cách xử lý (F1–F32), khung service:** [`docs/implementation-plan.md`](docs/implementation-plan.md)
- **Hợp đồng:** [`contracts/payment.proto`](contracts/payment.proto) (gRPC). JSON Schemas cho message RabbitMQ và event Kafka sẽ bổ sung ở Ngày 2–3.

### Cấu trúc thư mục

```text
contracts/          payment.proto + Maven module sinh gRPC stub (dùng chung cho Order và Payment)
order-service/      REST API, điều phối 2 luồng
payment-service/    ghi nhận thanh toán (RabbitMQ RPC + gRPC server)
policy-service/     phát hành hợp đồng (RabbitMQ RPC + Kafka consumer)
ui/                 HTML/JS tĩnh + nginx.conf (proxy /api)
docker/             service.Dockerfile dùng chung + mysql/init.sql
docs/               sơ đồ sequence, kế hoạch triển khai
current-state.md    tiến độ theo ngày
```

---

## Quyết định thiết kế chính

Đề bài có một số điểm mâu thuẫn hoặc để ngỏ. Các quyết định dưới đây là có chủ đích; danh sách đầy đủ D1–D18 ở mục 6 của [`CLAUDE.md`](CLAUDE.md).

- **Cache-aside, xóa key khi trạng thái đổi (D1):** để lần xem đầu hiện `CACHE MISS (DB)` và lần refresh hiện `CACHE HIT (REDIS)`, đúng yêu cầu demo.
- **`partner_transaction_id = TXN-{partner_order_id}` (D2):** chống gạch nợ trùng thêm một lớp ở Payment.
- **Request trùng trả `200` kèm kết quả cũ (D4); request trùng đến đồng thời trả `409` (D11).**
- **Chống trùng khi consume Kafka bằng `consumer_inbox` và `UNIQUE(order_id)` (D10):** Kafka gửi lại event cũng không sinh hợp đồng thứ hai.
- **RPC timeout 3s, message tự hết hạn và vào DLQ (D7).**
- **Giới hạn đã biết (nói rõ khi demo):**
  - Payment có thể đã gạch nợ trong khi đơn bị timeout.
  - Payment "lưu DB rồi mới publish Kafka" (dual write), nên có thể mất event. Cách làm production là Transactional Outbox.
