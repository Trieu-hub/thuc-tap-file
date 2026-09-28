# Báo cáo bài tập: Insurance Sandbox (RabbitMQ RPC, gRPC + Kafka)

Thực tập sinh: <họ tên>. Thời gian: 5 ngày (2026-09-23 đến 2026-09-28). Demo nghiệm thu: dự kiến <ngày>. Đề: `intern-messaging-grpc-kafka-assignment.md`.

## 1. Mục tiêu và phạm vi

Xây dựng một sandbox microservice mô phỏng luồng bảo hiểm: **tạo đơn → ghi nhận thanh toán → phát hành hợp đồng → thông báo → cập nhật UI**. Người dùng chọn 1 trong 2 cơ chế để so sánh trực quan:

- **Luồng 1, RabbitMQ RPC:** Order gọi Payment rồi Policy theo kiểu request/reply qua broker.
- **Luồng 2, gRPC + Kafka:** Order gọi Payment bằng gRPC (cần kết quả ngay); Payment và Policy trao đổi sự kiện bất đồng bộ qua Kafka.

Kèm theo: Redis (chống trùng và cache đọc), log JSON có `correlation_id` xuyên suốt, DLQ/DLT.

Ngoài phạm vi, theo đề I.2: OAuth2, ký số, PDF/S3, SMS/Email thật, Kubernetes. Thông báo chỉ là mô phỏng.

## 2. Kiến trúc và 2 luồng

```text
Web UI (nginx :3000) ──/api──► Order :8080 ──┬── Luồng 1: RabbitMQ RPC ──► Payment :8081 / Policy :8082
                                   │         └── Luồng 2: gRPC ──► Payment :9090 ──Kafka──► Policy ──Kafka──► Order
                                   ├── Redis (idempotency:order:{partner_order_id} 24 h, order:{order_id} 10 phút)
                                   └── MySQL (order_db | payment_db | policy_db, mỗi service một schema)
```

| | Luồng 1: RabbitMQ RPC | Luồng 2: gRPC + Kafka |
|---|---|---|
| Kiểu | Đồng bộ: thread HTTP chờ Payment rồi Policy | Đồng bộ tới Payment, sau đó bất đồng bộ qua event |
| Trả lời HTTP | `200 ISSUED` (hoặc `PROCESSING_FAILED`) | `202 PAYMENT_RECORDED`, UI polling tới `ISSUED` |
| Timeout | reply-timeout 3 s, queue TTL 3 s rồi vào DLQ | deadline gRPC 3 s cho mỗi lần gọi |
| Chống trùng phía nhận | `UNIQUE(partner_transaction_id)`, `UNIQUE(order_id)` | Như Luồng 1, cộng `consumer_inbox` (event_id) |
| Message lỗi | `<queue>.dlq` | `<topic>.DLT` sau 3 lần thử lại |
| `correlation_id` | header `x-correlation-id` | metadata gRPC, trường `correlation_id` của envelope |

Sơ đồ sequence đầy đủ (kể cả nhánh lỗi) ở `docs/sequence-diagrams.md`; hợp đồng ở `contracts/` (`payment.proto`, 7 JSON Schema).

## 3. Kết quả

**Test tự động:** 103 test (order 62, payment 21, policy 20), 0 lỗi, chạy bằng `scripts\run-tests.ps1`. Test tích hợp dùng MySQL, RabbitMQ, Kafka và Redis thật qua Testcontainers.

**Kiểm chứng ngoài test:**
- Clone sạch từ GitHub rồi chạy `docker compose up --build`: 157 s, mọi service healthy.
- 11 message thật (event Kafka, request và reply RPC) khớp JSON Schema trong `contracts/` (kiểm bằng `ajv`).
- 100% dòng log của 3 service là JSON.
- Postman/newman: 7 request, 9 assertion đạt.

**Đối chiếu với đề:** đã tách đề thành 51 yêu cầu và kiểm từng yêu cầu bằng test, chạy thật trên sandbox và đọc code (ngày 27–28/9). Tóm tắt theo mục:

| Mục đề | Nội dung | Kết quả |
|---|---|---|
| I–II | 3 service, UI, RabbitMQ, Kafka, Redis, log JSON; trách nhiệm từng service | Đạt |
| III.1 | `reply_to`, `correlation_id`, timeout 3 s, `PROCESSING_FAILED`, HTTP không treo | Đạt |
| III.2 | `.proto`, envelope 5 trường, idempotent consumer, `duplicate_event_ignored` | Đạt |
| IV | Form, radio 2 luồng, timeline 4 bước, Correlation ID, `CACHE MISS` rồi `HIT`, nút gửi trùng | Đạt |
| V | Key chống trùng 24 h, cache 10 phút đọc Redis trước; log JSON đủ field | Đạt |
| VI | Ngày 1–5: hạ tầng, 2 luồng, UI, Redis, log, kịch bản ngoại lệ, README 1 lệnh | Đạt; buổi demo nghiệm thu dự kiến <ngày> |
| VIII | compose 1 lệnh, README (API, ảnh UI, sơ đồ), `contracts/` | Đạt |

**Số đo** (máy Windows 8 GB RAM, Docker Desktop, 2026-09-27/28):

| Đo | Luồng 1 | Luồng 2 |
|---|---|---|
| 1 đơn (sau warm-up) | 81–305 ms, `200 ISSUED` | 68–127 ms cho `202`; `ISSUED` sau khoảng 0,35 s |
| Payment tắt | `PAYMENT_TIMEOUT` sau khoảng 3,1 s, request vào DLQ | `UNAVAILABLE` sau 70–925 ms hoặc `TIMEOUT` sau khoảng 3,2 s |
| 20 đơn cùng lúc (4 lần) | 20/20 `ISSUED`, p50 439–619 ms | 20/20 `202`, `ISSUED` hết sau khoảng 1 s |
| 100 đơn cùng lúc (3 lần) | Lần 1: 49 `ISSUED`, **51 `PAYMENT_TIMEOUT`** (p50 4,5 s). Lần 2 và 3: 100/100 `ISSUED`, p50 2,9 s và 2,0 s | 100/100 `202` cả 3 lần; `ISSUED` hết sau 3–13 s |
| Redis tắt | Vẫn tạo đơn (121–454 ms), vẫn chặn trùng qua MySQL | Như Luồng 1 |

## 4. Các điểm khác đề (có chủ đích)

Tất cả đều ghi trong `docs/design-decisions.md`, kèm lý do:

| Mã | Khác đề | Lý do |
|---|---|---|
| D9 | Java 21 + Spring Boot thay .NET 10 | Lead đồng ý; luồng, hợp đồng và demo không phụ thuộc ngôn ngữ |
| D10 | MySQL 8 thay Postgres/SQLite | Đề chỉ nêu ví dụ; 1 container, 3 schema, mỗi service 1 user |
| D1 | Cache chỉ ghi khi GET, **xóa** khi đơn đổi trạng thái (đề: ghi cache khi phát hành) | Nếu ghi khi phát hành thì lần xem đầu đã là HIT, không thể hiện `CACHE MISS (DB)` như đề IV.3 |
| D3 | File `payment.proto` (đề ghi cả `PaymentService.proto`) | Theo quy ước đặt tên Protobuf; tên service giữ `PaymentService` |
| D5 | Thông báo là bước mô phỏng trong Order | Đề loại SMS thật; không thêm service thứ 4 |
| D4, D20 | gRPC lỗi thì trả `200 PROCESSING_FAILED` | Request đã xử lý xong, chỉ là kết quả thất bại; giống Luồng 1 |
| **D21** | **Request trùng `partner_order_id` bị chặn bằng `409`** (đề V.1: "trả về kết quả cũ") | **Yêu cầu của lead ngày 2026-09-28:** request hợp lệ đầu tiên được giữ; trùng y hệt trả `409 DUPLICATE_ORDER` (vẫn kèm `order_id` và trạng thái đơn gốc, UI hiển thị đơn gốc); khác dữ liệu thì trả `409 DUPLICATE_ORDER_MISMATCH` |

## 5. Giới hạn đã biết và hướng production

| Giới hạn | Bằng chứng | Hướng xử lý trong production |
|---|---|---|
| RPC timeout nhưng Payment vẫn xử lý: **đã trừ tiền, đơn báo lỗi** | Lần đo 100 đơn có timeout: 51/51 đơn `PAYMENT_TIMEOUT` đều có payment, 51 dòng `late_reply_ignored` | Compensation / reconciliation (Saga): job đối soát hoàn tiền hoặc hoàn tất đơn |
| Luồng 1 chịu tải kém: 1 consumer mỗi queue RPC, request xếp hàng | 100 đơn cùng lúc: p50 từ khoảng 90 ms lên 2–4,5 s; 1 trong 3 lần đo có 51% timeout (tùy máy nặng hay nhẹ) | Tăng `concurrency`, scale instance, từ chối sớm (503); luồng chịu tải thì dùng bất đồng bộ như Luồng 2 |
| Dual write ở Payment (commit DB rồi mới publish Kafka) | README: Kafka tắt thì event mất, đơn kẹt `PAYMENT_RECORDED` | Transactional Outbox |
| Deadline 3 s chỉ bao lời gọi gRPC; ghi DB trước và sau không có giới hạn | Máy thiếu RAM: MySQL COMMIT có lần mất 9,3 s | Đủ RAM; timeout truy vấn; metrics để phát hiện |
| Quan sát chỉ bằng log | Truy vết phải grep `correlation_id` | Metrics (Prometheus), tracing (OpenTelemetry), gom log (Loki) |

## 6. Trả lời 5 câu hỏi nghiệm thu

1. **Vì sao Luồng 1 qua broker mà vẫn đồng bộ?** Thread HTTP gọi `convertSendAndReceive` và **chờ** reply (tối đa 3 s) cho từng bước. Broker chỉ là đường truyền; người gọi vẫn bị chặn. Luồng 2 trả `202` ngay sau gRPC; phát hành xảy ra khi Policy consume event, Order không chờ. Bằng chứng: `RabbitRpcOrderFlow.java`, `GrpcKafkaOrderFlow.java`, bảng tải ở mục 3.
2. **`reply_to` và `correlation_id`? Payment chậm 10 s?** `reply_to` là địa chỉ để Payment gửi trả lời (Direct Reply-to); `correlation_id` ghép reply với đúng request. Payment chậm 10 s thì Order hết chờ sau 3 s và trả `PROCESSING_FAILED` / `PAYMENT_TIMEOUT` (HTTP không treo). Reply đến muộn bị bỏ và log `late_reply_ignored`; request quá TTL 3 s trong queue vào DLQ; nếu Payment đã xử lý thì tiền đã bị trừ (giới hạn ở mục 5). Bằng chứng: `OrderRpcClient.java`, `application.yml` (reply-timeout 3000), test `paymentTimeoutFailsOrderWithinThreeAndAHalfSeconds`.
3. **Vì sao gRPC thay REST?** Hợp đồng Protobuf kiểu mạnh sinh code cho cả client và server (`contracts/payment.proto`, dùng chung qua module `contracts`), message nhị phân gọn, HTTP/2 dùng lại kết nối, deadline có sẵn cho từng lời gọi (`PaymentGrpcClient.java`, 3000 ms).
4. **Consumer sập trước khi commit offset?** Kafka gửi lại event (at-least-once, `ack-mode: record`, offset chỉ commit sau khi transaction DB xong). Policy ghi `consumer_inbox` (khóa `event_id`) và hợp đồng trong cùng transaction, bảng hợp đồng có `UNIQUE(order_id)`: lần 2 bị chặn, log `duplicate_event_ignored`. Đã thử gửi lại 3 lần: vẫn 1 hợp đồng. Bằng chứng: `policy V1__init.sql`, `PolicyIssuer.java`, `PaymentRecordedListenerIntegrationTests`.
5. **Redis ở đâu? Truy vết thế nào?** Key `idempotency:order:{partner_order_id}` (24 h) khi tạo đơn, và cache `order:{order_id}` (10 phút) khi xem đơn: GET đọc Redis trước. Redis chết thì dùng MySQL. Truy vết: mọi dòng log là JSON có `correlation_id` và `transport`; lệnh có sẵn trên UI lọc log của 3 service theo `correlation_id`. Bằng chứng: `IdempotencyStore.java`, `OrderReadCache.java`, README mục "Demo trên Web UI".

## 7. Bài học

- **Đồng bộ qua broker vẫn là đồng bộ:** Luồng 1 chịu tải kém hơn hẳn, và timeout không có nghĩa là bên kia chưa làm. Đây là lý do cần idempotency ở bên nhận và compensation.
- **"Exactly-once" thực tế là at-least-once cộng chống trùng ở bên nhận:** ràng buộc `UNIQUE` và inbox trong cùng transaction đáng tin hơn kiểm tra "đọc rồi ghi".
- **Kiểm chứng bằng số liệu thật:** nhiều kết luận ban đầu chỉ là giả thuyết. Ví dụ: đơn chậm hóa ra do MySQL trên máy thiếu RAM, không do code; lệnh PowerShell chèn BOM làm event gửi lại rơi sai partition.
- **Test đo thời gian phải đo đúng thứ cần chứng minh:** test deadline từng fail vì đo cả request HTTP thay vì chỉ lời gọi gRPC.
- **Yêu cầu có thể thay đổi:** thay đổi chặn trùng của lead (D21) được ghi lại cùng lý do và điểm khác đề, để người đọc sau vẫn hiểu vì sao hành vi khác đề.
