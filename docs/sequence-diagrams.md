# Sơ đồ Sequence mở rộng

Hai sơ đồ dưới đây mở rộng sơ đồ trong `intern-messaging-grpc-kafka-assignment.md`, bổ sung các nhánh lỗi, cơ chế chống trùng và hành vi cache. Các mã D1–D10 tham chiếu mục 6 "Design Decisions" trong `CLAUDE.md`.

Khối `break` nghĩa là luồng **dừng tại đó** và trả kết quả về ngay.

## Luồng 1: RabbitMQ RPC

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant UI as Web UI
    participant Order as Order Service
    participant Redis
    participant ODB as MySQL (order_db)
    participant Rabbit as RabbitMQ
    participant Pay as Payment Service
    participant Pol as Policy Service

    User->>UI: Nhập form, chọn Luồng 1 (RabbitMQ RPC)
    UI->>Order: POST /api/v1/orders (mode=RABBITMQ_RPC)
    Order->>Order: Validate schema, sinh order_id, correlation_id, partner_transaction_id=TXN-{partner_order_id}

    break Input không hợp lệ (thiếu trường, amount ≤ 0)
        Order-->>UI: 400 Bad Request, không tạo đơn
    end

    Note over Order,Redis: Ngày 2 chưa có Redis: tra partner_order_id trong order_db; hai request đồng thời thì request thua INSERT (UNIQUE) nhận 409 (D11, D12)
    Order->>Redis: SET idempotency:order:{partner_order_id} {order_id} NX EX 86400
    break Key đã tồn tại (request trùng)
        Redis-->>Order: nil, GET key lấy order_id gốc
        Order->>ODB: Đọc đơn gốc
        Order-->>UI: 200 OK, kết quả đơn gốc, idempotent_replay=true
    end
    Redis-->>Order: OK (giữ key thành công)
    Order->>ODB: INSERT order (CREATED), timeline [Tạo đơn]

    Note over Order,Pay: Bước 1 - RPC sang Payment (Order chờ đồng bộ, tối đa 3000 ms)
    Order->>Rabbit: publish payment.rpc.request<br/>reply_to=amq.rabbitmq.reply-to + AMQP correlation_id (RabbitTemplate tự đặt)<br/>header x-correlation-id (correlation_id nghiệp vụ)

    break Timeout 3000 ms (Payment chết, chậm, hoặc message lỗi)
        Order->>Order: convertSendAndReceive trả null → PAYMENT_TIMEOUT, log WARN rpc_timeout
        Order->>ODB: UPDATE PROCESSING_FAILED, timeline [PROCESSING_FAILED] TIMEOUT
        Order->>Redis: DEL order:{order_id}
        Order-->>UI: 200 OK, status=PROCESSING_FAILED (HTTP không bị treo)
        Rabbit->>Rabbit: Message quá x-message-ttl=3000 của queue → sandbox.dlx → payment.rpc.request.dlq
        Note over Order,Rabbit: Nếu Payment trả lời muộn → reply bị bỏ qua, log late_reply_ignored
    end

    Rabbit->>Pay: deliver payment.rpc.request
    Pay->>Pay: Kiểm tra amount, INSERT payment (UNIQUE partner_transaction_id)
    Note right of Pay: Message lỗi (parse/exception) → nack(requeue=false) → DLQ, Order rơi vào nhánh Timeout
    Pay->>Rabbit: reply tới reply_to (cùng correlation_id), status=RECORDED hoặc REJECTED
    Rabbit-->>Order: reply khớp correlation_id

    break status=REJECTED (sai số tiền, hoặc cùng partner_transaction_id nhưng khác nội dung)
        Order->>ODB: UPDATE PROCESSING_FAILED (failure_reason = reject_reason, vd. INVALID_AMOUNT), không gọi Policy
        Order-->>UI: 200 OK, status=PROCESSING_FAILED
    end
    Order->>ODB: UPDATE PAYMENT_RECORDED, timeline [Thanh toán] via RabbitMQ

    Note over Order,Pol: Bước 2 - RPC sang Policy (Order chờ đồng bộ, tối đa 3000 ms)
    Order->>Rabbit: publish policy.rpc.request<br/>reply_to, AMQP correlation_id, header x-correlation-id

    break Timeout 3000 ms
        Order->>ODB: UPDATE PROCESSING_FAILED (POLICY_TIMEOUT)
        Order->>Redis: DEL order:{order_id}
        Order-->>UI: 200 OK, status=PROCESSING_FAILED
        Note over Order,Pay: Giới hạn đã biết - Payment đã gạch nợ nhưng đơn thất bại (cần bù trừ / đối soát, ngoài phạm vi)
    end

    Rabbit->>Pol: deliver policy.rpc.request
    Pol->>Pol: INSERT policy (UNIQUE order_id), sinh policy_id, policy_number ACBI-2026-xxxx
    Note right of Pol: Đã có policy cho order_id → trả lại policy cũ, không tạo mới
    Pol->>Rabbit: reply tới reply_to, status=ISSUED, policy_number
    Rabbit-->>Order: reply khớp correlation_id

    Order->>ODB: UPDATE ISSUED, timeline [POLICY_ISSUANCE]
    Order->>Order: Notification Worker - gửi SMS mô phỏng, timeline [NOTIFICATION]
    Order->>Redis: DEL order:{order_id} (invalidate)
    Order-->>UI: 200 OK {order_id, status=ISSUED, correlation_id, timeline}

    Note over UI,ODB: Xem chi tiết đơn - cache-aside (D1)
    UI->>Order: GET /api/v1/orders/{order_id}
    Order->>Redis: GET order:{order_id}
    alt Lần tải đầu - MISS
        Redis-->>Order: nil
        Order->>ODB: SELECT order + timeline
        Order->>Redis: SET order:{order_id} EX 600
        Order-->>UI: 200 OK, cache_status=CACHE_MISS_DB
    else Refresh - HIT
        Redis-->>Order: dữ liệu đơn
        Order-->>UI: 200 OK, cache_status=CACHE_HIT_REDIS
    end
```

## Luồng 2: gRPC + Kafka

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant UI as Web UI
    participant Order as Order Service
    participant Redis
    participant ODB as MySQL (order_db)
    participant Pay as Payment Service
    participant Kafka
    participant Pol as Policy Service

    User->>UI: Nhập form, chọn Luồng 2 (gRPC + Kafka)
    UI->>Order: POST /api/v1/orders (mode=GRPC_KAFKA)
    Order->>Order: Validate schema, sinh order_id, correlation_id, partner_transaction_id
    Order->>Redis: SET idempotency:order:{partner_order_id} {order_id} NX EX 86400
    Note right of Order: Input sai → 400, request trùng → 200 idempotent_replay (giống hệt Luồng 1)
    Order->>ODB: INSERT order (CREATED), timeline [Tạo đơn]

    Note over Order,Pay: Bước 1 - gRPC đồng bộ (cần kết quả thanh toán ngay)
    Order->>Pay: gRPC RecordPayment(RecordPaymentRequest)<br/>metadata x-correlation-id, deadline 3000 ms
    Pay->>Pay: Kiểm tra amount, INSERT payment (UNIQUE partner_transaction_id)

    break DEADLINE_EXCEEDED, UNAVAILABLE hoặc status=REJECTED
        Pay-->>Order: gRPC error hoặc RecordPaymentResponse(status=REJECTED)
        Order->>ODB: UPDATE PROCESSING_FAILED + failure_reason
        Order-->>UI: 200 OK, status=PROCESSING_FAILED
    end

    Pay-->>Order: RecordPaymentResponse(status=RECORDED)
    Order->>ODB: UPDATE PAYMENT_RECORDED, timeline [Thanh toán] via gRPC
    Order-->>UI: 202 Accepted {order_id, status=PAYMENT_RECORDED, correlation_id}

    Note over UI,Pol: Bước 2 - HTTP đã trả về. Xử lý Kafka và UI polling chạy song song

    par Xử lý bất đồng bộ qua Kafka
        Pay-)Kafka: publish payment.recorded (key=order_id)<br/>envelope: event_id, event_type, correlation_id, occurred_at, payload
        Note over Pay,Kafka: Giới hạn đã biết - dual write (lưu DB rồi mới publish). Publish lỗi thì mất event. Production dùng Transactional Outbox

        Kafka-)Pol: deliver payment.recorded (group policy-service)
        Pol->>Pol: @Transactional INSERT consumer_inbox(event_id) + INSERT policy (UNIQUE order_id)
        alt Event mới
            Pol-)Kafka: publish policy.issued (key=order_id, cùng correlation_id, event_id tất định từ policy_id)
            Pol->>Kafka: commit offset
        else Trùng (redelivery hoặc replay) - vi phạm UNIQUE
            Pol->>Pol: log duplicate_event_ignored, không tạo hợp đồng thứ 2
            Pol-)Kafka: publish LẠI policy.issued của hợp đồng đã có (cùng event_id → Order bỏ qua nếu đã nhận)
            Pol->>Kafka: commit offset
        end
        Note over Kafka,Pol: Crash sau khi ghi DB, trước khi commit offset → Kafka gửi lại (at-least-once) → rơi vào nhánh Trùng. Publish lỗi → ném exception → retry 3 lần → payment.recorded.DLT

        Kafka-)Order: deliver policy.issued (group order-service)
        Order->>ODB: @Transactional INSERT consumer_inbox(event_id) + UPDATE ISSUED, timeline [Phát hành e-GCN] via Kafka
        Note right of Order: event_id trùng → log duplicate_event_ignored, bỏ qua. order_id không tồn tại → rollback, retry 3 lần → policy.issued.DLT
        Order->>Order: Notification Worker - gửi SMS mô phỏng, timeline [Thông báo]
        Order->>Redis: DEL order:{order_id} (invalidate)
        Order->>Kafka: commit offset
    and UI polling mỗi 1s đến khi ISSUED hoặc PROCESSING_FAILED
        loop Polling
            UI->>Order: GET /api/v1/orders/{order_id}
            Order->>Redis: GET order:{order_id}
            alt MISS (lần đầu, hoặc ngay sau khi bị DEL)
                Order->>ODB: SELECT order + timeline
                Order->>Redis: SET order:{order_id} EX 600
                Order-->>UI: status hiện tại, cache_status=CACHE_MISS_DB
            else HIT
                Redis-->>Order: dữ liệu đơn
                Order-->>UI: status hiện tại, cache_status=CACHE_HIT_REDIS
            end
        end
    end
```
