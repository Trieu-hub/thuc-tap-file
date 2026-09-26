# Kế hoạch triển khai: rủi ro, Docker, khung 3 microservices

> File này giải thích **hướng đi**, được viết trước khi code. Các phần có ghi chú "Đã làm" đã được đối chiếu với code thật (Ngày 1, PR #1, Ngày 2). Những phần còn lại vẫn là **phác thảo, chưa chạy thử**. Khi code thật đã chạy được, cập nhật lại file này và `CLAUDE.md` bằng số liệu thật.
>
> Các mã D1–D10 tham chiếu mục 6 "Design Decisions" trong `CLAUDE.md`. Sơ đồ luồng xem `docs/sequence-diagrams.md`.

Mục lục:

1. [Các trường hợp xấu có thể xảy ra](#1-các-trường-hợp-xấu-có-thể-xảy-ra)
2. [Setup Docker](#2-setup-docker)
3. [Khung 3 microservices](#3-khung-3-microservices)
4. [Phạm vi Ngày 1 và tiêu chí xong](#4-phạm-vi-ngày-1-và-tiêu-chí-xong)
5. [Điểm cần chốt thêm](#5-điểm-cần-chốt-thêm)

---

## 1. Các trường hợp xấu có thể xảy ra

Mỗi dòng gồm: chuyện gì xảy ra, hệ thống phải phản ứng thế nào, và cách tái hiện khi demo hoặc test. Cột **Mức** có ba giá trị:
- **Bắt buộc:** đề yêu cầu, hoặc lead chắc chắn sẽ hỏi.
- **Nên có:** tốn ít công, được điểm cộng.
- **Giới hạn:** không sửa, chỉ cần hiểu và nói ra khi demo.

### 1.1 Đầu vào và chống trùng request

| ID | Tình huống | Phản ứng mong muốn | Cách tái hiện | Mức |
|---|---|---|---|---|
| F1 | Input sai: thiếu trường, `amount ≤ 0`, sai `mode` | `400`, không tạo đơn, không đụng Redis hay broker | Gửi form thiếu trường | Bắt buộc |
| F2 | Bấm **Gửi lại Request trùng** (gửi tuần tự) | `SET NX` thất bại → trả kết quả đơn gốc, `idempotent_replay=true`, không tạo đơn mới (D4) | Nút trên UI | Bắt buộc |
| F3 | Hai request trùng đến **cùng lúc** | Chỉ một request giữ được key nhờ `SET NX`. Request kia thấy key nhưng đơn gốc có thể **chưa kịp INSERT** vào DB → trả `409 Conflict` "đơn đang được xử lý" | Gửi 2 request song song bằng script hoặc Postman Runner | Nên có |
| F4 | Order crash sau khi giữ key Redis nhưng trước khi INSERT đơn | Key "mồ côi" chặn mọi lần thử lại trong 24h. Nếu lỗi có thể bắt được (exception) thì `DEL` key trong khối `catch`. Nếu process chết hẳn thì chấp nhận | Khó tái hiện, chỉ giải thích | Giới hạn |
| F5 | Redis bị tắt | Tạo đơn: dựa vào `UNIQUE(partner_order_id)` trong MySQL làm lớp chống trùng thứ hai. Đọc đơn: đọc thẳng DB, `cache_status=CACHE_MISS_DB`. Hệ thống chậm hơn nhưng **không sập** | `docker compose stop redis` | Nên có |

### 1.2 Luồng 1: RabbitMQ RPC

| ID | Tình huống | Phản ứng mong muốn | Cách tái hiện | Mức |
|---|---|---|---|---|
| F6 | Payment Service chết | Hết 3000 ms thì `convertSendAndReceive` trả `null` → `PROCESSING_FAILED (PAYMENT_TIMEOUT)`, HTTP trả về ngay, message hết hạn và vào DLQ | `docker compose stop payment-service` | Bắt buộc |
| F7 | Payment chậm (>3s) nhưng vẫn sống | Như F6. Khi Payment trả lời muộn, Order bỏ qua và log `late_reply_ignored`. Nhờ `x-message-ttl=3000` của queue, request nằm quá hạn trong queue sẽ bị broker bỏ chứ không xử lý muộn | Biến môi trường `SIMULATED_DELAY_MS=5000` (chỉ dùng khi demo) | Bắt buộc |
| F8 | Message lỗi (JSON hỏng, handler ném exception) | Payment `nack(requeue=false)` → message vào `payment.rpc.request.dlq`. **Không** requeue vô hạn, vì một message lỗi sẽ quay vòng mãi và làm CPU chạy 100% | Publish tay một message rác qua RabbitMQ UI (cổng 15672) | Bắt buộc |
| F9 | Payment từ chối (`REJECTED`) | `PROCESSING_FAILED`, `failure_reason` = `reject_reason` của Payment (vd. `INVALID_AMOUNT`), không gọi sang Policy | Gọi Payment với số tiền không hợp lệ | Bắt buộc |
| F10 | Payment đã gạch nợ, nhưng **Policy** bị timeout | `PROCESSING_FAILED (POLICY_TIMEOUT)`. Tiền đã bị trừ mà không có hợp đồng | `docker compose stop policy-service` | Giới hạn: cần bù trừ hoặc đối soát, ngoài phạm vi |
| F11 | Payment nhận **lại cùng một request** (crash trước khi ack nên broker gửi lại) | `UNIQUE(partner_transaction_id)` chặn gạch nợ lần 2. Payment trả lại kết quả cũ (`RECORDED`) thay vì ghi thêm bản mới | Khó tái hiện, test bằng unit test | Bắt buộc (không gạch nợ 2 lần) |
| F12 | RabbitMQ broker chết | Publish lỗi ngay (`AmqpConnectException`) → `PROCESSING_FAILED (BROKER_UNAVAILABLE)` ngay lập tức, không cần chờ 3s | `docker compose stop rabbitmq` | Nên có |
| F13 | Order crash giữa lúc chờ RPC | Đơn kẹt ở `CREATED` hoặc `PAYMENT_RECORDED` | Chỉ giải thích | Giới hạn: production dùng job quét đơn treo |

### 1.3 Luồng 2: gRPC

| ID | Tình huống | Phản ứng mong muốn | Cách tái hiện | Mức |
|---|---|---|---|---|
| F14 | Payment chết | gRPC `UNAVAILABLE` → `PROCESSING_FAILED`, trả về ngay | `docker compose stop payment-service` | Bắt buộc |
| F15 | Payment chậm vượt deadline 3s | `DEADLINE_EXCEEDED` → `PROCESSING_FAILED`. **Nhưng** phía Payment có thể vẫn chạy xong, ghi thanh toán và publish `payment.recorded` → Policy phát hành hợp đồng cho một đơn đã thất bại. **Đã chốt Ngày 3 (thay bản cũ "chỉ `PAYMENT_RECORDED → ISSUED`"):** trạng thái chỉ đi tiến; `ISSUED` được phép từ `CREATED`, `PAYMENT_RECORDED` và `PROCESSING_FAILED` (hợp đồng thật sự tồn tại thì hiển thị `ISSUED`). Chuyển lùi bị bỏ qua, log `stale_transition_ignored` | `SIMULATED_DELAY_MS=5000` (chưa làm) | Bắt buộc phải hiểu. Sửa triệt để thì ngoài phạm vi (Giới hạn) |
| F16 | File `.proto` giữa Order và Payment lệch nhau | Không xảy ra được, vì cả hai dùng code sinh từ **cùng** `contracts/payment.proto` (mục 3.2) | – | Thiết kế |

### 1.4 Luồng 2: Kafka

| ID | Tình huống | Phản ứng mong muốn | Cách tái hiện | Mức |
|---|---|---|---|---|
| F17 | Policy crash sau khi ghi DB nhưng **trước khi commit offset** | Kafka gửi lại event → vi phạm `UNIQUE` → log `duplicate_event_ignored` → commit offset. Không có hợp đồng thứ 2 | Endpoint debug phát lại event (D8) | Bắt buộc (câu nghiệm thu 4) |
| F18 | Event trùng nhưng **`event_id` mới**, cùng `order_id` | `UNIQUE(order_id)` trên bảng `policies` chặn lại | Replay kèm `event_id` mới | Bắt buộc |
| F19 | Event lỗi, không parse được | Nếu không xử lý, consumer thử lại mãi và **kẹt cả partition**, các đơn sau không được xử lý. `DefaultErrorHandler` của Spring Kafka thử lại có giới hạn rồi log và bỏ qua. **Đã làm Ngày 3:** retry 3 lần cách 1 s rồi `DeadLetterPublishingRecoverer` → `<topic>.DLT`; JSON không đọc được (`JacksonException`) vào DLT ngay | Produce tay một message rác | Bắt buộc phải hiểu, cấu hình tối thiểu |
| F20 | Kafka chết đúng lúc Payment publish | Payment đã lưu DB, publish lỗi → **mất event**, đơn kẹt ở `PAYMENT_RECORDED` (dual write) | `docker compose stop kafka` rồi tạo đơn Luồng 2 | Giới hạn: production dùng Transactional Outbox |
| F21 | Các event của cùng một đơn đến sai thứ tự | Không xảy ra được, vì message key là `order_id` nên mọi event của một đơn nằm cùng partition (D8) | – | Thiết kế |
| F22 | Order nhận `policy.issued` của đơn không tồn tại | **Đã chốt Ngày 3:** ném `OrderNotFoundException` → rollback (kể cả inbox) → retry 3 lần → `policy.issued.DLT`. Không âm thầm ack | Produce `policy.issued` với `order_id` giả | Đã làm, có test |
| F23 | Service khởi động khi topic chưa tồn tại | Mỗi service tự khai báo bean `NewTopic` để Spring tạo topic lúc khởi động (idempotent). Tắt auto-create topic của broker để tránh tạo nhầm tên | Khởi động lại từ đầu | Bắt buộc |

### 1.5 Hạ tầng và môi trường (Windows + Docker Desktop)

| ID | Tình huống | Phản ứng hoặc phòng ngừa | Mức |
|---|---|---|---|
| F24 | Service khởi động trước broker hoặc DB, crash ngay | `healthcheck` cho mọi container hạ tầng + `depends_on: condition: service_healthy`. Kafka và MySQL khởi động chậm nhất | Bắt buộc |
| F25 | Kafka **advertised listeners** sai: chạy trong Docker thì được, từ máy host thì không, hoặc ngược lại | Hai listener: `PLAINTEXT://kafka:9092` cho các container, `EXTERNAL://localhost:9094` cho máy host (mục 2.3). Đây là lỗi Kafka phổ biến nhất | Bắt buộc |
| F26 | Trùng cổng với phần mềm đã cài trên máy (MySQL 3306, Redis 6379…) | Map MySQL ra cổng host `3307`. Nếu `docker compose up` báo "port is already allocated" thì đổi cổng host | Bắt buộc |
| F27 | Thiếu RAM: 3 JVM + Kafka + MySQL + RabbitMQ khoảng 3–4 GB | Giới hạn `mem_limit` và `-XX:MaxRAMPercentage=75` cho từng JVM. Cấp cho Docker Desktop ≥ 6 GB | Nên có |
| F28 | File `.sh` bị lưu với dòng kết thúc CRLF (Windows) → container báo `exec format error` hoặc `\r: not found` | File `.gitattributes`: `*.sh text eol=lf` | Bắt buộc |
| F29 | Lần build đầu rất lâu vì Maven tải dependency | Dockerfile multi-stage với BuildKit cache mount cho `~/.m2`, để lần build sau không phải tải lại | Nên có |

### 1.6 Observability và UI

| ID | Tình huống | Phản ứng mong muốn | Mức |
|---|---|---|---|
| F30 | **Mất `correlation_id` trong log** khi đổi luồng xử lý. MDC gắn với thread, còn listener RabbitMQ/Kafka và gRPC chạy trên thread khác | Mỗi điểm vào (HTTP filter, RabbitMQ listener, Kafka listener, gRPC interceptor) phải `MDC.put` từ message và `MDC.clear()` trong `finally` | Bắt buộc |
| F31 | UI polling không bao giờ dừng (event bị mất, F20) | Polling có giới hạn, ví dụ tối đa 30 giây, hết thì hiện "Chưa nhận được kết quả, xem log theo correlation_id" | Nên có |
| F32 | Trình duyệt chặn CORS vì UI và API khác cổng | Chạy UI bằng nginx và proxy `/api` sang `order-service:8080`, để trình duyệt thấy UI và API cùng nguồn | Bắt buộc |

---

## 2. Setup Docker

### 2.1 Danh sách container

| Container | Image (chốt phiên bản khi dựng) | Cổng host → container | Vai trò | Healthcheck |
|---|---|---|---|---|
| `mysql` | `mysql:8.x` | `3307 → 3306` | 3 schema `order_db`, `payment_db`, `policy_db` | `mysqladmin ping` |
| `redis` | `redis:7-alpine` | `6379 → 6379` | Idempotency + cache | `redis-cli ping` |
| `rabbitmq` | `rabbitmq:<ver>-management` | `5672`, `15672` (UI) | RPC queues + DLQ | `rabbitmq-diagnostics -q ping` |
| `kafka` | `apache/kafka` (KRaft, **không** Zookeeper) | `9094` (cho host) | Topics `payment.recorded`, `policy.issued` | `kafka-broker-api-versions.sh` |
| `kafka-ui` *(tùy chọn)* | `provectuslabs/kafka-ui` | `8090` | Xem topic và message khi demo | – |
| `order-service` | build từ repo | `8080` | REST API cho UI | `/actuator/health` |
| `payment-service` | build từ repo | `8081` (HTTP), `9090` (gRPC) | gRPC server + RPC consumer | `/actuator/health` |
| `policy-service` | build từ repo | `8082` | RPC consumer + Kafka consumer | `/actuator/health` |
| `ui` | `nginx:alpine` + file tĩnh | `3000 → 80` | Web UI + proxy `/api` | – |

Hai trang quản trị dùng khi demo:
- RabbitMQ UI tại `localhost:15672`: xem DLQ, publish message rác (F8).
- Kafka UI tại `localhost:8090`: xem event và envelope.

### 2.2 Thứ tự khởi động

```text
mysql, redis, rabbitmq, kafka   (chạy song song, chờ healthy)
        │
        ├── payment-service  (cần mysql, rabbitmq, kafka)
        ├── policy-service   (cần mysql, rabbitmq, kafka)
        └── order-service    (cần mysql, redis, rabbitmq, kafka, payment-service)
                │
                └── ui (nginx)
```

### 2.3 Phác thảo `docker-compose.yml` (chưa chạy thử)

```yaml
services:
  mysql:
    image: mysql:8.4
    environment:
      MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD}
    command: --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci
    ports: ["3307:3306"]
    volumes:
      - ./docker/mysql/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-uroot", "-p${MYSQL_ROOT_PASSWORD}"]
      interval: 5s
      retries: 20

  kafka:
    image: apache/kafka:<ver>
    ports: ["9094:9094"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"   # F23
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 10s
      retries: 20

  # redis, rabbitmq: tương tự, mỗi container kèm healthcheck

  payment-service:
    build: { context: ., dockerfile: docker/service.Dockerfile, args: { SERVICE: payment-service } }
    ports: ["8081:8081", "9090:9090"]
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/payment_db
      SPRING_RABBITMQ_HOST: rabbitmq
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      JAVA_TOOL_OPTIONS: -XX:MaxRAMPercentage=75
    mem_limit: 512m
    depends_on:
      mysql:    { condition: service_healthy }
      rabbitmq: { condition: service_healthy }
      kafka:    { condition: service_healthy }
    healthcheck:
      test: ["CMD-SHELL", "curl -fs localhost:8081/actuator/health || exit 1"]  # kiểm tra image có curl chưa

  # order-service, policy-service: cùng khuôn mẫu
  # ui: nginx:alpine, mount ui/ + nginx.conf (proxy /api -> order-service:8080)
```

Một số điểm cần lưu ý:
- **Mật khẩu** để trong file `.env`. Đây là sandbox demo, nhưng vẫn không nên hard-code trong compose.
- **`init.sql`** tạo 3 database và 3 user. Mỗi user chỉ có quyền trên database của service mình, nên nếu service này lỡ truy cập database của service khác sẽ bị lỗi quyền ngay.
- **Không cần container khởi tạo topic.** Topic được tạo bằng bean `NewTopic` trong code (F23).

### 2.4 Dockerfile (một file dùng chung: `docker/service.Dockerfile`)

> **Đã làm (Ngày 1), khác bản phác thảo ban đầu ở hai điểm:**
> - Dùng **một** Dockerfile có tham số `SERVICE` thay vì 3 file riêng, để khi thay đổi không phải sửa 3 chỗ.
> - Bỏ `dependency:go-offline`, vì trong reactor nhiều module lệnh này không resolve được module `contracts` chưa được install. Thay bằng BuildKit cache mount cho `~/.m2` (F29).

```dockerfile
FROM maven:3.9-eclipse-temurin-21 AS build
ARG SERVICE
WORKDIR /src
COPY pom.xml .
COPY contracts contracts
COPY order-service/pom.xml order-service/
COPY payment-service/pom.xml payment-service/
COPY policy-service/pom.xml policy-service/
COPY ${SERVICE}/src ${SERVICE}/src
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    mvn -B -q -pl ${SERVICE} -am package -DskipTests

FROM eclipse-temurin:21-jre
ARG SERVICE
WORKDIR /app
COPY --from=build /src/${SERVICE}/target/${SERVICE}.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
```

---

## 3. Khung 3 microservices

### 3.1 Cây thư mục

```text
thuc-tap-file/
├── pom.xml                         # Maven aggregator: modules = contracts, order/payment/policy-service
├── docker-compose.yml
├── .env                            # mật khẩu demo
├── .gitattributes                  # *.sh eol=lf (F28)
├── .gitignore                      # target/, .idea/, *.iml, .env.local
├── README.md
├── CLAUDE.md
├── docs/
│   ├── sequence-diagrams.md
│   └── implementation-plan.md      # file này
├── docker/
│   └── mysql/init.sql              # tạo 3 schema + 3 user
├── contracts/                      # vừa là thư mục hợp đồng, vừa là Maven module
│   ├── pom.xml                     # protobuf-maven-plugin: sinh Java stub từ payment.proto
│   ├── payment.proto
│   └── schemas/
│       ├── event-envelope.schema.json
│       ├── payment-recorded.schema.json
│       ├── policy-issued.schema.json
│       ├── payment-rpc-request.schema.json / payment-rpc-reply.schema.json
│       └── policy-rpc-request.schema.json  / policy-rpc-reply.schema.json
├── order-service/
├── payment-service/
├── policy-service/
└── ui/
    ├── index.html, app.js, style.css   # HTML/JS thuần, không cần bước build
    └── nginx.conf
```

### 3.2 Hai quyết định cấu trúc

1. **`contracts/` là một Maven module riêng.** Order (gRPC client) và Payment (gRPC server) cùng phụ thuộc vào module này, nên cả hai dùng code sinh từ **cùng một** `payment.proto`. Hai bên không thể lệch hợp đồng (F16), và file `.proto` vẫn nằm ở `contracts/` đúng như đề yêu cầu.
2. **Không có module `common`.** Các class nhỏ như `EventEnvelope` hay bộ lọc `correlation_id` được **chép riêng vào từng service**.
   - Lý do: microservice không nên dính nhau qua thư viện dùng chung. Hợp đồng giữa các service là JSON Schema và `.proto`, không phải class Java dùng chung.
   - Đánh đổi: có vài chục dòng code bị lặp. Chấp nhận được, và dễ giải thích khi lead hỏi.

### 3.3 Nguyên tắc bên trong mỗi service: transport mỏng, nghiệp vụ dùng chung

Payment và Policy đều nhận yêu cầu qua **hai đường**: RabbitMQ ở Luồng 1, và gRPC hoặc Kafka ở Luồng 2. Nếu viết logic nghiệp vụ hai lần thì sẽ lệch nhau. Vì vậy:

```text
             ┌── PaymentRpcListener (RabbitMQ) ──┐
Luồng 1/2 →  │                                   ├──► PaymentService.record(...)  ──► PaymentRepository (MySQL)
             └── PaymentGrpcService (gRPC) ──────┘         (validate, UNIQUE txn, trả kết quả)
```

Lớp adapter transport chỉ làm bốn việc: đọc message, đặt `correlation_id` vào MDC, gọi service, rồi trả kết quả. **Toàn bộ nghiệp vụ nằm trong một class duy nhất.** Khi lead hỏi "hai luồng khác nhau ở đâu?", câu trả lời rất rõ: chỉ khác ở lớp transport.

### 3.4 Từng service

> **Đã làm (PR #1 và Ngày 2), khác bản phác thảo bên dưới:**
> - Persistence dùng `JdbcTemplate`, không dùng JPA entity/repository.
>   - Order: `order/OrderRepository`, `order/OrderProgressService`.
>   - Payment: `payment/PaymentRecorder`.
>   - Policy: `policy/PolicyIssuer`.
> - Order có các gói `api/` (`OrderController`, `CreateOrderRequest`, `OrderResponse`, `ApiExceptionHandler`), `flow/RabbitRpcOrderFlow`, `rpc/` (`RabbitConfig`: converter + log `late_reply_ignored`; `OrderRpcClient`), `notification/NotificationWorker`.
>   - Order **không khai báo queue**, vì bên consumer sở hữu queue.
>   - Chưa có `CorrelationIdFilter`: `correlation_id` được đưa vào MDC ngay trong flow.
> - Payment và Policy: gói `rpc/` gồm `RabbitConfig` (queue durable, `x-message-ttl=3000`, `sandbox.dlx` → `<queue>.dlq`), `*RpcListener` và các record message.
> - Log JSON dùng structured logging có sẵn của Boot (`SandboxJsonLogFormatter`), không dùng `logback-spring.xml`.

Package gốc dự kiến: `com.sandbox.<service>`.

**order-service** (cổng 8080)

```text
order-service/src/main/java/com/sandbox/order/
├── OrderServiceApplication.java
├── api/            OrderController, CreateOrderRequest, OrderResponse, TimelineStepDto, GlobalExceptionHandler
├── domain/         Order, OrderStatus (CREATED, PAYMENT_RECORDED, ISSUED, PROCESSING_FAILED), TimelineStep
├── repository/     OrderRepository, TimelineStepRepository, ConsumerInboxRepository
├── flow/           RabbitRpcOrderFlow (Luồng 1), GrpcKafkaOrderFlow (Luồng 2)   ← điều phối
├── rpc/            RabbitConfig (queue, DLX, template replyTimeout=3000)
├── grpc/           PaymentGrpcClient (deadline 3000 ms, gắn correlation_id vào metadata)
├── kafka/          KafkaConfig, NewTopic beans, PolicyIssuedListener
├── cache/          IdempotencyService (SET NX), OrderCacheService (cache-aside, DEL)
├── notification/   NotificationWorker (mô phỏng SMS)
└── observability/  CorrelationIdFilter (HTTP → MDC)
resources/  application.yml, logback-spring.xml (JSON), db/migration/V1__init.sql
```

**payment-service** (HTTP 8081, gRPC 9090)

```text
├── PaymentServiceApplication.java
├── service/        PaymentService          ← nghiệp vụ duy nhất (3.3)
├── domain/         Payment, PaymentStatus (RECORDED, REJECTED)
├── repository/     PaymentRepository
├── rpc/            RabbitConfig, PaymentRpcListener         (Luồng 1)
├── grpc/           PaymentGrpcService, CorrelationIdServerInterceptor   (Luồng 2)
├── kafka/          PaymentRecordedPublisher, NewTopic bean
├── debug/          ReplayController (chỉ để demo F17/F18, D8)
└── observability/  (MDC helper)
```

**policy-service** (cổng 8082)

```text
├── PolicyServiceApplication.java
├── service/        PolicyIssuanceService   ← nghiệp vụ duy nhất, @Transactional inbox + policy
├── domain/         Policy, ConsumerInbox
├── repository/     PolicyRepository, ConsumerInboxRepository
├── rpc/            RabbitConfig, PolicyRpcListener          (Luồng 1)
├── kafka/          PaymentRecordedListener, PolicyIssuedPublisher, NewTopic beans   (Luồng 2)
└── observability/
```

### 3.5 Bảng dữ liệu (dự kiến, dạng migration Flyway)

| Schema | Bảng | Cột chính | Ràng buộc quan trọng |
|---|---|---|---|
| `order_db` | `orders` | `order_id` PK, `partner_order_id`, `customer_name`, `phone`, `amount`, `mode`, `status`, `failure_reason`, `correlation_id`, `partner_transaction_id`, `policy_number`, `created_at`, `updated_at` | `UNIQUE(partner_order_id)`, lớp chống trùng thứ hai (F5) |
| `order_db` | `order_timeline` | `order_id`, `seq`, `step`, `service`, `transport`, `status`, `duration_ms`, `detail` | – |
| `order_db` | `consumer_inbox` | `event_id` PK, `processed_at` | PK chặn trùng `policy.issued` |
| `payment_db` | `payments` | `payment_id` PK, `order_id`, `partner_transaction_id`, `amount`, `status`, `created_at` | `UNIQUE(partner_transaction_id)` (F11) |
| `policy_db` | `policies` | `policy_id` PK, `order_id`, `policy_number`, `issued_at` | `UNIQUE(order_id)` (F18), `UNIQUE(policy_number)` |
| `policy_db` | `consumer_inbox` | `event_id` PK, `processed_at` | PK chặn trùng `payment.recorded` |

`amount` là số nguyên VND: `BIGINT` trong MySQL, `long` trong Java, `int64` trong proto (D18). VND không có đơn vị lẻ nên không cần `DECIMAL`. **Không dùng `double` cho tiền.**

---

## 4. Phạm vi Ngày 1 và tiêu chí xong

Ngày 1 **chỉ dựng khung**, chưa có nghiệp vụ:

- [ ] `pom.xml` gốc với 4 module (contracts + 3 service); `mvn package` build thành công
- [ ] `contracts/payment.proto` có bản đầu và sinh được Java stub
- [ ] Mỗi service: class `Application`, `application.yml`, Actuator, log JSON ra stdout, migration Flyway `V1__init.sql` tạo bảng
- [ ] `docker/mysql/init.sql`, `.env`, `.gitattributes`, `.gitignore`
- [ ] `docker compose up --build`: **mọi container `healthy`** (`docker compose ps`)
- [ ] Mở được RabbitMQ UI (15672) và thấy bảng đã được tạo trong 3 schema MySQL
- [ ] `docs/sequence-diagrams.md` (đã xong)

Chưa làm trong Ngày 1: queue và topic thật, RPC, gRPC, Kafka listener, Redis, UI. Các phần này thuộc Ngày 2–4 theo mục 16 của `CLAUDE.md`.

---

## 5. Điểm cần chốt thêm

Các điểm dưới đây xuất hiện khi phân tích rủi ro, nhưng **chưa có trong `CLAUDE.md`**. Nếu bạn đồng ý, chúng sẽ được ghi thành decision:

| # | Đề xuất | Liên quan |
|---|---|---|
| P1 | Hai request trùng đồng thời, bản gốc chưa kịp INSERT → trả `409 Conflict` | F3 |
| P2 | Redis chết → không sập hệ thống; dùng `UNIQUE(partner_order_id)` trong DB làm lớp chống trùng thứ hai | F5 |
| P3 | Payment nhận lại cùng `partner_transaction_id` → **trả kết quả cũ** (`RECORDED`), không trả `REJECTED`. Vẫn không gạch nợ lần 2, nhưng người gọi thử lại sẽ nhận đúng kết quả. `REJECTED` chỉ dùng cho số tiền sai | F11, điều chỉnh D4 |
| P4 | ~~Order chỉ cho phép `PAYMENT_RECORDED → ISSUED`~~ Thay ở Ngày 3: trạng thái chỉ đi tiến (xem F15), log `stale_transition_ignored` | F15 |
| P5 | UI polling dừng sau tối đa 30 giây | F31 |
| P6 | UI là HTML/JS thuần chạy bằng nginx, proxy `/api` | F32 |
| P7 | Cổng: 8080/8081/8082, gRPC 9090, MySQL 3307, UI 3000, Kafka host 9094 | Mục 2.1 |
| P8 | `contracts/` là Maven module; không có module `common` | Mục 3.2 |
