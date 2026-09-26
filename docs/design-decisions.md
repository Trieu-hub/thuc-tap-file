# Design Decisions (D1–D20)

> Decisions taken where the assignment (`intern-messaging-grpc-kafka-assignment.md`) is open or contradicts itself, chosen for demo clarity and explainability. Kept in sync with the code; the behaviour-changing ones are also summarised in the README. F-numbers refer to [`implementation-plan.md`](implementation-plan.md).


- **D1: Read cache is cache-aside with invalidate-on-write.**
  - *Problem:* the spec says the first load shows `CACHE MISS (DB)` and a refresh shows `CACHE HIT (REDIS)`. But it also has Order *write* `order:{order_id}` right after issuing (Flow 1) or on `policy.issued` (Flow 2). If Order writes on update, the first GET is always a HIT and MISS never appears.
  - *Decision:* only `GET /api/v1/orders/{id}` populates `order:{order_id}` (TTL 10 min), and only on a MISS. Every order state change **deletes** the key instead of writing it. Result: the first GET is a MISS (DB), a refresh is a HIT (REDIS), and after `policy.issued` the next poll is a MISS again and returns fresh data.
  - *Day 4 refinement:* the key is only written for a **finished** order: `ISSUED` with its `NOTIFICATION` step. `PROCESSING_FAILED` is not final (it can become `ISSUED`, D13), and the notification is its own transaction after `ISSUED`; a GET racing either change could otherwise keep a stale copy for 10 minutes. GET still asks Redis first in every case. The key is deleted after the changing transaction commits (`OrderChangedEvent` + `@TransactionalEventListener`).
  - This also gives the demo a cache-invalidation talking point. The response carries `cache_status: "CACHE_MISS_DB" | "CACHE_HIT_REDIS"`, which the UI renders as `CACHE MISS (DB)` / `CACHE HIT (REDIS)`. Never label a DB read as a Redis hit.

- **D2: Order Service generates `partner_transaction_id`.**
  - The UI form has no such field, but Payment must dedupe on it.
  - Order derives it deterministically as `TXN-{partner_order_id}`, so a repeated order always maps to the same transaction and the Payment dedupe works as a second line of defense behind the Redis idempotency key.
  - Demo: call Payment directly (gRPC or RPC) with a repeated `partner_transaction_id`, or delete the Redis idempotency key and resubmit. Payment returns the original result with `duplicate=true` and does not charge twice (D4).

- **D3: The proto file is `contracts/payment.proto`, with `service PaymentService { rpc RecordPayment(RecordPaymentRequest) returns (RecordPaymentResponse); }`.**
  - The spec uses both `PaymentService.proto` and `payment.proto`. This follows Protobuf's lower_snake_case file naming and keeps the service name from the spec.

- **D4: Order statuses and the duplicate-request response.**
  - Order statuses: `CREATED` → `PAYMENT_RECORDED` → `ISSUED`, or `PROCESSING_FAILED` at any step, with a `failure_reason` and a `PROCESSING_FAILED` timeline step. Flow 1 reasons: `PAYMENT_TIMEOUT`, `POLICY_TIMEOUT`, `BROKER_UNAVAILABLE`, or Payment's own `reject_reason` (e.g. `INVALID_AMOUNT`, `IDEMPOTENCY_KEY_MISMATCH`) so the cause stays visible. Flow 2 reasons (Day 3): `PAYMENT_TIMEOUT` (`DEADLINE_EXCEEDED`), `PAYMENT_SERVICE_UNAVAILABLE` (`UNAVAILABLE`), `PAYMENT_GRPC_ERROR` (any other gRPC code), or Payment's `reject_reason`; the original gRPC code goes into the timeline `detail` and the log field `grpc_status`.
  - Payment and Policy reply statuses are `RECORDED` / `REJECTED` and `ISSUED`.
  - `REJECTED` is only for invalid payments, such as a bad amount. A **repeated `partner_transaction_id` is not rejected.** Payment returns the **existing** result (`RECORDED`, same `payment_id`) with `duplicate=true`, logs `duplicate_payment_ignored`, and never records a second payment. A caller that retries after a redelivery or a lost reply then gets the correct answer.
  - Input that fails schema validation (missing fields, amount ≤ 0) returns `400` and creates no order.
  - A duplicate `POST` with the same `partner_order_id` returns **`200 OK` with the original order's current state** plus `"idempotent_replay": true`, and never creates a new order. The UI shows a visible "duplicate, original result returned" badge.
  - If the duplicate arrives while the original is still being created (the key exists but the order row doesn't yet), the response is `409 Conflict` (D11).

- **D5: Notification is a simulated in-process step in Order Service (a "Notification Worker" component).**
  - It runs once the order reaches `ISSUED`, in both flows.
  - It logs and appends the timeline step `[Thông báo] → Notification Worker → Success (SMS Sent – simulated)`.
  - There is no fourth service, no real SMS or email, and no extra infrastructure.

- **D6: List endpoint.** `GET /api/v1/orders` reads from the database (it is not cached) and returns the newest orders first.

- **D7: RabbitMQ RPC robustness.**
  - **Late replies:** a reply whose `correlation_id` is no longer pending (it arrived after the timeout) is discarded and logged as `late_reply_ignored`. `RabbitTemplate` already discards such replies internally. Verify that behavior, and make sure the log line is emitted (for example with a custom log or listener), not merely assumed.
  - **Spring AMQP gotcha:** with `replyTimeout = 3000`, `RabbitTemplate.convertSendAndReceive` **returns `null` on timeout; it does not throw.** Treat `null` explicitly as a timeout (`failure_reason = PAYMENT_TIMEOUT` / `POLICY_TIMEOUT`, WARN log `action=rpc_timeout`). `AsyncRabbitTemplate` throws instead. Pick one style and use it consistently. Implemented with `spring.rabbitmq.template.reply-timeout: 3000` and Direct Reply-to (RabbitTemplate's default, no temporary reply queue).
  - **Expiration:** each request queue is declared by its **consumer** with `x-message-ttl = 3000`, so a request that sat in the queue past the caller's timeout is dead-lettered instead of being processed late. Per-message `expiration` is not set (same effect, one place to configure). Changing queue arguments later requires deleting the queue, because RabbitMQ rejects a redeclare with different arguments.
  - **Two correlation ids, never merged:** the AMQP `correlation_id` property is owned by RabbitTemplate (it matches replies to requests). The business `correlation_id` travels in header `x-correlation-id`; listeners put it in the MDC and clear it in `finally`, and echo it on the reply so late replies can be traced.
  - **Late-reply logging is implemented** with `RabbitTemplate.setReplyErrorHandler` (a `RabbitTemplateCustomizer` in order-service `rpc/RabbitConfig`). Spring AMQP also logs its own WARN/ERROR for the same event.
  - **Known limitation, state it openly in the demo:** if Payment already processed a request when Order timed out, the payment is recorded while the order is `PROCESSING_FAILED`. Production systems would fix this with compensation or reconciliation, which is out of scope.
  - **DLQ (the spec's Day 5 test scenario, required):** each RPC request queue has a dead-letter exchange routing to `<queue>.dlq`. A message that cannot be processed (poison/unparseable, or it throws) is `nack`ed without requeue and lands in the DLQ. Expired messages also go to the DLQ, which makes the timeout demo visible in the RabbitMQ UI.
  - **Demo triggers:**
    - `docker compose stop payment-service` gives a real timeout.
    - An optional dev-only header or env var such as `SIMULATED_DELAY_MS` adds artificial latency, clearly labeled as a demo tool.

- **D8: gRPC and Kafka details.**
  - **gRPC deadline:** 3000 ms, consistent with the RPC timeout. A deadline or error sets the order to `PROCESSING_FAILED`.
  - **Kafka message key:** `order_id`, which keeps each order's events in order within one partition.
  - **Consumer groups:** `policy-service` and `order-service`. Offsets are committed **after** processing, so delivery is at-least-once and the inbox (section 9.3) makes processing effectively once.
  - **Known limitation, state it in the demo:** Payment's "save to DB, then publish to Kafka" is a dual write. If the publish fails after the save, the event is lost. The production fix is the Transactional Outbox pattern, which is not implemented here.
  - **Spring Kafka (implemented Day 3):** `@KafkaListener` taking `ConsumerRecord<String, String>`, the listener parses the envelope with Boot's `JsonMapper` (`FAIL_ON_MISSING/NULL_CREATOR_PROPERTIES`), no `__TypeId__` header. `enable-auto-commit=false`, `ack-mode=record`. `DefaultErrorHandler`: 3 retries 1 s apart, then `DeadLetterPublishingRecoverer` to `<topic>.DLT` (named explicitly; Spring Kafka 4 defaults to `-dlt`), `JacksonException` not retried. The consumer declares its DLT (`NewTopic`, 3 partitions). Producer `acks=all`, `enable.idempotence=true`, `max.block.ms=5000`.
  - **Deterministic `event_id`:** name-based UUID of `event_type:business id` (`payment_id` for payment.recorded, `policy_id` for policy.issued), so any re-publish of the same fact is deduplicated by the receiver's inbox.
  - **Payment publishes after the gRPC response** (and after the DB commit), waits ≤ 5 s, logs `event_publish_failed` on failure. A duplicate `RecordPayment` publishes again.
  - **Policy re-publishes `policy.issued` on `DUPLICATE_EVENT_IGNORED`** (user decision Day 3, overrides the day's prompt): no new policy, `duplicate_event_ignored` still logged, but the stored policy is published again, synchronously with a 5 s timeout; a failure throws so the offset is not committed.
  - **Duplicate-delivery demo:** the debug replay endpoint was **not built** on Day 3 (agreed); re-send an event from Kafka UI (Produce message) instead.

- **D9: Stack is Java 21 + Spring Boot, not .NET 10.**
  - The spec names .NET 10 / ASP.NET Core. The lead approved using another language.
  - Nothing else changes: flows, contracts, identifiers, idempotency rules and the demo are language-independent.

- **D10: Database is MySQL 8.**
  - The spec lists Postgres or SQLite only as examples.
  - Use one MySQL container with the schemas `order_db`, `payment_db` and `policy_db`. This keeps a database-per-service boundary without extra containers. The init SQL script, mounted into `/docker-entrypoint-initdb.d`, creates the schemas and one user per service.
  - Use charset `utf8mb4` so Vietnamese customer names are stored correctly.
  - Enforce dedupe with `UNIQUE` constraints:
    - `payment.partner_transaction_id`
    - `policy.order_id`
    - `consumer_inbox.event_id` in each consuming service
  - Write the inbox row and the business row in **one `@Transactional` method**. A duplicate-key violation (`DataIntegrityViolationException`) means "already processed", which logs `duplicate_event_ignored` and then acks. Do not rely on a read-then-insert check alone.
  - MySQL starts slowly, so services must wait on its healthcheck.

The decisions below come from the failure analysis in `docs/implementation-plan.md` (P1–P8, approved by the intern). The F-numbers refer to that file.

- **D11: Concurrent duplicates → `409 Conflict` (F3).** Of two identical requests racing, `SET NX` lets only one through. If the loser finds the key but not yet the order row, it returns `409` with a "being processed, retry" message instead of an empty or incorrect result.
  - *Implemented Day 4 in two stages:* `SET idempotency:order:{partner_order_id} {order_id} NX EX 30` before the order row exists, then `EXPIRE 86400` once it is committed, and the key is released if the insert fails. A crash between claim and insert blocks the partner_order_id for 30 s, not 24 h.
- **D12: Redis outage does not take the system down (F5).**
  - `orders.partner_order_id` has a `UNIQUE` constraint, which is the second line of idempotency when Redis is unavailable or was flushed.
  - Reads fall back to the database with `cache_status=CACHE_MISS_DB`.
  - Wrap Redis calls so that a connection failure logs a warning and degrades gracefully instead of returning 500.
  - *Implemented Day 4:* every Redis error logs `redis_unavailable`; command and connect timeouts are 300 ms; `management.health.redis.enabled=false`, so `/actuator/health` and readiness stay UP with Redis down.
- **D13: Forward-only order state transitions (F15), revised Day 3 to match `OrderStatus` from PR #1.** Allowed: `CREATED → PAYMENT_RECORDED`, `CREATED | PAYMENT_RECORDED | PROCESSING_FAILED → ISSUED`, `CREATED | PAYMENT_RECORDED → PROCESSING_FAILED`. Anything else is ignored and logged as `stale_transition_ignored`; the timeline step is still recorded once. Reason: `policy.issued` can overtake the gRPC thread (CREATED → ISSUED), and after a deadline the policy may really be issued, so showing FAILED would be wrong. Documented in the README.
- **D19 (Day 3): unknown order in `policy.issued` (F22)** throws `OrderNotFoundException`: rollback including the inbox row, 3 retries, then `policy.issued.DLT`. Never acked silently.
- **D20 (Day 3): Flow 2 HTTP answer.** `202` with the stored status (usually `PAYMENT_RECORDED`, already `ISSUED` if the event won the race); gRPC failure `200 PROCESSING_FAILED`; replay `200`.
- **D14: UI polling stops after at most 30 s (F31)** and shows "no result yet, trace with correlation_id".
- **D15: The UI is static HTML/JS served by nginx (F32).** nginx proxies `/api/` to `order-service:8080`, so the browser sees one origin and CORS is not needed. There is no frontend build step.
- **D16: Ports.**

  | Component | Port(s) |
  | :--- | :--- |
  | UI (nginx) | 3000 |
  | order-service | 8080 |
  | payment-service | 8081 (HTTP/actuator), gRPC 9090 |
  | policy-service | 8082 |
  | MySQL (on the host) | 3307 → 3306 in the container |
  | Redis | 6379 |
  | RabbitMQ | 5672, management UI 15672 |
  | Kafka | `kafka:9092` inside Docker, `localhost:9094` from the host (F25) |
  | kafka-ui (optional profile `tools`) | 8090 |

- **D17: `contracts/` is a Maven module; there is no shared `common` module.**
  - Order and Payment both depend on `contracts`, so they compile against stubs generated from the same `payment.proto` and cannot drift apart (F16).
  - Small helpers such as `EventEnvelope`, the correlation-id filter and the log formatter are **copied per service**. The contracts between services are the `.proto` and JSON Schemas, never shared Java classes.
- **D18: Money is an integer amount in VND.** VND has no minor unit, so the amount is `int64 amount` in proto, `integer` in the JSON Schemas, `BIGINT` in MySQL and `long` in Java. Never use `double` or `float` for money.

---

