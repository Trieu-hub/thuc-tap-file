-- order_db schema (Day 1). Constraints that enforce idempotency are named explicitly.

CREATE TABLE orders (
    order_id               VARCHAR(32)  NOT NULL,
    partner_order_id       VARCHAR(64)  NOT NULL,
    customer_name          VARCHAR(255) NOT NULL,
    phone                  VARCHAR(20)  NOT NULL,
    amount                 BIGINT       NOT NULL,          -- VND, integer (D18)
    mode                   VARCHAR(20)  NOT NULL,          -- RABBITMQ_RPC | GRPC_KAFKA
    status                 VARCHAR(30)  NOT NULL,          -- CREATED | PAYMENT_RECORDED | ISSUED | PROCESSING_FAILED
    failure_reason         VARCHAR(64)  NULL,
    correlation_id         VARCHAR(64)  NOT NULL,
    partner_transaction_id VARCHAR(80)  NOT NULL,          -- TXN-{partner_order_id} (D2)
    policy_number          VARCHAR(32)  NULL,
    created_at             DATETIME(3)  NOT NULL,
    updated_at             DATETIME(3)  NOT NULL,
    PRIMARY KEY (order_id),
    CONSTRAINT uk_orders_partner_order_id UNIQUE (partner_order_id)   -- 2nd idempotency line when Redis is down (D12)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

CREATE TABLE order_timeline (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    order_id    VARCHAR(32)  NOT NULL,
    seq         INT          NOT NULL,
    step        VARCHAR(40)  NOT NULL,
    service     VARCHAR(40)  NOT NULL,
    transport   VARCHAR(20)  NOT NULL,
    status      VARCHAR(20)  NOT NULL,
    duration_ms BIGINT       NULL,
    detail      VARCHAR(255) NULL,
    created_at  DATETIME(3)  NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_order_timeline_order_seq UNIQUE (order_id, seq),
    CONSTRAINT fk_order_timeline_order FOREIGN KEY (order_id) REFERENCES orders (order_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

-- Dedupe for consumed policy.issued events (section 9.3)
CREATE TABLE consumer_inbox (
    event_id     VARCHAR(64) NOT NULL,
    event_type   VARCHAR(64) NOT NULL,
    processed_at DATETIME(3) NOT NULL,
    PRIMARY KEY (event_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
