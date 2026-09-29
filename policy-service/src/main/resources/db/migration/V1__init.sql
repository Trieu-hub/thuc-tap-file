-- policy_db schema (Day 1).

CREATE TABLE policies (
    policy_id     VARCHAR(40) NOT NULL,
    order_id      VARCHAR(32) NOT NULL,
    payment_id    VARCHAR(40) NOT NULL,
    policy_number VARCHAR(32) NOT NULL,   -- e.g. ACBI-2026-8899
    issued_at     DATETIME(3) NOT NULL,
    PRIMARY KEY (policy_id),
    CONSTRAINT uk_policies_order_id UNIQUE (order_id),            -- one policy per order, even for a new event_id (F18)
    CONSTRAINT uk_policies_policy_number UNIQUE (policy_number)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

-- Dedupe for consumed payment.recorded events (section 9.3)
CREATE TABLE consumer_inbox (
    event_id     VARCHAR(64) NOT NULL,
    event_type   VARCHAR(64) NOT NULL,
    processed_at DATETIME(3) NOT NULL,
    PRIMARY KEY (event_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
