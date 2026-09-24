-- payment_db schema (Day 1).
-- Only RECORDED payments are stored; a REJECTED request (invalid amount) charges nothing and leaves no row.

CREATE TABLE payments (
    payment_id             VARCHAR(40) NOT NULL,
    order_id               VARCHAR(32) NOT NULL,
    partner_transaction_id VARCHAR(80) NOT NULL,
    amount                 BIGINT      NOT NULL,   -- VND, integer (D18)
    status                 VARCHAR(20) NOT NULL,   -- RECORDED
    created_at             DATETIME(3) NOT NULL,
    PRIMARY KEY (payment_id),
    CONSTRAINT uk_payments_partner_transaction_id UNIQUE (partner_transaction_id)   -- never charge twice (D4, F11)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
