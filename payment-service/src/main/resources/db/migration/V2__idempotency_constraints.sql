-- One payment per order. uk_payments_partner_transaction_id already stops a replayed request;
-- this stops a second payment for the same order under a different partner_transaction_id.
ALTER TABLE payments ADD CONSTRAINT uk_payments_order_id UNIQUE (order_id);
