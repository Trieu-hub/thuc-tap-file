-- Each step appears at most once per order, so a redelivered event or a late RPC reply cannot add
-- a second "Payment" or "Policy issued" row to the timeline. seq only orders the rows; uniqueness
-- on seq alone did not stop duplicates because a replay gets the next seq.
ALTER TABLE order_timeline ADD CONSTRAINT uk_order_timeline_order_step UNIQUE (order_id, step);
