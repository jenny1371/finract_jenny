-- Saga orchestration state lives on the order: status is the state machine, the rest supports recovery.
ALTER TABLE orders
    ADD COLUMN merchant_id      VARCHAR(64),
    ADD COLUMN payment_method   VARCHAR(64),
    ADD COLUMN payment_id       UUID,
    ADD COLUMN failure_reason   TEXT,
    ADD COLUMN saga_updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),   -- last time the saga moved or was re-driven
    ADD COLUMN saga_redrives    INT         NOT NULL DEFAULT 0;       -- watchdog attempts in the current state

-- The watchdog only looks at orders that are still in flight.
CREATE INDEX orders_in_flight ON orders (saga_updated_at)
    WHERE status NOT IN ('CREATED', 'COMPLETED', 'CANCELED', 'PAYMENT_FAILED', 'MANUAL_REVIEW');
