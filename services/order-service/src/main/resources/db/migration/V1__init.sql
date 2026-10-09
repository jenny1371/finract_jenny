CREATE TABLE orders (
    id          UUID PRIMARY KEY,
    amount      BIGINT      NOT NULL CHECK (amount > 0),   -- minor units (cents)
    currency    CHAR(3)     NOT NULL,
    status      VARCHAR(32) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Authority for API idempotency. The UNIQUE key (primary key) is the last line of defence
-- even if Redis is down or loses the lock.
CREATE TABLE idempotency_keys (
    idem_key      VARCHAR(255) PRIMARY KEY,
    request_hash  CHAR(64)     NOT NULL,
    order_id      UUID         NOT NULL REFERENCES orders (id),
    response_body TEXT         NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Transactional outbox: written in the same transaction as the business row, published by a poller.
-- outbox.id doubles as the event id that consumers use for de-duplication.
CREATE TABLE outbox (
    id           UUID PRIMARY KEY,
    aggregate_id VARCHAR(64)  NOT NULL,   -- Kafka partition key (orderId) => per-order ordering
    topic        VARCHAR(128) NOT NULL,
    event_type   VARCHAR(64)  NOT NULL,
    payload      TEXT         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
