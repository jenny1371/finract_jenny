CREATE TABLE payments (
    id                 UUID PRIMARY KEY,
    order_id           UUID        NOT NULL UNIQUE,          -- one payment per order
    amount             BIGINT      NOT NULL CHECK (amount > 0),
    currency           CHAR(3)     NOT NULL,
    status             VARCHAR(16) NOT NULL,
    stripe_intent_id   VARCHAR(255),
    failure_code       VARCHAR(64),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_keys (
    idem_key      VARCHAR(255) PRIMARY KEY,
    request_hash  CHAR(64)     NOT NULL,
    response_body TEXT         NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id           UUID PRIMARY KEY,
    aggregate_id VARCHAR(64)  NOT NULL,
    topic        VARCHAR(128) NOT NULL,
    event_type   VARCHAR(64)  NOT NULL,
    payload      TEXT         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
