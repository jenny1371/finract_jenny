-- One row per payment we must book. UNIQUE(payment_id) is what makes re-emitted events harmless.
CREATE TABLE ledger_entries (
    id               UUID PRIMARY KEY,
    payment_id       UUID         NOT NULL UNIQUE,
    order_id         UUID         NOT NULL,
    merchant_id      VARCHAR(64)  NOT NULL,
    amount           BIGINT       NOT NULL CHECK (amount > 0),
    currency         CHAR(3)      NOT NULL,
    status           VARCHAR(16)  NOT NULL,          -- PENDING | BOOKED | FAILED
    attempt          INT          NOT NULL DEFAULT 1, -- part of the Fineract Idempotency-Key; bumped only after a definitive 409
    fineract_txn_id  VARCHAR(64),
    failure_reason   TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Consumer-side dedup: Kafka is at-least-once, so the same event can arrive twice.
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Which Fineract savings account receives a merchant's money.
CREATE TABLE merchant_accounts (
    merchant_id         VARCHAR(64) PRIMARY KEY,
    fineract_client_id  BIGINT,
    fineract_savings_id BIGINT NOT NULL
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
