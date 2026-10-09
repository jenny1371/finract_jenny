-- One row per reconciliation run, including failed ones (a failed run must never look like "all clear").
CREATE TABLE reconciliation_runs (
    id            UUID PRIMARY KEY,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        VARCHAR(16) NOT NULL,          -- RUNNING | OK | FAILED
    charges       INT,
    deposits      INT,
    found         INT,
    alerts        INT,
    error         TEXT
);

-- One row per (payment, kind of difference). Re-detecting the same difference updates last_seen;
-- first_seen is what the grace period is measured from. A difference that disappears becomes RESOLVED.
CREATE TABLE discrepancies (
    id          UUID PRIMARY KEY,
    payment_id  UUID         NOT NULL,
    type        VARCHAR(32)  NOT NULL,           -- MISSING_IN_LEDGER | MISSING_IN_STRIPE | AMOUNT_MISMATCH | DUPLICATE_BOOKING
    status      VARCHAR(16)  NOT NULL,           -- OPEN | RESOLVED
    detail      TEXT         NOT NULL,
    first_seen  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    UNIQUE (payment_id, type)
);
