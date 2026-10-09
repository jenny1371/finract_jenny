-- Stripe retries webhooks and may deliver one event several times. The event id is the de-duplication key;
-- it is inserted in the SAME transaction that applies the event, so a failed attempt is retried, not lost.
CREATE TABLE webhook_events (
    event_id    VARCHAR(255) PRIMARY KEY,
    type        VARCHAR(128) NOT NULL,
    received_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
