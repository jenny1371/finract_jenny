-- The background resolver must be able to re-run an authorization on its own, so it needs the payment method
-- (a Stripe test/real token like pm_card_visa, not a card number) and a counter for monitoring.
ALTER TABLE payments
    ADD COLUMN payment_method   VARCHAR(64) NOT NULL DEFAULT 'pm_card_visa',
    ADD COLUMN resolve_attempts INT         NOT NULL DEFAULT 0;
