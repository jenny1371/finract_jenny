package dev.jenny.payments.paymentservice;

import dev.jenny.payments.common.RetentionCleaner;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Background jobs: resolve payments stuck in PENDING, and purge old webhook de-duplication rows. */
@Component
@ConditionalOnProperty(name = "payments.maintenance.enabled", havingValue = "true", matchIfMissing = true)
class PaymentMaintenance {

    private static final Logger log = LoggerFactory.getLogger(PaymentMaintenance.class);

    private final PaymentService payments;
    private final RetentionCleaner cleaner;
    private final Duration webhookRetention;

    PaymentMaintenance(PaymentService payments, RetentionCleaner cleaner,
                       @Value("${payments.webhook-events-retention-days:30}") int webhookRetentionDays) {
        this.payments = payments;
        this.cleaner = cleaner;
        this.webhookRetention = Duration.ofDays(webhookRetentionDays);
    }

    @Scheduled(fixedDelayString = "${payments.resolver.interval-ms:15000}")
    void resolvePending() {
        try {
            payments.sweepPending();
        } catch (RuntimeException e) {
            log.error("Pending-payment resolver failed, will retry next tick: {}", e.toString());
        }
    }

    /** Stripe retries webhooks for days, so keep the de-dup rows well beyond that (default 30 days). */
    @Scheduled(fixedDelayString = "${payments.retention.interval-ms:3600000}", initialDelayString = "60000")
    void purgeOldWebhookEvents() {
        try {
            cleaner.purge("webhook_events", "received_at", webhookRetention, 1000);
        } catch (RuntimeException e) {
            log.error("Webhook retention purge failed: {}", e.toString());
        }
    }
}
