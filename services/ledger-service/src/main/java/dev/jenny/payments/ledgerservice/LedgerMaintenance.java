package dev.jenny.payments.ledgerservice;

import dev.jenny.payments.common.RetentionCleaner;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Purges old processed_events rows so the de-duplication table cannot grow forever.
 * Retention must be longer than anything that can redeliver a command: Kafka topic retention (7 days by default)
 * and a DLQ replay by an operator. Deleting earlier would let a redelivered command be processed twice
 * (the Fineract idempotency key would still catch it, but only for as long as Fineract remembers the key).
 */
@Component
@ConditionalOnProperty(name = "ledger.maintenance.enabled", havingValue = "true", matchIfMissing = true)
class LedgerMaintenance {

    private static final Logger log = LoggerFactory.getLogger(LedgerMaintenance.class);

    private final RetentionCleaner cleaner;
    private final Duration retention;

    LedgerMaintenance(RetentionCleaner cleaner, @Value("${ledger.processed-events-retention-days:14}") int days) {
        this.cleaner = cleaner;
        this.retention = Duration.ofDays(days);
    }

    @Scheduled(fixedDelayString = "${ledger.retention.interval-ms:3600000}", initialDelayString = "60000")
    void purgeProcessedEvents() {
        try {
            cleaner.purge("processed_events", "processed_at", retention, 1000);
        } catch (RuntimeException e) {
            log.error("processed_events retention purge failed: {}", e.toString());
        }
    }
}
