package dev.jenny.payments.reconciliationservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "reconciliation.schedule.enabled", havingValue = "true", matchIfMissing = true)
class ReconciliationScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationScheduler.class);

    private final ReconciliationService service;

    ReconciliationScheduler(ReconciliationService service) {
        this.service = service;
    }

    @Scheduled(initialDelayString = "${reconciliation.schedule.interval-ms:300000}",
            fixedDelayString = "${reconciliation.schedule.interval-ms:300000}")
    void run() {
        try {
            service.run();
        } catch (RuntimeException e) {
            log.error("Scheduled reconciliation crashed: {}", e.toString());   // never let the scheduler thread die
        }
    }
}
