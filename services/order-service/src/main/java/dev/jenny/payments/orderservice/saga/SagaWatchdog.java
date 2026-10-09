package dev.jenny.payments.orderservice.saga;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Safety net for lost messages and lost replies: any order that has made no progress for too long gets the
 * command for its current state re-issued (all commands are idempotent). After too many re-drives the order
 * goes to MANUAL_REVIEW instead of looping forever.
 */
@Component
@ConditionalOnProperty(name = "saga.watchdog.enabled", havingValue = "true", matchIfMissing = true)
class SagaWatchdog {

    private static final Logger log = LoggerFactory.getLogger(SagaWatchdog.class);

    private final OrderSaga saga;
    private final long stuckAfterMs;

    SagaWatchdog(OrderSaga saga, @Value("${saga.watchdog.stuck-after-ms:60000}") long stuckAfterMs) {
        this.saga = saga;
        this.stuckAfterMs = stuckAfterMs;
    }

    @Scheduled(fixedDelayString = "${saga.watchdog.interval-ms:5000}")
    void sweep() {
        for (OrderSaga.Order o : saga.stuck(stuckAfterMs)) {
            try {
                saga.redrive(o);
            } catch (RuntimeException e) {
                log.warn("Watchdog failed on order {}: {}", o.id(), e.toString());
            }
        }
    }
}
