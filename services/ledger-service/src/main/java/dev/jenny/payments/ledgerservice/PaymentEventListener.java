package dev.jenny.payments.ledgerservice;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Consumes saga commands (ledger.commands). Failures flow: main topic -> retry topics (exponential backoff) -> DLT.
 * Permanent errors skip the retries. Retry topics give up strict per-key ordering, which is acceptable because
 * every command is idempotent and state-guarded (a late BookLedger after UndoLedger is refused).
 */
@Component
class PaymentEventListener {

    static final String COMMANDS_TOPIC = "ledger.commands";

    private final LedgerService ledger;

    PaymentEventListener(LedgerService ledger) {
        this.ledger = ledger;
    }

    @RetryableTopic(
            attempts = "${ledger.retry.attempts:4}",
            backoff = @Backoff(delayExpression = "${ledger.retry.delay-ms:1000}",
                    multiplierExpression = "${ledger.retry.multiplier:2.0}"),
            numPartitions = "${ledger.partitions:6}",
            exclude = BookingException.Permanent.class,
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
    @KafkaListener(topics = COMMANDS_TOPIC, groupId = "ledger-service")
    void onCommand(String payload) {
        ledger.handle(payload);
    }

    @DltHandler
    void onDeadLetter(String payload,
                      @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String reason) {
        ledger.deadLettered(payload, reason);
    }
}
