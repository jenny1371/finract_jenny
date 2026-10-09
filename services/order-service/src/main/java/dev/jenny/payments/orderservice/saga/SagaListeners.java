package dev.jenny.payments.orderservice.saga;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * One consumer per topic, each with its own thread pool, retry topics and DLT.
 *
 * Why not one listener for all three topics: the load test showed head-of-line blocking. OrderCreated handling makes a
 * synchronous HTTP call (~300 ms), and with a shared pool the fast events (PaymentAuthorized, LedgerBooked) queued
 * behind them, which capped the whole saga at about 4 orders/s while the CPU sat idle.
 * Failures flow: topic -> retry topics (exponential backoff) -> DLT -> MANUAL_REVIEW.
 */
final class SagaListeners {

    private SagaListeners() {}

    @Component
    static class Orders {
        private final OrderSaga saga;

        Orders(OrderSaga saga) {
            this.saga = saga;
        }

        @RetryableTopic(attempts = "${saga.retry.attempts:4}",
                backoff = @Backoff(delayExpression = "${saga.retry.delay-ms:1000}", multiplierExpression = "${saga.retry.multiplier:2.0}"),
                numPartitions = "${saga.partitions:6}", exclude = SagaException.Permanent.class,
                topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
        @KafkaListener(topics = "order.events", groupId = "order-saga-orders", concurrency = "${saga.concurrency.orders:6}")
        void onEvent(String payload) {
            saga.onEvent(payload);
        }

        @DltHandler
        void onDeadLetter(String payload, @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String reason) {
            saga.onDeadLetter(payload, reason);
        }
    }

    @Component
    static class Payments {
        private final OrderSaga saga;

        Payments(OrderSaga saga) {
            this.saga = saga;
        }

        @RetryableTopic(attempts = "${saga.retry.attempts:4}",
                backoff = @Backoff(delayExpression = "${saga.retry.delay-ms:1000}", multiplierExpression = "${saga.retry.multiplier:2.0}"),
                numPartitions = "${saga.partitions:6}", exclude = SagaException.Permanent.class,
                topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
        @KafkaListener(topics = "payment.events", groupId = "order-saga-payments", concurrency = "${saga.concurrency.payments:3}")
        void onEvent(String payload) {
            saga.onEvent(payload);
        }

        @DltHandler
        void onDeadLetter(String payload, @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String reason) {
            saga.onDeadLetter(payload, reason);
        }
    }

    @Component
    static class Ledger {
        private final OrderSaga saga;

        Ledger(OrderSaga saga) {
            this.saga = saga;
        }

        @RetryableTopic(attempts = "${saga.retry.attempts:4}",
                backoff = @Backoff(delayExpression = "${saga.retry.delay-ms:1000}", multiplierExpression = "${saga.retry.multiplier:2.0}"),
                numPartitions = "${saga.partitions:6}", exclude = SagaException.Permanent.class,
                topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
        @KafkaListener(topics = "ledger.events", groupId = "order-saga-ledger", concurrency = "${saga.concurrency.ledger:6}")
        void onEvent(String payload) {
            saga.onEvent(payload);
        }

        @DltHandler
        void onDeadLetter(String payload, @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String reason) {
            saga.onDeadLetter(payload, reason);
        }
    }
}
