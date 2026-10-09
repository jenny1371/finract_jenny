package dev.jenny.payments.orderservice.saga;

/** How a saga step failed decides what Kafka does with the message. */
public abstract class SagaException extends RuntimeException {

    SagaException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Worth retrying later (retry topics with backoff). */
    public static class Transient extends SagaException {
        public Transient(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Retrying cannot help (unreadable event): straight to the DLQ. */
    public static class Permanent extends SagaException {
        public Permanent(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
