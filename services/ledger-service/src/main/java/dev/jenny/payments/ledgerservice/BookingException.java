package dev.jenny.payments.ledgerservice;

/** How a booking attempt failed decides what happens to the Kafka message. */
public abstract class BookingException extends RuntimeException {

    BookingException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Worth retrying later (retry topics with backoff): Fineract busy/unreachable/unknown outcome. */
    public static class Transient extends BookingException {
        public Transient(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Retrying cannot help (bad event, unknown merchant, Fineract rejected the request): straight to the DLQ. */
    public static class Permanent extends BookingException {
        public Permanent(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
