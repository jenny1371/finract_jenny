package dev.jenny.payments.paymentservice;

/**
 * Port to the payment processor. Every call takes a STABLE idempotency key (derived from the payment id,
 * not from the attempt), so re-issuing a call after a timeout or crash can never charge twice.
 */
public interface PaymentGateway {

    /** Authorizes (reserves) funds without capturing them. Returns the processor's intent id. */
    String authorize(String idempotencyKey, long amount, String currency, String paymentMethod, String paymentId);

    void capture(String idempotencyKey, String intentId);

    /** Releases an authorization (void). No money moves, so no refund fee. */
    void cancel(String idempotencyKey, String intentId);

    /** Definitive "no": the card was declined. Not retryable. */
    class CardDeclinedException extends RuntimeException {
        private final String code;

        public CardDeclinedException(String code) {
            super("card declined: " + code);
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    /** We do not know whether the processor acted (timeout, connection reset, 5xx). Retry with the SAME key. */
    class GatewayUnknownException extends RuntimeException {
        public GatewayUnknownException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
