package dev.jenny.payments.paymentservice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Stand-in for Stripe used ONLY for load tests (stripe.simulated=true): Stripe's test mode is rate limited and its
 * latency would drown out what we want to measure (our own code, Kafka, Postgres and Fineract).
 * Behaves like Stripe in the ways that matter: fixed latency, idempotent per key.
 */
@Component
@ConditionalOnProperty(name = "stripe.simulated", havingValue = "true")
public class SimulatedPaymentGateway implements PaymentGateway {

    private final long latencyMs;
    private final Map<String, String> intentsByKey = new ConcurrentHashMap<>();

    SimulatedPaymentGateway(@Value("${stripe.simulated-latency-ms:150}") long latencyMs) {
        this.latencyMs = latencyMs;
    }

    @Override
    public String authorize(String idempotencyKey, long amount, String currency, String paymentMethod, String paymentId) {
        pause();
        return intentsByKey.computeIfAbsent(idempotencyKey, k -> "pi_sim_" + k);
    }

    @Override
    public void capture(String idempotencyKey, String intentId) {
        pause();
    }

    @Override
    public void cancel(String idempotencyKey, String intentId) {
        pause();
    }

    private void pause() {
        try {
            Thread.sleep(latencyMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
