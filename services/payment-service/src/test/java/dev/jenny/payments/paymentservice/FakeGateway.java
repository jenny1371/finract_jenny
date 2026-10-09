package dev.jenny.payments.paymentservice;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.stereotype.Component;

/**
 * Scriptable stand-in for Stripe. Like the real thing it is idempotent per key, and TIMEOUT models the
 * nasty case: the processor DID act but the caller never got the answer.
 */
@Component
class FakeGateway implements PaymentGateway {

    enum Mode { OK, DECLINE, TIMEOUT, CRASH }

    record Call(String op, String key) {}

    final List<Call> calls = new CopyOnWriteArrayList<>();
    private final Map<String, String> intentsByKey = new ConcurrentHashMap<>();
    volatile Mode authorizeMode = Mode.OK;
    volatile Mode captureMode = Mode.OK;
    volatile long captureDelayMs = 0;
    volatile long authorizeDelayMs = 0;

    void reset() {
        authorizeDelayMs = 0;
        calls.clear();
        intentsByKey.clear();
        authorizeMode = Mode.OK;
        captureMode = Mode.OK;
        captureDelayMs = 0;
    }

    long count(String op) {
        return calls.stream().filter(c -> c.op().equals(op)).count();
    }

    long count(String op, String key) {
        return calls.stream().filter(c -> c.op().equals(op) && c.key().equals(key)).count();
    }

    long distinctIntents() {
        return intentsByKey.values().stream().distinct().count();
    }

    @Override
    public String authorize(String key, long amount, String currency, String paymentMethod, String paymentId) {
        calls.add(new Call("authorize", key));
        sleep(authorizeDelayMs);
        switch (authorizeMode) {
            case DECLINE -> throw new CardDeclinedException("card_declined");
            case CRASH -> throw new RuntimeException("boom");
            default -> { }
        }
        String intent = intentsByKey.computeIfAbsent(key, k -> "pi_" + k);   // processor acts exactly once per key
        if (authorizeMode == Mode.TIMEOUT) {
            throw new GatewayUnknownException("timeout", null);
        }
        return intent;
    }

    @Override
    public void capture(String key, String intentId) {
        calls.add(new Call("capture", key));
        sleep(captureDelayMs);
        if (captureMode == Mode.TIMEOUT) {
            throw new GatewayUnknownException("timeout", null);
        }
    }

    @Override
    public void cancel(String key, String intentId) {
        calls.add(new Call("cancel", key));
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
