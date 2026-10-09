package dev.jenny.payments.paymentservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jenny.payments.paymentservice.PaymentGateway.CardDeclinedException;
import dev.jenny.payments.paymentservice.PaymentGateway.GatewayUnknownException;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

/**
 * Payment lifecycle: PENDING -> AUTHORIZED -> CAPTURING -> CAPTURED, or AUTHORIZED -> CANCELING -> CANCELED,
 * or PENDING -> FAILED.
 *
 * Rules that make it safe to retry and to run concurrently:
 * - No DB connection is held while calling the processor.
 * - Processor calls use a stable idempotency key per (payment, operation).
 * - State changes are conditional UPDATEs (compare-and-set); the winner calls the processor, losers get 409.
 * - A status change and its outbox event commit in the same transaction.
 */
@Service
public class PaymentService {

    static final String TOPIC = "payment.events";

    record Payment(UUID id, UUID orderId, long amount, String currency, String status, String intentId, String failureCode,
                   String paymentMethod) {}

    @FunctionalInterface
    private interface GatewayOp {
        void run(String idempotencyKey, String intentId);
    }

    private static final RowMapper<Payment> MAPPER = (rs, i) -> new Payment(
            rs.getObject("id", UUID.class), rs.getObject("order_id", UUID.class), rs.getLong("amount"),
            rs.getString("currency").trim(), rs.getString("status"), rs.getString("stripe_intent_id"),
            rs.getString("failure_code"), rs.getString("payment_method"));

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final ObjectMapper mapper;
    private final PaymentGateway gateway;
    private final MeterRegistry meters;
    private final double claimTimeoutSeconds;
    private final double pendingAfterSeconds;
    private final int giveUpHours;

    PaymentService(JdbcTemplate jdbc, TransactionTemplate tx, ObjectMapper mapper, PaymentGateway gateway,
                   MeterRegistry meters, @Value("${payments.claim-timeout-seconds:30}") int claimTimeoutSeconds,
                   @Value("${payments.resolver.pending-after-seconds:60}") int pendingAfterSeconds,
                   @Value("${payments.resolver.give-up-hours:12}") int giveUpHours) {
        this.pendingAfterSeconds = pendingAfterSeconds;
        this.giveUpHours = giveUpHours;
        this.jdbc = jdbc;
        this.tx = tx;
        this.mapper = mapper;
        this.gateway = gateway;
        this.meters = meters;
        this.claimTimeoutSeconds = claimTimeoutSeconds;
    }

    // ---- authorize ---------------------------------------------------------------------------

    /** Returns the JSON body to replay for this Idempotency-Key. */
    public String authorize(String idemKey, String requestHash, PaymentRequest req) {
        Payment p = findByOrder(req.orderId()).orElseGet(() -> insertPending(req));
        if (p.amount() != req.amount() || !p.currency().equals(req.currency())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "This order already has a payment with different amount/currency");
        }
        if (!p.status().equals("PENDING")) {
            // Already decided (possibly by an earlier request with another key): report it, remember this key.
            return tx.execute(s -> {
                storeIdempotency(idemKey, requestHash, json(p));
                return json(p);
            });
        }

        return runAuthorization(p, req.method(), idemKey, requestHash);
    }

    /**
     * PENDING: either brand new or an earlier attempt died / timed out. The stable key makes re-calling safe.
     * Used by the API and by the background resolver (which has no idempotency key to store).
     */
    private String runAuthorization(Payment p, String method, String idemKey, String requestHash) {
        String intentId;
        try {
            intentId = gateway.authorize("pay-auth-" + p.id(), p.amount(), p.currency(), method, p.id().toString());
        } catch (CardDeclinedException e) {
            meters.counter("payments.authorize", "outcome", "declined").increment();
            return complete(p.id(), "PENDING", "FAILED", null, e.code(), "PaymentFailed", idemKey, requestHash);
        } catch (GatewayUnknownException e) {
            meters.counter("payments.authorize", "outcome", "unknown").increment();
            // Deliberately not stored: the client retries with the same Idempotency-Key and we resume.
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Payment outcome unknown; retry with the same Idempotency-Key");
        }
        meters.counter("payments.authorize", "outcome", "authorized").increment();
        return complete(p.id(), "PENDING", "AUTHORIZED", intentId, null, "PaymentAuthorized", idemKey, requestHash);
    }

    // ---- background resolver for payments stuck in PENDING -------------------------------------

    /**
     * A payment can stay PENDING when the processor timed out and nobody retried (the customer left, the order
     * service gave up). This re-runs the SAME authorization (same stable processor key, so it cannot charge twice)
     * until it gets a definite answer. Past the give-up age it fails the payment instead: the processor's key
     * expires after ~24 h, and re-authorizing after that could create a second authorization.
     */
    public int sweepPending() {
        List<Map<String, Object>> due = jdbc.queryForList("SELECT id, updated_at FROM payments WHERE status = 'PENDING' "
                + "AND updated_at < now() - make_interval(secs => ?) ORDER BY updated_at LIMIT 20", (double) pendingAfterSeconds);
        int handled = 0;
        for (Map<String, Object> row : due) {
            UUID id = (UUID) row.get("id");
            // optimistic claim: another instance (or a webhook) may have moved it in the meantime
            int claimed = jdbc.update("UPDATE payments SET resolve_attempts = resolve_attempts + 1, updated_at = now() "
                    + "WHERE id = ? AND status = 'PENDING' AND updated_at = ?", id, row.get("updated_at"));
            if (claimed != 1) {
                continue;
            }
            Payment p = findById(id).orElseThrow();
            boolean tooOld = jdbc.queryForObject("SELECT created_at < now() - make_interval(hours => ?) FROM payments WHERE id = ?",
                    Boolean.class, giveUpHours, id);
            handled++;
            if (tooOld) {
                meters.counter("payments.resolver", "outcome", "gave_up").increment();
                tryComplete(id, "PENDING", "FAILED", null, "unresolved_timeout", "PaymentFailed", null, null);
                continue;
            }
            try {
                runAuthorization(p, p.paymentMethod(), null, null);
                meters.counter("payments.resolver", "outcome", "resolved").increment();
            } catch (ResponseStatusException stillUnknown) {
                meters.counter("payments.resolver", "outcome", "still_unknown").increment();
            }
        }
        return handled;
    }

    // ---- capture / cancel --------------------------------------------------------------------

    public String capture(UUID id) {
        return transition(id, "AUTHORIZED", "CAPTURING", "CAPTURED", "PaymentCaptured", "pay-cap-", gateway::capture);
    }

    public String cancel(UUID id) {
        return transition(id, "AUTHORIZED", "CANCELING", "CANCELED", "PaymentCanceled", "pay-cancel-", gateway::cancel);
    }

    public String get(UUID id) {
        return json(findById(id).orElseThrow(PaymentService::notFound));
    }

    private String transition(UUID id, String from, String via, String to, String event, String keyPrefix, GatewayOp op) {
        Payment p = findById(id).orElseThrow(PaymentService::notFound);
        if (p.status().equals(to)) {
            return json(p);                                   // already done: idempotent
        }
        if (!claim(id, from, via)) {
            p = findById(id).orElseThrow(PaymentService::notFound);
            if (p.status().equals(to)) {
                return json(p);
            }
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Payment is " + p.status() + ", cannot move to " + to);
        }
        try {
            op.run(keyPrefix + id, p.intentId());
        } catch (GatewayUnknownException e) {
            // Claim stays; after claim-timeout a retry can re-claim, and the stable key keeps the processor safe.
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Outcome unknown; retry shortly");
        }
        return complete(id, via, to, null, null, event, null, null);
    }

    /** Compare-and-set: only one caller can move FROM -> VIA (or re-claim a stale VIA). */
    private boolean claim(UUID id, String from, String via) {
        return jdbc.update("UPDATE payments SET status = ?, updated_at = now() WHERE id = ? "
                        + "AND (status = ? OR (status = ? AND updated_at < now() - make_interval(secs => ?)))",
                via, id, from, via, claimTimeoutSeconds) == 1;
    }

    // ---- persistence -------------------------------------------------------------------------

    private Payment insertPending(PaymentRequest req) {
        UUID id = UUID.randomUUID();
        try {
            jdbc.update("INSERT INTO payments (id, order_id, amount, currency, status, payment_method) VALUES (?, ?, ?, ?, 'PENDING', ?)",
                    id, req.orderId(), req.amount(), req.currency(), req.method());
        } catch (DuplicateKeyException e) {
            return findByOrder(req.orderId()).orElseThrow();   // lost a race: use the winner's row
        }
        return findById(id).orElseThrow();
    }

    /** Status change + outbox event (+ idempotency record) in ONE transaction. */
    private String complete(UUID id, String from, String to, String intentId, String failureCode, String eventType,
                            String idemKey, String requestHash) {
        return tryComplete(id, from, to, intentId, failureCode, eventType, idemKey, requestHash)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.CONFLICT, "Payment changed concurrently"));
    }

    /**
     * Compare-and-set from -> to plus outbox event. If someone else (typically the Stripe webhook) already moved the
     * payment to {@code to}, that is success without a second event; if it moved elsewhere, returns empty.
     * Never throws for a lost race, so it is safe to call inside an outer transaction (webhook handling).
     */
    private java.util.Optional<String> tryComplete(UUID id, String from, String to, String intentId, String failureCode,
                                                   String eventType, String idemKey, String requestHash) {
        return tx.execute(s -> {
            int updated = jdbc.update("UPDATE payments SET status = ?, stripe_intent_id = COALESCE(?, stripe_intent_id), "
                            + "failure_code = ?, updated_at = now() WHERE id = ? AND status = ?",
                    to, intentId, failureCode, id, from);
            if (updated != 1) {
                Payment current = findById(id).orElseThrow();
                if (current.status().equals(to)) {
                    if (idemKey != null) {
                        storeIdempotency(idemKey, requestHash, json(current));
                    }
                    return java.util.Optional.of(json(current));
                }
                return java.util.Optional.<String>empty();
            }
            Payment p = findById(id).orElseThrow();
            UUID eventId = UUID.randomUUID();
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("eventId", eventId);
            event.put("type", eventType);
            event.put("paymentId", p.id());
            event.put("orderId", p.orderId());
            event.put("amount", p.amount());
            event.put("currency", p.currency());
            event.put("status", p.status());
            jdbc.update("INSERT INTO outbox (id, aggregate_id, topic, event_type, payload) VALUES (?, ?, ?, ?, ?)",
                    eventId, p.orderId().toString(), TOPIC, eventType, json(event));
            String body = json(p);
            if (idemKey != null) {
                storeIdempotency(idemKey, requestHash, body);
            }
            return java.util.Optional.of(body);
        });
    }

    // ---- Stripe webhook: the processor's word beats our guesses ------------------------------------

    /**
     * Applies one verified, de-duplicated Stripe event. Only forward transitions are allowed, so re-ordered or late
     * events are ignored. Resolves the "timeout, did the money move?" case: PENDING becomes AUTHORIZED or FAILED.
     */
    public void applyWebhook(String type, String intentId, UUID metadataPaymentId, String failureCode) {
        Payment p = findByIntent(intentId).or(() -> metadataPaymentId == null ? Optional.empty() : findById(metadataPaymentId))
                .orElse(null);
        if (p == null) {
            meters.counter("payments.webhook", "outcome", "unknown_payment").increment();
            return;
        }
        switch (type) {
            case "payment_intent.amount_capturable_updated" -> webhookMove(p, List.of("PENDING"), "AUTHORIZED", intentId, null, "PaymentAuthorized");
            case "payment_intent.payment_failed" -> webhookMove(p, List.of("PENDING"), "FAILED", intentId, failureCode, "PaymentFailed");
            case "payment_intent.succeeded" -> webhookMove(p, List.of("AUTHORIZED", "CAPTURING"), "CAPTURED", intentId, null, "PaymentCaptured");
            case "payment_intent.canceled" -> webhookMove(p, List.of("PENDING", "AUTHORIZED", "CANCELING"), "CANCELED", intentId, null, "PaymentCanceled");
            default -> meters.counter("payments.webhook", "outcome", "ignored_type").increment();
        }
    }

    private void webhookMove(Payment p, List<String> allowedFrom, String to, String intentId, String failureCode, String event) {
        if (!allowedFrom.contains(p.status())) {
            meters.counter("payments.webhook", "outcome", "ignored_state").increment();   // late / re-ordered
            return;
        }
        boolean moved = tryComplete(p.id(), p.status(), to, intentId, failureCode, event, null, null).isPresent();
        meters.counter("payments.webhook", "outcome", moved ? "applied" : "lost_race").increment();
    }

    private Optional<Payment> findByIntent(String intentId) {
        if (intentId == null) {
            return Optional.empty();
        }
        return jdbc.query("SELECT * FROM payments WHERE stripe_intent_id = ?", MAPPER, intentId).stream().findFirst();
    }

    private void storeIdempotency(String key, String hash, String body) {
        jdbc.update("INSERT INTO idempotency_keys (idem_key, request_hash, response_body) VALUES (?, ?, ?)", key, hash, body);
    }

    private Optional<Payment> findById(UUID id) {
        return jdbc.query("SELECT * FROM payments WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    private Optional<Payment> findByOrder(UUID orderId) {
        return jdbc.query("SELECT * FROM payments WHERE order_id = ?", MAPPER, orderId).stream().findFirst();
    }

    private static ResponseStatusException notFound() {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "Payment not found");
    }

    private String json(Payment p) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", p.id());
        m.put("orderId", p.orderId());
        m.put("amount", p.amount());
        m.put("currency", p.currency());
        m.put("status", p.status());
        m.put("failureCode", p.failureCode());
        return json(m);
    }

    private String json(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
