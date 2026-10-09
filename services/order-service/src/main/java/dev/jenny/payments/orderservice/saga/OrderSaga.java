package dev.jenny.payments.orderservice.saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Orchestrates: authorize payment -> book in the ledger -> capture payment, compensating on failure.
 *
 * <pre>
 * CREATED -> AUTHORIZING -> BOOKING -> CAPTURING -> COMPLETED
 *               |              |           |
 *        PAYMENT_FAILED        |     (capture refused)
 *                              v           v
 *                      (ledger failed)  UNDOING_LEDGER -> VOIDING -> CANCELED
 *                              +-----------------------------^
 * anything unrecoverable -> MANUAL_REVIEW
 * </pre>
 *
 * Why booking comes BEFORE capture: if the ledger cannot book, we only release the authorization (no money moved,
 * no fee). If capture is refused after booking, we undo the ledger entry, then release the authorization.
 *
 * Every transition is a compare-and-set on orders.status, so duplicate, re-ordered and late events are harmless.
 * Kafka commands are written to the outbox in the same transaction as the transition; HTTP commands are
 * idempotent and are re-issued when an event is redelivered or by the watchdog.
 */
@Service
public class OrderSaga {

    private static final Logger log = LoggerFactory.getLogger(OrderSaga.class);
    static final String LEDGER_COMMANDS = "ledger.commands";

    static final String CREATED = "CREATED";
    static final String AUTHORIZING = "AUTHORIZING";
    static final String BOOKING = "BOOKING";
    static final String CAPTURING = "CAPTURING";
    static final String COMPLETED = "COMPLETED";
    static final String UNDOING_LEDGER = "UNDOING_LEDGER";
    static final String VOIDING = "VOIDING";
    static final String CANCELED = "CANCELED";
    static final String PAYMENT_FAILED = "PAYMENT_FAILED";
    static final String MANUAL_REVIEW = "MANUAL_REVIEW";
    static final String TERMINAL = "'COMPLETED','CANCELED','PAYMENT_FAILED','MANUAL_REVIEW'";

    record Order(UUID id, long amount, String currency, String status, String merchantId, String paymentMethod,
                 UUID paymentId, int redrives, Timestamp updatedAt) {}

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final ObjectMapper mapper;
    private final PaymentClient payments;
    private final MeterRegistry meters;
    private final int maxRedrives;

    OrderSaga(JdbcTemplate jdbc, TransactionTemplate tx, ObjectMapper mapper, PaymentClient payments,
              MeterRegistry meters, @Value("${saga.watchdog.max-redrives:5}") int maxRedrives) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.mapper = mapper;
        this.payments = payments;
        this.meters = meters;
        this.maxRedrives = maxRedrives;
    }

    // ---- event handling ---------------------------------------------------------------------------

    /** Handles one message from order.events / payment.events / ledger.events. */
    public void onEvent(String payload) {
        JsonNode n = read(payload);
        String type = n.path("type").asText("");
        UUID orderId = uuid(n, "orderId");
        if (orderId == null) {
            return;                                            // not an event we care about
        }
        switch (type) {
            case "OrderCreated" -> start(orderId);
            case "PaymentAuthorized" -> paymentAuthorized(orderId, uuid(n, "paymentId"));
            case "PaymentFailed" -> advance(orderId, AUTHORIZING, PAYMENT_FAILED, "payment failed");
            case "LedgerBooked" -> ledgerBooked(orderId);
            case "LedgerBookingFailed" -> beginVoid(orderId, BOOKING);
            case "PaymentCaptured" -> advance(orderId, CAPTURING, COMPLETED, null);
            case "PaymentCanceled" -> advance(orderId, VOIDING, CANCELED, null);
            case "LedgerUndone" -> beginVoid(orderId, UNDOING_LEDGER);
            case "LedgerUndoFailed" -> advance(orderId, UNDOING_LEDGER, MANUAL_REVIEW, "ledger undo failed: " + n.path("reason").asText());
            default -> { }
        }
    }

    /** Retries exhausted for a message: stop guessing, hand the order to a human. */
    public void onDeadLetter(String payload, String reason) {
        UUID orderId;
        try {
            orderId = uuid(read(payload), "orderId");
        } catch (SagaException.Permanent e) {
            log.error("Unreadable saga message in DLQ: {}", payload);
            return;
        }
        if (orderId == null) {
            return;
        }
        int changed = jdbc.update("UPDATE orders SET status = ?, failure_reason = ?, saga_updated_at = now() "
                + "WHERE id = ? AND status NOT IN (" + TERMINAL + ")", MANUAL_REVIEW, "saga step failed: " + reason, orderId);
        if (changed == 1) {
            meters.counter("saga.manual_review", "cause", "dead_letter").increment();
        }
    }

    // ---- steps ------------------------------------------------------------------------------------

    private void start(UUID orderId) {
        if (!enter(orderId, CREATED, AUTHORIZING)) {
            return;
        }
        authorizePayment(load(orderId));
    }

    private void authorizePayment(Order o) {
        try {
            payments.authorize(o.id(), o.amount(), o.currency(), o.paymentMethod());
        } catch (PaymentClient.Unavailable e) {
            throw new SagaException.Transient("authorize: " + e.getMessage(), e);   // same key on retry
        } catch (PaymentClient.Rejected e) {
            advance(o.id(), AUTHORIZING, PAYMENT_FAILED, e.getMessage());           // nothing was reserved
        }
    }

    private void paymentAuthorized(UUID orderId, UUID paymentId) {
        tx.executeWithoutResult(s -> {
            int flipped = jdbc.update("UPDATE orders SET status = ?, payment_id = ?, saga_updated_at = now(), saga_redrives = 0 "
                    + "WHERE id = ? AND status = ?", BOOKING, paymentId, orderId, AUTHORIZING);
            if (flipped == 1) {
                sendLedgerCommand("BookLedger", load(orderId));
                meters.counter("saga.transitions", "to", BOOKING).increment();
            }
        });
    }

    private void ledgerBooked(UUID orderId) {
        if (!enter(orderId, BOOKING, CAPTURING)) {
            return;
        }
        capturePayment(load(orderId));
    }

    private void capturePayment(Order o) {
        try {
            payments.capture(o.paymentId());
        } catch (PaymentClient.Unavailable e) {
            throw new SagaException.Transient("capture: " + e.getMessage(), e);
        } catch (PaymentClient.Rejected e) {
            // Payment cannot be captured (expired / canceled): the ledger entry must not stay.
            tx.executeWithoutResult(s -> {
                if (move(o.id(), CAPTURING, UNDOING_LEDGER, "capture refused: " + e.getMessage())) {
                    sendLedgerCommand("UndoLedger", load(o.id()));
                }
            });
        }
    }

    private void beginVoid(UUID orderId, String from) {
        if (!enter(orderId, from, VOIDING)) {
            return;
        }
        voidPayment(load(orderId));
    }

    private void voidPayment(Order o) {
        try {
            payments.cancel(o.paymentId());
        } catch (PaymentClient.Unavailable e) {
            throw new SagaException.Transient("void: " + e.getMessage(), e);
        } catch (PaymentClient.Rejected e) {
            advance(o.id(), VOIDING, MANUAL_REVIEW, "void refused: " + e.getMessage());
        }
    }

    // ---- watchdog support: re-issue the command for the state the order is stuck in -----------------

    /** Re-drives one stuck order. Returns false if another instance got there first. */
    boolean redrive(Order o) {
        int claimed = jdbc.update("UPDATE orders SET saga_redrives = saga_redrives + 1, saga_updated_at = now() "
                + "WHERE id = ? AND status = ? AND saga_updated_at = ?", o.id(), o.status(), o.updatedAt());
        if (claimed != 1) {
            return false;
        }
        if (o.redrives() + 1 > maxRedrives) {
            advance(o.id(), o.status(), MANUAL_REVIEW, "stuck in " + o.status() + " after " + maxRedrives + " re-drives");
            meters.counter("saga.manual_review", "cause", "stuck").increment();
            return true;
        }
        meters.counter("saga.redrives", "state", o.status()).increment();
        try {
            switch (o.status()) {
                case AUTHORIZING -> authorizePayment(o);
                case BOOKING -> tx.executeWithoutResult(s -> sendLedgerCommand("BookLedger", o));
                case CAPTURING -> capturePayment(o);
                case UNDOING_LEDGER -> tx.executeWithoutResult(s -> sendLedgerCommand("UndoLedger", o));
                case VOIDING -> voidPayment(o);
                default -> { }
            }
        } catch (SagaException.Transient e) {
            log.warn("Re-drive of order {} failed, watchdog will try again: {}", o.id(), e.getMessage());
        }
        return true;
    }

    List<Order> stuck(long stuckAfterMs) {
        return jdbc.query("SELECT id, amount, currency, status, merchant_id, payment_method, payment_id, saga_redrives, saga_updated_at "
                        + "FROM orders WHERE status NOT IN ('CREATED'," + TERMINAL + ") "
                        + "AND saga_updated_at < now() - make_interval(secs => ? * power(2, saga_redrives)) ORDER BY saga_updated_at LIMIT 50",
                (rs, i) -> map(rs), stuckAfterMs / 1000.0);
    }

    // ---- state machine primitives -------------------------------------------------------------------

    /** Compare-and-set. Returns true only for the caller that actually changed the state. */
    private boolean move(UUID id, String from, String to, String reason) {
        int changed = jdbc.update("UPDATE orders SET status = ?, saga_updated_at = now(), saga_redrives = 0, "
                + "failure_reason = COALESCE(?, failure_reason) WHERE id = ? AND status = ?", to, reason, id, from);
        if (changed == 1) {
            meters.counter("saga.transitions", "to", to).increment();
        }
        return changed == 1;
    }

    private void advance(UUID id, String from, String to, String reason) {
        move(id, from, to, reason);
    }

    /** True if we moved the order into {@code to}, or it was already there (a redelivery after a failed HTTP call). */
    private boolean enter(UUID id, String from, String to) {
        if (move(id, from, to, null)) {
            return true;
        }
        List<String> current = jdbc.queryForList("SELECT status FROM orders WHERE id = ?", String.class, id);
        return !current.isEmpty() && to.equals(current.get(0));          // unknown order: nothing to do
    }

    private void sendLedgerCommand(String type, Order o) {
        UUID commandId = UUID.randomUUID();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", commandId);
        m.put("type", type);
        m.put("paymentId", o.paymentId());
        m.put("orderId", o.id());
        m.put("amount", o.amount());
        m.put("currency", o.currency());
        if (o.merchantId() != null) {
            m.put("merchantId", o.merchantId());
        }
        try {
            jdbc.update("INSERT INTO outbox (id, aggregate_id, topic, event_type, payload) VALUES (?, ?, ?, ?, ?)",
                    commandId, o.id().toString(), LEDGER_COMMANDS, type, mapper.writeValueAsString(m));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private Order load(UUID id) {
        List<Order> rows = jdbc.query("SELECT id, amount, currency, status, merchant_id, payment_method, payment_id, "
                + "saga_redrives, saga_updated_at FROM orders WHERE id = ?", (rs, i) -> map(rs), id);
        if (rows.isEmpty()) {
            throw new SagaException.Permanent("Unknown order " + id, null);
        }
        return rows.get(0);
    }

    private static Order map(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new Order(rs.getObject("id", UUID.class), rs.getLong("amount"), rs.getString("currency").trim(),
                rs.getString("status"), rs.getString("merchant_id"), rs.getString("payment_method"),
                rs.getObject("payment_id", UUID.class), rs.getInt("saga_redrives"), rs.getTimestamp("saga_updated_at"));
    }

    private JsonNode read(String payload) {
        try {
            return mapper.readTree(payload);
        } catch (JsonProcessingException e) {
            throw new SagaException.Permanent("Unreadable message: " + e.getMessage(), e);
        }
    }

    private static UUID uuid(JsonNode n, String field) {
        String v = n.path(field).asText("");
        try {
            return v.isEmpty() ? null : UUID.fromString(v);
        } catch (IllegalArgumentException e) {
            throw new SagaException.Permanent("Bad " + field + ": " + v, e);
        }
    }
}
