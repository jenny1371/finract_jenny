package dev.jenny.payments.ledgerservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jenny.payments.ledgerservice.FineractClient.ConflictException;
import dev.jenny.payments.ledgerservice.FineractClient.DepositResult;
import dev.jenny.payments.ledgerservice.FineractClient.RejectedException;
import dev.jenny.payments.ledgerservice.FineractClient.UnknownOutcomeException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Executes ledger commands from the saga against Fineract, exactly once in effect, on top of at-least-once Kafka.
 *
 * Commands: BookLedger (deposit into the merchant's Fineract account) and UndoLedger (the saga's compensation).
 * Replies (outbox -> ledger.events): LedgerBooked, LedgerBookingFailed, LedgerUndone, LedgerUndoFailed.
 *
 * Layers of protection, outermost first:
 * 1. processed_events: the same command id is never handled twice.
 * 2. ledger_entries UNIQUE(payment_id): one entry per payment, with a status that only moves forward.
 * 3. Fineract Idempotency-Key "ledger-{paymentId}-{attempt}":
 *    - unknown outcome (timeout/5xx): SAME key next time, so Fineract cannot apply it twice;
 *    - definitive 409 (Fineract rolled back): NEW key (attempt+1), as FINERACT-2000 instructs.
 */
@Service
public class LedgerService {

    private static final Logger log = LoggerFactory.getLogger(LedgerService.class);
    static final String TOPIC = "ledger.events";
    static final String BOOK = "BookLedger";
    static final String UNDO = "UndoLedger";

    record Event(String type, UUID eventId, UUID paymentId, UUID orderId, long amount, String currency, String merchantId) {}

    record Entry(UUID id, UUID paymentId, long amount, String status, int attempt, String txnId, String merchantId) {}

    @FunctionalInterface
    private interface FineractCall {
        String run(long savingsId, String idempotencyKey);
    }

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final ObjectMapper mapper;
    private final FineractClient fineract;
    private final AccountLocks locks;
    private final MeterRegistry meters;
    private final String defaultMerchant;
    private final int conflictRetries;
    private final Object[] stripes = new Object[64];

    LedgerService(JdbcTemplate jdbc, TransactionTemplate tx, ObjectMapper mapper, FineractClient fineract,
                  AccountLocks locks, MeterRegistry meters,
                  @Value("${ledger.default-merchant-id}") String defaultMerchant,
                  @Value("${ledger.conflict-retries}") int conflictRetries) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.mapper = mapper;
        this.fineract = fineract;
        this.locks = locks;
        this.meters = meters;
        this.defaultMerchant = defaultMerchant;
        this.conflictRetries = conflictRetries;
        for (int i = 0; i < stripes.length; i++) {
            stripes[i] = new Object();
        }
    }

    /** Handles one ledger.commands message. Throws Transient (retry later) or Permanent (DLQ now). */
    public void handle(String payload) {
        Event ev = parse(payload);
        if (!ev.type().equals(BOOK) && !ev.type().equals(UNDO)) {
            return;
        }
        // Duplicates can land on different partitions/consumers at the same time. Serialize per payment within
        // this instance; across instances the Fineract idempotency key and the atomic updates below still hold.
        synchronized (stripes[Math.floorMod(ev.paymentId().hashCode(), stripes.length)]) {
            if (isProcessed(ev.eventId())) {
                meters.counter("ledger.commands", "outcome", "duplicate").increment();
                return;
            }
            if (ev.type().equals(BOOK)) {
                book(ev);
            } else {
                undo(ev);
            }
        }
    }

    // ---- BookLedger ----------------------------------------------------------------------------------

    private void book(Event ev) {
        Entry entry = findOrCreate(ev, "PENDING");
        switch (entry.status()) {
            case "BOOKED" -> {
                // The saga is asking again (it probably lost our reply): answer again, do not touch Fineract.
                tx.executeWithoutResult(s -> {
                    markProcessed(ev.eventId());
                    outbox(ev, "LedgerBooked", Map.of("fineractTransactionId", String.valueOf(entry.txnId())));
                });
                meters.counter("ledger.commands", "outcome", "already_booked").increment();
                return;
            }
            case "UNDONE" -> {
                // A late/re-ordered BookLedger after the saga already compensated: must NOT book.
                markProcessed(ev.eventId());
                meters.counter("ledger.commands", "outcome", "refused_after_undo").increment();
                return;
            }
            case "FAILED" -> jdbc.update("UPDATE ledger_entries SET status = 'PENDING', failure_reason = NULL, "
                    + "updated_at = now() WHERE id = ?", entry.id());   // replayed from the DLQ after a fix
            default -> { }
        }

        long savingsId = savingsAccountFor(ev.merchantId());
        String txnId = callFineract(entry, savingsId, "ledger-",
                (acct, key) -> fineract.deposit(acct, entry.amount(), key, "payment " + entry.paymentId()).transactionId());

        tx.executeWithoutResult(s -> {
            // Conditional update: only the caller that flips the row to BOOKED emits the event.
            int flipped = jdbc.update("UPDATE ledger_entries SET status = 'BOOKED', fineract_txn_id = ?, updated_at = now() "
                    + "WHERE id = ? AND status IN ('PENDING','FAILED')", txnId, entry.id());
            markProcessed(ev.eventId());
            if (flipped == 1) {
                outbox(ev, "LedgerBooked", Map.of("fineractTransactionId", txnId));
            }
        });
        meters.counter("ledger.commands", "outcome", "booked").increment();
    }

    // ---- UndoLedger (saga compensation) --------------------------------------------------------------

    private void undo(Event ev) {
        Entry entry = findOrCreate(ev, "UNDONE");   // no entry yet: record UNDONE so a late BookLedger is refused
        switch (entry.status()) {
            case "BOOKED" -> {
                long savingsId = savingsAccountFor(entry.merchantId());
                callFineract(entry, savingsId, "ledger-undo-",
                        (acct, key) -> fineract.undo(acct, entry.txnId(), key));
                tx.executeWithoutResult(s -> {
                    jdbc.update("UPDATE ledger_entries SET status = 'UNDONE', updated_at = now() WHERE id = ?", entry.id());
                    markProcessed(ev.eventId());
                    outbox(ev, "LedgerUndone", Map.of());
                });
            }
            default -> tx.executeWithoutResult(s -> {      // PENDING / FAILED / UNDONE: nothing (left) to reverse
                jdbc.update("UPDATE ledger_entries SET status = 'UNDONE', updated_at = now() WHERE id = ?", entry.id());
                markProcessed(ev.eventId());
                outbox(ev, "LedgerUndone", Map.of());
            });
        }
        meters.counter("ledger.commands", "outcome", "undone").increment();
    }

    /** Called when retries are exhausted (or the command is unprocessable). Tells the saga it failed. */
    public void deadLettered(String payload, String reason) {
        Event ev;
        try {
            ev = parse(payload);
        } catch (BookingException.Permanent e) {
            log.error("Unparseable message in DLQ, needs manual inspection: {}", payload);
            meters.counter("ledger.commands", "outcome", "poison").increment();
            return;
        }
        if (ev.type().equals(BOOK)) {
            tx.executeWithoutResult(s -> {
                findOrCreate(ev, "PENDING");
                int changed = jdbc.update("UPDATE ledger_entries SET status = 'FAILED', failure_reason = ?, updated_at = now() "
                        + "WHERE payment_id = ? AND status = 'PENDING'", reason, ev.paymentId());
                if (changed == 1) {
                    outbox(ev, "LedgerBookingFailed", Map.of("reason", String.valueOf(reason)));
                }
            });
        } else if (ev.type().equals(UNDO)) {
            tx.executeWithoutResult(s -> {
                jdbc.update("UPDATE ledger_entries SET failure_reason = ?, updated_at = now() WHERE payment_id = ?", reason, ev.paymentId());
                outbox(ev, "LedgerUndoFailed", Map.of("reason", String.valueOf(reason)));
            });
        } else {
            return;
        }
        meters.counter("ledger.commands", "outcome", "dead_lettered").increment();
    }

    // ---- Fineract call with 409 handling -------------------------------------------------------------

    private String callFineract(Entry entry, long savingsId, String keyPrefix, FineractCall call) {
        int attempt = entry.attempt();
        for (int i = 0; ; i++) {
            String key = keyPrefix + entry.paymentId() + "-" + attempt;
            try {
                return timed(() -> locks.withLock(savingsId, () -> call.run(savingsId, key)));
            } catch (ConflictException e) {
                meters.counter("ledger.fineract.conflicts").increment();
                attempt++;                                    // definitive failure: next try needs a new key
                jdbc.update("UPDATE ledger_entries SET attempt = ?, updated_at = now() WHERE id = ?", attempt, entry.id());
                if (i + 1 >= conflictRetries) {
                    throw new BookingException.Transient("Fineract kept returning conflicts: " + e.getMessage(), e);
                }
                sleep(50 + ThreadLocalRandom.current().nextLong(100));
            } catch (UnknownOutcomeException e) {
                // attempt NOT bumped: the next try reuses the same key, so Fineract can dedupe if it did apply it.
                throw new BookingException.Transient("Fineract outcome unknown: " + e.getMessage(), e);
            } catch (RejectedException e) {
                throw new BookingException.Permanent("Fineract rejected the request: " + e.getMessage(), e);
            }
        }
    }

    private <T> T timed(Supplier<T> call) {
        long start = System.nanoTime();
        String result = "ok";
        try {
            return call.get();
        } catch (ConflictException e) {
            result = "conflict";
            throw e;
        } catch (UnknownOutcomeException e) {
            result = "unknown";
            throw e;
        } catch (RejectedException e) {
            result = "rejected";
            throw e;
        } finally {
            Timer.builder("ledger.fineract.deposit").tag("outcome", result).publishPercentileHistogram()
                    .register(meters).record(Duration.ofNanos(System.nanoTime() - start));
        }
    }

    // ---- persistence ---------------------------------------------------------------------------------

    private Entry findOrCreate(Event ev, String initialStatus) {
        jdbc.update("INSERT INTO ledger_entries (id, payment_id, order_id, merchant_id, amount, currency, status) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (payment_id) DO NOTHING",
                UUID.randomUUID(), ev.paymentId(), ev.orderId(), ev.merchantId(), ev.amount(), ev.currency(), initialStatus);
        return jdbc.queryForObject("SELECT id, payment_id, amount, status, attempt, fineract_txn_id, merchant_id "
                        + "FROM ledger_entries WHERE payment_id = ?",
                (rs, i) -> new Entry(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class), rs.getLong(3),
                        rs.getString(4), rs.getInt(5), rs.getString(6), rs.getString(7)), ev.paymentId());
    }

    private long savingsAccountFor(String merchantId) {
        return jdbc.query("SELECT fineract_savings_id FROM merchant_accounts WHERE merchant_id = ?",
                        (rs, i) -> rs.getLong(1), merchantId).stream().findFirst()
                .orElseThrow(() -> new BookingException.Permanent("No Fineract account for merchant " + merchantId, null));
    }

    private boolean isProcessed(UUID eventId) {
        return jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_id = ?", Integer.class, eventId) > 0;
    }

    private void markProcessed(UUID eventId) {
        jdbc.update("INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING", eventId);
    }

    private void outbox(Event ev, String type, Map<String, Object> extra) {
        UUID id = UUID.randomUUID();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", id);
        m.put("type", type);
        m.put("paymentId", ev.paymentId());
        m.put("orderId", ev.orderId());
        m.put("amount", ev.amount());
        m.put("currency", ev.currency());
        m.putAll(extra);
        try {
            jdbc.update("INSERT INTO outbox (id, aggregate_id, topic, event_type, payload) VALUES (?, ?, ?, ?, ?)",
                    id, ev.orderId().toString(), TOPIC, type, mapper.writeValueAsString(m));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private Event parse(String payload) {
        try {
            JsonNode n = mapper.readTree(payload);
            String type = n.path("type").asText("");
            if (!type.equals(BOOK) && !type.equals(UNDO)) {
                return new Event(type, null, null, null, 0, null, null);
            }
            Event ev = new Event(type, UUID.fromString(n.path("eventId").asText()), UUID.fromString(n.path("paymentId").asText()),
                    UUID.fromString(n.path("orderId").asText()), n.path("amount").asLong(), n.path("currency").asText(""),
                    n.path("merchantId").asText(defaultMerchant));
            if (ev.amount() <= 0 || ev.currency().length() != 3) {
                throw new IllegalArgumentException("invalid amount/currency");
            }
            return ev;
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new BookingException.Permanent("Unprocessable command: " + e.getMessage(), e);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
