package dev.jenny.payments.reconciliationservice;

import dev.jenny.payments.reconciliationservice.Reconciler.Discrepancy;
import dev.jenny.payments.reconciliationservice.Sources.Charge;
import dev.jenny.payments.reconciliationservice.Sources.ChargeSource;
import dev.jenny.payments.reconciliationservice.Sources.Deposit;
import dev.jenny.payments.reconciliationservice.Sources.DepositSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Runs a reconciliation and keeps the discrepancy list up to date.
 *
 * Guarantees:
 * - If either source cannot be read completely, the run is recorded as FAILED and NOTHING else changes:
 *   a Stripe outage must not turn into "every payment is missing from the ledger".
 * - Differences are upserted per (payment, type): re-running does not create duplicates.
 * - A difference raises an alert only after it has been open for the grace period (work may be in flight);
 *   one that disappears is marked RESOLVED automatically.
 */
@Service
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    public record RunResult(UUID runId, String status, int charges, int deposits, int found, int alerts, String error) {}

    private final ChargeSource chargeSource;
    private final DepositSource depositSource;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;
    private final Duration lookback;
    private final Duration grace;
    private final Map<Reconciler.Type, AtomicInteger> openAlerts = new EnumMap<>(Reconciler.Type.class);

    ReconciliationService(ChargeSource chargeSource, DepositSource depositSource, JdbcTemplate jdbc, TransactionTemplate tx,
                          MeterRegistry meters, @Value("${reconciliation.lookback-hours:48}") long lookbackHours,
                          @Value("${reconciliation.grace-seconds:300}") long graceSeconds) {
        this.chargeSource = chargeSource;
        this.depositSource = depositSource;
        this.jdbc = jdbc;
        this.tx = tx;
        this.meters = meters;
        this.lookback = Duration.ofHours(lookbackHours);
        this.grace = Duration.ofSeconds(graceSeconds);
        for (Reconciler.Type t : Reconciler.Type.values()) {
            AtomicInteger g = new AtomicInteger();
            openAlerts.put(t, g);
            meters.gauge("reconciliation.open_alerts", io.micrometer.core.instrument.Tags.of("type", t.name()), g);
        }
    }

    public RunResult run() {
        UUID runId = UUID.randomUUID();
        jdbc.update("INSERT INTO reconciliation_runs (id, status) VALUES (?, 'RUNNING')", runId);
        Instant to = Instant.now();
        Instant from = to.minus(lookback);

        List<Charge> charges;
        List<Deposit> deposits;
        try {
            charges = chargeSource.fetch(from, to);
            deposits = depositSource.fetch(from, to);
        } catch (RuntimeException e) {
            log.error("Reconciliation run {} FAILED, no conclusions drawn: {}", runId, e.toString());
            jdbc.update("UPDATE reconciliation_runs SET status = 'FAILED', finished_at = now(), error = ? WHERE id = ?",
                    String.valueOf(e.getMessage()), runId);
            meters.counter("reconciliation.runs", "status", "failed").increment();
            return new RunResult(runId, "FAILED", 0, 0, 0, 0, e.getMessage());
        }

        List<Discrepancy> found = Reconciler.compare(charges, deposits);
        Set<UUID> seen = new HashSet<>();
        charges.forEach(c -> seen.add(c.paymentId()));
        deposits.forEach(d -> seen.add(d.paymentId()));

        int alerts = tx.execute(s -> {
            for (Discrepancy d : found) {
                jdbc.update("INSERT INTO discrepancies (id, payment_id, type, status, detail) VALUES (?, ?, ?, 'OPEN', ?) "
                                + "ON CONFLICT (payment_id, type) DO UPDATE SET detail = EXCLUDED.detail, last_seen = now(), "
                                + "status = 'OPEN', resolved_at = NULL, "
                                // reopening after RESOLVED starts a new grace period
                                + "first_seen = CASE WHEN discrepancies.status = 'RESOLVED' THEN now() ELSE discrepancies.first_seen END",
                        UUID.randomUUID(), d.paymentId(), d.type().name(), d.detail());
            }
            resolveGone(found, seen);
            return refreshAlerts();
        });

        jdbc.update("UPDATE reconciliation_runs SET status = 'OK', finished_at = now(), charges = ?, deposits = ?, found = ?, alerts = ? "
                + "WHERE id = ?", charges.size(), deposits.size(), found.size(), alerts, runId);
        meters.counter("reconciliation.runs", "status", "ok").increment();
        return new RunResult(runId, "OK", charges.size(), deposits.size(), found.size(), alerts, null);
    }

    /** Open differences that are no longer present, for payments we could actually see this time, are resolved. */
    private void resolveGone(List<Discrepancy> found, Set<UUID> seen) {
        Set<String> stillOpen = new HashSet<>();
        found.forEach(d -> stillOpen.add(d.paymentId() + "|" + d.type()));
        List<Map<String, Object>> open = jdbc.queryForList("SELECT payment_id, type FROM discrepancies WHERE status = 'OPEN'");
        for (Map<String, Object> row : open) {
            UUID paymentId = (UUID) row.get("payment_id");
            String key = paymentId + "|" + row.get("type");
            if (!stillOpen.contains(key) && seen.contains(paymentId)) {
                jdbc.update("UPDATE discrepancies SET status = 'RESOLVED', resolved_at = now() WHERE payment_id = ? AND type = ?",
                        paymentId, row.get("type"));
            }
        }
    }

    /** Differences open longer than the grace period are real alerts: log them and expose them as metrics. */
    private int refreshAlerts() {
        openAlerts.values().forEach(g -> g.set(0));
        int total = 0;
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT payment_id, type, detail FROM discrepancies WHERE status = 'OPEN' "
                        + "AND first_seen < now() - make_interval(secs => ?)", (double) grace.toSeconds());
        for (Map<String, Object> r : rows) {
            Reconciler.Type type = Reconciler.Type.valueOf((String) r.get("type"));
            openAlerts.get(type).incrementAndGet();
            total++;
            log.error("RECONCILIATION ALERT {} payment={} {}", type, r.get("payment_id"), r.get("detail"));
        }
        return total;
    }

    public List<Map<String, Object>> discrepancies(String status) {
        return jdbc.queryForList("SELECT payment_id, type, status, detail, first_seen, last_seen, resolved_at "
                + "FROM discrepancies WHERE status = ? ORDER BY first_seen", status);
    }
}
