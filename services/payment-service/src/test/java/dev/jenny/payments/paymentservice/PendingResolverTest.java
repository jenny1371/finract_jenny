package dev.jenny.payments.paymentservice;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jenny.payments.common.RetentionCleaner;
import dev.jenny.payments.paymentservice.FakeGateway.Mode;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

/** Payments stuck in PENDING (processor timed out, nobody retried) and the housekeeping around webhooks. */
class PendingResolverTest extends AbstractIntegrationTest {

    @Autowired PaymentService payments;
    @Autowired RetentionCleaner cleaner;

    private UUID[] stuckPending() {
        UUID order = UUID.randomUUID();
        gateway.authorizeMode = Mode.TIMEOUT;
        assertThat(authorize(UUID.randomUUID().toString(), body(order, 5000)).getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        gateway.authorizeMode = Mode.OK;
        UUID payment = jdbc.queryForObject("SELECT id FROM payments WHERE order_id = ?", UUID.class, order);
        jdbc.update("UPDATE payments SET updated_at = now() - interval '5 minutes' WHERE id = ?", payment);
        return new UUID[] {order, payment};
    }

    private int events(UUID order, String type) {
        return count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = ?", order.toString(), type);
    }

    @Test
    void resolverCompletesAStuckPaymentWithTheSameProcessorKey() {
        UUID[] p = stuckPending();
        payments.sweepPending();

        assertThat(dbStatus(p[1].toString())).isEqualTo("AUTHORIZED");
        assertThat(events(p[0], "PaymentAuthorized")).isEqualTo(1);
        assertThat(gateway.count("authorize", "pay-auth-" + p[1])).isEqualTo(2);     // timeout, then the resolver: same key
        assertThat(gateway.distinctIntents()).isEqualTo(1);                           // the processor acted once
    }

    @Test
    void resolverTurnsADeclineIntoFailed() {
        UUID[] p = stuckPending();
        gateway.authorizeMode = Mode.DECLINE;
        payments.sweepPending();
        assertThat(dbStatus(p[1].toString())).isEqualTo("FAILED");
        assertThat(events(p[0], "PaymentFailed")).isEqualTo(1);
    }

    @Test
    void resolverKeepsTryingWhileTheProcessorIsStillUnreachable() {
        UUID[] p = stuckPending();
        gateway.authorizeMode = Mode.TIMEOUT;
        payments.sweepPending();

        assertThat(dbStatus(p[1].toString())).isEqualTo("PENDING");
        assertThat(count("SELECT resolve_attempts FROM payments WHERE id = ?::uuid", p[1].toString())).isEqualTo(1);
        assertThat(events(p[0], "PaymentAuthorized")).isZero();
        assertThat(events(p[0], "PaymentFailed")).isZero();
    }

    /** Past the give-up age the processor key may have expired: fail instead of risking a second authorization. */
    @Test
    void resolverGivesUpOnVeryOldPaymentsWithoutCallingTheProcessor() {
        UUID[] p = stuckPending();
        jdbc.update("UPDATE payments SET created_at = now() - interval '13 hours' WHERE id = ?", p[1]);
        long callsBefore = gateway.count("authorize");
        payments.sweepPending();

        assertThat(dbStatus(p[1].toString())).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT failure_code FROM payments WHERE id = ?::uuid", String.class, p[1].toString()))
                .isEqualTo("unresolved_timeout");
        assertThat(events(p[0], "PaymentFailed")).isEqualTo(1);
        assertThat(gateway.count("authorize")).isEqualTo(callsBefore);
    }

    @Test
    void resolverLeavesFreshPendingAndFinishedPaymentsAlone() {
        UUID order = UUID.randomUUID();
        gateway.authorizeMode = Mode.TIMEOUT;
        authorize(UUID.randomUUID().toString(), body(order, 5000));                   // PENDING, but updated just now
        gateway.authorizeMode = Mode.OK;
        UUID done = UUID.randomUUID();
        authorize(UUID.randomUUID().toString(), body(done, 5000));                    // AUTHORIZED
        long callsBefore = gateway.count("authorize");

        payments.sweepPending();

        assertThat(jdbc.queryForObject("SELECT status FROM payments WHERE order_id = ?", String.class, order)).isEqualTo("PENDING");
        assertThat(gateway.count("authorize")).isEqualTo(callsBefore);
    }

    /** A webhook may settle the payment between the resolver reading it and acting on it: nothing may be duplicated. */
    @Test
    void resolverDoesNotDuplicateAnEventWhenAWebhookAlreadyResolvedThePayment() {
        UUID[] p = stuckPending();
        jdbc.update("UPDATE payments SET status = 'AUTHORIZED', stripe_intent_id = 'pi_webhook' WHERE id = ?", p[1]);
        payments.sweepPending();
        assertThat(dbStatus(p[1].toString())).isEqualTo("AUTHORIZED");
        assertThat(events(p[0], "PaymentAuthorized")).isZero();                       // none: it was not the resolver's doing
    }

    // ---- retention of webhook de-dup rows ----------------------------------------------------------

    @Test
    void retentionDeletesOnlyOldDeduplicationRowsInBatches() {
        for (int i = 0; i < 25; i++) {
            jdbc.update("INSERT INTO webhook_events (event_id, type, received_at) VALUES (?, 't', now() - interval '40 days')", "old-" + UUID.randomUUID());
        }
        String fresh = "fresh-" + UUID.randomUUID();
        jdbc.update("INSERT INTO webhook_events (event_id, type) VALUES (?, 't')", fresh);

        int deleted = cleaner.purge("webhook_events", "received_at", Duration.ofDays(30), 10);   // 3 batches: 10 + 10 + 5

        assertThat(deleted).isGreaterThanOrEqualTo(25);
        assertThat(count("SELECT count(*) FROM webhook_events WHERE event_id = ?", fresh)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM webhook_events WHERE received_at < now() - interval '30 days'")).isZero();
    }
}
