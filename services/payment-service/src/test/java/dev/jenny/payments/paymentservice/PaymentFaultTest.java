package dev.jenny.payments.paymentservice;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jenny.payments.paymentservice.FakeGateway.Mode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** Payment lifecycle under "things that go wrong": declines, timeouts, crashes, double clicks, races. */
class PaymentFaultTest extends AbstractIntegrationTest {

    private String authorized(UUID orderId) {
        ResponseEntity<String> r = authorize(UUID.randomUUID().toString(), body(orderId, 5000));
        assertThat(statusOf(r)).isEqualTo("AUTHORIZED");
        return idOf(r);
    }

    @Test
    void happyPathAuthorizesOnceAndEmitsEvent() {
        UUID order = UUID.randomUUID();
        ResponseEntity<String> r = authorize(UUID.randomUUID().toString(), body(order, 5000));

        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(statusOf(r)).isEqualTo("AUTHORIZED");
        assertThat(gateway.count("authorize", "pay-auth-" + idOf(r))).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = 'PaymentAuthorized'",
                order.toString())).isEqualTo(1);
    }

    @Test
    void doubleClickWithSameKeyCallsProcessorOnce() {
        UUID order = UUID.randomUUID();
        String key = UUID.randomUUID().toString();
        ResponseEntity<String> first = authorize(key, body(order, 5000));
        ResponseEntity<String> second = authorize(key, body(order, 5000));

        assertThat(second.getBody()).isEqualTo(first.getBody());
        assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(gateway.count("authorize")).isEqualTo(1);
    }

    @Test
    void declinedCardEndsInFailedAndCannotBeCaptured() {
        gateway.authorizeMode = Mode.DECLINE;
        UUID order = UUID.randomUUID();
        ResponseEntity<String> r = authorize(UUID.randomUUID().toString(), body(order, 5000));

        assertThat(statusOf(r)).isEqualTo("FAILED");
        assertThat(r.getBody()).contains("card_declined");
        assertThat(action(idOf(r), "capture").getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = 'PaymentFailed'",
                order.toString())).isEqualTo(1);
    }

    /** Stripe timed out but DID reserve the funds. Retrying with the same key must not reserve twice. */
    @Test
    void timeoutThenRetryWithSameKeyResumesWithoutDoubleCharge() {
        UUID order = UUID.randomUUID();
        String key = UUID.randomUUID().toString();

        gateway.authorizeMode = Mode.TIMEOUT;
        ResponseEntity<String> first = authorize(key, body(order, 5000));
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(jdbc.queryForObject("SELECT status FROM payments WHERE order_id = ?", String.class, order)).isEqualTo("PENDING");

        gateway.authorizeMode = Mode.OK;
        ResponseEntity<String> retry = authorize(key, body(order, 5000));
        assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(statusOf(retry)).isEqualTo("AUTHORIZED");
        assertThat(gateway.count("authorize", "pay-auth-" + idOf(retry))).isEqualTo(2);   // same stable key both times
        assertThat(gateway.distinctIntents()).isEqualTo(1);                               // processor acted once
    }

    /** Process dies mid-flow (unexpected error): row is left PENDING and a later request finishes the job. */
    @Test
    void crashLeavesPendingAndANewRequestResumesTheSamePayment() {
        UUID order = UUID.randomUUID();
        gateway.authorizeMode = Mode.CRASH;
        assertThat(authorize(UUID.randomUUID().toString(), body(order, 5000)).getStatusCode().is5xxServerError()).isTrue();
        String paymentId = jdbc.queryForObject("SELECT id::text FROM payments WHERE order_id = ?", String.class, order);

        gateway.authorizeMode = Mode.OK;
        ResponseEntity<String> retry = authorize(UUID.randomUUID().toString(), body(order, 5000));
        assertThat(idOf(retry)).isEqualTo(paymentId);
        assertThat(statusOf(retry)).isEqualTo("AUTHORIZED");
        assertThat(count("SELECT count(*) FROM payments WHERE order_id = ?", order)).isEqualTo(1);
    }

    @Test
    void sameOrderWithDifferentAmountIsRejected() {
        UUID order = UUID.randomUUID();
        authorize(UUID.randomUUID().toString(), body(order, 5000));
        assertThat(authorize(UUID.randomUUID().toString(), body(order, 9999)).getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void captureTwiceChargesOnce() {
        String id = authorized(UUID.randomUUID());
        assertThat(statusOf(action(id, "capture"))).isEqualTo("CAPTURED");
        assertThat(statusOf(action(id, "capture"))).isEqualTo("CAPTURED");
        assertThat(gateway.count("capture")).isEqualTo(1);
    }

    @Test
    void cannotCancelAfterCaptureOrCaptureAfterCancel() {
        String captured = authorized(UUID.randomUUID());
        action(captured, "capture");
        assertThat(action(captured, "cancel").getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        String canceled = authorized(UUID.randomUUID());
        assertThat(statusOf(action(canceled, "cancel"))).isEqualTo("CANCELED");
        assertThat(action(canceled, "capture").getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(gateway.count("capture")).isEqualTo(1);
    }

    /** Ten simultaneous capture clicks: the compare-and-set lets exactly one reach the processor. */
    @Test
    void concurrentCapturesReachTheProcessorOnce() throws Exception {
        String id = authorized(UUID.randomUUID());
        gateway.captureDelayMs = 400;

        ExecutorService pool = Executors.newFixedThreadPool(10);
        List<Future<HttpStatus>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            results.add(pool.submit(() -> HttpStatus.valueOf(action(id, "capture").getStatusCode().value())));
        }
        for (Future<HttpStatus> f : results) {
            assertThat(f.get()).isIn(HttpStatus.OK, HttpStatus.CONFLICT);
        }
        pool.shutdown();

        assertThat(gateway.count("capture")).isEqualTo(1);
        assertThat(dbStatus(id)).isEqualTo("CAPTURED");
    }

    /** Capture times out: claim is held (no double capture), and becomes retryable once it goes stale. */
    @Test
    void captureTimeoutIsRetryableAfterClaimGoesStale() {
        String id = authorized(UUID.randomUUID());
        gateway.captureMode = Mode.TIMEOUT;
        assertThat(action(id, "capture").getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(dbStatus(id)).isEqualTo("CAPTURING");

        gateway.captureMode = Mode.OK;
        assertThat(action(id, "capture").getStatusCode()).isEqualTo(HttpStatus.CONFLICT);   // fresh claim: not yet

        jdbc.update("UPDATE payments SET updated_at = now() - interval '1 minute' WHERE id = ?::uuid", id);
        assertThat(statusOf(action(id, "capture"))).isEqualTo("CAPTURED");
        assertThat(gateway.count("capture", "pay-cap-" + id)).isEqualTo(2);                  // stable key both times
    }

    /** Events for one order reach Kafka in lifecycle order (partition key = orderId). */
    @Test
    void lifecycleEventsAreKeyedByOrderAndInOrder() {
        UUID order = UUID.randomUUID();
        String id = idOf(authorize(UUID.randomUUID().toString(), body(order, 5000)));
        action(id, "capture");

        List<String> types = readTopic("payment.events", Duration.ofSeconds(8)).stream()
                .filter(r -> order.toString().equals(r.key()))
                .map(ConsumerRecord::value)
                .map(v -> v.replaceAll(".*\"type\":\"([A-Za-z]+)\".*", "$1"))
                .toList();
        assertThat(types).containsExactly("PaymentAuthorized", "PaymentCaptured");
    }
}
