package dev.jenny.payments.orderservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.jenny.payments.orderservice.FakePayments.Reply;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * The order saga against a fake payment-service (HTTP) while the test plays the roles of payment-service and
 * ledger-service by publishing their events. Covers the happy path, every compensation, and the nasty cases:
 * duplicate / re-ordered / late events, a down dependency, lost replies and exhausted retries.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SagaTest extends AbstractIntegrationTest {

    @Autowired KafkaTemplate<String, String> kafka;

    @BeforeEach
    void reset() {
        PAYMENTS.reset();
    }

    // ---- helpers ------------------------------------------------------------------------------------

    record Flow(String orderId, UUID paymentId) {}

    private String newOrder() {
        return orderIdOf(post(UUID.randomUUID().toString(), "{\"amount\":1000,\"currency\":\"USD\",\"merchantId\":\"m1\"}"));
    }

    private void emit(String topic, String type, String orderId, UUID paymentId) {
        kafka.send(topic, orderId, "{\"eventId\":\"" + UUID.randomUUID() + "\",\"type\":\"" + type + "\",\"orderId\":\""
                + orderId + "\",\"paymentId\":\"" + paymentId + "\",\"reason\":\"test\"}");
    }

    private String status(String orderId) {
        return jdbc.queryForObject("SELECT status FROM orders WHERE id = ?::uuid", String.class, orderId);
    }

    private void awaitStatus(String orderId, String expected) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200)).until(() -> expected.equals(status(orderId)));
    }

    private int outbox(String orderId, String type) {
        return count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = ?", orderId, type);
    }

    private void awaitAuthorizeCalled(String orderId) {
        await().atMost(Duration.ofSeconds(30)).until(() -> !PAYMENTS.callsFor("authorize", orderId).isEmpty());
    }

    /** Order created, payment authorized by "payment-service", saga now waiting for the ledger. */
    private Flow orderInBooking() {
        String orderId = newOrder();
        awaitAuthorizeCalled(orderId);
        UUID paymentId = UUID.randomUUID();
        emit("payment.events", "PaymentAuthorized", orderId, paymentId);
        awaitStatus(orderId, "BOOKING");
        return new Flow(orderId, paymentId);
    }

    private void settle() {
        await().pollDelay(Duration.ofMillis(1500)).atMost(Duration.ofSeconds(5)).until(() -> true);
    }

    // ---- happy path -----------------------------------------------------------------------------------

    @Test
    void happyPathAuthorizesBooksCapturesAndCompletes() {
        String orderId = newOrder();
        awaitAuthorizeCalled(orderId);
        assertThat(PAYMENTS.callsFor("authorize", orderId).get(0).idempotencyKey()).isEqualTo("saga-" + orderId + "-authorize");
        assertThat(status(orderId)).isEqualTo("AUTHORIZING");

        UUID paymentId = UUID.randomUUID();
        emit("payment.events", "PaymentAuthorized", orderId, paymentId);
        awaitStatus(orderId, "BOOKING");
        await().atMost(Duration.ofSeconds(30)).until(() -> outbox(orderId, "BookLedger") == 1);
        List<ConsumerRecord<String, String>> commands = readTopic("ledger.commands", Duration.ofSeconds(2)).stream()
                .filter(r -> orderId.equals(r.key())).toList();
        assertThat(commands).isNotEmpty();      // the watchdog may legitimately re-send if reading takes long
        assertThat(commands.get(0).value()).contains("BookLedger").contains(paymentId.toString()).contains("\"merchantId\":\"m1\"");

        emit("ledger.events", "LedgerBooked", orderId, paymentId);
        awaitStatus(orderId, "CAPTURING");
        await().atMost(Duration.ofSeconds(30)).until(() -> PAYMENTS.callsFor("capture", paymentId.toString()).size() >= 1);

        emit("payment.events", "PaymentCaptured", orderId, paymentId);
        awaitStatus(orderId, "COMPLETED");
        assertThat(outbox(orderId, "UndoLedger")).isZero();
    }

    // ---- compensations --------------------------------------------------------------------------------

    @Test
    void declinedCardEndsInPaymentFailedWithoutTouchingTheLedger() {
        String orderId = newOrder();
        awaitAuthorizeCalled(orderId);
        emit("payment.events", "PaymentFailed", orderId, UUID.randomUUID());
        awaitStatus(orderId, "PAYMENT_FAILED");
        settle();
        assertThat(outbox(orderId, "BookLedger")).isZero();
        assertThat(PAYMENTS.calls.stream().filter(c -> c.op().equals("capture"))).isEmpty();
    }

    /** Ledger cannot book: only release the authorization. Capture must never happen. */
    @Test
    void ledgerFailureVoidsTheAuthorizationAndNeverCaptures() {
        Flow f = orderInBooking();
        emit("ledger.events", "LedgerBookingFailed", f.orderId(), f.paymentId());
        awaitStatus(f.orderId(), "VOIDING");
        await().atMost(Duration.ofSeconds(30)).until(() -> PAYMENTS.callsFor("cancel", f.paymentId().toString()).size() >= 1);

        emit("payment.events", "PaymentCanceled", f.orderId(), f.paymentId());
        awaitStatus(f.orderId(), "CANCELED");
        assertThat(PAYMENTS.callsFor("capture", f.paymentId().toString())).isEmpty();
    }

    /** Capture refused after the ledger already booked: undo the ledger entry first, then void. */
    @Test
    void captureRefusedUndoesTheLedgerThenVoids() {
        PAYMENTS.always("capture", Reply.CONFLICT);
        Flow f = orderInBooking();
        emit("ledger.events", "LedgerBooked", f.orderId(), f.paymentId());
        awaitStatus(f.orderId(), "UNDOING_LEDGER");
        await().atMost(Duration.ofSeconds(30)).until(() -> outbox(f.orderId(), "UndoLedger") == 1);

        emit("ledger.events", "LedgerUndone", f.orderId(), f.paymentId());
        awaitStatus(f.orderId(), "VOIDING");
        await().atMost(Duration.ofSeconds(30)).until(() -> PAYMENTS.callsFor("cancel", f.paymentId().toString()).size() >= 1);

        emit("payment.events", "PaymentCanceled", f.orderId(), f.paymentId());
        awaitStatus(f.orderId(), "CANCELED");
    }

    @Test
    void failedLedgerUndoNeedsAHuman() {
        PAYMENTS.always("capture", Reply.CONFLICT);
        Flow f = orderInBooking();
        emit("ledger.events", "LedgerBooked", f.orderId(), f.paymentId());
        awaitStatus(f.orderId(), "UNDOING_LEDGER");

        emit("ledger.events", "LedgerUndoFailed", f.orderId(), f.paymentId());
        awaitStatus(f.orderId(), "MANUAL_REVIEW");
        assertThat(jdbc.queryForObject("SELECT failure_reason FROM orders WHERE id = ?::uuid", String.class, f.orderId()))
                .contains("ledger undo failed");
    }

    @Test
    void refusedVoidNeedsAHuman() {
        PAYMENTS.always("cancel", Reply.CONFLICT);
        Flow f = orderInBooking();
        emit("ledger.events", "LedgerBookingFailed", f.orderId(), f.paymentId());
        awaitStatus(f.orderId(), "MANUAL_REVIEW");
    }

    // ---- unreliable world -----------------------------------------------------------------------------

    /** payment-service is down when the order arrives: retried with the SAME idempotency key until it answers. */
    @Test
    void paymentServiceOutageAtStartIsRetriedWithTheSameKey() {
        PAYMENTS.script("authorize", Reply.UNAVAILABLE, Reply.UNAVAILABLE);
        String orderId = newOrder();
        await().atMost(Duration.ofSeconds(60)).until(() -> PAYMENTS.callsFor("authorize", orderId).size() >= 3);

        List<String> keys = PAYMENTS.callsFor("authorize", orderId).stream().map(FakePayments.Call::idempotencyKey).distinct().toList();
        assertThat(keys).containsExactly("saga-" + orderId + "-authorize");
        assertThat(status(orderId)).isEqualTo("AUTHORIZING");
    }

    /** Duplicate, out-of-order and late events must not double-book, double-capture or move the order backwards. */
    @Test
    void duplicateOutOfOrderAndLateEventsAreHarmless() {
        Flow f = orderInBooking();
        await().atMost(Duration.ofSeconds(30)).until(() -> outbox(f.orderId(), "BookLedger") == 1);

        emit("payment.events", "PaymentAuthorized", f.orderId(), f.paymentId());        // duplicate
        emit("payment.events", "PaymentCaptured", f.orderId(), f.paymentId());          // too early: still BOOKING
        settle();
        assertThat(status(f.orderId())).isEqualTo("BOOKING");
        assertThat(outbox(f.orderId(), "BookLedger")).isEqualTo(1);

        emit("ledger.events", "LedgerBooked", f.orderId(), f.paymentId());
        emit("ledger.events", "LedgerBooked", f.orderId(), f.paymentId());              // duplicate
        awaitStatus(f.orderId(), "CAPTURING");
        emit("payment.events", "PaymentCaptured", f.orderId(), f.paymentId());
        awaitStatus(f.orderId(), "COMPLETED");

        emit("payment.events", "PaymentAuthorized", f.orderId(), f.paymentId());        // late
        emit("ledger.events", "LedgerBookingFailed", f.orderId(), f.paymentId());       // late
        emit("payment.events", "PaymentCanceled", f.orderId(), f.paymentId());          // late
        settle();
        assertThat(status(f.orderId())).isEqualTo("COMPLETED");
        assertThat(outbox(f.orderId(), "BookLedger")).isEqualTo(1);
        assertThat(outbox(f.orderId(), "UndoLedger")).isZero();
    }

    /** The ledger reply is lost: the watchdog re-sends the command, and gives up (MANUAL_REVIEW) after the limit. */
    @Test
    void lostLedgerReplyIsRedrivenThenEscalated() {
        Flow f = orderInBooking();
        await().atMost(Duration.ofSeconds(60)).until(() -> outbox(f.orderId(), "BookLedger") >= 2);
        awaitStatus(f.orderId(), "MANUAL_REVIEW");
        assertThat(outbox(f.orderId(), "BookLedger")).isEqualTo(3);                      // initial + 2 re-drives
        assertThat(jdbc.queryForObject("SELECT failure_reason FROM orders WHERE id = ?::uuid", String.class, f.orderId()))
                .contains("stuck in BOOKING");
    }

    /** Capture keeps failing: bounded retries, then a human, never an endless loop. */
    @Test
    void exhaustedCaptureRetriesEndInManualReview() {
        PAYMENTS.always("capture", Reply.UNAVAILABLE);
        Flow f = orderInBooking();
        emit("ledger.events", "LedgerBooked", f.orderId(), f.paymentId());
        awaitStatus(f.orderId(), "MANUAL_REVIEW");
        assertThat(jdbc.queryForObject("SELECT failure_reason FROM orders WHERE id = ?::uuid", String.class, f.orderId())).isNotNull();
    }

    @Test
    void eventsForUnknownOrdersAreIgnoredAndDoNotBlockOthers() {
        String ghost = UUID.randomUUID().toString();
        emit("payment.events", "PaymentAuthorized", ghost, UUID.randomUUID());
        emit("ledger.events", "LedgerBooked", ghost, UUID.randomUUID());
        emit("payment.events", "PaymentCaptured", ghost, UUID.randomUUID());

        String orderId = newOrder();
        awaitAuthorizeCalled(orderId);
        assertThat(status(orderId)).isEqualTo("AUTHORIZING");
    }
}
