package dev.jenny.payments.ledgerservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.jenny.payments.ledgerservice.FakeFineract.Mode;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Booking a captured payment into Fineract while Fineract and Kafka misbehave. */
class LedgerFaultTest extends AbstractIntegrationTest {

    @Test
    void capturedEventIsBookedOnceWithTheRightAmountAndKey() {
        UUID pid = UUID.randomUUID();
        publish(captured(pid, 1050));
        awaitStatus(pid, "BOOKED");

        List<FakeFineract.Req> reqs = FINERACT.requestsFor(pid.toString());
        assertThat(reqs).hasSize(1);
        assertThat(reqs.get(0).key()).isEqualTo("ledger-" + pid + "-1");
        assertThat(reqs.get(0).savingsId()).isEqualTo(77);
        assertThat(reqs.get(0).body()).contains("10.50");           // 1050 cents -> 10.50
        assertThat(outboxCount(pid, "LedgerBooked")).isEqualTo(1);
    }

    @Test
    void sameEventDeliveredTwiceBooksOnce() {
        UUID pid = UUID.randomUUID();
        String payload = captured(pid, 1000);
        publish(payload);
        publish(payload);
        awaitStatus(pid, "BOOKED");
        sleep(1500);
        assertThat(FINERACT.requestsFor(pid.toString())).hasSize(1);
        assertThat(outboxCount(pid, "LedgerBooked")).isEqualTo(1);
    }

    @Test
    void samePaymentUnderADifferentEventIdBooksOnce() {
        UUID pid = UUID.randomUUID();
        UUID order = UUID.randomUUID();
        publish(event("BookLedger", UUID.randomUUID(), pid, order, 1000, null));
        awaitStatus(pid, "BOOKED");
        publish(event("BookLedger", UUID.randomUUID(), pid, order, 1000, null));   // saga re-sent it (lost our reply)
        await().atMost(Duration.ofSeconds(30)).until(() -> outboxCount(pid, "LedgerBooked") == 2);
        assertThat(FINERACT.requestsFor(pid.toString())).hasSize(1);               // answered again, Fineract untouched
    }

    @Test
    void authorizeEventsAreIgnored() {
        UUID pid = UUID.randomUUID();
        publish(event("PaymentAuthorized", UUID.randomUUID(), pid, UUID.randomUUID(), 1000, null));
        sleep(2000);
        assertThat(statusOf(pid)).isNull();
        assertThat(FINERACT.requestsFor(pid.toString())).isEmpty();
    }

    /** Fineract rolled back (409): the retry MUST use a new key, otherwise Fineract replays the failure. */
    @Test
    void conflictsAreRetriedWithANewKeyEachTime() {
        UUID pid = UUID.randomUUID();
        FINERACT.script.addAll(List.of(Mode.CONFLICT, Mode.CONFLICT));
        publish(captured(pid, 1000));
        awaitStatus(pid, "BOOKED");

        List<String> keys = FINERACT.requestsFor(pid.toString()).stream().map(FakeFineract.Req::key).toList();
        assertThat(keys).containsExactly("ledger-" + pid + "-1", "ledger-" + pid + "-2", "ledger-" + pid + "-3");
        assertThat(FINERACT.appliedFor(pid.toString())).isEqualTo(1);
    }

    /** The scary one: Fineract applied the deposit but we never saw the answer. Must reuse the SAME key. */
    @Test
    void timeoutAfterApplyIsRetriedWithTheSameKeyAndNeverDoubleBooks() {
        UUID pid = UUID.randomUUID();
        FINERACT.script.add(Mode.TIMEOUT_AFTER_APPLY);
        publish(captured(pid, 1000));
        awaitStatus(pid, "BOOKED");

        List<String> keys = FINERACT.requestsFor(pid.toString()).stream().map(FakeFineract.Req::key).toList();
        assertThat(keys).hasSize(2).containsOnly("ledger-" + pid + "-1");
        assertThat(FINERACT.appliedFor(pid.toString())).isEqualTo(1);
    }

    @Test
    void fineractOutageRecoversThroughRetryTopics() {
        UUID pid = UUID.randomUUID();
        FINERACT.script.addAll(List.of(Mode.SERVER_ERROR, Mode.SERVER_ERROR));
        publish(captured(pid, 1000));
        awaitStatus(pid, "BOOKED");

        List<String> keys = FINERACT.requestsFor(pid.toString()).stream().map(FakeFineract.Req::key).toList();
        assertThat(keys).hasSize(3).containsOnly("ledger-" + pid + "-1");        // unknown outcome -> same key
    }

    /** Fineract stays down: retries are bounded, then the DLQ handler marks FAILED and tells the saga. */
    @Test
    void permanentOutageEndsInFailedAndEmitsLedgerBookingFailed() {
        UUID pid = UUID.randomUUID();
        FINERACT.defaultMode = Mode.SERVER_ERROR;
        publish(captured(pid, 1000));
        awaitStatus(pid, "FAILED");

        assertThat(FINERACT.requestsFor(pid.toString())).hasSize(4);              // configured attempts, no more
        assertThat(outboxCount(pid, "LedgerBookingFailed")).isEqualTo(1);
        assertThat(outboxCount(pid, "LedgerBooked")).isZero();
    }

    @Test
    void rejectedByFineractGoesStraightToTheDlqWithoutRetries() {
        UUID pid = UUID.randomUUID();
        FINERACT.defaultMode = Mode.BAD_REQUEST;
        publish(captured(pid, 1000));
        awaitStatus(pid, "FAILED");

        sleep(1500);
        assertThat(FINERACT.requestsFor(pid.toString())).hasSize(1);
        assertThat(outboxCount(pid, "LedgerBookingFailed")).isEqualTo(1);
    }

    @Test
    void unknownMerchantIsDeadLetteredWithoutCallingFineract() {
        UUID pid = UUID.randomUUID();
        publish(event("BookLedger", UUID.randomUUID(), pid, UUID.randomUUID(), 1000, "no-such-merchant"));
        awaitStatus(pid, "FAILED");
        assertThat(FINERACT.requestsFor(pid.toString())).isEmpty();
    }

    /** A garbage message must not wedge the consumer: later messages still get processed. */
    @Test
    void poisonMessageDoesNotBlockTheQueue() {
        publish("this is not json");
        UUID pid = UUID.randomUUID();
        publish(captured(pid, 1000));
        awaitStatus(pid, "BOOKED");
    }

    /** Many payments to ONE Fineract account at once: our per-account lock keeps Fineract writes serial. */
    @Test
    void hotAccountWritesAreSerialized() {
        FINERACT.delayMs = 60;
        List<UUID> pids = java.util.stream.IntStream.range(0, 24).mapToObj(i -> UUID.randomUUID()).toList();
        pids.forEach(pid -> publish(captured(pid, 1000)));
        pids.forEach(pid -> awaitStatus(pid, "BOOKED"));

        assertThat(FINERACT.maxInFlight.get()).isEqualTo(1);
        pids.forEach(pid -> assertThat(FINERACT.appliedFor(pid.toString())).isEqualTo(1));
    }

    // ---- saga compensation: UndoLedger --------------------------------------------------------------

    @Test
    void undoReversesTheBookedDepositInFineract() {
        UUID pid = UUID.randomUUID();
        publish(captured(pid, 1000));
        awaitStatus(pid, "BOOKED");
        String txn = jdbc.queryForObject("SELECT fineract_txn_id FROM ledger_entries WHERE payment_id = ?", String.class, pid);

        publish(undoCommand(pid, 1000));
        awaitStatus(pid, "UNDONE");

        assertThat(FINERACT.undoneTxns).contains(txn);
        assertThat(FINERACT.undoRequests).hasSize(1);
        assertThat(FINERACT.undoRequests.get(0).key()).startsWith("ledger-undo-" + pid);
        await().atMost(Duration.ofSeconds(30)).until(() -> outboxCount(pid, "LedgerUndone") == 1);
    }

    @Test
    void undoWithNothingBookedSucceedsWithoutTouchingFineract() {
        UUID pid = UUID.randomUUID();
        publish(undoCommand(pid, 1000));
        awaitStatus(pid, "UNDONE");
        await().atMost(Duration.ofSeconds(30)).until(() -> outboxCount(pid, "LedgerUndone") == 1);
        assertThat(FINERACT.requestsFor(pid.toString())).isEmpty();
        assertThat(FINERACT.undoRequests).isEmpty();
    }

    /** Re-ordering: the compensation overtook the original command. The late BookLedger must be refused. */
    @Test
    void lateBookLedgerAfterUndoIsRefused() {
        UUID pid = UUID.randomUUID();
        publish(undoCommand(pid, 1000));
        awaitStatus(pid, "UNDONE");
        publish(captured(pid, 1000));
        sleep(2000);
        assertThat(statusOf(pid)).isEqualTo("UNDONE");
        assertThat(FINERACT.requestsFor(pid.toString())).isEmpty();
    }

    /** Compensation itself fails: the entry stays BOOKED and the saga is told so a human can step in. */
    @Test
    void failedUndoStaysBookedAndReportsLedgerUndoFailed() {
        UUID pid = UUID.randomUUID();
        publish(captured(pid, 1000));
        awaitStatus(pid, "BOOKED");

        FINERACT.undoMode = Mode.BAD_REQUEST;
        publish(undoCommand(pid, 1000));
        await().atMost(Duration.ofSeconds(60)).until(() -> outboxCount(pid, "LedgerUndoFailed") == 1);
        assertThat(statusOf(pid)).isEqualTo("BOOKED");
    }

    // ---- operations: DLQ replay ----------------------------------------------------------------------

    @Autowired DlqAdminController dlqAdmin;

    /** Fineract rejected the booking (misconfigured account). After the fix, the operator replays the DLQ. */
    @Test
    void dlqReplayBooksAfterTheCauseIsFixed() {
        UUID pid = UUID.randomUUID();
        FINERACT.defaultMode = Mode.BAD_REQUEST;
        publish(captured(pid, 1000));
        awaitStatus(pid, "FAILED");

        publish("poison that can never succeed");        // must be skipped, not replayed forever
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300)).until(() -> true);

        FINERACT.defaultMode = Mode.OK;                 // "fix the cause"
        long start = System.nanoTime();
        var result = dlqAdmin.replay(500);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).as("replay must terminate").isLessThan(Duration.ofSeconds(30));
        assertThat((int) result.get("skipped")).isGreaterThanOrEqualTo(1);
        awaitStatus(pid, "BOOKED");
        assertThat(FINERACT.appliedFor(pid.toString())).isEqualTo(1);
    }

    private static void sleep(long ms) {
        await().pollDelay(Duration.ofMillis(ms)).atMost(Duration.ofMillis(ms + 2000)).until(() -> true);
    }
}
