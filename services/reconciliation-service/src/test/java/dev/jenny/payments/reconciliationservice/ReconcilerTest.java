package dev.jenny.payments.reconciliationservice;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jenny.payments.reconciliationservice.Reconciler.Discrepancy;
import dev.jenny.payments.reconciliationservice.Reconciler.Type;
import dev.jenny.payments.reconciliationservice.Sources.Charge;
import dev.jenny.payments.reconciliationservice.Sources.Deposit;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Pure comparison logic: no database, no Docker, runs in milliseconds. */
class ReconcilerTest {

    private static Charge charge(UUID id, long amount, String currency) {
        return new Charge(id, amount, currency, Instant.now());
    }

    private static Deposit deposit(UUID id, String txn, long amount, String currency) {
        return new Deposit(id, txn, amount, currency);
    }

    @Test
    void identicalBooksProduceNoDiscrepancies() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        assertThat(Reconciler.compare(
                List.of(charge(a, 1000, "USD"), charge(b, 250, "USD")),
                List.of(deposit(a, "t1", 1000, "USD"), deposit(b, "t2", 250, "USD")))).isEmpty();
    }

    @Test
    void emptySidesAreFine() {
        assertThat(Reconciler.compare(List.of(), List.of())).isEmpty();
    }

    @Test
    void collectedButNeverBookedIsMissingInLedger() {
        UUID a = UUID.randomUUID();
        List<Discrepancy> d = Reconciler.compare(List.of(charge(a, 1000, "USD")), List.of());
        assertThat(d).extracting(Discrepancy::type).containsExactly(Type.MISSING_IN_LEDGER);
        assertThat(d.get(0).paymentId()).isEqualTo(a);
    }

    @Test
    void bookedButNeverCollectedIsMissingInStripe() {
        UUID a = UUID.randomUUID();
        List<Discrepancy> d = Reconciler.compare(List.of(), List.of(deposit(a, "t1", 1000, "USD")));
        assertThat(d).extracting(Discrepancy::type).containsExactly(Type.MISSING_IN_STRIPE);
    }

    @Test
    void differentAmountsAreFlaggedNotGuessed() {
        UUID a = UUID.randomUUID();
        List<Discrepancy> d = Reconciler.compare(List.of(charge(a, 1000, "USD")), List.of(deposit(a, "t1", 999, "USD")));
        assertThat(d).extracting(Discrepancy::type).containsExactly(Type.AMOUNT_MISMATCH);
        assertThat(d.get(0).detail()).contains("1000").contains("999");
    }

    @Test
    void differentCurrenciesAreAMismatchEvenWithEqualNumbers() {
        UUID a = UUID.randomUUID();
        assertThat(Reconciler.compare(List.of(charge(a, 1000, "USD")), List.of(deposit(a, "t1", 1000, "EUR"))))
                .extracting(Discrepancy::type).containsExactly(Type.AMOUNT_MISMATCH);
    }

    @Test
    void currencyCaseDoesNotMatter() {
        UUID a = UUID.randomUUID();
        assertThat(Reconciler.compare(List.of(charge(a, 1000, "USD")), List.of(deposit(a, "t1", 1000, "usd")))).isEmpty();
    }

    @Test
    void twoDepositsForOnePaymentIsADuplicateBooking() {
        UUID a = UUID.randomUUID();
        List<Discrepancy> d = Reconciler.compare(List.of(charge(a, 1000, "USD")),
                List.of(deposit(a, "t1", 1000, "USD"), deposit(a, "t2", 1000, "USD")));
        assertThat(d).extracting(Discrepancy::type).containsExactly(Type.DUPLICATE_BOOKING);
        assertThat(d.get(0).detail()).contains("t1").contains("t2");
    }

    @Test
    void severalProblemsAreAllReportedIndependently() {
        UUID ok = UUID.randomUUID();
        UUID missingLedger = UUID.randomUUID();
        UUID missingStripe = UUID.randomUUID();
        UUID wrongAmount = UUID.randomUUID();
        List<Discrepancy> d = Reconciler.compare(
                List.of(charge(ok, 100, "USD"), charge(missingLedger, 200, "USD"), charge(wrongAmount, 300, "USD")),
                List.of(deposit(ok, "t1", 100, "USD"), deposit(missingStripe, "t2", 400, "USD"), deposit(wrongAmount, "t3", 301, "USD")));
        assertThat(d).extracting(Discrepancy::type)
                .containsExactlyInAnyOrder(Type.MISSING_IN_LEDGER, Type.MISSING_IN_STRIPE, Type.AMOUNT_MISMATCH);
        assertThat(d).extracting(Discrepancy::paymentId).doesNotContain(ok);
    }
}
