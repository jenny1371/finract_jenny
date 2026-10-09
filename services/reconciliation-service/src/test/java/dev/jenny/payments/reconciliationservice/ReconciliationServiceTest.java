package dev.jenny.payments.reconciliationservice;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jenny.payments.reconciliationservice.Sources.Charge;
import dev.jenny.payments.reconciliationservice.Sources.ChargeSource;
import dev.jenny.payments.reconciliationservice.Sources.Deposit;
import dev.jenny.payments.reconciliationservice.Sources.DepositSource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/** Persistence and run behaviour with controllable Stripe / ledger fakes and a real Postgres. */
@SpringBootTest(properties = {"reconciliation.schedule.enabled=false", "reconciliation.grace-seconds=2"})
@Import(ReconciliationServiceTest.Fakes.class)
class ReconciliationServiceTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", POSTGRES::getUsername);
        r.add("spring.datasource.password", POSTGRES::getPassword);
    }

    /** Both sides are scripted by the tests; either can be made to fail. */
    @TestConfiguration
    static class Fakes {
        @Bean
        @Primary
        FakeCharges fakeCharges() {
            return new FakeCharges();
        }

        @Bean
        @Primary
        FakeDeposits fakeDeposits() {
            return new FakeDeposits();
        }
    }

    static class FakeCharges implements ChargeSource {
        final List<Charge> data = new CopyOnWriteArrayList<>();
        volatile boolean down;

        @Override
        public List<Charge> fetch(Instant from, Instant to) {
            if (down) {
                throw new IllegalStateException("Stripe unavailable");
            }
            return new ArrayList<>(data);
        }
    }

    static class FakeDeposits implements DepositSource {
        final List<Deposit> data = new CopyOnWriteArrayList<>();
        volatile boolean down;

        @Override
        public List<Deposit> fetch(Instant from, Instant to) {
            if (down) {
                throw new IllegalStateException("Fineract unavailable");
            }
            return new ArrayList<>(data);
        }
    }

    @Autowired ReconciliationService service;
    @Autowired FakeCharges stripe;
    @Autowired FakeDeposits ledger;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void reset() {
        stripe.data.clear();
        stripe.down = false;
        ledger.data.clear();
        ledger.down = false;
        jdbc.update("DELETE FROM discrepancies");
    }

    private static Charge charge(UUID id, long amount) {
        return new Charge(id, amount, "USD", Instant.now());
    }

    private static Deposit deposit(UUID id, long amount) {
        return new Deposit(id, "txn-" + id.toString().substring(0, 4), amount, "USD");
    }

    private int open(UUID id, String type) {
        return jdbc.queryForObject("SELECT count(*) FROM discrepancies WHERE payment_id = ? AND type = ? AND status = 'OPEN'",
                Integer.class, id, type);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void matchingBooksProduceAnOkRunWithNoDiscrepancies() {
        UUID a = UUID.randomUUID();
        stripe.data.add(charge(a, 1000));
        ledger.data.add(deposit(a, 1000));

        ReconciliationService.RunResult r = service.run();
        assertThat(r.status()).isEqualTo("OK");
        assertThat(r.found()).isZero();
        assertThat(r.charges()).isEqualTo(1);
    }

    @Test
    void differencesAreRecordedOnceEvenWhenRunRepeatedly() {
        UUID a = UUID.randomUUID();
        stripe.data.add(charge(a, 1000));                    // collected, never booked

        service.run();
        service.run();
        service.run();
        assertThat(open(a, "MISSING_IN_LEDGER")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM discrepancies WHERE payment_id = ?", Integer.class, a)).isEqualTo(1);
    }

    /** The most important failure mode: an unreachable Stripe must NOT look like "everything is missing". */
    @Test
    void stripeOutageFailsTheRunWithoutInventingDiscrepancies() {
        UUID a = UUID.randomUUID();
        ledger.data.add(deposit(a, 1000));                   // booked; Stripe would normally confirm it
        stripe.down = true;

        ReconciliationService.RunResult r = service.run();
        assertThat(r.status()).isEqualTo("FAILED");
        assertThat(r.error()).contains("Stripe");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM discrepancies", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT status FROM reconciliation_runs WHERE id = ?", String.class, r.runId()))
                .isEqualTo("FAILED");
    }

    @Test
    void ledgerOutageDoesNotResolveOrCreateAnything() {
        UUID a = UUID.randomUUID();
        stripe.data.add(charge(a, 1000));
        service.run();                                        // MISSING_IN_LEDGER now open
        ledger.down = true;

        assertThat(service.run().status()).isEqualTo("FAILED");
        assertThat(open(a, "MISSING_IN_LEDGER")).isEqualTo(1);   // untouched
    }

    @Test
    void aDifferenceThatDisappearsIsResolvedAutomatically() {
        UUID a = UUID.randomUUID();
        stripe.data.add(charge(a, 1000));
        service.run();
        assertThat(open(a, "MISSING_IN_LEDGER")).isEqualTo(1);

        ledger.data.add(deposit(a, 1000));                   // the booking finally arrived
        service.run();
        assertThat(open(a, "MISSING_IN_LEDGER")).isZero();
        assertThat(jdbc.queryForObject("SELECT status FROM discrepancies WHERE payment_id = ?", String.class, a))
                .isEqualTo("RESOLVED");
    }

    @Test
    void aResolvedDifferenceThatReturnsIsReopenedWithAFreshGracePeriod() {
        UUID a = UUID.randomUUID();
        stripe.data.add(charge(a, 1000));
        service.run();
        ledger.data.add(deposit(a, 1000));
        service.run();                                        // resolved
        ledger.data.clear();                                  // deposit vanished again
        service.run();
        assertThat(open(a, "MISSING_IN_LEDGER")).isEqualTo(1);
        assertThat(service.run().alerts()).isZero();          // brand new: still inside the grace period
    }

    /** Work in flight (we book before we capture) must not page anyone until it has persisted past the grace period. */
    @Test
    void alertsOnlyAfterTheGracePeriod() {
        UUID a = UUID.randomUUID();
        ledger.data.add(deposit(a, 1000));                   // booked, capture not visible in Stripe yet

        ReconciliationService.RunResult first = service.run();
        assertThat(first.found()).isEqualTo(1);
        assertThat(first.alerts()).isZero();                  // detected, but not alarming yet

        sleep(2500);                                          // grace-seconds=2 in this test
        assertThat(service.run().alerts()).isEqualTo(1);
    }

    @Test
    void aPaymentOutsideTheCurrentViewIsNotResolvedByMistake() {
        UUID a = UUID.randomUUID();
        stripe.data.add(charge(a, 1000));
        service.run();                                        // open: missing in ledger
        stripe.data.clear();                                  // charge fell out of the lookback window; ledger still empty

        service.run();
        assertThat(open(a, "MISSING_IN_LEDGER")).isEqualTo(1);   // not seen => not resolved
    }

    @Test
    void runsAreAlwaysRecorded() {
        int before = jdbc.queryForObject("SELECT count(*) FROM reconciliation_runs", Integer.class);
        service.run();
        stripe.down = true;
        service.run();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM reconciliation_runs", Integer.class)).isEqualTo(before + 2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM reconciliation_runs WHERE status = 'RUNNING'", Integer.class)).isZero();
    }
}
