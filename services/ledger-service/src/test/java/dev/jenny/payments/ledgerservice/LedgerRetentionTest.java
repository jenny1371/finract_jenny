package dev.jenny.payments.ledgerservice;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** The de-duplication table must not grow forever, but must never lose a row that could still matter. */
class LedgerRetentionTest extends AbstractIntegrationTest {

    @Autowired LedgerMaintenance maintenance;

    private UUID insert(String age) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO processed_events (event_id, processed_at) VALUES (?, now() - ?::interval)", id, age);
        return id;
    }

    private boolean exists(UUID id) {
        return jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_id = ?", Integer.class, id) > 0;
    }

    @Test
    void purgeRemovesRowsOlderThanTheRetentionAndKeepsTheRest() {
        UUID old = insert("15 days");
        UUID justInside = insert("13 days");
        UUID fresh = insert("1 minute");

        maintenance.purgeProcessedEvents();                 // default retention: 14 days

        assertThat(exists(old)).isFalse();
        assertThat(exists(justInside)).isTrue();
        assertThat(exists(fresh)).isTrue();
    }

    /** A purged event id that is redelivered is processed again; the ledger entry's own state still prevents a double booking. */
    @Test
    void aRedeliveryAfterThePurgeStillBooksOnce() {
        UUID pid = UUID.randomUUID();
        UUID order = UUID.randomUUID();
        String command = event("BookLedger", UUID.randomUUID(), pid, order, 1000, null);
        publish(command);
        awaitStatus(pid, "BOOKED");

        jdbc.update("UPDATE processed_events SET processed_at = now() - interval '30 days'");
        maintenance.purgeProcessedEvents();
        publish(command);                                    // same event id, now unknown to processed_events

        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(30)).until(() -> outboxCount(pid, "LedgerBooked") == 2);
        assertThat(FINERACT.requestsFor(pid.toString())).hasSize(1);   // answered again, Fineract untouched
    }
}
