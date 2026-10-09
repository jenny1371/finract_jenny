package dev.jenny.payments.common;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Deletes old rows of de-duplication tables (processed_events, webhook_events, ...) in small batches so the
 * table cannot grow forever and a purge never holds long locks.
 *
 * The retention period must be LONGER than anything that can redeliver a message: Kafka topic retention for
 * consumer dedup, Stripe's retry window (days) for webhooks. Dropping a row earlier than that re-opens the
 * door to double processing.
 */
public class RetentionCleaner {

    private static final Logger log = LoggerFactory.getLogger(RetentionCleaner.class);

    private final JdbcTemplate jdbc;

    public RetentionCleaner(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** table and column are code constants, never user input. Returns how many rows were deleted. */
    public int purge(String table, String timestampColumn, Duration olderThan, int batchSize) {
        int total = 0;
        int deleted;
        do {
            deleted = jdbc.update("DELETE FROM " + table + " WHERE ctid IN (SELECT ctid FROM " + table
                    + " WHERE " + timestampColumn + " < now() - make_interval(secs => ?) LIMIT ?)",
                    (double) olderThan.toSeconds(), batchSize);
            total += deleted;
        } while (deleted == batchSize);
        if (total > 0) {
            log.info("Retention: deleted {} rows from {} older than {}", total, table, olderThan);
        }
        return total;
    }
}
