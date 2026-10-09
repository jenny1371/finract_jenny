package dev.jenny.payments.common;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

/**
 * Publishes unpublished outbox rows to Kafka. Multiple instances can run safely:
 * FOR UPDATE SKIP LOCKED gives each poller a disjoint batch. Delivery is at-least-once
 * (a crash between send and commit re-sends), so consumers must de-duplicate on eventId.
 */

public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private record Row(UUID id, String aggregateId, String topic, String payload) {}

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final MeterRegistry meters;
    private final int batchSize;

    public OutboxPoller(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, MeterRegistry meters,
                 @Value("${outbox.batch-size:100}") int batchSize) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.meters = meters;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-ms:300}")
    @Transactional
    public void publish() {
        List<Row> rows = jdbc.query(
                "SELECT id, aggregate_id, topic, payload FROM outbox WHERE published_at IS NULL "
                        + "ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED",
                (rs, i) -> new Row(rs.getObject(1, UUID.class), rs.getString(2), rs.getString(3), rs.getString(4)),
                batchSize);

        for (Row row : rows) {
            try {
                ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.aggregateId(), row.payload());
                record.headers().add("event-id", row.id().toString().getBytes());
                kafka.send(record).get(10, TimeUnit.SECONDS);
                jdbc.update("UPDATE outbox SET published_at = now() WHERE id = ?", row.id());
                meters.counter("outbox.published").increment();
            } catch (Exception e) {
                // Keep what was already sent (commit it) and retry the rest on the next tick.
                log.warn("Outbox publish failed for {}, will retry: {}", row.id(), e.toString());
                meters.counter("outbox.failed").increment();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
        }
    }
}
