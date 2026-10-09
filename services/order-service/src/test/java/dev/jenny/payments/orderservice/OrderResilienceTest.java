package dev.jenny.payments.orderservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.jenny.payments.common.OutboxPoller;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Fault-injection ("things that go wrong") tests. Each one breaks a dependency on purpose and
 * asserts the guarantee the design promises. Reuse the pattern for other services.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderResilienceTest extends AbstractIntegrationTest {

    @Autowired StringRedisTemplate redis;
    @Autowired OutboxPoller poller;

    /** Dual-write problem: Kafka is down, the order must still be accepted and the event not lost. */
    @Test
    void kafkaOutageDoesNotLoseOrdersOrEvents() {
        pause(KAFKA);
        String orderId;
        try {
            ResponseEntity<String> r = post(UUID.randomUUID().toString(), BODY);
            assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED);   // order path does not depend on Kafka
            orderId = orderIdOf(r);
            assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NULL", orderId))
                    .isEqualTo(1);                                          // event is safely parked in the outbox
        } finally {
            unpause(KAFKA);
        }

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NOT NULL",
                        orderId)).isEqualTo(1));
        List<ConsumerRecord<String, String>> mine = readTopic("order.events", Duration.ofSeconds(5)).stream()
                .filter(rec -> orderId.equals(rec.key())).toList();
        assertThat(mine).isNotEmpty();
        // at-least-once: duplicates are allowed but must carry the same event id for consumer dedup
        assertThat(mine.stream().map(rec -> new String(rec.headers().lastHeader("event-id").value()))
                .collect(Collectors.toSet())).hasSize(1);
    }

    /** Redis hangs: idempotency must still hold (DB is the authority) and requests must not hang. */
    @Test
    void redisOutageFallsBackToDatabaseIdempotency() {
        String key = UUID.randomUUID().toString();
        pause(REDIS);
        try {
            long start = System.nanoTime();
            ResponseEntity<String> first = post(key, BODY);
            ResponseEntity<String> second = post(key, BODY);
            Duration took = Duration.ofNanos(System.nanoTime() - start);

            assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
            assertThat(second.getBody()).isEqualTo(first.getBody());
            assertThat(took).as("requests must fail fast on a hung Redis").isLessThan(Duration.ofSeconds(15));
        } finally {
            unpause(REDIS);
        }
        assertThat(count("SELECT count(*) FROM idempotency_keys WHERE idem_key = ?", key)).isEqualTo(1);
    }

    /** Failure in the middle of the transaction: nothing half-written, key not stuck, retry works. */
    @Test
    void failureMidTransactionRollsBackEverythingAndRetrySucceeds() {
        String key = UUID.randomUUID().toString();
        int ordersBefore = count("SELECT count(*) FROM orders");

        jdbc.execute("ALTER TABLE outbox RENAME TO outbox_broken");           // 3rd insert in the txn will fail
        try {
            ResponseEntity<String> r = post(key, BODY);
            assertThat(r.getStatusCode().is5xxServerError()).isTrue();
        } finally {
            jdbc.execute("ALTER TABLE outbox_broken RENAME TO outbox");
        }

        assertThat(count("SELECT count(*) FROM orders")).isEqualTo(ordersBefore);                 // rolled back
        assertThat(count("SELECT count(*) FROM idempotency_keys WHERE idem_key = ?", key)).isZero();
        assertThat(redis.hasKey("idem:lock:" + key)).isFalse();                                   // lock released
        assertThat(redis.hasKey("idem:resp:" + key)).isFalse();                                   // nothing cached

        ResponseEntity<String> retry = post(key, BODY);
        assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(retry.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("false");
    }

    /** Several pollers at once (e.g. 3 replicas): SKIP LOCKED must hand each row to exactly one of them. */
    @Test
    void concurrentPollersNeverPublishSameRowTwice() throws Exception {
        String topic = "chaos.dup." + UUID.randomUUID();
        int rows = 250;
        for (int i = 0; i < rows; i++) {
            jdbc.update("INSERT INTO outbox (id, aggregate_id, topic, event_type, payload) VALUES (?, ?, ?, 'Chaos', '{}')",
                    UUID.randomUUID(), "agg-" + i, topic);
        }

        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            futures.add(pool.submit(() -> {
                while (count("SELECT count(*) FROM outbox WHERE topic = ? AND published_at IS NULL", topic) > 0) {
                    poller.publish();
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        Map<String, Long> perAggregate = readTopic(topic, Duration.ofSeconds(8)).stream()
                .collect(Collectors.groupingBy(ConsumerRecord::key, Collectors.counting()));
        assertThat(perAggregate).hasSize(rows);
        assertThat(perAggregate.values()).allMatch(c -> c == 1L);
    }
}
