package dev.jenny.payments.orderservice;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderIdempotencyAndOutboxTest extends AbstractIntegrationTest {

    @Autowired StringRedisTemplate redis;

    @Test
    void sameKeySameRequestReplaysAndCreatesOneOrder() {
        String key = UUID.randomUUID().toString();
        ResponseEntity<String> first = post(key, BODY);
        ResponseEntity<String> second = post(key, BODY);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(first.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("false");
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(second.getBody()).isEqualTo(first.getBody());
        assertThat(count("SELECT count(*) FROM idempotency_keys WHERE idem_key = ?", key)).isEqualTo(1);
    }

    @Test
    void sameKeyDifferentRequestIsRejectedWith422() {
        String key = UUID.randomUUID().toString();
        post(key, BODY);
        assertThat(post(key, "{\"amount\":2000,\"currency\":\"USD\"}").getStatusCode())
                .isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
    }

    @Test
    void requestInFlightGets409() {
        String key = UUID.randomUUID().toString();
        redis.opsForValue().set("idem:lock:" + key, "1", Duration.ofSeconds(30));
        assertThat(post(key, BODY).getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void missingKeyIs400() {
        assertThat(post(null, BODY).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void invalidBodyIs400() {
        assertThat(post(UUID.randomUUID().toString(), "{\"amount\":-5,\"currency\":\"usd\"}").getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void concurrentRequestsWithSameKeyCreateExactlyOneOrder() throws Exception {
        String key = UUID.randomUUID().toString();
        ExecutorService pool = Executors.newFixedThreadPool(10);
        List<Future<HttpStatus>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Callable<HttpStatus> call = () -> HttpStatus.valueOf(post(key, BODY).getStatusCode().value());
            results.add(pool.submit(call));
        }
        for (Future<HttpStatus> f : results) {
            assertThat(f.get()).isIn(HttpStatus.CREATED, HttpStatus.CONFLICT);
        }
        pool.shutdown();
        assertThat(count("SELECT count(*) FROM idempotency_keys WHERE idem_key = ?", key)).isEqualTo(1);
    }

    @Test
    void outboxEventIsPublishedToKafkaKeyedByOrderId() {
        String orderId = orderIdOf(post(UUID.randomUUID().toString(), BODY));

        List<ConsumerRecord<String, String>> mine = readTopic("order.events", Duration.ofSeconds(8)).stream()
                .filter(r -> orderId.equals(r.key())).toList();

        assertThat(mine).hasSize(1);
        assertThat(mine.get(0).value()).contains("OrderCreated");
        assertThat(mine.get(0).headers().lastHeader("event-id")).isNotNull();
    }
}
