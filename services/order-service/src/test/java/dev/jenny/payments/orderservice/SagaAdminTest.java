package dev.jenny.payments.orderservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;

/** Operator tool: replaying dead-lettered saga messages. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SagaAdminTest extends AbstractIntegrationTest {

    @Autowired KafkaTemplate<String, String> kafka;

    private ResponseEntity<String> replay(String topic) {
        return http.exchange("/admin/dlq/replay?topic=" + topic, HttpMethod.POST, null, String.class);
    }

    private static String message(UUID orderId) {
        return "{\"eventId\":\"" + UUID.randomUUID() + "\",\"type\":\"PaymentAuthorized\",\"orderId\":\"" + orderId
                + "\",\"paymentId\":\"" + UUID.randomUUID() + "\"}";
    }

    private boolean onTopic(String topic, UUID marker, Duration listenFor) {
        return readTopic(topic, listenFor).stream().anyMatch(r -> r.value().contains(marker.toString()));
    }

    @Test
    void replayPutsDeadLettersBackOnTheSourceTopicOnceAndSkipsPoison() throws Exception {
        UUID marker = UUID.randomUUID();
        kafka.send("payment.events-dlt", marker.toString(), message(marker)).get();
        kafka.send("payment.events-dlt", "poison", "not json at all").get();

        ResponseEntity<String> first = replay("payment.events");
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody()).contains("\"replayed\":").contains("\"skipped\":");
        assertThat(onTopic("payment.events", marker, Duration.ofSeconds(6))).isTrue();

        // a second call must not replay the same dead letter again (offsets are committed per DLT)
        long copies = readTopic("payment.events", Duration.ofSeconds(4)).stream().filter(r -> r.value().contains(marker.toString())).count();
        replay("payment.events");
        long after = readTopic("payment.events", Duration.ofSeconds(4)).stream().filter(r -> r.value().contains(marker.toString())).count();
        assertThat(after).isEqualTo(copies);
    }

    @Test
    void unknownTopicsAreRejected() {
        assertThat(replay("some.other.topic").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(replay("payments").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void replayingAnEmptyDeadLetterTopicIsHarmless() {
        ResponseEntity<String> r = replay("ledger.events");
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.OK);
        await().atMost(Duration.ofSeconds(5)).until(() -> true);
    }
}
