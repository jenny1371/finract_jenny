package dev.jenny.payments.orderservice;

import com.github.dockerjava.api.DockerClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared infrastructure for integration and fault-injection tests.
 * Containers are JVM-wide singletons (started once, reaped by Testcontainers' Ryuk at exit) so the
 * Spring context is cached across test classes and shuts down while the containers are still alive.
 *
 * Fault injection helpers pause/unpause a container: to the app this looks like a hung dependency
 * (connections stay open but nothing answers), which is the nastiest realistic failure mode.
 */
abstract class AbstractIntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7").withExposedPorts(6379);

    static final FakePayments PAYMENTS;

    static {
        POSTGRES.start();
        KAFKA.start();
        REDIS.start();
        PAYMENTS = new FakePayments();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", POSTGRES::getUsername);
        r.add("spring.datasource.password", POSTGRES::getPassword);
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.data.redis.host", REDIS::getHost);
        r.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        r.add("payment-service.base-url", () -> "http://localhost:" + PAYMENTS.port());
        r.add("payment-service.read-timeout-ms", () -> "2000");
        // fast saga timings for tests
        r.add("saga.retry.delay-ms", () -> "300");
        r.add("saga.retry.multiplier", () -> "1.5");
        r.add("saga.watchdog.interval-ms", () -> "500");
        r.add("saga.watchdog.stuck-after-ms", () -> "4000");
        r.add("saga.watchdog.max-redrives", () -> "2");
    }

    static final String BODY = "{\"amount\":1000,\"currency\":\"USD\"}";

    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;

    ResponseEntity<String> post(String key, String body) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        if (key != null) {
            h.set("Idempotency-Key", key);
        }
        return http.exchange("/orders", HttpMethod.POST, new HttpEntity<>(body, h), String.class);
    }

    static String orderIdOf(ResponseEntity<String> response) {
        return response.getBody().replaceAll(".*\"id\":\"([^\"]+)\".*", "$1");
    }

    int count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Integer.class, args);
    }

    // --- Fault injection -------------------------------------------------------------------

    static void pause(GenericContainer<?> c) {
        docker().pauseContainerCmd(c.getContainerId()).exec();
    }

    static void unpause(GenericContainer<?> c) {
        docker().unpauseContainerCmd(c.getContainerId()).exec();
    }

    private static DockerClient docker() {
        return DockerClientFactory.instance().client();
    }

    // --- Kafka helper ----------------------------------------------------------------------

    /** Reads a topic from the beginning until {@code timeout} elapses; returns every record seen. */
    static List<ConsumerRecord<String, String>> readTopic(String topic, Duration timeout) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(300)).forEach(out::add);
            }
        }
        return out;
    }
}
