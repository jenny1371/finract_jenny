package dev.jenny.payments.ledgerservice;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** Real Postgres + Kafka (Testcontainers), a scriptable fake Fineract, and short retry delays. */
@SpringBootTest(properties = {
        "ledger.retry.delay-ms=300", "ledger.retry.multiplier=1.5", "ledger.retry.attempts=4",
        "ledger.default-savings-account-id=77", "fineract.read-timeout-ms=500", "server.port=0"})
abstract class AbstractIntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    static final FakeFineract FINERACT;

    static {
        POSTGRES.start();
        KAFKA.start();
        FINERACT = new FakeFineract();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", POSTGRES::getUsername);
        r.add("spring.datasource.password", POSTGRES::getPassword);
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("fineract.base-url", () -> "http://localhost:" + FINERACT.port());
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaTemplate<String, String> kafka;

    @BeforeEach
    void resetFake() {
        FINERACT.reset();
    }

    static String event(String type, UUID eventId, UUID paymentId, UUID orderId, long amount, String merchantId) {
        return "{\"eventId\":\"" + eventId + "\",\"type\":\"" + type + "\",\"paymentId\":\"" + paymentId
                + "\",\"orderId\":\"" + orderId + "\",\"amount\":" + amount + ",\"currency\":\"USD\""
                + (merchantId == null ? "" : ",\"merchantId\":\"" + merchantId + "\"") + "}";
    }

    /** A BookLedger command as the saga would send it. */
    static String captured(UUID paymentId, long amount) {
        return event("BookLedger", UUID.randomUUID(), paymentId, UUID.randomUUID(), amount, null);
    }

    static String undoCommand(UUID paymentId, long amount) {
        return event("UndoLedger", UUID.randomUUID(), paymentId, UUID.randomUUID(), amount, null);
    }

    void publish(String payload) {
        kafka.send("ledger.commands", UUID.randomUUID().toString(), payload);
    }

    String statusOf(UUID paymentId) {
        return jdbc.query("SELECT status FROM ledger_entries WHERE payment_id = ?", (rs, i) -> rs.getString(1), paymentId)
                .stream().findFirst().orElse(null);
    }

    void awaitStatus(UUID paymentId, String status) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .until(() -> status.equals(statusOf(paymentId)));
    }

    int outboxCount(UUID paymentId, String type) {
        return jdbc.queryForObject("SELECT count(*) FROM outbox WHERE event_type = ? AND payload LIKE ?", Integer.class,
                type, "%" + paymentId + "%");
    }
}
