package dev.jenny.payments.reconciliationservice;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jenny.payments.reconciliationservice.Sources.Deposit;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

/**
 * Runs the REAL FineractDepositSource against a REAL Fineract. Skipped unless FINERACT_LIVE=true, because it needs
 * Fineract running (docker compose up -d fineract-db fineract) and writes one deposit, then reverses it, on savings
 * account 2 (created by scripts/first-deposit.ps1).
 *
 *   FINERACT_LIVE=true mvn -pl services/reconciliation-service test -Dtest=FineractDepositSourceLiveTest
 */
@EnabledIfEnvironmentVariable(named = "FINERACT_LIVE", matches = "true")
class FineractDepositSourceLiveTest {

    private static final String BASE = System.getenv().getOrDefault("FINERACT_URL", "http://localhost:8443/fineract-provider/api/v1");
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("dd MMMM yyyy", Locale.ENGLISH);

    private final RestClient rest = RestClient.builder().baseUrl(BASE)
            .defaultHeaders(h -> h.setBasicAuth("mifos", "password"))
            .defaultHeader("Fineract-Platform-TenantId", "default").build();

    private String post(String uri, Map<String, Object> body, String key) {
        return rest.post().uri(uri).header("Idempotency-Key", key).contentType(MediaType.APPLICATION_JSON).body(body)
                .retrieve().body(String.class);
    }

    @Test
    void depositAppearsThenDisappearsAfterUndo() {
        UUID paymentId = UUID.randomUUID();
        String date = LocalDate.now(ZoneOffset.UTC).minusDays(1).format(DATE);     // never "in the future" for Fineract
        FineractDepositSource source = new FineractDepositSource(BASE, "mifos", "password", "default", 2000, 15000, List.of(2L));

        String created = post("/savingsaccounts/2/transactions?command=deposit", Map.of(
                "transactionDate", date, "transactionAmount", "12.34", "paymentTypeId", 4,
                "note", "payment " + paymentId, "dateFormat", "dd MMMM yyyy", "locale", "en"), "live-" + paymentId);
        String txnId = created.replaceAll(".*\"resourceId\":(\\d+).*", "$1");

        List<Deposit> before = source.fetch(Instant.now().minusSeconds(3600), Instant.now());
        assertThat(before).filteredOn(d -> d.paymentId().equals(paymentId)).singleElement().satisfies(d -> {
            assertThat(d.amount()).isEqualTo(1234);
            assertThat(d.currency()).isEqualTo("USD");
            assertThat(d.transactionId()).isEqualTo(txnId);
        });

        post("/savingsaccounts/2/transactions/" + txnId + "?command=undo",
                Map.of("transactionDate", date, "dateFormat", "dd MMMM yyyy", "locale", "en"), "live-undo-" + paymentId);

        List<Deposit> after = source.fetch(Instant.now().minusSeconds(3600), Instant.now());
        assertThat(after).noneMatch(d -> d.paymentId().equals(paymentId));          // reversed deposits do not count
    }
}
