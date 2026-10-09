package dev.jenny.payments.reconciliationservice;

import com.fasterxml.jackson.databind.JsonNode;
import dev.jenny.payments.reconciliationservice.Sources.Deposit;
import dev.jenny.payments.reconciliationservice.Sources.DepositSource;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.ClientHttpRequestFactorySettings;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Deposits in the Fineract savings accounts. The ledger-service writes the note "payment {paymentId}" on every
 * deposit, which is how a ledger transaction is tied back to a payment. Reversed (undone) deposits do not count.
 */
@Component
class FineractDepositSource implements DepositSource {

    private static final Pattern NOTE = Pattern.compile("payment ([0-9a-fA-F-]{36})");
    private static final int PAGE_SIZE = 200;
    private static final java.time.format.DateTimeFormatter FINERACT_DATE =
            java.time.format.DateTimeFormatter.ofPattern("dd MMMM yyyy", java.util.Locale.ENGLISH);

    private final RestClient rest;
    private final List<Long> accountIds;

    FineractDepositSource(@Value("${fineract.base-url}") String baseUrl,
                          @Value("${fineract.username}") String username,
                          @Value("${fineract.password}") String password,
                          @Value("${fineract.tenant}") String tenant,
                          @Value("${fineract.connect-timeout-ms}") long connectTimeoutMs,
                          @Value("${fineract.read-timeout-ms}") long readTimeoutMs,
                          @Value("${reconciliation.savings-account-ids}") List<Long> accountIds) {
        var settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(Duration.ofMillis(connectTimeoutMs))
                .withReadTimeout(Duration.ofMillis(readTimeoutMs));
        this.rest = RestClient.builder().baseUrl(baseUrl)
                .requestFactory(ClientHttpRequestFactoryBuilder.jdk().build(settings))
                .defaultHeaders(h -> h.setBasicAuth(username, password))
                .defaultHeader("Fineract-Platform-TenantId", tenant)
                .build();
        this.accountIds = accountIds;
    }

    @Override
    public List<Deposit> fetch(Instant from, Instant to) {
        LocalDate earliest = from.atZone(ZoneOffset.UTC).toLocalDate().minusDays(1);   // Fineract dates have no time of day
        List<Deposit> out = new ArrayList<>();
        for (long accountId : accountIds) {
            // Page through /transactions/search with a date filter instead of reading the whole account.
            // Measured on 714 transactions: ?associations=transactions = 3.3 MB / 1.9 s and growing with the account;
            // one search page of 100 = 120 KB / 144 ms regardless of the account's history.
            int offset = 0;
            long total;
            int rows;
            do {
                final int pageOffset = offset;
                JsonNode page;
                try {
                    page = rest.get().uri(b -> b.path("/savingsaccounts/{id}/transactions/search")
                                    .queryParam("limit", PAGE_SIZE).queryParam("offset", pageOffset)
                                    .queryParam("orderBy", "id").queryParam("sortOrder", "desc")
                                    .queryParam("fromDate", earliest.format(FINERACT_DATE))
                                    .queryParam("dateFormat", "dd MMMM yyyy").queryParam("locale", "en")
                                    .build(accountId))
                            .retrieve().body(JsonNode.class);
                } catch (RestClientException e) {
                    throw new IllegalStateException("Fineract unavailable: " + e.getMessage(), e);
                }
                total = page.path("total").asLong(0);
                rows = page.path("content").size();
                for (JsonNode t : page.path("content")) {
                    collect(t, earliest, out);
                }
                offset += rows;
            } while (rows > 0 && offset < total);
        }
        return out;
    }

    private static void collect(JsonNode t, LocalDate earliest, List<Deposit> out) {
        if (!t.path("transactionType").path("deposit").asBoolean(false) || t.path("reversed").asBoolean(false)) {
            return;
        }
        Matcher m = NOTE.matcher(t.path("note").asText(""));
        if (!m.find() || date(t.path("date")).isBefore(earliest)) {
            return;                                                    // not ours, or outside the window
        }
        out.add(new Deposit(UUID.fromString(m.group(1)), t.path("id").asText(),
                new BigDecimal(t.path("amount").asText("0")).movePointRight(2).longValueExact(),
                t.path("currency").path("code").asText("")));
    }

    private static LocalDate date(JsonNode array) {
        return array.isArray() && array.size() == 3
                ? LocalDate.of(array.get(0).asInt(), array.get(1).asInt(), array.get(2).asInt())
                : LocalDate.MIN;
    }
}
