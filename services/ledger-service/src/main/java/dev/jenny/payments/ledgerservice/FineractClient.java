package dev.jenny.payments.ledgerservice;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.ClientHttpRequestFactorySettings;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/** Thin Fineract REST client. Classifies every failure so callers know whether a retry is safe. */
@Component
public class FineractClient {

    /** Fineract rolled the command back (e.g. optimistic lock after its internal retries). Retry with a NEW key. */
    public static class ConflictException extends RuntimeException {
        ConflictException(String m) {
            super(m);
        }
    }

    /** Timeout / connection error / 5xx: we do not know if it was applied. Retry with the SAME key. */
    public static class UnknownOutcomeException extends RuntimeException {
        UnknownOutcomeException(String m, Throwable c) {
            super(m, c);
        }
    }

    /** Fineract refused the request (validation, unknown account, auth): retrying is pointless. */
    public static class RejectedException extends RuntimeException {
        RejectedException(String m) {
            super(m);
        }
    }

    public record DepositResult(String transactionId) {}

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("dd MMMM yyyy", Locale.ENGLISH);

    private final RestClient rest;
    private final String paymentTypeName;
    private final AtomicReference<Long> paymentTypeId = new AtomicReference<>();

    FineractClient(@Value("${fineract.base-url}") String baseUrl,
                   @Value("${fineract.username}") String username,
                   @Value("${fineract.password}") String password,
                   @Value("${fineract.tenant}") String tenant,
                   @Value("${fineract.payment-type-name}") String paymentTypeName,
                   @Value("${fineract.connect-timeout-ms}") long connectTimeoutMs,
                   @Value("${fineract.read-timeout-ms}") long readTimeoutMs) {
        this.paymentTypeName = paymentTypeName;
        var settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(Duration.ofMillis(connectTimeoutMs))
                .withReadTimeout(Duration.ofMillis(readTimeoutMs));
        this.rest = RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(ClientHttpRequestFactoryBuilder.jdk().build(settings))
                .defaultHeaders(h -> h.setBasicAuth(username, password))
                .defaultHeader("Fineract-Platform-TenantId", tenant)
                .build();
    }

    /** Amount is in minor units and assumes a 2-decimal currency (USD); documented limitation for now. */
    public DepositResult deposit(long savingsId, long amountMinor, String idempotencyKey, String note) {
        Map<String, Object> body = Map.of(
                "transactionDate", LocalDate.now(ZoneOffset.UTC).format(DATE),
                "transactionAmount", BigDecimal.valueOf(amountMinor, 2),
                "paymentTypeId", paymentTypeId(),
                "note", note,
                "dateFormat", "dd MMMM yyyy",
                "locale", "en");
        try {
            JsonNode res = rest.post()
                    .uri("/savingsaccounts/{id}/transactions?command=deposit", savingsId)
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .onStatus(HttpStatusCode::isError, (req, resp) -> {
                        throw classify(resp.getStatusCode().value(), readBody(resp.getBody()));
                    })
                    .body(JsonNode.class);
            return new DepositResult(res.path("resourceId").asText());
        } catch (ResourceAccessException e) {
            throw new UnknownOutcomeException("Fineract I/O failure: " + e.getMessage(), e);
        }
    }

    /** Reverses a previously booked deposit (saga compensation). Returns the reversed transaction id. */
    public String undo(long savingsId, String transactionId, String idempotencyKey) {
        Map<String, Object> body = Map.of(
                "transactionDate", LocalDate.now(ZoneOffset.UTC).format(DATE),
                "dateFormat", "dd MMMM yyyy",
                "locale", "en");
        try {
            JsonNode res = rest.post()
                    .uri("/savingsaccounts/{id}/transactions/{txn}?command=undo", savingsId, transactionId)
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .onStatus(HttpStatusCode::isError, (req, resp) -> {
                        throw classify(resp.getStatusCode().value(), readBody(resp.getBody()));
                    })
                    .body(JsonNode.class);
            return res.path("resourceId").asText();
        } catch (ResourceAccessException e) {
            throw new UnknownOutcomeException("Fineract I/O failure: " + e.getMessage(), e);
        }
    }

    private long paymentTypeId() {
        Long cached = paymentTypeId.get();
        if (cached != null) {
            return cached;
        }
        try {
            JsonNode types = rest.get().uri("/paymenttypes").retrieve().body(JsonNode.class);
            for (JsonNode t : types) {
                if (paymentTypeName.equals(t.path("name").asText())) {
                    paymentTypeId.set(t.path("id").asLong());
                    return paymentTypeId.get();
                }
            }
            JsonNode created = rest.post().uri("/paymenttypes").contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("name", paymentTypeName, "description", "Stripe card payment",
                            "isCashPayment", false, "position", 1))
                    .retrieve().body(JsonNode.class);
            paymentTypeId.set(created.path("resourceId").asLong());
            return paymentTypeId.get();
        } catch (ResourceAccessException e) {
            throw new UnknownOutcomeException("Fineract I/O failure: " + e.getMessage(), e);
        }
    }

    private static RuntimeException classify(int status, String body) {
        String msg = "Fineract HTTP " + status + (body.isBlank() ? "" : ": " + body);
        if (status == 409 || status == 423) {
            return new ConflictException(msg);
        }
        if (status >= 500 || status == 408 || status == 429) {
            return new UnknownOutcomeException(msg, null);
        }
        return new RejectedException(msg);
    }

    private static String readBody(java.io.InputStream in) {
        try {
            return new String(in.readAllBytes());
        } catch (IOException e) {
            return "";
        }
    }
}
