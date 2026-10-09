package dev.jenny.payments.orderservice.saga;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.ClientHttpRequestFactorySettings;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/**
 * Commands to payment-service. Outcomes are reported back through payment.events, so the response body is not used;
 * what matters is telling "try again" (Unavailable) apart from "the answer is no" (Rejected).
 */
@Component
public class PaymentClient {

    /** Timeout, connection error, 5xx: we do not know what happened. Safe to repeat: every call is idempotent. */
    public static class Unavailable extends RuntimeException {
        Unavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** payment-service said no (state conflict or invalid request). Repeating will not help. */
    public static class Rejected extends RuntimeException {
        Rejected(String message) {
            super(message);
        }
    }

    private final RestClient rest;

    PaymentClient(@Value("${payment-service.base-url}") String baseUrl,
                  @Value("${payment-service.connect-timeout-ms}") long connectTimeoutMs,
                  @Value("${payment-service.read-timeout-ms}") long readTimeoutMs) {
        var settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(Duration.ofMillis(connectTimeoutMs))
                .withReadTimeout(Duration.ofMillis(readTimeoutMs));
        this.rest = RestClient.builder().baseUrl(baseUrl)
                .requestFactory(ClientHttpRequestFactoryBuilder.jdk().build(settings)).build();
    }

    /** The key is derived from the order id, so every retry of "authorize this order" is the same request. */
    public void authorize(UUID orderId, long amount, String currency, String paymentMethod) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("orderId", orderId);
        body.put("amount", amount);
        body.put("currency", currency);
        body.put("paymentMethod", paymentMethod);
        call(() -> rest.post().uri("/payments").header("Idempotency-Key", "saga-" + orderId + "-authorize")
                .contentType(MediaType.APPLICATION_JSON).body(body));
    }

    public void capture(UUID paymentId) {
        call(() -> rest.post().uri("/payments/{id}/capture", paymentId));
    }

    public void cancel(UUID paymentId) {
        call(() -> rest.post().uri("/payments/{id}/cancel", paymentId));
    }

    private void call(java.util.function.Supplier<RestClient.RequestHeadersSpec<?>> request) {
        try {
            request.get().retrieve()
                    .onStatus(HttpStatusCode::isError, (req, resp) -> {
                        int status = resp.getStatusCode().value();
                        String detail = read(resp.getBody());
                        if (status >= 500 || status == 408 || status == 429) {
                            throw new Unavailable("payment-service HTTP " + status + " " + detail, null);
                        }
                        throw new Rejected("payment-service HTTP " + status + " " + detail);
                    })
                    .toBodilessEntity();
        } catch (ResourceAccessException e) {
            throw new Unavailable("payment-service unreachable: " + e.getMessage(), e);
        }
    }

    private static String read(java.io.InputStream in) {
        try {
            return new String(in.readAllBytes());
        } catch (IOException e) {
            return "";
        }
    }
}
