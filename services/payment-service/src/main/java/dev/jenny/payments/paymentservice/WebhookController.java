package dev.jenny.payments.paymentservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Stripe webhook endpoint. Response codes matter because Stripe retries on anything but 2xx:
 * 400 for forged/stale requests (no retry value), 2xx for events we ignore or already processed,
 * 5xx only when processing failed, so Stripe retries and nothing is lost.
 */
@RestController
@RequestMapping("/webhooks")
class WebhookController {

    private final StripeSignatureVerifier verifier;
    private final PaymentService payments;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final ObjectMapper mapper;
    private final MeterRegistry meters;

    WebhookController(StripeSignatureVerifier verifier, PaymentService payments, JdbcTemplate jdbc,
                      TransactionTemplate tx, ObjectMapper mapper, MeterRegistry meters) {
        this.verifier = verifier;
        this.payments = payments;
        this.jdbc = jdbc;
        this.tx = tx;
        this.mapper = mapper;
        this.meters = meters;
    }

    @PostMapping("/stripe")
    String stripe(@RequestBody String payload,
                  @RequestHeader(name = "Stripe-Signature", required = false) String signature) {
        if (!verifier.configured()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Webhook secret not configured");
        }
        if (!verifier.verify(payload, signature)) {
            meters.counter("payments.webhook", "outcome", "bad_signature").increment();
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid signature");
        }
        JsonNode event;
        try {
            event = mapper.readTree(payload);
        } catch (JsonProcessingException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid payload");
        }
        String eventId = event.path("id").asText("");
        String type = event.path("type").asText("");
        if (eventId.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Missing event id");
        }
        JsonNode object = event.path("data").path("object");

        tx.executeWithoutResult(s -> {
            int inserted = jdbc.update("INSERT INTO webhook_events (event_id, type) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    eventId, type);
            if (inserted == 0) {
                meters.counter("payments.webhook", "outcome", "duplicate").increment();
                return;
            }
            payments.applyWebhook(type, textOrNull(object.path("id")), uuidOrNull(object.path("metadata").path("paymentId")),
                    textOrNull(object.path("last_payment_error").path("code")));
        });
        return "{\"received\":true}";
    }

    private static String textOrNull(JsonNode n) {
        return n.isMissingNode() || n.isNull() ? null : n.asText();
    }

    private static UUID uuidOrNull(JsonNode n) {
        try {
            return n.isMissingNode() || n.isNull() ? null : UUID.fromString(n.asText());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
