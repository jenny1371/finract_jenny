package dev.jenny.payments.paymentservice;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jenny.payments.paymentservice.FakeGateway.Mode;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * Stripe webhooks: signature checks, de-duplication, re-ordering, and resolving "the call timed out, did the
 * money move?". The test signs payloads exactly like Stripe does.
 */
class WebhookTest extends AbstractIntegrationTest {

    private static final String SECRET = "whsec_test_secret";

    private static String sign(String payload, long timestamp) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return "t=" + timestamp + ",v1=" + HexFormat.of().formatHex(mac.doFinal((timestamp + "." + payload).getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String event(String eventId, String type, String intentId, String paymentId, String errorCode) {
        return "{\"id\":\"" + eventId + "\",\"type\":\"" + type + "\",\"data\":{\"object\":{\"id\":\"" + intentId
                + "\",\"metadata\":{\"paymentId\":\"" + paymentId + "\"}"
                + (errorCode == null ? "" : ",\"last_payment_error\":{\"code\":\"" + errorCode + "\"}") + "}}}";
    }

    private ResponseEntity<String> deliver(String payload, String signatureHeader) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        if (signatureHeader != null) {
            h.set("Stripe-Signature", signatureHeader);
        }
        return http.exchange("/webhooks/stripe", HttpMethod.POST, new HttpEntity<>(payload, h), String.class);
    }

    private ResponseEntity<String> deliverSigned(String payload) {
        return deliver(payload, sign(payload, System.currentTimeMillis() / 1000));
    }

    private static String newEventId() {
        return "evt_" + UUID.randomUUID();
    }

    private int outbox(UUID orderId, String type) {
        return count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = ?", orderId.toString(), type);
    }

    /** Creates a payment whose authorize call timed out: PENDING, outcome unknown. Returns (orderId, paymentId). */
    private UUID[] pendingPayment() {
        UUID order = UUID.randomUUID();
        gateway.authorizeMode = Mode.TIMEOUT;
        assertThat(authorize(UUID.randomUUID().toString(), body(order, 5000)).getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        gateway.authorizeMode = Mode.OK;
        UUID payment = jdbc.queryForObject("SELECT id FROM payments WHERE order_id = ?", UUID.class, order);
        return new UUID[] {order, payment};
    }

    // ---- authenticity ---------------------------------------------------------------------------

    @Test
    void forgedStaleOrMissingSignaturesAreRejectedAndChangeNothing() {
        UUID[] p = pendingPayment();
        String payload = event(newEventId(), "payment_intent.amount_capturable_updated", "pi_x", p[1].toString(), null);

        assertThat(deliver(payload, null).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(deliver(payload, "t=" + (System.currentTimeMillis() / 1000) + ",v1=deadbeef").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        long old = System.currentTimeMillis() / 1000 - 3600;                       // correctly signed, but an hour old (replay)
        assertThat(deliver(payload, sign(payload, old)).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        String tampered = payload.replace("amount_capturable_updated", "payment_failed");
        assertThat(deliver(tampered, sign(payload, System.currentTimeMillis() / 1000)).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        assertThat(dbStatus(p[1].toString())).isEqualTo("PENDING");
    }

    // ---- resolving the unknown ------------------------------------------------------------------

    @Test
    void webhookResolvesATimedOutAuthorizationToAuthorized() {
        UUID[] p = pendingPayment();
        ResponseEntity<String> r = deliverSigned(event(newEventId(), "payment_intent.amount_capturable_updated", "pi_resolved", p[1].toString(), null));

        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(dbStatus(p[1].toString())).isEqualTo("AUTHORIZED");
        assertThat(outbox(p[0], "PaymentAuthorized")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT stripe_intent_id FROM payments WHERE id = ?::uuid", String.class, p[1].toString()))
                .isEqualTo("pi_resolved");
    }

    @Test
    void webhookResolvesATimedOutAuthorizationToFailed() {
        UUID[] p = pendingPayment();
        deliverSigned(event(newEventId(), "payment_intent.payment_failed", "pi_failed", p[1].toString(), "card_declined"));

        assertThat(dbStatus(p[1].toString())).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT failure_code FROM payments WHERE id = ?::uuid", String.class, p[1].toString()))
                .isEqualTo("card_declined");
        assertThat(outbox(p[0], "PaymentFailed")).isEqualTo(1);
    }

    // ---- duplicates and ordering ----------------------------------------------------------------

    @Test
    void sameEventDeliveredTwiceIsAppliedOnce() {
        UUID[] p = pendingPayment();
        String payload = event(newEventId(), "payment_intent.amount_capturable_updated", "pi_dup", p[1].toString(), null);
        assertThat(deliverSigned(payload).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(deliverSigned(payload).getStatusCode()).isEqualTo(HttpStatus.OK);      // Stripe retry: still 2xx
        assertThat(outbox(p[0], "PaymentAuthorized")).isEqualTo(1);
    }

    @Test
    void lateAuthorizationEventAfterTheFinalStateIsIgnored() {
        UUID[] p = pendingPayment();
        deliverSigned(event(newEventId(), "payment_intent.amount_capturable_updated", "pi_late", p[1].toString(), null));
        assertThat(statusOf(action(p[1].toString(), "capture"))).isEqualTo("CAPTURED");

        // a different event id for the same fact, arriving after the payment already moved on
        deliverSigned(event(newEventId(), "payment_intent.amount_capturable_updated", "pi_late", p[1].toString(), null));
        assertThat(dbStatus(p[1].toString())).isEqualTo("CAPTURED");
        assertThat(outbox(p[0], "PaymentAuthorized")).isEqualTo(1);
    }

    @Test
    void failedThenLateAuthorizedEventDoesNotResurrectThePayment() {
        UUID[] p = pendingPayment();
        deliverSigned(event(newEventId(), "payment_intent.payment_failed", "pi_f", p[1].toString(), "card_declined"));
        deliverSigned(event(newEventId(), "payment_intent.amount_capturable_updated", "pi_f", p[1].toString(), null));
        assertThat(dbStatus(p[1].toString())).isEqualTo("FAILED");
        assertThat(outbox(p[0], "PaymentAuthorized")).isZero();
    }

    @Test
    void captureConfirmedByWebhookDoesNotDuplicateTheCapturedEvent() {
        UUID order = UUID.randomUUID();
        String id = idOf(authorize(UUID.randomUUID().toString(), body(order, 5000)));
        action(id, "capture");
        deliverSigned(event(newEventId(), "payment_intent.succeeded", "pi_" + id, id, null));
        assertThat(dbStatus(id)).isEqualTo("CAPTURED");
        assertThat(outbox(order, "PaymentCaptured")).isEqualTo(1);
    }

    @Test
    void cancelConfirmedByWebhookDoesNotDuplicateTheCanceledEvent() {
        UUID order = UUID.randomUUID();
        String id = idOf(authorize(UUID.randomUUID().toString(), body(order, 5000)));
        action(id, "cancel");
        deliverSigned(event(newEventId(), "payment_intent.canceled", "pi_" + id, id, null));
        assertThat(dbStatus(id)).isEqualTo("CANCELED");
        assertThat(outbox(order, "PaymentCanceled")).isEqualTo(1);
    }

    // ---- noise ----------------------------------------------------------------------------------

    @Test
    void unknownEventTypesAndUnknownPaymentsAreAcknowledged() {
        assertThat(deliverSigned(event(newEventId(), "charge.refunded", "ch_1", UUID.randomUUID().toString(), null)).getStatusCode())
                .isEqualTo(HttpStatus.OK);
        assertThat(deliverSigned(event(newEventId(), "payment_intent.succeeded", "pi_unknown", UUID.randomUUID().toString(), null)).getStatusCode())
                .isEqualTo(HttpStatus.OK);
    }

    // ---- races ----------------------------------------------------------------------------------

    /** The webhook beats our own slow authorize call: the HTTP call must finish cleanly with no second event. */
    @Test
    void webhookArrivingWhileTheAuthorizeCallIsStillRunningCausesNoDoubleEvent() throws Exception {
        UUID order = UUID.randomUUID();
        gateway.authorizeDelayMs = 1500;
        CompletableFuture<ResponseEntity<String>> http = CompletableFuture.supplyAsync(
                () -> authorize(UUID.randomUUID().toString(), body(order, 5000)));

        UUID payment = null;
        for (int i = 0; i < 50 && payment == null; i++) {                     // wait for the PENDING row
            Thread.sleep(50);
            payment = jdbc.query("SELECT id FROM payments WHERE order_id = ?", (rs, n) -> rs.getObject(1, UUID.class), order)
                    .stream().findFirst().orElse(null);
        }
        assertThat(payment).isNotNull();
        deliverSigned(event(newEventId(), "payment_intent.amount_capturable_updated", "pi_race", payment.toString(), null));

        ResponseEntity<String> response = http.get();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(statusOf(response)).isEqualTo("AUTHORIZED");
        assertThat(outbox(order, "PaymentAuthorized")).isEqualTo(1);
    }
}
