package dev.jenny.payments.paymentservice;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Verifies the Stripe-Signature header: "t=timestamp,v1=hex(HMAC_SHA256(secret, t + '.' + rawBody))".
 * Checks the timestamp (replay protection) and compares in constant time. The raw request body must be used:
 * re-serialized JSON would not match the signature.
 */
@Component
class StripeSignatureVerifier {

    private final String secret;
    private final long toleranceSeconds;

    StripeSignatureVerifier(@Value("${stripe.webhook-secret:}") String secret,
                            @Value("${stripe.webhook-tolerance-seconds:300}") long toleranceSeconds) {
        this.secret = secret;
        this.toleranceSeconds = toleranceSeconds;
    }

    boolean configured() {
        return !secret.isBlank();
    }

    boolean verify(String payload, String header) {
        if (!configured() || header == null) {
            return false;
        }
        long timestamp = -1;
        java.util.List<String> signatures = new java.util.ArrayList<>();
        for (String part : header.split(",")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length != 2) {
                continue;
            }
            if (kv[0].equals("t")) {
                try {
                    timestamp = Long.parseLong(kv[1]);
                } catch (NumberFormatException e) {
                    return false;
                }
            } else if (kv[0].equals("v1")) {
                signatures.add(kv[1]);
            }
        }
        if (timestamp < 0 || signatures.isEmpty()) {
            return false;
        }
        if (Math.abs(System.currentTimeMillis() / 1000 - timestamp) > toleranceSeconds) {
            return false;
        }
        byte[] expected = hmac(timestamp + "." + payload).getBytes(StandardCharsets.UTF_8);
        return signatures.stream().anyMatch(s -> MessageDigest.isEqual(expected, s.getBytes(StandardCharsets.UTF_8)));
    }

    String hmac(String signedPayload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(signedPayload.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException(e);
        }
    }
}
