package dev.jenny.payments.paymentservice;

import dev.jenny.payments.common.IdempotencyGuard;
import jakarta.validation.Valid;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final IdempotencyGuard guard;
    private final PaymentService payments;

    PaymentController(IdempotencyGuard guard, PaymentService payments) {
        this.guard = guard;
        this.payments = payments;
    }

    @PostMapping
    public ResponseEntity<String> authorize(@RequestHeader("Idempotency-Key") String key,
                                            @Valid @RequestBody PaymentRequest req) {
        if (key.isBlank() || key.length() > 255) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key must be 1-255 characters");
        }
        String hash = sha256(req.canonical());
        return guard.execute(key, hash, () -> payments.authorize(key, hash, req));
    }

    @PostMapping("/{id}/capture")
    public ResponseEntity<String> capture(@PathVariable UUID id) {
        return json(payments.capture(id));
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<String> cancel(@PathVariable UUID id) {
        return json(payments.cancel(id));
    }

    @GetMapping("/{id}")
    public ResponseEntity<String> get(@PathVariable UUID id) {
        return json(payments.get(id));
    }

    private static ResponseEntity<String> json(String body) {
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(body);
    }

    private static String sha256(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
