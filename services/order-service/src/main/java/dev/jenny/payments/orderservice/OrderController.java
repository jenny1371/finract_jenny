package dev.jenny.payments.orderservice;

import dev.jenny.payments.common.IdempotencyGuard;
import jakarta.validation.Valid;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final IdempotencyGuard guard;
    private final OrderService orders;

    OrderController(IdempotencyGuard guard, OrderService orders) {
        this.guard = guard;
        this.orders = orders;
    }

    @PostMapping
    public ResponseEntity<String> create(@RequestHeader("Idempotency-Key") String key,
                                         @Valid @RequestBody OrderRequest req) {
        if (key.isBlank() || key.length() > 255) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key must be 1-255 characters");
        }
        String hash = sha256(req.canonical());
        return guard.execute(key, hash, () -> orders.create(key, hash, req));
    }

    @GetMapping("/{id}")
    public java.util.Map<String, Object> get(@PathVariable UUID id) {
        return orders.get(id);
    }

    private static String sha256(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
