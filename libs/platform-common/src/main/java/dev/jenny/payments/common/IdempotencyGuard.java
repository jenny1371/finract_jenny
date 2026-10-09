package dev.jenny.payments.common;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.server.ResponseStatusException;

/**
 * Idempotency-Key handling:
 * <ul>
 *   <li>same key, request still in flight   -> 409 (do not block a thread waiting)</li>
 *   <li>same key, same request, finished     -> original response replayed (header Idempotent-Replayed)</li>
 *   <li>same key, different request          -> 422</li>
 * </ul>
 * Redis (SETNX lock + response cache) is the fast path; the Postgres primary key is the authority,
 * so a Redis outage degrades performance, not correctness.
 */
public class IdempotencyGuard {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyGuard.class);
    static final Duration LOCK_TTL = Duration.ofSeconds(30);
    static final Duration KEY_TTL = Duration.ofHours(24);

    private final StringRedisTemplate redis;
    private final JdbcTemplate jdbc;
    private final MeterRegistry meters;

    public IdempotencyGuard(StringRedisTemplate redis, JdbcTemplate jdbc, MeterRegistry meters) {
        this.redis = redis;
        this.jdbc = jdbc;
        this.meters = meters;
    }

    public ResponseEntity<String> execute(String key, String requestHash, Supplier<String> action) {
        String cached = redisGet("idem:resp:" + key);
        if (cached != null) {
            return replay(cached.substring(0, cached.indexOf(':')), cached.substring(cached.indexOf(':') + 1), requestHash);
        }

        boolean locked = tryLock(key);
        if (!locked) {
            outcome("in_progress");
            throw new ResponseStatusException(HttpStatus.CONFLICT, "A request with this Idempotency-Key is still being processed");
        }
        try {
            List<String[]> stored = jdbc.query(
                    "SELECT request_hash, response_body FROM idempotency_keys WHERE idem_key = ?",
                    (rs, i) -> new String[] {rs.getString(1).trim(), rs.getString(2)}, key);
            if (!stored.isEmpty()) {
                redisSet("idem:resp:" + key, stored.get(0)[0] + ":" + stored.get(0)[1]);
                return replay(stored.get(0)[0], stored.get(0)[1], requestHash);
            }

            String body = action.get();
            redisSet("idem:resp:" + key, requestHash + ":" + body);
            outcome("created");
            return response(body, false);
        } catch (DuplicateKeyException e) {
            // Lost the race (e.g. Redis lock lost): the DB primary key stopped the duplicate.
            outcome("in_progress");
            throw new ResponseStatusException(HttpStatus.CONFLICT, "A request with this Idempotency-Key is still being processed");
        } finally {
            redisDelete("idem:lock:" + key);
        }
    }

    private ResponseEntity<String> replay(String storedHash, String body, String requestHash) {
        if (!storedHash.equals(requestHash)) {
            outcome("mismatch");
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Idempotency-Key was already used with a different request");
        }
        outcome("replayed");
        return response(body, true);
    }

    private ResponseEntity<String> response(String body, boolean replayed) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotent-Replayed", String.valueOf(replayed))
                .body(body);
    }

    private void outcome(String name) {
        meters.counter("orders.idempotency", "outcome", name).increment();
    }

    // --- Redis helpers: never fail the request because Redis is unavailable ---

    private boolean tryLock(String key) {
        try {
            return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent("idem:lock:" + key, "1", LOCK_TTL));
        } catch (RuntimeException e) {
            log.warn("Redis unavailable, relying on DB unique key: {}", e.toString());
            return true;
        }
    }

    private String redisGet(String k) {
        try {
            return redis.opsForValue().get(k);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private void redisSet(String k, String v) {
        try {
            redis.opsForValue().set(k, v, KEY_TTL);
        } catch (RuntimeException e) {
            log.warn("Redis set failed: {}", e.toString());
        }
    }

    private void redisDelete(String k) {
        try {
            redis.delete(k);
        } catch (RuntimeException e) {
            log.warn("Redis delete failed: {}", e.toString());
        }
    }
}
