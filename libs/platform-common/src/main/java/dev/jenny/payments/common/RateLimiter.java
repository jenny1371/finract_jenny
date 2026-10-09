package dev.jenny.payments.common;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Distributed token-bucket limiter backed by one Lua script in Redis (atomic, so concurrent requests on
 * any number of instances cannot over-spend a bucket).
 *
 * Failure policy: if Redis is unavailable we FAIL OPEN (allow) and count it. Blocking every payment because the
 * limiter's store is down is a worse outage than briefly not limiting; the idempotency layer still protects data.
 */
public class RateLimiter {

    public record Decision(boolean allowed, long remaining, long retryAfterMs) {}

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> script = new DefaultRedisScript<>();
    private final StringRedisTemplate redis;
    private final MeterRegistry meters;

    public RateLimiter(StringRedisTemplate redis, MeterRegistry meters) {
        this.redis = redis;
        this.meters = meters;
        script.setLocation(new ClassPathResource("ratelimit.lua"));
        script.setResultType(List.class);
    }

    public Decision tryAcquire(String key, int capacity, double refillPerSecond) {
        try {
            List<?> r = redis.execute(script, List.of(key), String.valueOf(capacity), String.valueOf(refillPerSecond), "1");
            boolean allowed = ((Number) r.get(0)).longValue() == 1;
            meters.counter("ratelimit.decisions", "outcome", allowed ? "allowed" : "limited").increment();
            return new Decision(allowed, ((Number) r.get(1)).longValue(), ((Number) r.get(2)).longValue());
        } catch (RuntimeException e) {
            log.warn("Rate limiter unavailable, failing open: {}", e.toString());
            meters.counter("ratelimit.decisions", "outcome", "error_fail_open").increment();
            return new Decision(true, capacity, 0);
        }
    }
}
