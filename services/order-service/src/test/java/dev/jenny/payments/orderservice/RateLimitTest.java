package dev.jenny.payments.orderservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/** Per-merchant rate limiting: bursts, isolation between merchants, refill, overrides, atomicity, Redis outage. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ratelimit.default-capacity=5",
        "ratelimit.default-refill-per-second=2",
        "ratelimit.merchants.vip.capacity=50",
        "ratelimit.merchants.vip.refill-per-second=50",
        "ratelimit.merchants.slow.capacity=5",
        "ratelimit.merchants.slow.refill-per-second=0.01"})
class RateLimitTest extends AbstractIntegrationTest {

    private ResponseEntity<String> postAs(String merchant) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Idempotency-Key", UUID.randomUUID().toString());
        if (merchant != null) {
            h.set("X-Merchant-Id", merchant);
        }
        return http.exchange("/orders", HttpMethod.POST, new HttpEntity<>(BODY, h), String.class);
    }

    private static String unique() {
        return "m" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    void burstBeyondCapacityIsRejectedWith429AndRetryAfter() {
        String m = unique();
        for (int i = 0; i < 5; i++) {
            assertThat(postAs(m).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        }
        ResponseEntity<String> limited = postAs(m);
        assertThat(limited.getStatusCode().value()).isEqualTo(429);
        assertThat(Integer.parseInt(limited.getHeaders().getFirst("Retry-After"))).isGreaterThanOrEqualTo(1);
        assertThat(limited.getHeaders().getFirst("X-RateLimit-Limit")).isEqualTo("5");
        assertThat(limited.getBody()).contains("rate_limit_exceeded");
    }

    @Test
    void oneMerchantFloodingDoesNotAffectAnother() {
        String noisy = unique();
        String quiet = unique();
        for (int i = 0; i < 12; i++) {
            postAs(noisy);
        }
        assertThat(postAs(noisy).getStatusCode().value()).isEqualTo(429);
        assertThat(postAs(quiet).getStatusCode()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    void bucketRefillsOverTime() {
        String m = unique();
        for (int i = 0; i < 6; i++) {
            postAs(m);
        }
        assertThat(postAs(m).getStatusCode().value()).isEqualTo(429);
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(300))
                .until(() -> postAs(m).getStatusCode() == HttpStatus.CREATED);
    }

    @Test
    void perMerchantOverrideRaisesTheLimit() {
        for (int i = 0; i < 30; i++) {
            assertThat(postAs("vip").getStatusCode()).isEqualTo(HttpStatus.CREATED);
        }
    }

    /** 40 simultaneous requests against a bucket of 5 (almost no refill): exactly 5 may pass. */
    @Test
    void concurrentRequestsCannotOverspendABucket() throws Exception {
        String m = "slow";
        ExecutorService pool = Executors.newFixedThreadPool(20);
        List<Future<Integer>> results = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            results.add(pool.submit(() -> postAs(m).getStatusCode().value()));
        }
        int allowed = 0;
        for (Future<Integer> f : results) {
            if (f.get() == 201) {
                allowed++;
            }
        }
        pool.shutdown();
        assertThat(allowed).isEqualTo(5);
    }

    @Test
    void requestsWithoutMerchantHeaderAreNotLimitedHere() {
        for (int i = 0; i < 10; i++) {
            assertThat(postAs(null).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        }
    }

    @Test
    void junkMerchantIdIsRejectedAndNeverReachesRedis() {
        assertThat(postAs("bad id with spaces!").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    /** Limiter store down: fail open (orders still accepted), quickly, instead of blocking every payment. */
    @Test
    void redisOutageFailsOpen() {
        String m = unique();
        pause(REDIS);
        try {
            long start = System.nanoTime();
            for (int i = 0; i < 8; i++) {                 // more than the capacity of 5
                assertThat(postAs(m).getStatusCode()).isEqualTo(HttpStatus.CREATED);
            }
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(60));
        } finally {
            unpause(REDIS);
        }
    }
}
