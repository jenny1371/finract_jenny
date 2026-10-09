package dev.jenny.payments.common;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.regex.Pattern;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Per-merchant rate limit on write requests. The merchant is read from the X-Merchant-Id header for now;
 * once JWT authentication exists it must come from the verified token instead (a header can be spoofed).
 * Requests without the header are not limited here (they will be rejected by authentication later).
 */
public class RateLimitFilter extends OncePerRequestFilter {

    private static final Pattern MERCHANT = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    private final RateLimiter limiter;
    private final RateLimitProperties props;

    public RateLimitFilter(RateLimiter limiter, RateLimitProperties props) {
        this.limiter = limiter;
        this.props = props;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        if (!"POST".equals(request.getMethod())) {
            return true;
        }
        String path = request.getRequestURI();
        return props.getPaths().stream().noneMatch(path::startsWith);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String merchant = request.getHeader("X-Merchant-Id");
        if (merchant == null || merchant.isBlank()) {
            chain.doFilter(request, response);
            return;
        }
        if (!MERCHANT.matcher(merchant).matches()) {                  // also keeps junk out of Redis keys
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid X-Merchant-Id");
            return;
        }
        RateLimitProperties.Limit limit = props.limitFor(merchant);
        RateLimiter.Decision d = limiter.tryAcquire("rl:" + merchant, limit.getCapacity(), limit.getRefillPerSecond());
        response.setHeader("X-RateLimit-Limit", String.valueOf(limit.getCapacity()));
        response.setHeader("X-RateLimit-Remaining", String.valueOf(d.remaining()));
        if (d.allowed()) {
            chain.doFilter(request, response);
            return;
        }
        response.setStatus(429);
        response.setHeader("Retry-After", String.valueOf(Math.max(1, (d.retryAfterMs() + 999) / 1000)));
        response.setContentType("application/json");
        response.getWriter().write("{\"error\":\"rate_limit_exceeded\",\"retryAfterMs\":" + d.retryAfterMs() + "}");
    }
}
