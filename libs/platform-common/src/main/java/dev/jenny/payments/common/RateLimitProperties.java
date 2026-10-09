package dev.jenny.payments.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * ratelimit.default-capacity / default-refill-per-second apply to every merchant;
 * ratelimit.merchants.{id}.capacity / refill-per-second override them for one merchant.
 * capacity = burst size, refill = sustained requests per second.
 */
@ConfigurationProperties("ratelimit")
public class RateLimitProperties {

    public static class Limit {
        private int capacity;
        private double refillPerSecond;

        public int getCapacity() { return capacity; }
        public void setCapacity(int capacity) { this.capacity = capacity; }
        public double getRefillPerSecond() { return refillPerSecond; }
        public void setRefillPerSecond(double refillPerSecond) { this.refillPerSecond = refillPerSecond; }
    }

    private boolean enabled = true;
    private int defaultCapacity = 20;
    private double defaultRefillPerSecond = 10;
    private List<String> paths = List.of("/orders", "/payments");
    private Map<String, Limit> merchants = new HashMap<>();

    public Limit limitFor(String merchantId) {
        Limit override = merchants.get(merchantId);
        if (override != null) {
            return override;
        }
        Limit l = new Limit();
        l.setCapacity(defaultCapacity);
        l.setRefillPerSecond(defaultRefillPerSecond);
        return l;
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public int getDefaultCapacity() { return defaultCapacity; }
    public void setDefaultCapacity(int defaultCapacity) { this.defaultCapacity = defaultCapacity; }
    public double getDefaultRefillPerSecond() { return defaultRefillPerSecond; }
    public void setDefaultRefillPerSecond(double defaultRefillPerSecond) { this.defaultRefillPerSecond = defaultRefillPerSecond; }
    public List<String> getPaths() { return paths; }
    public void setPaths(List<String> paths) { this.paths = paths; }
    public Map<String, Limit> getMerchants() { return merchants; }
    public void setMerchants(Map<String, Limit> merchants) { this.merchants = merchants; }
}
