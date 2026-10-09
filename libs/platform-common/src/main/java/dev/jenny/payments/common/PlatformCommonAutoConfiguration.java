package dev.jenny.payments.common;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Wires the shared beans into any service that has this module on its classpath. */
@AutoConfiguration(after = {JdbcTemplateAutoConfiguration.class, RedisAutoConfiguration.class, KafkaAutoConfiguration.class})
@EnableScheduling
@EnableConfigurationProperties(RateLimitProperties.class)
public class PlatformCommonAutoConfiguration {

    @Bean
    IdempotencyGuard idempotencyGuard(StringRedisTemplate redis, JdbcTemplate jdbc, MeterRegistry meters) {
        return new IdempotencyGuard(redis, jdbc, meters);
    }

    @Bean
    @ConditionalOnProperty(name = "outbox.enabled", havingValue = "true", matchIfMissing = true)
    OutboxPoller outboxPoller(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, MeterRegistry meters,
                              @Value("${outbox.batch-size:100}") int batchSize) {
        return new OutboxPoller(jdbc, kafka, meters, batchSize);
    }

    @Bean
    RetentionCleaner retentionCleaner(JdbcTemplate jdbc) {
        return new RetentionCleaner(jdbc);
    }

    @Bean
    DlqReplayer dlqReplayer(KafkaTemplate<String, String> kafka, com.fasterxml.jackson.databind.ObjectMapper mapper,
                            @Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        return new DlqReplayer(kafka, mapper, bootstrap);
    }

    @Bean
    RateLimiter rateLimiter(StringRedisTemplate redis, MeterRegistry meters) {
        return new RateLimiter(redis, meters);
    }

    @Bean
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    @ConditionalOnProperty(name = "ratelimit.enabled", havingValue = "true", matchIfMissing = true)
    FilterRegistrationBean<RateLimitFilter> rateLimitFilter(RateLimiter limiter, RateLimitProperties props) {
        FilterRegistrationBean<RateLimitFilter> bean = new FilterRegistrationBean<>(new RateLimitFilter(limiter, props));
        bean.setOrder(1);
        return bean;
    }
}
