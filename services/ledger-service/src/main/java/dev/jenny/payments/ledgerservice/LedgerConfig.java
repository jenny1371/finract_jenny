package dev.jenny.payments.ledgerservice;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
class LedgerConfig {

    /** Several partitions so several consumers can run; key = orderId keeps one order's commands in order. */
    @Bean
    NewTopic ledgerCommands(@Value("${ledger.partitions:6}") int partitions) {
        return TopicBuilder.name(PaymentEventListener.COMMANDS_TOPIC).partitions(partitions).replicas(1).build();
    }

    /** Local-dev convenience: map the default merchant to a Fineract savings account. */
    @Bean
    ApplicationRunner seedDefaultMerchant(JdbcTemplate jdbc,
                                          @Value("${ledger.default-merchant-id}") String merchant,
                                          @Value("${ledger.default-savings-account-id}") long savingsId) {
        return args -> jdbc.update("INSERT INTO merchant_accounts (merchant_id, fineract_savings_id) VALUES (?, ?) "
                + "ON CONFLICT (merchant_id) DO NOTHING", merchant, savingsId);
    }
}
