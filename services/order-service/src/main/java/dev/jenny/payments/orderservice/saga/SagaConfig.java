package dev.jenny.payments.orderservice.saga;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Topics the saga reads or writes. Declared here too so the listener never subscribes to a missing topic. */
@Configuration
class SagaConfig {

    @Bean
    NewTopic orderEvents(@Value("${saga.partitions:6}") int partitions) {
        return TopicBuilder.name("order.events").partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic paymentEvents(@Value("${saga.partitions:6}") int partitions) {
        return TopicBuilder.name("payment.events").partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic ledgerEvents(@Value("${saga.partitions:6}") int partitions) {
        return TopicBuilder.name("ledger.events").partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic ledgerCommands(@Value("${saga.partitions:6}") int partitions) {
        return TopicBuilder.name(OrderSaga.LEDGER_COMMANDS).partitions(partitions).replicas(1).build();
    }
}
