package dev.jenny.payments.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Operator tool: after fixing the cause of dead letters, push them back through the normal pipeline.
 *
 * Safe to call repeatedly and cannot loop:
 * - only dead letters that existed when the call started are replayed (messages that fail again and land in the
 *   DLT during the replay are left for the next call);
 * - offsets are tracked per DLT in a consumer group, so each dead letter is replayed once per call that reaches it;
 * - unparseable (poison) messages are skipped, replaying them can never succeed.
 */
public class DlqReplayer {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final String bootstrap;

    public DlqReplayer(KafkaTemplate<String, String> kafka, ObjectMapper mapper, String bootstrap) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.bootstrap = bootstrap;
    }

    public Map<String, Object> replay(String dltTopic, String targetTopic, int max) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "dlq-replay-" + dltTopic,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        int replayed = 0;
        int skipped = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            var infos = consumer.partitionsFor(dltTopic, Duration.ofSeconds(5));
            if (infos == null || infos.isEmpty()) {
                return Map.of("replayed", 0, "skipped", 0);
            }
            List<TopicPartition> parts = infos.stream().map(i -> new TopicPartition(i.topic(), i.partition())).toList();
            consumer.assign(parts);
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(parts));
            for (TopicPartition tp : parts) {
                OffsetAndMetadata c = committed.get(tp);
                if (c == null) {
                    consumer.seekToBeginning(List.of(tp));
                } else {
                    consumer.seek(tp, c.offset());
                }
            }
            Map<TopicPartition, Long> ends = consumer.endOffsets(parts);   // snapshot: ignore anything newer

            while (replayed + skipped < max && behindSnapshot(consumer, parts, ends)) {
                Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    if (r.offset() >= ends.get(tp)) {
                        continue;
                    }
                    if (parseable(r.value())) {
                        kafka.send(targetTopic, r.key(), r.value()).join();
                        replayed++;
                    } else {
                        skipped++;
                    }
                    toCommit.put(tp, new OffsetAndMetadata(r.offset() + 1));
                }
                if (!toCommit.isEmpty()) {
                    consumer.commitSync(toCommit);       // only after the batch was re-published
                }
            }
        }
        return Map.of("replayed", replayed, "skipped", skipped);
    }

    private static boolean behindSnapshot(KafkaConsumer<String, String> consumer, List<TopicPartition> parts,
                                          Map<TopicPartition, Long> ends) {
        return parts.stream().anyMatch(tp -> consumer.position(tp) < ends.get(tp));
    }

    private boolean parseable(String value) {
        try {
            return mapper.readTree(value).isObject();
        } catch (Exception e) {
            return false;
        }
    }
}
