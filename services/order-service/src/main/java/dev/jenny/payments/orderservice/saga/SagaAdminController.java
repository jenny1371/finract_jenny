package dev.jenny.payments.orderservice.saga;

import dev.jenny.payments.common.DlqReplayer;
import java.util.Map;
import java.util.Set;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Operator tool: replay dead-lettered saga messages from one source topic after the cause was fixed.
 * Only the topics the saga actually reads are allowed. See DlqReplayer for the guarantees (cannot loop, skips poison).
 * TODO: protect with an admin role once JWT authentication is wired in.
 */
@RestController
@RequestMapping("/admin/dlq")
class SagaAdminController {

    static final Set<String> TOPICS = Set.of("order.events", "payment.events", "ledger.events");

    private final DlqReplayer replayer;

    SagaAdminController(DlqReplayer replayer) {
        this.replayer = replayer;
    }

    @PostMapping("/replay")
    Map<String, Object> replay(@RequestParam String topic, @RequestParam(defaultValue = "100") int max) {
        if (!TOPICS.contains(topic)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "topic must be one of " + TOPICS);
        }
        return replayer.replay(topic + "-dlt", topic, max);
    }
}
