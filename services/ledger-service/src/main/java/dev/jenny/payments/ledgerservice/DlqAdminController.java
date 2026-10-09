package dev.jenny.payments.ledgerservice;

import dev.jenny.payments.common.DlqReplayer;
import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Operator tool: replay dead-lettered ledger commands once the cause is fixed. See DlqReplayer for the guarantees. */
@RestController
@RequestMapping("/admin/dlq")
class DlqAdminController {

    static final String DLT = PaymentEventListener.COMMANDS_TOPIC + "-dlt";

    private final DlqReplayer replayer;

    DlqAdminController(DlqReplayer replayer) {
        this.replayer = replayer;
    }

    @PostMapping("/replay")
    Map<String, Object> replay(@RequestParam(defaultValue = "100") int max) {
        return replayer.replay(DLT, PaymentEventListener.COMMANDS_TOPIC, max);
    }
}
