package dev.jenny.payments.ledgerservice;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/ledger")
class LedgerController {

    private final JdbcTemplate jdbc;

    LedgerController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/entries/{paymentId}")
    Map<String, Object> entry(@PathVariable UUID paymentId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT payment_id, order_id, merchant_id, amount, currency, status, attempt, fineract_txn_id, failure_reason "
                        + "FROM ledger_entries WHERE payment_id = ?", paymentId);
        if (rows.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No ledger entry for payment");
        }
        return rows.get(0);
    }
}
