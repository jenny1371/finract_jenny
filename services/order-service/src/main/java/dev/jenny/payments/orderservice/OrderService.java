package dev.jenny.payments.orderservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class OrderService {

    static final String TOPIC = "order.events";

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    OrderService(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /**
     * One transaction writes the order, the idempotency record and the outbox event, so either all
     * exist or none do. Returns the JSON response body that is replayed for repeated requests.
     */
    @Transactional
    public String create(String idemKey, String requestHash, OrderRequest req) {
        UUID orderId = UUID.randomUUID();
        jdbc.update("INSERT INTO orders (id, amount, currency, status, merchant_id, payment_method) "
                        + "VALUES (?, ?, ?, 'CREATED', ?, ?)",
                orderId, req.amount(), req.currency(), req.merchantId(), req.method());

        String body = json(Map.of("id", orderId, "amount", req.amount(), "currency", req.currency(),
                "status", "CREATED"));
        jdbc.update("INSERT INTO idempotency_keys (idem_key, request_hash, order_id, response_body) VALUES (?, ?, ?, ?)",
                idemKey, requestHash, orderId, body);

        UUID eventId = UUID.randomUUID();
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventId", eventId);
        event.put("type", "OrderCreated");
        event.put("orderId", orderId);
        event.put("amount", req.amount());
        event.put("currency", req.currency());
        event.put("merchantId", req.merchantId());
        event.put("paymentMethod", req.method());
        jdbc.update("INSERT INTO outbox (id, aggregate_id, topic, event_type, payload) VALUES (?, ?, ?, 'OrderCreated', ?)",
                eventId, orderId.toString(), TOPIC, json(event));
        return body;
    }

    public Map<String, Object> get(UUID id) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT id, amount, currency, status, merchant_id, payment_id, failure_reason FROM orders WHERE id = ?", id);
        if (rows.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found");
        }
        return rows.get(0);
    }

    private String json(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
