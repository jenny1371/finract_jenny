package dev.jenny.payments.ledgerservice;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Scriptable stand-in for the Fineract deposit API. Like real Fineract it honours Idempotency-Key
 * (the same key is applied once). TIMEOUT_AFTER_APPLY is the dangerous case: applied, but the caller
 * never gets the answer.
 */
class FakeFineract {

    enum Mode { OK, CONFLICT, SERVER_ERROR, BAD_REQUEST, TIMEOUT_AFTER_APPLY }

    record Req(String key, long savingsId, String body) {}

    final List<Req> requests = new CopyOnWriteArrayList<>();
    final List<Req> undoRequests = new CopyOnWriteArrayList<>();    // Req.body holds the transaction id here
    final java.util.Set<String> undoneTxns = ConcurrentHashMap.newKeySet();
    volatile Mode undoMode = Mode.OK;
    final ConcurrentLinkedQueue<Mode> script = new ConcurrentLinkedQueue<>();
    final Map<String, String> appliedByKey = new ConcurrentHashMap<>();
    final AtomicInteger inFlight = new AtomicInteger();
    final AtomicInteger maxInFlight = new AtomicInteger();
    private final AtomicLong txnSeq = new AtomicLong(1000);
    volatile Mode defaultMode = Mode.OK;
    volatile long delayMs = 0;
    private final HttpServer server;

    FakeFineract() {
        try {
            server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/", this::handle);
        server.start();
    }

    int port() {
        return server.getAddress().getPort();
    }

    void reset() {
        requests.clear();
        undoRequests.clear();
        undoMode = Mode.OK;
        script.clear();
        defaultMode = Mode.OK;
        delayMs = 0;
        maxInFlight.set(0);
    }

    List<Req> requestsFor(String paymentId) {
        return requests.stream().filter(r -> r.key().startsWith("ledger-" + paymentId + "-")).toList();
    }

    long appliedFor(String paymentId) {
        return appliedByKey.keySet().stream().filter(k -> k.startsWith("ledger-" + paymentId + "-")).count();
    }

    private void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        if (path.endsWith("/paymenttypes")) {
            respond(ex, 200, "[{\"id\":4,\"name\":\"Stripe\"}]");
            return;
        }
        String[] parts = path.split("/");
        String query = ex.getRequestURI().getQuery() == null ? "" : ex.getRequestURI().getQuery();
        if (query.contains("command=undo")) {          // .../savingsaccounts/{id}/transactions/{txn}?command=undo
            String undoKey = ex.getRequestHeaders().getFirst("Idempotency-Key");
            String txn = parts[parts.length - 1];
            undoRequests.add(new Req(undoKey, Long.parseLong(parts[parts.length - 3]), txn));
            switch (undoMode) {
                case SERVER_ERROR -> respond(ex, 500, "{\"error\":\"boom\"}");
                case BAD_REQUEST -> respond(ex, 400, "{\"errors\":[]}");
                default -> {
                    undoneTxns.add(txn);
                    respond(ex, 200, "{\"resourceId\":\"" + txn + "\"}");
                }
            }
            return;
        }
        // .../savingsaccounts/{id}/transactions
        long savingsId = Long.parseLong(parts[parts.length - 2]);
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String key = ex.getRequestHeaders().getFirst("Idempotency-Key");

        int current = inFlight.incrementAndGet();
        maxInFlight.accumulateAndGet(current, Math::max);
        try {
            requests.add(new Req(key, savingsId, body));
            sleep(delayMs);
            Mode mode = script.isEmpty() ? defaultMode : script.poll();
            switch (mode) {
                case CONFLICT -> respond(ex, 409, "");
                case SERVER_ERROR -> respond(ex, 500, "{\"error\":\"boom\"}");
                case BAD_REQUEST -> respond(ex, 400, "{\"errors\":[{\"defaultUserMessage\":\"invalid\"}]}");
                case TIMEOUT_AFTER_APPLY -> {
                    apply(key);
                    sleep(2_000);                       // client read timeout is shorter than this
                    respond(ex, 200, "{\"resourceId\":\"" + apply(key) + "\"}");
                }
                default -> respond(ex, 200, "{\"resourceId\":\"" + apply(key) + "\"}");
            }
        } finally {
            inFlight.decrementAndGet();
        }
    }

    private String apply(String key) {
        return appliedByKey.computeIfAbsent(key, k -> "txn-" + txnSeq.incrementAndGet());
    }

    private static void respond(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        try {
            ex.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                ex.getResponseBody().write(bytes);
            }
        } catch (IOException ignored) {
            // client already gave up (timeout): expected in TIMEOUT_AFTER_APPLY
        } finally {
            ex.close();
        }
    }

    private static void sleep(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
