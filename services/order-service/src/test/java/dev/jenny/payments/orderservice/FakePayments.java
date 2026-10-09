package dev.jenny.payments.orderservice;

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

/** Scriptable stand-in for payment-service's HTTP API (authorize, capture, cancel). Records every call. */
class FakePayments {

    enum Reply { OK, UNAVAILABLE, CONFLICT }

    record Call(String op, String idempotencyKey, String path, String body) {}

    final List<Call> calls = new CopyOnWriteArrayList<>();
    private final Map<String, ConcurrentLinkedQueue<Reply>> scripts = new ConcurrentHashMap<>();
    private final Map<String, Reply> defaults = new ConcurrentHashMap<>();
    private final HttpServer server;

    FakePayments() {
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
        calls.clear();
        scripts.clear();
        defaults.clear();
    }

    /** Next calls of {@code op} answer with these replies in order, then fall back to the default (OK). */
    void script(String op, Reply... replies) {
        scripts.computeIfAbsent(op, k -> new ConcurrentLinkedQueue<>()).addAll(List.of(replies));
    }

    void always(String op, Reply reply) {
        defaults.put(op, reply);
    }

    List<Call> callsFor(String op, String contains) {
        return calls.stream().filter(c -> c.op().equals(op) && (c.path() + c.body()).contains(contains)).toList();
    }

    private void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String op = path.endsWith("/capture") ? "capture" : path.endsWith("/cancel") ? "cancel" : "authorize";
        calls.add(new Call(op, ex.getRequestHeaders().getFirst("Idempotency-Key"), path, body));

        ConcurrentLinkedQueue<Reply> script = scripts.get(op);
        Reply reply = script != null && !script.isEmpty() ? script.poll() : defaults.getOrDefault(op, Reply.OK);
        int status = switch (reply) {
            case OK -> op.equals("authorize") ? 201 : 200;
            case UNAVAILABLE -> 503;
            case CONFLICT -> 409;
        };
        byte[] bytes = "{}".getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }
}
