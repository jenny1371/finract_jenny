package dev.jenny.payments.reconciliationservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import dev.jenny.payments.reconciliationservice.Sources.Deposit;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The fake mimics GET /savingsaccounts/{id}/transactions/search as real Fineract returns it
 * (captured from apache/fineract on 2026-10-06): {"total": N, "content": [ ...transactions... ], "pageable": ...}
 * with offset/limit paging. If Fineract changes the format, this test (and FineractDepositSourceLiveTest) say so.
 */
class FineractDepositSourceTest {

    private HttpServer server;
    private volatile int status = 200;
    private final List<String> transactions = new CopyOnWriteArrayList<>();
    private final List<String> queries = new CopyOnWriteArrayList<>();

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", ex -> {
            String query = ex.getRequestURI().getRawQuery() == null ? "" : ex.getRequestURI().getRawQuery();
            queries.add(ex.getRequestURI().getPath() + "?" + URLDecoder.decode(query, StandardCharsets.UTF_8));
            Map<String, String> p = new HashMap<>();
            for (String kv : query.split("&")) {
                String[] parts = kv.split("=", 2);
                if (parts.length == 2) {
                    p.put(parts[0], URLDecoder.decode(parts[1], StandardCharsets.UTF_8));
                }
            }
            int offset = Integer.parseInt(p.getOrDefault("offset", "0"));
            int limit = Integer.parseInt(p.getOrDefault("limit", "20"));
            List<String> slice = transactions.subList(Math.min(offset, transactions.size()),
                    Math.min(offset + limit, transactions.size()));
            byte[] bytes = ("{\"total\":" + transactions.size() + ",\"content\":[" + String.join(",", slice)
                    + "],\"pageable\":{\"offset\":" + offset + "}}").getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(status, bytes.length);
            ex.getResponseBody().write(bytes);
            ex.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private FineractDepositSource source() {
        return new FineractDepositSource("http://localhost:" + server.getAddress().getPort(), "mifos", "password",
                "default", 1000, 2000, List.of(2L));
    }

    private static String tx(int id, boolean deposit, boolean reversed, String amount, String note, LocalDate date) {
        return "{\"id\":" + id + ",\"transactionType\":{\"code\":\"savingsAccountTransactionType."
                + (deposit ? "deposit" : "withdrawal") + "\",\"deposit\":" + deposit + ",\"withdrawal\":" + !deposit + "},"
                + "\"currency\":{\"code\":\"USD\",\"decimalPlaces\":2},\"amount\":" + amount + ",\"reversed\":" + reversed + ","
                + "\"note\":" + (note == null ? "null" : "\"" + note + "\"") + ","
                + "\"date\":[" + date.getYear() + "," + date.getMonthValue() + "," + date.getDayOfMonth() + "]}";
    }

    private List<Deposit> fetch() {
        return source().fetch(Instant.now().minusSeconds(3600), Instant.now());
    }

    @Test
    void readsOnlyLiveDepositsThatCarryAPaymentNoteAndConvertsAmountsToMinorUnits() {
        UUID ours = UUID.randomUUID();
        UUID undone = UUID.randomUUID();
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        transactions.add(tx(96, true, false, "12.340000", "payment " + ours, today));              // counts
        transactions.add(tx(97, true, true, "5.000000", "payment " + undone, today));              // reversed: ignored
        transactions.add(tx(98, false, false, "1.000000", "payment " + UUID.randomUUID(), today)); // withdrawal: ignored
        transactions.add(tx(99, true, false, "2.000000", null, today));                            // no note: not ours
        transactions.add(tx(100, true, false, "3.000000", "salary", today));                       // note without payment id
        transactions.add(tx(101, true, false, "9.000000", "payment " + UUID.randomUUID(), today.minusDays(30)));   // outside window

        List<Deposit> deposits = fetch();

        assertThat(deposits).hasSize(1);
        assertThat(deposits.get(0).paymentId()).isEqualTo(ours);
        assertThat(deposits.get(0).transactionId()).isEqualTo("96");
        assertThat(deposits.get(0).amount()).isEqualTo(1234);          // 12.34 -> 1234 cents
        assertThat(deposits.get(0).currency()).isEqualTo("USD");
    }

    /** The point of the change: never read the whole account at once, and follow every page. */
    @Test
    void followsEveryPageAndAsksFineractForADateRangeNotTheWholeHistory() {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 450; i++) {                                 // PAGE_SIZE is 200 -> three pages
            UUID id = UUID.randomUUID();
            ids.add(id);
            transactions.add(tx(1000 + i, true, false, "1.000000", "payment " + id, today));
        }

        List<Deposit> deposits = fetch();

        assertThat(deposits).hasSize(450);
        assertThat(deposits).extracting(Deposit::paymentId).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(queries).hasSize(3);
        assertThat(queries).allSatisfy(q -> {
            assertThat(q).startsWith("/savingsaccounts/2/transactions/search?");
            assertThat(q).contains("limit=200").contains("fromDate=").contains("dateFormat=dd MMMM yyyy");
        });
        assertThat(queries.get(1)).contains("offset=200");
        assertThat(queries.get(2)).contains("offset=400");
    }

    @Test
    void anAccountWithoutTransactionsYieldsNothingAfterOneRequest() {
        assertThat(fetch()).isEmpty();
        assertThat(queries).hasSize(1);
    }

    /** A Fineract error must surface as an exception so the run fails instead of reporting an empty ledger. */
    @Test
    void fineractErrorsFailTheFetchInsteadOfLookingLikeAnEmptyLedger() {
        status = 500;
        assertThatThrownBy(this::fetch).isInstanceOf(IllegalStateException.class).hasMessageContaining("Fineract unavailable");
    }

    @Test
    void anUnreachableFineractAlsoFailsTheFetch() {
        server.stop(0);
        assertThatThrownBy(this::fetch).isInstanceOf(IllegalStateException.class);
    }
}
