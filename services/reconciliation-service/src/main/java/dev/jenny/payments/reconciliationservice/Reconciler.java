package dev.jenny.payments.reconciliationservice;

import dev.jenny.payments.reconciliationservice.Sources.Charge;
import dev.jenny.payments.reconciliationservice.Sources.Deposit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * The comparison itself, free of I/O so every case is a plain unit test.
 * Stripe is the evidence that money was collected; the Fineract ledger is the books. They must agree per payment.
 * This only finds differences. It never fixes anything: deciding which side is wrong is a human's job.
 */
final class Reconciler {

    enum Type { MISSING_IN_LEDGER, MISSING_IN_STRIPE, AMOUNT_MISMATCH, DUPLICATE_BOOKING }

    record Discrepancy(UUID paymentId, Type type, String detail) {}

    private Reconciler() {}

    static List<Discrepancy> compare(List<Charge> charges, List<Deposit> deposits) {
        Map<UUID, List<Deposit>> depositsByPayment = new LinkedHashMap<>();
        for (Deposit d : deposits) {
            depositsByPayment.computeIfAbsent(d.paymentId(), k -> new ArrayList<>()).add(d);
        }
        Set<UUID> chargedPayments = new HashSet<>();
        List<Discrepancy> found = new ArrayList<>();

        for (Charge c : charges) {
            chargedPayments.add(c.paymentId());
            List<Deposit> booked = depositsByPayment.getOrDefault(c.paymentId(), List.of());
            if (booked.isEmpty()) {
                found.add(new Discrepancy(c.paymentId(), Type.MISSING_IN_LEDGER,
                        "Stripe collected " + c.amount() + " " + c.currency() + " but the ledger has no deposit"));
            } else if (booked.size() > 1) {
                found.add(new Discrepancy(c.paymentId(), Type.DUPLICATE_BOOKING,
                        booked.size() + " ledger deposits for one payment: " + booked.stream().map(Deposit::transactionId).toList()));
            } else {
                Deposit d = booked.get(0);
                if (d.amount() != c.amount() || !d.currency().equalsIgnoreCase(c.currency())) {
                    found.add(new Discrepancy(c.paymentId(), Type.AMOUNT_MISMATCH,
                            "Stripe " + c.amount() + " " + c.currency() + " vs ledger " + d.amount() + " " + d.currency()));
                }
            }
        }
        for (Map.Entry<UUID, List<Deposit>> e : depositsByPayment.entrySet()) {
            if (!chargedPayments.contains(e.getKey())) {
                found.add(new Discrepancy(e.getKey(), Type.MISSING_IN_STRIPE,
                        "Ledger deposit " + e.getValue().get(0).transactionId() + " has no collected payment in Stripe"));
            }
        }
        return found;
    }
}
