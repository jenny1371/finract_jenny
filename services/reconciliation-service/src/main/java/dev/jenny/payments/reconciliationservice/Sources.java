package dev.jenny.payments.reconciliationservice;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** The two views of the same money. Both are keyed by our payment id. */
final class Sources {

    private Sources() {}

    /** A payment Stripe actually collected (PaymentIntent succeeded). Amount in minor units. */
    record Charge(UUID paymentId, long amount, String currency, Instant createdAt) {}

    /** A deposit in the Fineract ledger that was not reversed. Amount in minor units. */
    record Deposit(UUID paymentId, String transactionId, long amount, String currency) {}

    /** Stripe side. Implementations must throw if the data could not be fetched completely. */
    interface ChargeSource {
        List<Charge> fetch(Instant from, Instant to);
    }

    /** Ledger side. Implementations must throw if the data could not be fetched completely. */
    interface DepositSource {
        List<Deposit> fetch(Instant from, Instant to);
    }
}
