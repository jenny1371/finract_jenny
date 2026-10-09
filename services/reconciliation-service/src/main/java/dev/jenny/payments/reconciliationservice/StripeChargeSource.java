package dev.jenny.payments.reconciliationservice;

import com.stripe.Stripe;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.param.PaymentIntentListParams;
import dev.jenny.payments.reconciliationservice.Sources.Charge;
import dev.jenny.payments.reconciliationservice.Sources.ChargeSource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Collected payments straight from Stripe: succeeded PaymentIntents that carry our payment id in metadata. */
@Component
class StripeChargeSource implements ChargeSource {

    StripeChargeSource(@Value("${stripe.secret-key:}") String secretKey) {
        Stripe.apiKey = secretKey;
        Stripe.setConnectTimeout(3_000);
        Stripe.setReadTimeout(20_000);
    }

    @Override
    public List<Charge> fetch(Instant from, Instant to) {
        PaymentIntentListParams params = PaymentIntentListParams.builder()
                .setCreated(PaymentIntentListParams.Created.builder()
                        .setGte(from.getEpochSecond()).setLte(to.getEpochSecond()).build())
                .setLimit(100L)
                .build();
        List<Charge> out = new ArrayList<>();
        try {
            // autoPagingIterable follows every page; a mid-way failure throws, so we never return a partial list
            for (PaymentIntent pi : PaymentIntent.list(params).autoPagingIterable()) {
                String paymentId = pi.getMetadata() == null ? null : pi.getMetadata().get("paymentId");
                if (!"succeeded".equals(pi.getStatus()) || paymentId == null) {
                    continue;
                }
                out.add(new Charge(UUID.fromString(paymentId), pi.getAmountReceived(), pi.getCurrency().toUpperCase(),
                        Instant.ofEpochSecond(pi.getCreated())));
            }
        } catch (StripeException e) {
            throw new IllegalStateException("Stripe unavailable: " + e.getMessage(), e);
        }
        return out;
    }
}
