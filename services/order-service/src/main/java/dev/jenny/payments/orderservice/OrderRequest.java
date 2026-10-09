package dev.jenny.payments.orderservice;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

/**
 * amount is in minor units (cents), like Stripe. merchantId is optional until real authentication exists;
 * paymentMethod defaults to Stripe's always-succeeding test card.
 */
public record OrderRequest(
        @NotNull @Positive Long amount,
        @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
        @Pattern(regexp = "[A-Za-z0-9_-]{1,64}") String merchantId,
        @Pattern(regexp = "pm_[A-Za-z0-9_]+") String paymentMethod) {

    String method() {
        return paymentMethod == null ? "pm_card_visa" : paymentMethod;
    }

    /** Canonical form used for the idempotency request hash. */
    String canonical() {
        return amount + "|" + currency + "|" + merchantId + "|" + method();
    }
}
