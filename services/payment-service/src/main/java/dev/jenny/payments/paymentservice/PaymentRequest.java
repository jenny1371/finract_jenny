package dev.jenny.payments.paymentservice;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.util.UUID;

/** amount in minor units. paymentMethod defaults to Stripe's always-succeeding test card. */
public record PaymentRequest(
        @NotNull UUID orderId,
        @NotNull @Positive Long amount,
        @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
        @Pattern(regexp = "pm_[A-Za-z0-9_]+") String paymentMethod) {

    String method() {
        return paymentMethod == null ? "pm_card_visa" : paymentMethod;
    }

    String canonical() {
        return orderId + "|" + amount + "|" + currency + "|" + method();
    }
}
