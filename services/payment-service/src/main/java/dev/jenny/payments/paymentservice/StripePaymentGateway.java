package dev.jenny.payments.paymentservice;

import com.stripe.Stripe;
import com.stripe.exception.ApiConnectionException;
import com.stripe.exception.ApiException;
import com.stripe.exception.CardException;
import com.stripe.exception.RateLimitException;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.net.RequestOptions;
import com.stripe.param.PaymentIntentCancelParams;
import com.stripe.param.PaymentIntentCaptureParams;
import com.stripe.param.PaymentIntentCreateParams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

/** Stripe (test mode) adapter: manual capture, so authorization and capture are separate steps. */
@Component
@ConditionalOnExpression("${stripe.enabled:true} and not ${stripe.simulated:false}")
public class StripePaymentGateway implements PaymentGateway {

    StripePaymentGateway(@Value("${stripe.secret-key:}") String secretKey) {
        Stripe.apiKey = secretKey;
        Stripe.setConnectTimeout(3_000);
        Stripe.setReadTimeout(10_000);
        Stripe.setMaxNetworkRetries(0);   // retries are our job (visible, bounded, same idempotency key)
    }

    @Override
    public String authorize(String idempotencyKey, long amount, String currency, String paymentMethod, String paymentId) {
        PaymentIntentCreateParams params = PaymentIntentCreateParams.builder()
                .setAmount(amount)
                .setCurrency(currency.toLowerCase())
                .setCaptureMethod(PaymentIntentCreateParams.CaptureMethod.MANUAL)
                .setConfirm(true)
                .setPaymentMethod(paymentMethod)
                .setAutomaticPaymentMethods(PaymentIntentCreateParams.AutomaticPaymentMethods.builder()
                        .setEnabled(true)
                        .setAllowRedirects(PaymentIntentCreateParams.AutomaticPaymentMethods.AllowRedirects.NEVER)
                        .build())
                .putMetadata("paymentId", paymentId)
                .build();
        try {
            PaymentIntent intent = PaymentIntent.create(params, options(idempotencyKey));
            if (!"requires_capture".equals(intent.getStatus())) {
                throw new IllegalStateException("Unexpected PaymentIntent status " + intent.getStatus());
            }
            return intent.getId();
        } catch (CardException e) {
            throw new CardDeclinedException(e.getCode());
        } catch (StripeException e) {
            throw classify(e);
        }
    }

    @Override
    public void capture(String idempotencyKey, String intentId) {
        try {
            PaymentIntent intent = PaymentIntent.retrieve(intentId);
            if ("succeeded".equals(intent.getStatus())) {
                return;                       // already captured by an earlier attempt
            }
            intent.capture(PaymentIntentCaptureParams.builder().build(), options(idempotencyKey));
        } catch (StripeException e) {
            throw classify(e);
        }
    }

    @Override
    public void cancel(String idempotencyKey, String intentId) {
        try {
            PaymentIntent intent = PaymentIntent.retrieve(intentId);
            if ("canceled".equals(intent.getStatus())) {
                return;
            }
            intent.cancel(PaymentIntentCancelParams.builder().build(), options(idempotencyKey));
        } catch (StripeException e) {
            throw classify(e);
        }
    }

    private static RequestOptions options(String idempotencyKey) {
        return RequestOptions.builder().setIdempotencyKey(idempotencyKey).build();
    }

    /** Timeouts, connection errors, rate limits and 5xx are "outcome unknown"; anything else is a bug/config error. */
    private static RuntimeException classify(StripeException e) {
        if (e instanceof ApiConnectionException || e instanceof RateLimitException || e instanceof ApiException) {
            return new GatewayUnknownException("Stripe outcome unknown: " + e.getMessage(), e);
        }
        return new IllegalStateException("Stripe rejected the request: " + e.getMessage(), e);
    }
}
