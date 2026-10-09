package dev.jenny.payments.ledgerservice;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Serializes our own writes per Fineract account. Concurrent writes to one Fineract savings account hit its
 * optimistic lock (observed ~7% 409s at 20-way concurrency), so we avoid creating that contention ourselves.
 * This is per instance; across instances the 409 retry in LedgerService is the safety net.
 */
@Component
public class AccountLocks {

    private final ConcurrentHashMap<Long, Semaphore> locks = new ConcurrentHashMap<>();
    private final long timeoutMs;

    AccountLocks(@Value("${ledger.account-lock-timeout-ms:30000}") long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public <T> T withLock(long accountId, Supplier<T> action) {
        Semaphore lock = locks.computeIfAbsent(accountId, id -> new Semaphore(1, true));
        try {
            if (!lock.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw new BookingException.Transient("Timed out waiting for account " + accountId, null);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BookingException.Transient("Interrupted waiting for account " + accountId, e);
        }
        try {
            return action.get();
        } finally {
            lock.release();
        }
    }
}
