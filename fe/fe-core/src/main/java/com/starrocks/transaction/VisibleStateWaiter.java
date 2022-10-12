// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.transaction;

import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

/**
 * Waits for a transaction becomes visible.
 */
public final class VisibleStateWaiter {
    private final TransactionState txnState;

    VisibleStateWaiter(@NotNull TransactionState txnState) {
        this.txnState = txnState;
    }

    /**
     * Causes the current thread to wait until the transaction state changed to {@code TransactionState.VISIBLE}.
     * If the current state is VISIBLE then this method returns immediately.
     */
    public void await() {
        while (true) {
            try {
                txnState.waitTransactionVisible();
                break;
            } catch (InterruptedException e) {
                // ignore InterruptedException
            }
        }
    }

    /**
     * Causes the current thread to wait until the transaction state changed to {@code TransactionState.VISIBLE}, unless the
     * specified waiting time elapses.
     * <p>
     * If the current state is VISIBLE then this method returns immediately with the value true.
     *
     * @param timeout – the maximum time to wait
     * @param unit    – the time unit of the timeout argument
     * @return true if the transaction state becomes VISIBLE and false if InterruptedException threw or, the waiting time
     * elapsed before the state becomes VISIBLE
     */
    public boolean await(long timeout, @NotNull TimeUnit unit) {
        try {
            return txnState.waitTransactionVisible(timeout, unit);
        } catch (InterruptedException e) {
            return false;
        }
    }
}
