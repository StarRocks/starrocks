// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
