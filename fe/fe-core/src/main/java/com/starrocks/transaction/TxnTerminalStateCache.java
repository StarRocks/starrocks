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

import com.starrocks.common.Config;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A bounded, self-evicting cache of terminal ({@code VISIBLE}/{@code ABORTED}) transaction
 * outcomes that survives removal of the full {@link TransactionState} by count-based eviction
 * (see {@link DatabaseTransactionMgr#removeExpiredTxns}).
 *
 * <p>Motivation: {@code label_keep_max_num} count-based eviction ignores age, so a finished
 * transaction can be removed long before {@code label_keep_max_second}. When a connector (e.g.
 * Flink) resumes from a savepoint after a long outage and re-commits such a transaction, the FE
 * would otherwise return "transaction not found", which the connector cannot distinguish from
 * "never committed", so it aborts and loops. This cache lets the FE answer the re-commit with the
 * real terminal outcome: idempotent success for {@code VISIBLE}/{@code COMMITTED}, commit-failed
 * for {@code ABORTED}.
 *
 * <p>Entries are looked up by transaction id and by label. The cache is bounded by
 * {@code transaction_terminal_state_cache_num} (LRU); entries older than
 * {@code label_keep_max_second} are treated as absent so nothing lives forever. Each entry keeps
 * only the id, label, status, reason and finish time, so the heap cost is a small fraction of a
 * full {@link TransactionState} (which carries per-partition commit info).
 *
 * <p>This class is self-synchronized and never calls back into {@link DatabaseTransactionMgr}, so
 * it is safe to invoke while holding the database transaction lock.
 */
class TxnTerminalStateCache {
    /**
     * Immutable snapshot of a terminal transaction outcome.
     */
    static class Record {
        final long txnId;
        final String label;
        final TransactionStatus status; // VISIBLE or ABORTED
        final String reason;
        final long finishTime;

        Record(long txnId, String label, TransactionStatus status, String reason, long finishTime) {
            this.txnId = txnId;
            this.label = label;
            this.status = status;
            this.reason = reason;
            this.finishTime = finishTime;
        }
    }

    // Access-order LRU keyed by transaction id.
    private final LinkedHashMap<Long, Record> byTxnId;
    // Secondary index: label -> latest terminal txn id currently in the cache.
    private final Map<String, Long> labelToTxnId;

    TxnTerminalStateCache() {
        labelToTxnId = new HashMap<>();
        byTxnId = new LinkedHashMap<Long, Record>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Record> eldest) {
                int capacity = Config.transaction_terminal_state_cache_num;
                if (capacity <= 0) {
                    // Disabled: drop whatever slips in and keep the label index clean.
                    Record r = eldest.getValue();
                    Long mapped = labelToTxnId.get(r.label);
                    if (mapped != null && mapped == r.txnId) {
                        labelToTxnId.remove(r.label);
                    }
                    return true;
                }
                if (size() > capacity) {
                    Record r = eldest.getValue();
                    Long mapped = labelToTxnId.get(r.label);
                    if (mapped != null && mapped == r.txnId) {
                        labelToTxnId.remove(r.label);
                    }
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * Record a terminal outcome. No-op unless the transaction is in a final status
     * ({@code VISIBLE} or {@code ABORTED}) and the cache is enabled.
     */
    synchronized void put(TransactionState state) {
        if (Config.transaction_terminal_state_cache_num <= 0) {
            return;
        }
        TransactionStatus status = state.getTransactionStatus();
        if (status != TransactionStatus.VISIBLE && status != TransactionStatus.ABORTED) {
            return;
        }
        long txnId = state.getTransactionId();
        Record r = new Record(txnId, state.getLabel(), status, state.getReason(), state.getFinishTime());
        byTxnId.put(txnId, r);
        // Latest txn id wins, matching getLabelState()'s "largest id" semantics.
        Long cur = labelToTxnId.get(r.label);
        if (cur == null || txnId >= cur) {
            labelToTxnId.put(r.label, txnId);
        }
    }

    /**
     * @return the cached outcome for {@code txnId}, or null if absent or too old.
     */
    synchronized Record getByTxnId(long txnId) {
        Record r = byTxnId.get(txnId);
        return valid(r) ? r : null;
    }

    /**
     * @return the latest cached outcome for {@code label}, or null if absent or too old.
     */
    synchronized Record getByLabel(String label) {
        Long txnId = labelToTxnId.get(label);
        if (txnId == null) {
            return null;
        }
        Record r = byTxnId.get(txnId);
        return valid(r) ? r : null;
    }

    private boolean valid(Record r) {
        if (r == null) {
            return false;
        }
        if (Config.transaction_terminal_state_cache_num <= 0) {
            // Cache disabled at runtime: reads must return nothing, even for records admitted while
            // it was enabled (otherwise "set to 0 to disable" would only stop writes, not reads).
            return false;
        }
        if (r.finishTime <= 0) {
            // Finish time unknown: fall back to the LRU capacity bound only.
            return true;
        }
        return (System.currentTimeMillis() - r.finishTime) / 1000 <= Config.label_keep_max_second;
    }

    synchronized int size() {
        return byTxnId.size();
    }
}
