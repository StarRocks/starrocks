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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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

    // Two independent access-order LRUs over the SAME Record objects: one keyed by transaction id,
    // one keyed by label. Keeping them independent (rather than a byTxnId map plus a label->txnId
    // pointer) avoids a drift bug: if a label is reused across two cached transactions, evicting the
    // newer one from a shared pointer index could strand the older one, making getByLabel return
    // nothing even though a record for that label is still cached. Each map self-evicts by capacity;
    // a miss in either simply falls back to "not found", which is always safe.
    private final LinkedHashMap<Long, Record> byTxnId;
    private final LinkedHashMap<String, Record> byLabel;

    TxnTerminalStateCache() {
        byTxnId = new LinkedHashMap<Long, Record>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Record> eldest) {
                int capacity = Config.transaction_terminal_state_cache_num;
                return capacity <= 0 || size() > capacity;
            }
        };
        byLabel = new LinkedHashMap<String, Record>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Record> eldest) {
                int capacity = Config.transaction_terminal_state_cache_num;
                return capacity <= 0 || size() > capacity;
            }
        };
    }

    /**
     * Record a terminal outcome. No-op unless the transaction is in a final status
     * ({@code VISIBLE} or {@code ABORTED}), the cache is enabled, and the outcome is still young
     * enough to satisfy a future read (a "born-dead" entry already past {@code label_keep_max_second}
     * could never be returned by {@link #valid}, so caching it would only waste capacity).
     */
    synchronized void put(TransactionState state) {
        insert(state.getTransactionId(), state.getLabel(), state.getTransactionStatus(), state.getReason(),
                state.getFinishTime());
    }

    /**
     * Restore an entry loaded from the FE image. Applies the same admission rules as {@link #put}
     * (enabled, terminal, not born-dead), so a stale record in an old image is dropped on load.
     */
    synchronized void restore(long txnId, String label, TransactionStatus status, String reason, long finishTime) {
        insert(txnId, label, status, reason, finishTime);
    }

    private void insert(long txnId, String label, TransactionStatus status, String reason, long finishTime) {
        if (Config.transaction_terminal_state_cache_num <= 0) {
            return;
        }
        if (status != TransactionStatus.VISIBLE && status != TransactionStatus.ABORTED) {
            return;
        }
        if (finishTime > 0 && (System.currentTimeMillis() - finishTime) / 1000 > Config.label_keep_max_second) {
            // Born dead: already older than the read window, so it would never satisfy a read.
            return;
        }
        Record r = new Record(txnId, label, status, reason, finishTime);
        byTxnId.put(txnId, r);
        // Largest txn id wins for a reused label, matching getLabelState()'s "largest id" semantics.
        Record existing = byLabel.get(label);
        if (existing == null || txnId >= existing.txnId) {
            byLabel.put(label, r);
        }
    }

    /**
     * @return a snapshot of the currently cached records, for FE image serialization.
     */
    synchronized List<Record> snapshot() {
        return new ArrayList<>(byTxnId.values());
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
        Record r = byLabel.get(label);
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
