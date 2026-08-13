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
import java.util.HashMap;
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
 * {@code transaction_terminal_state_cache_num} (FIFO, oldest inserted evicted first); entries older than
 * {@code label_keep_max_second} are treated as absent so nothing lives forever. Each entry keeps
 * only the id, label, status, reason, finish time and source type, so the heap cost is a small
 * fraction of a full {@link TransactionState} (which carries per-partition commit info).
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
        // Source type of the originating transaction (e.g. BYPASS_WRITE). Retained so a source-gated
        // handler can still validate an evicted outcome (see BypassWriteTransactionHandler) after the
        // full TransactionState is gone. May be null for a record loaded from an image that predates
        // source-type persistence; callers must treat null as "unknown source", never as a match.
        final TransactionState.LoadJobSourceType sourceType;

        Record(long txnId, String label, TransactionStatus status, String reason, long finishTime,
                TransactionState.LoadJobSourceType sourceType) {
            this.txnId = txnId;
            this.label = label;
            this.status = status;
            this.reason = reason;
            this.finishTime = finishTime;
            this.sourceType = sourceType;
        }
    }

    // Single canonical insertion-ordered (FIFO) map of Records, capped at transaction_terminal_state_cache_num.
    // Eviction drops the oldest inserted; reads deliberately do not promote, since a finished txn is only
    // re-read during recovery, so a hot old entry must not push out a newer one. This is the ONLY place
    // Records live, so total retention (and the image snapshot) is bounded to exactly the configured
    // maximum -- not doubled by a second Record index.
    private final LinkedHashMap<Long, Record> byTxnId;
    // Secondary index only: label -> latest txn id (a pointer, not a second Record store). Pruned when
    // its target is evicted from byTxnId, so it never dangles and adds no Record retention. getByLabel
    // resolves the pointer through byTxnId without reordering (FIFO), so a read never changes what the
    // cache keeps.
    private final Map<String, Long> labelToTxnId;

    TxnTerminalStateCache() {
        labelToTxnId = new HashMap<>();
        byTxnId = new LinkedHashMap<Long, Record>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Record> eldest) {
                int capacity = Config.transaction_terminal_state_cache_num;
                boolean evict = capacity <= 0 || size() > capacity;
                if (evict) {
                    // Prune the label pointer iff it targets the record being evicted (a newer txn for
                    // the same label keeps its own pointer). This keeps labelToTxnId free of dangling
                    // entries and bounded by byTxnId.
                    Record r = eldest.getValue();
                    Long mapped = labelToTxnId.get(r.label);
                    if (mapped != null && mapped == r.txnId) {
                        labelToTxnId.remove(r.label);
                    }
                }
                return evict;
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
                state.getFinishTime(), state.getSourceType());
    }

    /**
     * Restore an entry loaded from the FE image. Applies the same admission rules as {@link #put}
     * (enabled, terminal, not born-dead), so a stale record in an old image is dropped on load.
     */
    synchronized void restore(long txnId, String label, TransactionStatus status, String reason, long finishTime,
            TransactionState.LoadJobSourceType sourceType) {
        insert(txnId, label, status, reason, finishTime, sourceType);
    }

    private void insert(long txnId, String label, TransactionStatus status, String reason, long finishTime,
            TransactionState.LoadJobSourceType sourceType) {
        // Apply the current config first: a runtime reduction of the cap only evicts one entry per
        // insert via removeEldestEntry, so drain any excess here; if disabled, clear outright.
        trimToCapacity();
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
        Record r = new Record(txnId, label, status, reason, finishTime, sourceType);
        byTxnId.put(txnId, r);
        // Largest txn id wins for a reused label, matching getLabelState()'s "largest id" semantics.
        Long cur = labelToTxnId.get(label);
        if (cur == null || txnId >= cur) {
            labelToTxnId.put(label, txnId);
        }
    }

    /**
     * @return a snapshot of the currently cached records, for FE image serialization. This is exactly
     * byTxnId (the single Record store), so the snapshot is complete and bounded by the configured
     * capacity -- there is no second Record index to union in, and label lookups resolve through
     * byTxnId, so nothing is retained outside it.
     */
    synchronized List<Record> snapshot() {
        trimToCapacity(); // never serialize more than the configured cap into the image
        // Also drop age-expired records. valid()'s age check otherwise runs only when a record is looked
        // up, so a record that aged past label_keep_max_second but is never queried would be copied into
        // every FE image. The normal checkpoint runs snapshot() on a throwaway copy of the state, so this
        // prune keeps aged records out of the image (and off that copy), not out of the live serving map --
        // there they are reclaimed by capacity pressure and on-access pruning in getBy*. restore() already
        // drops the same records on load, so this only avoids writing dead weight in the first place (with
        // the default cap of 50k per db, an inactive db could otherwise bloat every checkpoint it appears in).
        List<Record> live = new ArrayList<>(byTxnId.size());
        for (Record r : new ArrayList<>(byTxnId.values())) {
            if (valid(r)) {
                live.add(r);
            } else {
                removeRecord(r);
            }
        }
        return live;
    }

    /**
     * Proactively drop age-expired records (and drain any excess left by a runtime cap reduction) so an
     * idle database releases the memory instead of holding it until the next read or checkpoint. Called
     * every cycle by the periodic cleanup ({@link DatabaseTransactionMgr#removeExpiredTxns}), which runs
     * on every FE node. Uses the same {@link #valid} predicate as reads, so it also reclaims memory when
     * the cache is disabled at runtime; mirrors {@link #snapshot}'s copy-then-remove to avoid a
     * concurrent modification while iterating.
     */
    synchronized void evictExpired() {
        trimToCapacity();
        for (Record r : new ArrayList<>(byTxnId.values())) {
            if (!valid(r)) {
                removeRecord(r);
            }
        }
    }

    /**
     * @return the cached outcome for {@code txnId}, or null if absent or too old. An expired (or
     * disabled-cache) record is pruned on access rather than left to occupy capacity.
     */
    synchronized Record getByTxnId(long txnId) {
        Record r = byTxnId.get(txnId);
        if (r == null) {
            return null;
        }
        if (!valid(r)) {
            removeRecord(r);
            return null;
        }
        return r;
    }

    /**
     * @return the latest cached outcome for {@code label}, or null if absent or too old. The lookup
     * resolves the label pointer through byTxnId without reordering (FIFO), so a read never changes
     * what the cache keeps. An expired record (or a dangling label pointer) is pruned on access.
     */
    synchronized Record getByLabel(String label) {
        Long txnId = labelToTxnId.get(label);
        if (txnId == null) {
            return null;
        }
        Record r = byTxnId.get(txnId);
        if (r == null) {
            labelToTxnId.remove(label); // dangling pointer; should not happen, prune defensively
            return null;
        }
        if (!valid(r)) {
            removeRecord(r);
            return null;
        }
        return r;
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
            // Finish time unknown: fall back to the capacity bound only.
            return true;
        }
        return (System.currentTimeMillis() - r.finishTime) / 1000 <= Config.label_keep_max_second;
    }

    synchronized int size() {
        return byTxnId.size();
    }

    // Remove a record from the id store and drop its label pointer iff the pointer targets it (a newer
    // txn reusing the same label keeps its own pointer). Mirrors removeEldestEntry's cleanup.
    private void removeRecord(Record r) {
        byTxnId.remove(r.txnId);
        Long mapped = labelToTxnId.get(r.label);
        if (mapped != null && mapped == r.txnId) {
            labelToTxnId.remove(r.label);
        }
    }

    // Bring the cache into line with the current config: clear everything when disabled, otherwise
    // evict the eldest (oldest-inserted) entries until size <= cap. removeEldestEntry only trims
    // one entry per insert, so a runtime reduction of the cap needs this proactive drain.
    private void trimToCapacity() {
        int capacity = Config.transaction_terminal_state_cache_num;
        if (capacity <= 0) {
            byTxnId.clear();
            labelToTxnId.clear();
            return;
        }
        while (byTxnId.size() > capacity) {
            Record eldest = byTxnId.values().iterator().next(); // insertion-order: eldest (oldest inserted) first
            removeRecord(eldest);
        }
    }
}
