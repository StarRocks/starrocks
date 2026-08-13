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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TxnTerminalStateCacheTest {
    private final int savedCap = Config.transaction_terminal_state_cache_num;
    private final int savedKeep = Config.label_keep_max_second;

    @AfterEach
    public void restore() {
        Config.transaction_terminal_state_cache_num = savedCap;
        Config.label_keep_max_second = savedKeep;
    }

    private static TransactionState terminalTxn(long id, String label, TransactionStatus status, long finishTime) {
        TransactionState s = new TransactionState(
                1L, Lists.newArrayList(1L), id, label, null,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "fe1"),
                -1L, 100000L);
        s.setTransactionStatus(status);
        s.setReason(status == TransactionStatus.ABORTED ? "aborted" : null);
        s.setFinishTime(finishTime);
        return s;
    }

    // Item 6: a "born-dead" outcome already older than label_keep_max_second must not be cached, since
    // it could never satisfy a read (valid() would reject it) and would only waste cache capacity.
    @Test
    public void testBornDeadEntryNotCached() {
        Config.transaction_terminal_state_cache_num = 100;
        Config.label_keep_max_second = 10; // 10s read window
        TxnTerminalStateCache cache = new TxnTerminalStateCache();

        long now = System.currentTimeMillis();
        cache.put(terminalTxn(1L, "old", TransactionStatus.VISIBLE, now - 20_000L)); // 20s old > 10s
        assertEquals(0, cache.size());
        assertNull(cache.getByTxnId(1L));
        assertNull(cache.getByLabel("old"));

        cache.put(terminalTxn(2L, "fresh", TransactionStatus.VISIBLE, now)); // within window
        assertEquals(1, cache.size());
        assertNotNull(cache.getByTxnId(2L));
        assertNotNull(cache.getByLabel("fresh"));
    }

    // Item 5: a reused label maps to the latest (largest-id) terminal outcome, while both transactions
    // stay independently retrievable by id. The two-index design keeps the label lookup and the id
    // lookup from drifting apart when entries are evicted.
    @Test
    public void testLabelReuseLatestWinsAndBothReachableById() {
        Config.transaction_terminal_state_cache_num = 100;
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache cache = new TxnTerminalStateCache();

        long now = System.currentTimeMillis();
        cache.put(terminalTxn(100L, "L", TransactionStatus.VISIBLE, now));
        cache.put(terminalTxn(200L, "L", TransactionStatus.ABORTED, now)); // same label reused, newer id

        // getByLabel returns the latest (largest id) outcome.
        TxnTerminalStateCache.Record byLabel = cache.getByLabel("L");
        assertNotNull(byLabel);
        assertEquals(200L, byLabel.txnId);
        assertEquals(TransactionStatus.ABORTED, byLabel.status);

        // Both transactions remain independently retrievable by id.
        assertNotNull(cache.getByTxnId(100L));
        assertEquals(TransactionStatus.VISIBLE, cache.getByTxnId(100L).status);
        assertNotNull(cache.getByTxnId(200L));

        // An out-of-order put of an older id does not clobber the latest-wins label mapping.
        cache.put(terminalTxn(150L, "L", TransactionStatus.VISIBLE, now));
        assertEquals(200L, cache.getByLabel("L").txnId);
    }

    // Disabled at runtime (capacity <= 0): writes are dropped and reads return nothing.
    @Test
    public void testDisabledDropsWritesAndReads() {
        Config.transaction_terminal_state_cache_num = 0;
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache cache = new TxnTerminalStateCache();

        cache.put(terminalTxn(1L, "x", TransactionStatus.VISIBLE, System.currentTimeMillis()));
        assertEquals(0, cache.size());
        assertNull(cache.getByTxnId(1L));
        assertNull(cache.getByLabel("x"));
    }

    // Image round-trip: snapshot() then restore() into a fresh cache reproduces the entries, so the
    // outcomes survive a checkpoint/restart (the P1 gap: checkpoint evicts before saveImage).
    @Test
    public void testSnapshotRestoreRoundTrip() {
        Config.transaction_terminal_state_cache_num = 100;
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache src = new TxnTerminalStateCache();
        long now = System.currentTimeMillis();
        src.put(terminalTxn(100L, "v", TransactionStatus.VISIBLE, now));
        src.put(terminalTxn(200L, "a", TransactionStatus.ABORTED, now));

        TxnTerminalStateCache dst = new TxnTerminalStateCache();
        for (TxnTerminalStateCache.Record r : src.snapshot()) {
            dst.restore(r.txnId, r.label, r.status, r.reason, r.finishTime, r.sourceType);
        }

        assertEquals(2, dst.size());
        assertNotNull(dst.getByTxnId(100L));
        assertEquals(TransactionStatus.VISIBLE, dst.getByLabel("v").status);
        assertEquals(TransactionStatus.ABORTED, dst.getByTxnId(200L).status);
        // The source type round-trips through snapshot()/restore() so a source-gated read stays valid.
        assertEquals(TransactionState.LoadJobSourceType.BACKEND_STREAMING, dst.getByTxnId(100L).sourceType);

        // restore() applies the same admission rules: a born-dead record is dropped on load.
        TxnTerminalStateCache dst2 = new TxnTerminalStateCache();
        Config.label_keep_max_second = 10;
        dst2.restore(300L, "old", TransactionStatus.VISIBLE, null, now - 20_000L,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING);
        assertEquals(0, dst2.size());
    }

    // FIFO eviction: a read does NOT promote an entry, so the oldest-inserted is evicted when the cap is
    // exceeded, even if it was just read. This keeps the most recent outcomes, which fits the one-shot,
    // time-ordered nature of transactions (an old entry is only re-read during recovery, so it must not
    // push out a newer one).
    @Test
    public void testFifoEvictsOldestInsertedRegardlessOfReads() {
        Config.transaction_terminal_state_cache_num = 2; // small cap to force eviction
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache cache = new TxnTerminalStateCache();
        long now = System.currentTimeMillis();

        cache.put(terminalTxn(1L, "A", TransactionStatus.VISIBLE, now));
        cache.put(terminalTxn(2L, "B", TransactionStatus.VISIBLE, now));
        cache.getByLabel("A");                                            // read the oldest -> no promotion under FIFO
        cache.put(terminalTxn(3L, "C", TransactionStatus.VISIBLE, now));  // evicts the oldest-inserted (txn1)

        assertNull(cache.getByTxnId(1L));      // oldest-inserted evicted despite being read
        assertNull(cache.getByLabel("A"));     // its label pointer was pruned on eviction
        assertNotNull(cache.getByTxnId(2L));   // newer entries kept
        assertNotNull(cache.getByTxnId(3L));

        java.util.Set<Long> ids = new java.util.HashSet<>();
        for (TxnTerminalStateCache.Record r : cache.snapshot()) {
            ids.add(r.txnId);
        }
        assertEquals(java.util.Set.of(2L, 3L), ids);
    }

    // Codex P2: total retention (and the snapshot) must not exceed the configured capacity. Inserting
    // far more than the cap, with distinct labels, keeps at most `cap` records -- not ~2x from a second
    // Record index.
    @Test
    public void testRetentionBoundedToConfiguredCapacity() {
        int cap = 10;
        Config.transaction_terminal_state_cache_num = cap;
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache cache = new TxnTerminalStateCache();
        long now = System.currentTimeMillis();
        for (long i = 0; i < cap * 5; i++) {
            cache.put(terminalTxn(i, "label_" + i, TransactionStatus.VISIBLE, now));
            cache.getByLabel("label_" + i); // exercise the label path too
        }
        assertEquals(cap, cache.size());
        assertTrue(cache.snapshot().size() <= cap, "snapshot must not exceed the configured cap");
    }

    // Only final statuses are cached; a non-terminal state is ignored.
    @Test
    public void testNonTerminalNotCached() {
        Config.transaction_terminal_state_cache_num = 100;
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache cache = new TxnTerminalStateCache();

        cache.put(terminalTxn(1L, "p", TransactionStatus.PREPARED, System.currentTimeMillis()));
        assertEquals(0, cache.size());
        assertNull(cache.getByTxnId(1L));
    }

    // Reducing the cap at runtime must drain the cache down to the new bound (not stay high, evicting
    // only one per insert), so the cache and the image snapshot honor the reduced configuration.
    @Test
    public void testRuntimeCapReductionDrainsToNewCap() {
        Config.transaction_terminal_state_cache_num = 100;
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache cache = new TxnTerminalStateCache();
        long now = System.currentTimeMillis();
        for (long i = 0; i < 100; i++) {
            cache.put(terminalTxn(i, "l" + i, TransactionStatus.VISIBLE, now));
        }
        assertEquals(100, cache.size());

        Config.transaction_terminal_state_cache_num = 10; // shrink at runtime
        cache.put(terminalTxn(999L, "l999", TransactionStatus.VISIBLE, now));
        assertEquals(10, cache.size());
        assertTrue(cache.snapshot().size() <= 10);
    }

    // An entry that was valid on admission but has since aged past label_keep_max_second must be pruned
    // on access, not just reported absent, so it stops occupying capacity and evicting valid entries.
    @Test
    public void testExpiredRecordPrunedOnAccess() {
        Config.transaction_terminal_state_cache_num = 100;
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache cache = new TxnTerminalStateCache();
        long now = System.currentTimeMillis();
        cache.put(terminalTxn(1L, "a", TransactionStatus.VISIBLE, now - 100_000L)); // 100s old, still valid
        assertEquals(1, cache.size());

        Config.label_keep_max_second = 10; // now the 100s-old record is past the window
        assertNull(cache.getByTxnId(1L));  // expired -> pruned, not just hidden
        assertEquals(0, cache.size());
        assertNull(cache.getByLabel("a")); // label pointer pruned too
    }

    // luohaha P2: snapshot() must prune age-expired records even when they are never looked up, so an
    // aged-out outcome neither lingers in memory nor is serialized into every FE image. Unlike
    // testExpiredRecordPrunedOnAccess (which prunes via a getBy* lookup), here the only interaction with
    // the expired record is the snapshot itself.
    @Test
    public void testSnapshotPrunesExpiredWithoutAccess() {
        Config.transaction_terminal_state_cache_num = 100;
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache cache = new TxnTerminalStateCache();
        long now = System.currentTimeMillis();
        cache.put(terminalTxn(1L, "aged", TransactionStatus.VISIBLE, now - 100_000L)); // valid on admission
        cache.put(terminalTxn(2L, "fresh", TransactionStatus.VISIBLE, now));
        assertEquals(2, cache.size());

        Config.label_keep_max_second = 10; // the 100s-old record is now past the read window

        java.util.Set<Long> ids = new java.util.HashSet<>();
        for (TxnTerminalStateCache.Record r : cache.snapshot()) { // no getByTxnId/getByLabel on the aged record
            ids.add(r.txnId);
        }
        assertEquals(java.util.Set.of(2L), ids); // only the fresh record is serialized
        assertEquals(1, cache.size());           // aged record removed from memory, not just hidden
        assertNull(cache.getByLabel("aged"));    // its label pointer is gone too
    }

    // Comment C: the proactive sweep drops age-expired records with no read, insert, or snapshot, so an
    // idle database releases the memory instead of holding it until the next read or checkpoint.
    @Test
    public void testEvictExpiredReleasesAgedRecordsWhenIdle() {
        Config.transaction_terminal_state_cache_num = 100;
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache cache = new TxnTerminalStateCache();
        long now = System.currentTimeMillis();
        cache.put(terminalTxn(1L, "aged", TransactionStatus.VISIBLE, now - 100_000L)); // valid on admission
        cache.put(terminalTxn(2L, "fresh", TransactionStatus.VISIBLE, now));
        assertEquals(2, cache.size());

        Config.label_keep_max_second = 10; // the 100s-old record is now past the read window

        cache.evictExpired(); // proactive sweep, no getBy*/put/snapshot on the aged record

        assertEquals(1, cache.size());        // aged record released from memory
        assertNull(cache.getByLabel("aged")); // and its label pointer
        assertNotNull(cache.getByTxnId(2L));  // fresh record kept
    }

    // evictExpired also reclaims everything when the cache is disabled at runtime, so a database that
    // goes idle after the cap is set to 0 does not keep old entries pinned.
    @Test
    public void testEvictExpiredClearsWhenDisabled() {
        Config.transaction_terminal_state_cache_num = 100;
        Config.label_keep_max_second = 3600;
        TxnTerminalStateCache cache = new TxnTerminalStateCache();
        long now = System.currentTimeMillis();
        cache.put(terminalTxn(1L, "a", TransactionStatus.VISIBLE, now));
        cache.put(terminalTxn(2L, "b", TransactionStatus.VISIBLE, now));
        assertEquals(2, cache.size());

        Config.transaction_terminal_state_cache_num = 0; // disabled at runtime
        cache.evictExpired();
        assertEquals(0, cache.size());
    }
}
