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
    // it could never satisfy a read (valid() would reject it) and would only waste LRU capacity.
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
            dst.restore(r.txnId, r.label, r.status, r.reason, r.finishTime);
        }

        assertEquals(2, dst.size());
        assertNotNull(dst.getByTxnId(100L));
        assertEquals(TransactionStatus.VISIBLE, dst.getByLabel("v").status);
        assertEquals(TransactionStatus.ABORTED, dst.getByTxnId(200L).status);

        // restore() applies the same admission rules: a born-dead record is dropped on load.
        TxnTerminalStateCache dst2 = new TxnTerminalStateCache();
        Config.label_keep_max_second = 10;
        dst2.restore(300L, "old", TransactionStatus.VISIBLE, null, now - 20_000L);
        assertEquals(0, dst2.size());
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
}
