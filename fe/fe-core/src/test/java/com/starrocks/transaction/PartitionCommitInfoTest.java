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
import com.starrocks.catalog.Partition;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.task.PublishVersionTask;
import com.starrocks.thrift.TPartitionVersionInfo;
import com.starrocks.thrift.TTabletInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class PartitionCommitInfoTest {
    private static final long PID = 12345L;
    private static final long TABLE_ID = 6789L;

    // Round-trip a PartitionCommitInfo through TableCommitInfo + TransactionState copies, the path
    // used by finish/replay, and return the copied PartitionCommitInfo.
    private PartitionCommitInfo copyViaTransactionState(PartitionCommitInfo pci) {
        TableCommitInfo tableCommitInfo = new TableCommitInfo(TABLE_ID);
        tableCommitInfo.addPartitionCommitInfo(pci);
        TransactionState state = new TransactionState(1000L, Lists.newArrayList(TABLE_ID),
                3000, "label", null, TransactionState.LoadJobSourceType.INSERT_STREAMING, null, 0, 60_000);
        state.putIdToTableCommitInfo(TABLE_ID, tableCommitInfo);

        TransactionState copied = new TransactionState(state);
        return copied.getTableCommitInfo(TABLE_ID).getPartitionCommitInfo(PID);
    }

    @Test
    public void testTabletStatsCopiedByCopyConstructor() {
        PartitionCommitInfo src = new PartitionCommitInfo(1L, 2L, 3L);
        TabletStatPB stat = new TabletStatPB();
        stat.numRows = 10L;
        stat.dataSize = 100L;
        src.getTabletStats().put(7L, stat);

        PartitionCommitInfo copy = new PartitionCommitInfo(src);
        Assertions.assertEquals(1, copy.getTabletStats().size());
        Assertions.assertEquals(100L, copy.getTabletStats().get(7L).dataSize);
    }

    @Test
    public void testPutAllTabletStatsSkipsNullValues() {
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, 2L, 3L);
        pci.putAllTabletStats(null);
        Assertions.assertTrue(pci.getTabletStats().isEmpty());

        TabletStatPB stat = new TabletStatPB();
        stat.numRows = 10L;
        Map<Long, TabletStatPB> stats = new HashMap<>();
        stats.put(1L, stat);
        stats.put(2L, null);
        pci.putAllTabletStats(stats);

        Assertions.assertEquals(1, pci.getTabletStats().size());
        Assertions.assertEquals(10L, pci.getTabletStats().get(1L).numRows);
    }

    private TransactionState newFirstLoadTxnState(PartitionCommitInfo pci) {
        TableCommitInfo tableCommitInfo = new TableCommitInfo(TABLE_ID);
        tableCommitInfo.addPartitionCommitInfo(pci);
        TransactionState state = new TransactionState(1000L, Lists.newArrayList(TABLE_ID),
                3000, "label", null, TransactionState.LoadJobSourceType.INSERT_STREAMING, null, 0, 60_000);
        state.putIdToTableCommitInfo(TABLE_ID, tableCommitInfo);
        return state;
    }

    private PublishVersionTask newPublishVersionTask(long backendId, TransactionState state) {
        // PARTITION_INIT_VERSION + 1 marks a first load, which is the only case stats are collected for.
        TPartitionVersionInfo versionInfo =
                new TPartitionVersionInfo(PID, Partition.PARTITION_INIT_VERSION + 1, 0);
        return new PublishVersionTask(backendId, state.getTransactionId(), state.getGlobalTransactionId(),
                state.getDbId(), 0, Lists.newArrayList(versionInfo), null, null, 0, state, false,
                TransactionType.TXN_NORMAL);
    }

    private TTabletInfo newTabletInfo(long tabletId, long rowCount) {
        TTabletInfo tabletInfo = new TTabletInfo(tabletId, 0, 2L, 0, rowCount, rowCount * 10);
        tabletInfo.setPartition_id(PID);
        return tabletInfo;
    }

    // The reported stats must survive the handoff: the finishTask handler parks them on its own task,
    // and the thread finishing the transaction folds them into the commit infos before snapshotting.
    @Test
    public void testPublishTaskTabletStatsAreAppliedBeforeSnapshot() {
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, Partition.PARTITION_INIT_VERSION + 1, 3L);
        TransactionState state = newFirstLoadTxnState(pci);

        PublishVersionTask task = newPublishVersionTask(10001L, state);
        state.addPublishVersionTask(10001L, task);
        task.setErrorTablets(null);
        task.collectFirstLoadTabletStats(Lists.newArrayList(newTabletInfo(7L, 123L)));

        // Collecting on the handler side must not touch the shared commit info.
        Assertions.assertTrue(pci.getTabletStats().isEmpty());

        task.setIsFinished(true);
        state.applyPublishTaskTabletStats();
        Assertions.assertEquals(123L, pci.getTabletStats().get(7L).numRows);

        PartitionCommitInfo copied = new TransactionState(state).getTableCommitInfo(TABLE_ID)
                .getPartitionCommitInfo(PID);
        Assertions.assertEquals(123L, copied.getTabletStats().get(7L).numRows);
    }

    // Stats of a task that has not reported yet must not be picked up: an unfinished task may still be
    // being written by its handler thread.
    @Test
    public void testUnfinishedPublishTaskStatsAreNotApplied() {
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, Partition.PARTITION_INIT_VERSION + 1, 3L);
        TransactionState state = newFirstLoadTxnState(pci);

        PublishVersionTask task = newPublishVersionTask(10001L, state);
        state.addPublishVersionTask(10001L, task);
        task.collectFirstLoadTabletStats(Lists.newArrayList(newTabletInfo(7L, 123L)));

        state.applyPublishTaskTabletStats();
        Assertions.assertTrue(pci.getTabletStats().isEmpty());
    }

    // Regression test for the ConcurrentModificationException reported in issue #77595. Reporting BEs
    // (thrift finishTask handler threads) used to write PartitionCommitInfo.tabletStats directly while
    // PublishVersionDaemon deep-copied the transaction state to finish it, with no lock covering the
    // map. Handling a report must not touch anything the finishing thread is copying.
    @Test
    public void testHandlingReportsDoesNotRaceWithSnapshot() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, Partition.PARTITION_INIT_VERSION + 1, 3L);
        TransactionState state = newFirstLoadTxnState(pci);

        // One task per reporting BE, all reporting concurrently for the same partition.
        List<PublishVersionTask> tasks = new ArrayList<>();
        for (long backendId = 10001L; backendId <= 10003L; backendId++) {
            PublishVersionTask task = newPublishVersionTask(backendId, state);
            state.addPublishVersionTask(backendId, task);
            tasks.add(task);
        }

        List<Thread> reporters = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            PublishVersionTask task = tasks.get(i);
            long tabletBase = i * 10_000L;
            Thread reporter = new Thread(() -> {
                try {
                    for (long tabletId = 0; tabletId < 20_000; tabletId++) {
                        task.collectFirstLoadTabletStats(
                                Lists.newArrayList(newTabletInfo(tabletBase + tabletId, tabletId)));
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            reporter.start();
            reporters.add(reporter);
        }

        try {
            while (reporters.stream().anyMatch(Thread::isAlive)) {
                // Never throws ConcurrentModificationException, whatever the reporters are doing.
                new TransactionState(state);
            }
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        } finally {
            for (Thread reporter : reporters) {
                reporter.join(30_000);
            }
        }
        Assertions.assertNull(failure.get(), () -> "unexpected failure: " + failure.get());
    }

    @Test
    public void testPartitionCommitInfoCopyAndJsonRoundTrip() {
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, 5, 0);

        // copy ctor preserves version
        Assertions.assertEquals(5L, new PartitionCommitInfo(pci).getVersion());

        // GSON round-trip preserves version
        Assertions.assertEquals(5L,
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(pci), PartitionCommitInfo.class).getVersion());

        // TableCommitInfo copy
        TableCommitInfo tableCommitInfo = new TableCommitInfo(TABLE_ID);
        tableCommitInfo.addPartitionCommitInfo(pci);
        Assertions.assertEquals(5L,
                new TableCommitInfo(tableCommitInfo).getPartitionCommitInfo(PID).getVersion());

        // through TransactionState copy used by finish/replay
        Assertions.assertEquals(5L, copyViaTransactionState(pci).getVersion());
    }

    @Test
    public void testShadowRewriteTxnUsesSourceType() {
        // Shadow-rewrite partitions use sentinel version -1; the guard is now on
        // TransactionState.isShadowRewrite() (sourceType == SHADOW_REWRITE), not on a
        // per-partition boolean.
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, -1, 0);

        // PartitionCommitInfo itself has no isShadowRewrite marker; verify it round-trips normally.
        Assertions.assertEquals(-1L, new PartitionCommitInfo(pci).getVersion());
        Assertions.assertEquals(-1L,
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(pci), PartitionCommitInfo.class).getVersion());

        // The txn-level isShadowRewrite is driven by sourceType.
        TransactionState txn = new TransactionState(1000L, Lists.newArrayList(TABLE_ID),
                3000, "label", null, TransactionState.LoadJobSourceType.SHADOW_REWRITE, null, 0, 60_000);
        Assertions.assertTrue(txn.isShadowRewrite());

        TransactionState normal = new TransactionState(1000L, Lists.newArrayList(TABLE_ID),
                3000, "label", null, TransactionState.LoadJobSourceType.INSERT_STREAMING, null, 0, 60_000);
        Assertions.assertFalse(normal.isShadowRewrite());
    }
}
