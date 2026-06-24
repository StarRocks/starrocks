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

import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LakeTableTxnLogApplierTest extends LakeTableTestHelper {
    @Test
    public void testCommitAndApply() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertEquals(partitionCommitInfo.getVersionTime(),
                table.getPartition(partitionId).getDefaultPhysicalPartition()
                        .getVisibleVersionTime());
    }

    @Test
    public void testCommitAndApplyCompaction() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newCompactionTransactionState();
        CompactionTxnCommitAttachment attachment = new CompactionTxnCommitAttachment(true);
        state.setTxnCommitAttachment(attachment);
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertEquals(partitionCommitInfo.getVersionTime(),
                table.getPartition(partitionId).getDefaultPhysicalPartition()
                        .getVisibleVersionTime());
    }

    @Test
    public void testApplyVisibleLogUpdatesLakeTabletAndEnqueues() {
        // Build a table with two tablets in the index: one WITH a matching stat entry, one WITHOUT.
        MaterializedIndex index = new MaterializedIndex(indexId);
        LakeTablet lakeTablet = new LakeTablet(tabletId[0]);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tabletId[0], tabletMeta);
        index.addTablet(lakeTablet, tabletMeta);

        // Second tablet has no entry in tabletStats — verifies per-tablet selectivity.
        LakeTablet noStatTablet = new LakeTablet(tabletId[1]);
        TabletMeta noStatMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tabletId[1], noStatMeta);
        index.addTablet(noStatTablet, noStatMeta);

        LakeTable table = buildLakeTableWithIndex(index);
        // Range distribution is required for the publish-driven reshard path to evaluate the table.
        table.setDefaultDistributionInfo(new RangeDistributionInfo());
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);

        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
        applier.applyCommitLog(state, tableCommitInfo);

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        long versionTime = System.currentTimeMillis();
        partitionCommitInfo.setVersionTime(versionTime);

        // Populate tabletStats for tabletId[0] only — tabletId[1] is intentionally absent.
        // Oversize tabletId[0] so the precomputed split signal crosses the threshold and the
        // table is enqueued as a reshard candidate.
        long oversize = Config.tablet_reshard_target_size * 2;
        TabletStatPB stat = new TabletStatPB();
        stat.numRows = 5L;
        stat.dataSize = oversize;
        Map<Long, TabletStatPB> stats = new HashMap<>();
        stats.put(tabletId[0], stat);
        partitionCommitInfo.getTabletStats().putAll(stats);

        // Mock leader=true, checkpoint=false; intercept addReshardCandidate to count calls
        AtomicInteger addCandidateCalls = new AtomicInteger(0);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }

            @Mock
            public static boolean isCheckpointThread() {
                return false;
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void addReshardCandidate(long dbId, long tableId, long maxTabletSize, long minAdjacentTabletPairSize) {
                addCandidateCalls.incrementAndGet();
            }
        };

        Database db = new Database(dbId, "test_db");
        applier.applyVisibleLog(state, tableCommitInfo, db);

        // Tablet with a stat entry: fields must be updated.
        Assertions.assertEquals(oversize, lakeTablet.getDataSize(true));
        Assertions.assertEquals(5L, lakeTablet.getRowCount(0));
        Assertions.assertEquals(versionTime, lakeTablet.getDataSizeUpdateTime());
        Assertions.assertEquals(1, addCandidateCalls.get(), "addReshardCandidate should be called once");

        // Tablet WITHOUT a stat entry: must remain at default values (per-tablet selectivity).
        Assertions.assertEquals(0L, noStatTablet.getDataSizeUpdateTime(),
                "tablet absent from tabletStats must not have its update-time modified");
        Assertions.assertEquals(0L, noStatTablet.getDataSize(true),
                "tablet absent from tabletStats must not have its data-size modified");
    }

    @Test
    public void testApplyVisibleLogSkippedOnNonLeader() {
        // Use indexId+100 to avoid any ID collision with the positive test's index (indexId).
        long negativeIndexId = indexId + 100;
        MaterializedIndex index = new MaterializedIndex(negativeIndexId);
        LakeTablet lakeTablet = new LakeTablet(tabletId[1]);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tabletId[1], tabletMeta);
        index.addTablet(lakeTablet, tabletMeta);

        LakeTable table = buildLakeTableWithIndex(index);
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);

        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
        applier.applyCommitLog(state, tableCommitInfo);

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());

        TabletStatPB stat = new TabletStatPB();
        stat.numRows = 10L;
        stat.dataSize = 888L;
        partitionCommitInfo.getTabletStats().put(tabletId[1], stat);

        // Mock leader=false; intercept addReshardCandidate to prove it is never called.
        AtomicInteger addCandidateCalls = new AtomicInteger(0);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return false;
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void addReshardCandidate(long dbId, long tableId, long maxTabletSize, long minAdjacentTabletPairSize) {
                addCandidateCalls.incrementAndGet();
            }
        };

        long beforeUpdateTime = lakeTablet.getDataSizeUpdateTime();
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);

        // LakeTablet fields must be unchanged on a non-leader node.
        Assertions.assertEquals(beforeUpdateTime, lakeTablet.getDataSizeUpdateTime());
        Assertions.assertEquals(0L, lakeTablet.getDataSize(true));
        // addReshardCandidate must not have been invoked at all.
        Assertions.assertEquals(0, addCandidateCalls.get(),
                "addReshardCandidate must not be called on a non-leader node");
    }

    @Test
    public void testApplyCommitLogWithDroppedPartition() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId - 1, 2, 0);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
    }
}
