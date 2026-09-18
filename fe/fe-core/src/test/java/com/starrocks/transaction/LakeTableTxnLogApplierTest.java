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
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
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

    @Test
    public void testApplyVisibleLogBatchPublishesOnlyTheFinalVersion() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);

        // Record every version the partition is ever made visible at, in order.
        List<Long> publishedVersions = Lists.newArrayList();
        new MockUp<PhysicalPartition>() {
            @Mock
            public void setVisibleVersion(Invocation invocation, long visibleVersion, long visibleVersionTime) {
                publishedVersions.add(visibleVersion);
                invocation.proceed(visibleVersion, visibleVersionTime);
            }
        };

        // Three batched load transactions taking the partition from version 1 to version 4.
        long baseVersionTime = System.currentTimeMillis();
        List<TransactionState> states = Lists.newArrayList();
        for (long version = 2; version <= 4; version++) {
            TransactionState state = newTransactionState();
            state.setTransactionStatus(TransactionStatus.VISIBLE);
            PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, version, 0);
            partitionCommitInfo.setVersionTime(baseVersionTime + version);
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            state.putIdToTableCommitInfo(tableId, tableCommitInfo);
            states.add(state);
        }

        applier.applyVisibleLogBatch(new TransactionStateBatch(states), /*unused*/null);

        // Versions 2 and 3 get no tablet metadata object of their own, so they must never become
        // visible: the partition jumps straight from 1 to the batch's final version.
        Assertions.assertEquals(Lists.newArrayList(4L), publishedVersions);
        PhysicalPartition partition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        Assertions.assertEquals(4, partition.getVisibleVersion());
        Assertions.assertEquals(baseVersionTime + 4, partition.getVisibleVersionTime());
    }
<<<<<<< HEAD
=======

    @Test
    public void testApplyVisibleLogBatchCutsOverUnshareLayoutAfterPublishingTheVersion() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        PhysicalPartition partition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        // Pin the parent layout, the way an in-flight UNSHARE does.
        partition.pinQueryableIndex(indexId, indexId);
        Assertions.assertTrue(partition.isUnsharing());

        // Visible version the partition carried at the moment the query-layout cutover ran.
        List<Long> versionAtCutover = Lists.newArrayList();
        new MockUp<PhysicalPartition>() {
            @Mock
            public boolean finishUnshare(Invocation invocation) {
                PhysicalPartition self = invocation.getInvokedInstance();
                versionAtCutover.add(self.getVisibleVersion());
                return invocation.proceed();
            }
        };

        // Batch of three: a load at version 2, an UNSHARE compaction at version 3, a load at version 4.
        long baseVersionTime = System.currentTimeMillis();
        List<TransactionState> states = Lists.newArrayList();
        for (long version = 2; version <= 4; version++) {
            TransactionState state;
            if (version == 3) {
                state = newCompactionTransactionState();
                state.setTxnCommitAttachment(new CompactionTxnCommitAttachment(false, true));
            } else {
                state = newTransactionState();
            }
            state.setTransactionStatus(TransactionStatus.VISIBLE);
            PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, version, 0);
            partitionCommitInfo.setVersionTime(baseVersionTime + version);
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            state.putIdToTableCommitInfo(tableId, tableCommitInfo);
            states.add(state);
        }

        applier.applyVisibleLogBatch(new TransactionStateBatch(states), /*unused*/null);

        // The planner resolves the queryable layout BEFORE it reads the visible version, so a cutover
        // that ran while the partition still carried a pre-batch version would let a lock-free plan pair
        // the child layout with a version whose child tablets have no metadata object. The cutover must
        // therefore run once, and only after the batch's final version is already published.
        Assertions.assertEquals(1, versionAtCutover.size(), "unshare cutover should run exactly once");
        Assertions.assertEquals(4L, versionAtCutover.get(0),
                "unshare cutover must not run before the batch's final version is visible");
        Assertions.assertEquals(4, partition.getVisibleVersion());
        Assertions.assertFalse(partition.isUnsharing(), "the query-layout pin must be cleared by the batch");
    }

    @Test
    public void testApplyVisibleLogCutsOverUnshareLayoutInTheSameTransaction() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        PhysicalPartition partition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        partition.pinQueryableIndex(indexId, indexId);

        List<Long> versionAtCutover = Lists.newArrayList();
        new MockUp<PhysicalPartition>() {
            @Mock
            public boolean finishUnshare(Invocation invocation) {
                PhysicalPartition self = invocation.getInvokedInstance();
                versionAtCutover.add(self.getVisibleVersion());
                return invocation.proceed();
            }
        };

        TransactionState state = newCompactionTransactionState();
        state.setTxnCommitAttachment(new CompactionTxnCommitAttachment(false, true));
        state.setTransactionStatus(TransactionStatus.VISIBLE);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        long schemaUpdateBefore = table.lastSchemaUpdateTime.get();
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);

        // Unbatched, the cutover runs inline - and already after the version it belongs to. This is the
        // ordering applyVisibleLogBatch has to reproduce for a batched UNSHARE.
        Assertions.assertEquals(1, versionAtCutover.size(), "unshare cutover should run exactly once");
        Assertions.assertEquals(2L, versionAtCutover.get(0),
                "unshare cutover must run after its own version is visible");
        Assertions.assertEquals(2, partition.getVisibleVersion());
        Assertions.assertFalse(partition.isUnsharing(), "the query-layout pin must be cleared");
        Assertions.assertTrue(table.lastSchemaUpdateTime.get() > schemaUpdateBefore,
                "the layout cutover must invalidate optimistic plans that captured the parent layout");
    }

    // ---------- nextVersion is derived from the journal, not counted ----------

    @Test
    public void testApplyCommitLogIsIdempotent() {
        // Regression for StarRocksTest#12225. nextVersion used to be a running counter that the
        // leader and every replaying FE maintained independently, so applying one entry twice
        // drifted them apart by one -- permanently, and silently, until a lake alter job asserted
        // nextVersion == its reserved commitVersion and aborted journal replay on every FE.
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(3,
                table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        // Re-applying the same entry must land on the same version, not one past it.
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(3,
                table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
    }

    @Test
    public void testApplyCommitLogHealsAnExistingDrift() {
        // A partition that has fallen behind catches up to the version the journal records, instead of
        // carrying the drift forward forever. The catch-up is one-way: see the second half.
        LakeTable table = buildLakeTable();
        PhysicalPartition physicalPartition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        // One version behind what the journal says this transaction took.
        physicalPartition.setNextVersion(1);
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(3, physicalPartition.getNextVersion());

        // ... but a counter that is already ahead is left alone. Overshooting only skips versions;
        // pulling it back would hand version 4 out a second time.
        physicalPartition.setNextVersion(5);
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(5, physicalPartition.getNextVersion());
    }

    @Test
    public void testApplyCommitLogFallsBackWhenNoVersionWasAllocated() {
        // A PartitionCommitInfo still carrying the sentinel version must not derive nextVersion from
        // it (that would corrupt the chain); it keeps the historical increment.
        LakeTable table = buildLakeTable();
        PhysicalPartition physicalPartition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, -1, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        long before = physicalPartition.getNextVersion();
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(before + 1, physicalPartition.getNextVersion());
    }

    @Test
    public void testApplyCommitLogDerivesDataVersionToo() {
        // Leaving nextDataVersion on the increment while nextVersion is derived would make a
        // re-applied entry advance one counter and not the other, breaking the committed-vs-visible
        // data version equality ReplicationJob.commitTransaction() preconditions on.
        LakeTable table = buildLakeTable();
        PhysicalPartition physicalPartition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        partitionCommitInfo.setDataVersion(2);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(3, physicalPartition.getNextVersion());
        Assertions.assertEquals(3, physicalPartition.getNextDataVersion());

        // Both counters must stay put when the same entry is applied again.
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(3, physicalPartition.getNextVersion());
        Assertions.assertEquals(3, physicalPartition.getNextDataVersion());
    }

    @Test
    public void testApplyCommitLogKeepsIncrementWhenDataVersionAbsent() {
        // A record predating the dataVersion field (or otherwise carrying none) keeps the increment.
        LakeTable table = buildLakeTable();
        PhysicalPartition physicalPartition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        long dataBefore = physicalPartition.getNextDataVersion();
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(3, physicalPartition.getNextVersion());
        Assertions.assertEquals(dataBefore + 1, physicalPartition.getNextDataVersion());
    }

    @Test
    public void testApplyCommitLogCompactionStillLeavesDataVersionAlone() {
        LakeTable table = buildLakeTable();
        PhysicalPartition physicalPartition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newCompactionTransactionState();
        state.setTxnCommitAttachment(new CompactionTxnCommitAttachment(true));
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        partitionCommitInfo.setDataVersion(2);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        long dataBefore = physicalPartition.getNextDataVersion();
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(3, physicalPartition.getNextVersion());
        Assertions.assertEquals(dataBefore, physicalPartition.getNextDataVersion(),
                "a compaction allocates no data version and must not advance one");
    }

    @Test
    public void testApplyCommitLogReplicationDerivesWithoutComparing() {
        // A replication transaction's versions are intentionally noncontiguous, so its commitVersion
        // bears no relation to the current counter: it must be derived outright, and must not be
        // mistaken for drift.
        LakeTable table = buildLakeTable();
        PhysicalPartition physicalPartition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        Deencapsulation.setField(state, "sourceType", TransactionState.LoadJobSourceType.REPLICATION);
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 40, 0);
        partitionCommitInfo.setDataVersion(40);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(41, physicalPartition.getNextVersion());
        Assertions.assertEquals(41, physicalPartition.getNextDataVersion());
    }

    @Test
    public void testApplyCommitLogVersionOverwriteDoesNotLowerNextVersion() {
        // INSERT OVERWRITE names an explicit version, which for a non-empty partition can be below the
        // current counter. OlapTableTxnLogApplier documents that nextVersion must not move then
        // ("otherwise, it's next version will not change"); lake must behave the same.
        LakeTable table = buildLakeTable();
        PhysicalPartition physicalPartition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        // the two-arg constructor is what marks a transaction as a version overwrite
        state.setTxnCommitAttachment(new InsertTxnCommitAttachment(0, 2));
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        partitionCommitInfo.setDataVersion(2);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        physicalPartition.setNextVersion(9);
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(9, physicalPartition.getNextVersion(),
                "an overwrite below the counter must not pull it back");
    }

    @Test
    public void testApplyCommitLogEmptyPartitionOverwriteAdvancesWithoutDriftClaim() {
        // The mirror of testApplyCommitLogVersionOverwriteDoesNotLowerNextVersion: overwriting an
        // EMPTY partition deliberately names a version ABOVE the counter, so the counter is legally
        // "behind". It must still advance, and must not be reported as drift -- the diagnostic
        // exempts overwrite for exactly this case.
        LakeTable table = buildLakeTable();
        PhysicalPartition physicalPartition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        state.setTxnCommitAttachment(new InsertTxnCommitAttachment(0, 30));
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 30, 0);
        partitionCommitInfo.setDataVersion(30);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        physicalPartition.setNextVersion(2);
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(31, physicalPartition.getNextVersion(),
                "an overwrite above the counter must still advance it");
    }

    @Test
    public void testApplyCommitLogDoubleWriteAdvancesToTheOriginalPartitionVersion() {
        // A double-write target's commit info carries the ORIGINAL partition's version, which can sit
        // above this partition's own counter. That is not drift either.
        LakeTable table = buildLakeTable();
        PhysicalPartition physicalPartition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 20, 0);
        partitionCommitInfo.setDataVersion(20);
        partitionCommitInfo.setIsDoubleWrite(true);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        physicalPartition.setNextVersion(2);
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(21, physicalPartition.getNextVersion());
    }
>>>>>>> 9159316 ([BugFix] Derive lake partition versions from the journal and only ever advance them (#79296))
}
