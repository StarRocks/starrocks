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
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.SyncMaterializedViewDefinition;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.AbortTxnRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class LakeTableTxnStateListenerTest extends LakeTableTestHelper {
    @Test
    public void testHasUnfinishedTablet() {
        LakeTable table = buildLakeTable();
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        TransactionCommitFailedException exception = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(
                        newTransactionState(), buildPartialTabletCommitInfo(), Collections.emptyList()));
        Assertions.assertTrue(exception.getMessage().contains("has unfinished tablets"));
    }

    @Test
    public void testCommitRateExceededAtFinalCommit() throws TransactionException {
        new MockUp<LakeTableTxnStateListener>() {
            @Mock
            boolean enableIngestSlowdown() {
                return true;
            }
        };

        LakeTable table = buildLakeTable();
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        makeCompactionScoreExceedSlowdownThreshold();
        TransactionState txnState = prepareTransactionState(listener);

        LakeTableTxnStateListener commitListener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        Assertions.assertThrows(CommitRateExceededException.class,
                () -> commitListener.preCommit(txnState));
        Assertions.assertTrue(txnState.getReason().contains("delay commit"));

        txnState.setAllowCommitTimeMs(System.currentTimeMillis() - 1);
        commitListener.preCommit(txnState);
        Assertions.assertEquals("", txnState.getReason());

        TransactionState committedState = new TransactionState(txnState);
        committedState.setTransactionStatus(TransactionStatus.COMMITTED);
        Assertions.assertEquals("", committedState.getReason());
    }

    @Test
    public void testCommitRateLimiterDisabled() throws TransactionException {
        new MockUp<LakeTableTxnStateListener>() {
            @Mock
            boolean enableIngestSlowdown() {
                return false;
            }
        };

        LakeTable table = buildLakeTable();
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        makeCompactionScoreExceedSlowdownThreshold();
        TransactionState txnState = prepareTransactionState(listener);

        LakeTableTxnStateListener commitListener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        commitListener.preCommit(txnState);
    }

    @ParameterizedTest
    @MethodSource("dataProvider")
    public void testPostAbort(boolean skipCleanup, List<ComputeNode> nodes) {
        new MockUp<LakeTableTxnStateListener>() {
            @Mock
            void sendAbortTxnRequestIgnoreResponse(AbortTxnRequest request, ComputeNode node) {
                Assertions.assertNotNull(request);
                Assertions.assertNotNull(node);
            }

            @Mock
            ComputeNode getAliveNode(Long nodeId) {
                return nodes.isEmpty() ? null : nodes.get(0);
            }

            @Mock
            List<ComputeNode> getAllAliveNodes() {
                return nodes;
            }
        };

        LakeTable table = buildLakeTable();
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        TransactionState txnState = newTransactionState();
        txnState.setTransactionStatus(TransactionStatus.ABORTED);
        txnState.setReason("timed out");
        List<TabletCommitInfo> finishedTablets = Collections.emptyList();
        if (!skipCleanup) {
            finishedTablets = Collections.singletonList(new TabletCommitInfo(tableId, 10001));
        }
        listener.postAbort(txnState, finishedTablets, Collections.emptyList());
    }

    /**
     * Verifies that compaction prePrepared succeeds after tablet split when loaded indexes are registered.
     * Simulates: compaction collects I_old tablets → tablet split adds I_new → prePrepared validates I_old.
     */
    @Test
    public void testCompactionPrePreparedWithTabletSplitAndRegisteredIndexes() throws TransactionException {
        // Build table with I_old (1 tablet)
        long oldIndexId = 20001;
        long oldTabletId = 20002;
        long newIndexId = 20003;
        long[] newTabletIds = {20004, 20005, 20006};

        MaterializedIndex oldIndex = new MaterializedIndex(oldIndexId);
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
        invertedIndex.addTablet(oldTabletId, tabletMeta);
        oldIndex.addTablet(new LakeTablet(oldTabletId), tabletMeta);

        LakeTable table = buildLakeTableWithIndex(oldIndex);
        PhysicalPartition partition = table.getPhysicalPartition(physicalPartitionId);

        // Create compaction transaction and register loaded indexes (the fix)
        TransactionState txnState = newCompactionTransactionState();
        List<Long> loadedIndexIds = partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)
                .stream().map(MaterializedIndex::getId).collect(Collectors.toList());
        txnState.addPartitionLoadedIndexes(tableId, partition.getId(), loadedIndexIds);

        // Simulate tablet split: add I_new with 3 new tablets (same metaId as I_old)
        MaterializedIndex newIndex = new MaterializedIndex(newIndexId, oldIndexId,
                MaterializedIndex.IndexState.NORMAL, 0L);
        for (long tid : newTabletIds) {
            TabletMeta meta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
            invertedIndex.addTablet(tid, meta);
            newIndex.addTablet(new LakeTablet(tid), meta);
        }
        partition.addMaterializedIndex(newIndex, true);

        // Build finishedTablets from I_old's tablet only
        List<TabletCommitInfo> finishedTablets = new ArrayList<>();
        finishedTablets.add(new TabletCommitInfo(oldTabletId, 1));

        // prePrepared should succeed because it validates I_old (registered), not I_new (latest)
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        listener.prePrepared(txnState, finishedTablets, Collections.emptyList());
    }

    /**
     * Confirms the bug: without registering loaded indexes, compaction prePrepared fails after tablet split
     * because it falls back to the latest index (I_new) whose tablets are not in finishedTablets.
     */
    @Test
    public void testCompactionPrePreparedWithTabletSplitWithoutRegisteredIndexes() {
        // Build table with I_old (1 tablet)
        long oldIndexId = 30001;
        long oldTabletId = 30002;
        long newIndexId = 30003;
        long[] newTabletIds = {30004, 30005, 30006};

        MaterializedIndex oldIndex = new MaterializedIndex(oldIndexId);
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
        invertedIndex.addTablet(oldTabletId, tabletMeta);
        oldIndex.addTablet(new LakeTablet(oldTabletId), tabletMeta);

        LakeTable table = buildLakeTableWithIndex(oldIndex);
        PhysicalPartition partition = table.getPhysicalPartition(physicalPartitionId);

        // Create compaction transaction WITHOUT registering loaded indexes
        TransactionState txnState = newCompactionTransactionState();

        // Simulate tablet split: add I_new with 3 new tablets
        MaterializedIndex newIndex = new MaterializedIndex(newIndexId, oldIndexId,
                MaterializedIndex.IndexState.NORMAL, 0L);
        for (long tid : newTabletIds) {
            TabletMeta meta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
            invertedIndex.addTablet(tid, meta);
            newIndex.addTablet(new LakeTablet(tid), meta);
        }
        partition.addMaterializedIndex(newIndex, true);

        // Build finishedTablets from I_old's tablet only
        List<TabletCommitInfo> finishedTablets = new ArrayList<>();
        finishedTablets.add(new TabletCommitInfo(oldTabletId, 1));

        // prePrepared should FAIL because it falls back to I_new (latest), whose tablets are not finished
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        TransactionCommitFailedException exception = Assertions.assertThrows(
                TransactionCommitFailedException.class,
                () -> listener.prePrepared(txnState, finishedTablets, Collections.emptyList()));
        Assertions.assertTrue(exception.getMessage().contains("has unfinished tablets"));
    }

    /**
     * A transaction drawn at or after an alter's watershed must write the shadow index. One whose recorded
     * index set lacks it resolved tablets before the index existed (replication, a bypass-write client with
     * cached metadata) and is rejected. The shadow index here keeps visibleTxnId at 0, as LakeRollupJob's
     * does, so the check relies only on the watershed stamp.
     */
    @Test
    public void testRejectTxnAtOrAfterWatershedMissingShadowIndex() {
        LakeTable table = buildLakeTableWithShadowIndex(40001, 40002, 9000);
        TransactionState txnState = newTransactionState(); // txn id 10000 >= watershed 9000
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));

        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        TransactionCommitFailedException exception = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(txnState, buildFullTabletCommitInfo(), Collections.emptyList()));
        Assertions.assertTrue(exception.getMessage().contains("without shadow index 40001"), exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("since 9000"), exception.getMessage());
    }

    /**
     * A transaction drawn before the watershed legitimately misses the shadow index: the alter waits for it
     * and its rows reach the shadow index through the conversion.
     */
    @Test
    public void testAcceptTxnBeforeWatershedMissingShadowIndex() throws TransactionException {
        LakeTable table = buildLakeTableWithShadowIndex(40011, 40012, 20000);
        TransactionState txnState = newTransactionState(); // txn id 10000 < watershed 20000
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));

        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        listener.prePrepared(txnState, buildFullTabletCommitInfo(), Collections.emptyList());
    }

    @Test
    public void testAcceptTxnCarryingShadowIndex() throws TransactionException {
        LakeTable table = buildLakeTableWithShadowIndex(40021, 40022, 9000);
        TransactionState txnState = newTransactionState();
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId, 40021L));
        List<TabletCommitInfo> finishedTablets = buildFullTabletCommitInfo();
        finishedTablets.add(new TabletCommitInfo(40022, 1));

        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        listener.prePrepared(txnState, finishedTablets, Collections.emptyList());
    }

    /**
     * A shadow index exposed by an FE without the watershed stamp is not checked.
     */
    @Test
    public void testUnstampedShadowIndexIsNotChecked() throws TransactionException {
        LakeTable table = buildLakeTableWithShadowIndex(40031, 40032, 0);
        TransactionState txnState = newTransactionState();
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));

        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        listener.prePrepared(txnState, buildFullTabletCommitInfo(), Collections.emptyList());
    }

    /**
     * Lake compaction compacts only visible tablets by design, so it is not held to the shadow index.
     */
    @Test
    public void testCompactionIsNotChecked() throws TransactionException {
        LakeTable table = buildLakeTableWithShadowIndex(40041, 40042, 9000);
        TransactionState txnState = newCompactionTransactionState();
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));

        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        listener.prePrepared(txnState, buildFullTabletCommitInfo(), Collections.emptyList());
    }

    @Test
    public void testRejectStaleTxnMissingNormalRangeSyncMv() {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 9000);
        TransactionState txnState = newTransactionState(); // txn 10000, planned before MV activation
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);

        TransactionCommitFailedException error = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(txnState, buildFullTabletCommitInfo(), List.of()));
        Assertions.assertTrue(error.getMessage().contains("10000"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("9000"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("90006"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("t0"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("50001"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("mv_50001"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("refresh"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("retry"), error.getMessage());
    }

    @Test
    public void testRejectMissingNormalSyncMvAmongSiblings() throws TransactionException {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 9000);
        addNormalSyncMv(table, 50011, 50012, 9500);
        addNormalSyncMv(table, 50021, 50022, 9500);
        TransactionState txnState = newTransactionState();
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId, 50001L, 50011L));
        List<TabletCommitInfo> commits = buildFullTabletCommitInfo();
        commits.add(new TabletCommitInfo(50002, 1));
        commits.add(new TabletCommitInfo(50012, 1));
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);

        TransactionCommitFailedException error = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(txnState, commits, List.of()));
        Assertions.assertTrue(error.getMessage().contains("logical index 50021"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("9500"), error.getMessage());
        commits.add(new TabletCommitInfo(50022, 1));
        TransactionState fresh = newTransactionState();
        fresh.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId, 50001L, 50011L, 50021L));
        listener.prePrepared(fresh, commits, List.of());
    }

    @Test
    public void testAcceptFreshTxnWithNormalRangeSyncMv() throws TransactionException {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 10000);
        TransactionState txnState = newTransactionState(); // exact watershed boundary
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId, 50001L));
        List<TabletCommitInfo> commits = buildFullTabletCommitInfo();
        commits.add(new TabletCommitInfo(50002, 1));
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        listener.prePrepared(txnState, commits, List.of());

        TransactionState stale = newTransactionState();
        table.getIndexMetaByMetaId(50001).setSyncMvRequiredTxnId(stale.getTransactionId());
        stale.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(stale, buildFullTabletCommitInfo(), List.of()));
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(txnState, buildFullTabletCommitInfo(), List.of()));
    }

    @Test
    public void testAcceptPreWatershedTxn() throws TransactionException {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 10001);
        TransactionState txnState = newTransactionState(); // one below the watermark
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));
        new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table)
                .prePrepared(txnState, buildFullTabletCommitInfo(), List.of());
    }

    @Test
    public void testAcceptPredecessorPhysicalMvIndex() throws TransactionException {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 9000);
        PhysicalPartition partition = table.getPhysicalPartition(physicalPartitionId);
        MaterializedIndexMeta meta = table.getIndexMetaByMetaId(50001);
        TransactionState txnState = newTransactionState();
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId, 50001L));
        List<TabletCommitInfo> commits = buildFullTabletCommitInfo();
        commits.add(new TabletCommitInfo(50002, 1));
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);

        // Split, then merge: both retain the earlier physical layouts under the same logical MV.
        addIndexTablet(partition, 50003, 50001, 50004, 50005);
        Assertions.assertEquals(2, partition.getLatestIndex(50001).getTablets().size());
        listener.prePrepared(txnState, commits, List.of());
        TransactionState splitWriter = newTransactionState();
        splitWriter.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId, 50003L));
        List<TabletCommitInfo> splitCommits = buildFullTabletCommitInfo();
        splitCommits.add(new TabletCommitInfo(50004, 1));
        splitCommits.add(new TabletCommitInfo(50005, 1));
        listener.prePrepared(splitWriter, splitCommits, List.of());
        addIndexTablet(partition, 50006, 50001, 50007);
        Assertions.assertEquals(50006, partition.getLatestIndex(50001).getId());
        Assertions.assertSame(meta, table.getIndexMetaByMetaId(50001));
        Assertions.assertEquals(9000, meta.getSyncMvRequiredTxnId());
        Assertions.assertNotNull(partition.getIndex(50001));
        listener.prePrepared(txnState, commits, List.of());
        listener.prePrepared(splitWriter, splitCommits, List.of());
        splitCommits.remove(splitCommits.size() - 1);
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(splitWriter, splitCommits, List.of()));
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(txnState, buildFullTabletCommitInfo(), List.of()));
        TransactionState stale = newTransactionState();
        stale.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(stale, buildFullTabletCommitInfo(), List.of()));
        partition.deleteMaterializedIndexByIndexId(50001);
        TransactionCommitFailedException missingPhysicalIndex = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(txnState, buildFullTabletCommitInfo(), List.of()));
        Assertions.assertTrue(missingPhysicalIndex.getMessage().contains("logical index 50001"),
                missingPhysicalIndex.getMessage());
    }

    @Test
    public void testRejectBaseOnlyTxnAfterMetadataGson() {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 9000);
        LakeTable restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), LakeTable.class);
        Assertions.assertEquals(9000, restored.getIndexMetaByMetaId(50001).getSyncMvRequiredTxnId());
        TransactionState txnState = newTransactionState();
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), restored);
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(txnState, buildFullTabletCommitInfo(), List.of()));
    }

    @Test
    public void testExemptCompactionAndSecondShadowRewrite() throws TransactionException {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 9000);
        TransactionState compaction = newCompactionTransactionState();
        compaction.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        listener.prePrepared(compaction, buildFullTabletCommitInfo(), List.of());

        MaterializedIndex shadow = new MaterializedIndex(50011, MaterializedIndex.IndexState.SHADOW);
        shadow.setShadowWatershedTxnId(9500);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 50011, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(50012, tabletMeta);
        shadow.addTablet(new LakeTablet(50012), tabletMeta);
        table.getPhysicalPartition(physicalPartitionId).createRollupIndex(shadow);
        TransactionState rewrite = newTransactionState(TransactionState.LoadJobSourceType.SHADOW_REWRITE);
        InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(1);
        attachment.setShadowRewriteWatershedTxnId(9500);
        attachment.setShadowRewriteAlterVersion(table.getPhysicalPartition(physicalPartitionId).getVisibleVersion());
        rewrite.setTxnCommitAttachment(attachment);
        rewrite.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(50011L));
        listener.prePrepared(rewrite, List.of(new TabletCommitInfo(50012, 1)), List.of());
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(rewrite, buildFullTabletCommitInfo(), List.of()));

        // The exemption is only for the NORMAL guard: existing SHADOW coverage remains enforced.
        TransactionState missingShadow = newTransactionState(TransactionState.LoadJobSourceType.SHADOW_REWRITE);
        missingShadow.setTxnCommitAttachment(attachment);
        missingShadow.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));
        TransactionCommitFailedException error = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(missingShadow, buildFullTabletCommitInfo(), List.of()));
        Assertions.assertTrue(error.getMessage().contains("without shadow index 50011"), error.getMessage());
    }

    @Test
    public void testPreserveHashAndZeroWatermarkBehavior() throws TransactionException {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 0);
        TransactionState txnState = newTransactionState();
        txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        listener.prePrepared(txnState, buildFullTabletCommitInfo(), List.of());
        table.getIndexMetaByMetaId(50001).setSyncMvRequiredTxnId(9000);
        table.setDefaultDistributionInfo(new HashDistributionInfo(1, table.getBaseSchema()));
        listener.prePrepared(txnState, buildFullTabletCommitInfo(), List.of());
        table.setDefaultDistributionInfo(new RangeDistributionInfo());
        table.getIndexMetaByMetaId(50001).setSyncMvDefinition(null); // legacy plain rollup
        listener.prePrepared(txnState, buildFullTabletCommitInfo(), List.of());
    }

    @Test
    public void testMissingAndEmptyIndexSnapshots() throws TransactionException {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 9000);
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        TransactionState noSnapshot = newTransactionState();
        TransactionCommitFailedException unfinished = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(noSnapshot, buildFullTabletCommitInfo(), List.of()));
        Assertions.assertTrue(unfinished.getMessage().contains("has unfinished tablets"), unfinished.getMessage());
        List<TabletCommitInfo> complete = buildFullTabletCommitInfo();
        complete.add(new TabletCommitInfo(50002, 1));
        listener.prePrepared(noSnapshot, complete, List.of());

        // Registration rejects empty sets; emulate a recorded set becoming empty through its existing list API.
        TransactionState emptySnapshot = newTransactionState();
        List<Long> indexes = Lists.newArrayList(indexId);
        emptySnapshot.addPartitionLoadedIndexes(tableId, physicalPartitionId, indexes);
        indexes.clear();
        TransactionCommitFailedException empty = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(emptySnapshot, buildFullTabletCommitInfo(), List.of()));
        Assertions.assertTrue(empty.getMessage().contains("logical index 50001"), empty.getMessage());
    }

    @Test
    public void testRequireNormalSyncMvInEveryDirtyPartition() throws TransactionException {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 9000);
        long secondPartitionId = physicalPartitionId + 1;
        MaterializedIndex base = new MaterializedIndex(50101, indexId, MaterializedIndex.IndexState.NORMAL, 0L);
        TabletMeta baseTabletMeta = new TabletMeta(dbId, tableId, secondPartitionId, indexId, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(50102, baseTabletMeta);
        base.addTablet(new LakeTablet(50102), baseTabletMeta);
        Partition second = new Partition(partitionId + 1, secondPartitionId, "p1", base, new RangeDistributionInfo());
        table.addPartition(second);
        addIndexTablet(second.getDefaultPhysicalPartition(), 50103, 50001, 50104);
        TransactionState stale = newTransactionState();
        stale.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId, 50001L));
        stale.addPartitionLoadedIndexes(tableId, secondPartitionId, Lists.newArrayList(50101L));
        List<TabletCommitInfo> commits = buildFullTabletCommitInfo();
        commits.add(new TabletCommitInfo(50002, 1));
        commits.add(new TabletCommitInfo(50102, 1));
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        TransactionCommitFailedException error = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> listener.prePrepared(stale, commits, List.of()));
        Assertions.assertTrue(error.getMessage().contains("partition 90007"), error.getMessage());
        TransactionState fresh = newTransactionState();
        fresh.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId, 50001L));
        fresh.addPartitionLoadedIndexes(tableId, secondPartitionId, Lists.newArrayList(50101L, 50103L));
        commits.add(new TabletCommitInfo(50104, 1));
        listener.prePrepared(fresh, commits, List.of());
    }

    @Test
    public void testOrdinaryTransactionSourcesCannotSkipNormalMvCoverage() {
        LakeTable table = buildRangeLakeTableWithNormalSyncMv(50001, 50002, 9000);
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(addDatabaseTransactionMgr(), table);
        for (TransactionState.LoadJobSourceType source : List.of(TransactionState.LoadJobSourceType.FRONTEND,
                TransactionState.LoadJobSourceType.REPLICATION,
                TransactionState.LoadJobSourceType.BYPASS_WRITE, TransactionState.LoadJobSourceType.INSERT_STREAMING,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING)) {
            TransactionState txnState = newTransactionState(source);
            txnState.addPartitionLoadedIndexes(tableId, physicalPartitionId, Lists.newArrayList(indexId));
            Assertions.assertThrows(TransactionCommitFailedException.class,
                    () -> listener.prePrepared(txnState, buildFullTabletCommitInfo(), List.of()), source.toString());
        }
    }

    private TransactionState newTransactionState(TransactionState.LoadJobSourceType source) {
        return new TransactionState(dbId, Lists.newArrayList(tableId), nextTxnId++, "label", null,
                source, new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"), 0, 60_000);
    }

    private LakeTable buildRangeLakeTableWithNormalSyncMv(long mvId, long mvTabletId, long requiredTxnId) {
        LakeTable table = buildLakeTable();
        table.setDefaultDistributionInfo(new RangeDistributionInfo());
        table.setBaseIndexMetaId(indexId);
        table.setIndexMeta(indexId, "t0", table.getFullSchema(), 0, 0, (short) 1,
                TStorageType.COLUMN, table.getKeysType());
        addNormalSyncMv(table, mvId, mvTabletId, requiredTxnId);
        return table;
    }

    private void addNormalSyncMv(LakeTable table, long mvId, long mvTabletId, long requiredTxnId) {
        table.setIndexMeta(mvId, "mv_" + mvId, table.getBaseSchema(), 0, 0, (short) 1,
                TStorageType.COLUMN, table.getKeysType());
        MaterializedIndexMeta meta = table.getIndexMetaByMetaId(mvId);
        meta.setSyncMvDefinition(SyncMaterializedViewDefinition.capture(meta.getSchema(), null));
        meta.setSyncMvRequiredTxnId(requiredTxnId);
        addIndexTablet(table.getPhysicalPartition(physicalPartitionId), mvId, mvId, mvTabletId);
    }

    private MaterializedIndex addIndexTablet(PhysicalPartition partition, long physicalIndexId,
                                             long metaId, long... tabletIds) {
        MaterializedIndex index = new MaterializedIndex(physicalIndexId, metaId, MaterializedIndex.IndexState.NORMAL, 0L);
        for (long tabletId : tabletIds) {
            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partition.getId(), metaId, TStorageMedium.HDD, true);
            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tabletId, tabletMeta);
            index.addTablet(new LakeTablet(tabletId), tabletMeta);
        }
        if (partition.getLatestIndex(metaId) == null) {
            partition.createRollupIndex(index);
        } else {
            partition.addMaterializedIndex(index, metaId == partition.getLatestBaseIndex().getMetaId());
        }
        return index;
    }

    private LakeTable buildLakeTableWithShadowIndex(long shadowIndexId, long shadowTabletId, long shadowWatershedTxnId) {
        LakeTable table = buildLakeTable();
        PhysicalPartition partition = table.getPhysicalPartition(physicalPartitionId);
        MaterializedIndex shadowIndex = new MaterializedIndex(shadowIndexId, MaterializedIndex.IndexState.SHADOW);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, shadowIndexId, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(shadowTabletId, tabletMeta);
        shadowIndex.addTablet(new LakeTablet(shadowTabletId), tabletMeta);
        if (shadowWatershedTxnId > 0) {
            shadowIndex.setShadowWatershedTxnId(shadowWatershedTxnId);
        }
        partition.createRollupIndex(shadowIndex);
        return table;
    }

    private void makeCompactionScoreExceedSlowdownThreshold() {
        long currentTimeMs = System.currentTimeMillis();
        CompactionMgr compactionMgr = GlobalStateMgr.getCurrentState().getCompactionMgr();
        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, physicalPartitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(Config.lake_ingest_slowdown_threshold + 10.0)));
    }

    private TransactionState prepareTransactionState(LakeTableTxnStateListener listener) throws TransactionException {
        TransactionState txnState = newTransactionState();
        listener.prePrepared(txnState, buildFullTabletCommitInfo(), Collections.emptyList());
        txnState.setTransactionStatus(TransactionStatus.PREPARED);
        txnState.setPreparedTimeAndTimeout(
                System.currentTimeMillis(), TransactionState.DEFAULT_PREPARED_TIMEOUT_MS);
        listener.preWriteCommitLog(txnState);
        return txnState;
    }

    private static Stream<Arguments> dataProvider() {
        return Stream.of(
                arguments(false, Collections.singletonList(new ComputeNode())),
                arguments(false, Collections.emptyList()),
                arguments(true, Collections.singletonList(new ComputeNode())),
                arguments(true, Collections.emptyList()),
                arguments(false, Lists.newArrayList(new ComputeNode(10001, "", 0), new ComputeNode(10002, "", 0))));
    }
}
