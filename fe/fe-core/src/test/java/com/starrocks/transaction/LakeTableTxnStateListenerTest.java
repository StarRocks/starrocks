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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.proto.AbortTxnRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStorageMedium;
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
                () -> listener.preCommit(newTransactionState(), buildPartialTabletCommitInfo(), Collections.emptyList()));
        Assertions.assertTrue(exception.getMessage().contains("has unfinished tablets"));
    }

    @Test
    public void testCommitRateExceeded() {
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
        Assertions.assertThrows(CommitRateExceededException.class,
                () -> listener.preCommit(newTransactionState(), buildFullTabletCommitInfo(), Collections.emptyList()));
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
        listener.preCommit(newTransactionState(), buildFullTabletCommitInfo(), Collections.emptyList());
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
     * Verifies that compaction preCommit succeeds after tablet split when loaded indexes are registered.
     * Simulates: compaction collects I_old tablets → tablet split adds I_new → preCommit validates I_old.
     */
    @Test
    public void testCompactionPreCommitWithTabletSplitAndRegisteredIndexes() throws TransactionException {
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

        // preCommit should succeed because it validates I_old (registered), not I_new (latest)
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        listener.preCommit(txnState, finishedTablets, Collections.emptyList());
    }

    /**
     * Confirms the bug: without registering loaded indexes, compaction preCommit fails after tablet split
     * because it falls back to the latest index (I_new) whose tablets are not in finishedTablets.
     */
    @Test
    public void testCompactionPreCommitWithTabletSplitWithoutRegisteredIndexes() {
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

        // preCommit should FAIL because it falls back to I_new (latest), whose tablets are not finished
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        TransactionCommitFailedException exception = Assertions.assertThrows(
                TransactionCommitFailedException.class,
                () -> listener.preCommit(txnState, finishedTablets, Collections.emptyList()));
        Assertions.assertTrue(exception.getMessage().contains("has unfinished tablets"));
    }

    private void makeCompactionScoreExceedSlowdownThreshold() {
        long currentTimeMs = System.currentTimeMillis();
        CompactionMgr compactionMgr = GlobalStateMgr.getCurrentState().getCompactionMgr();
        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(Config.lake_ingest_slowdown_threshold + 10.0)));
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
