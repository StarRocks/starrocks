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
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.proto.AbortTxnRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;
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

        txnState.setTransactionStatus(TransactionStatus.COMMITTED);
        Assertions.assertEquals("", txnState.getReason());
    }

    @Test
    public void testCommitRejectedByPhysicalPartitionScoreAboveUpperBound() throws TransactionException {
        new MockUp<LakeTableTxnStateListener>() {
            @Mock
            boolean enableIngestSlowdown() {
                return true;
            }
        };

        LakeTable table = buildLakeTable();
        Assertions.assertNotEquals(partitionId, physicalPartitionId);
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        CompactionMgr compactionMgr = GlobalStateMgr.getCurrentState().getCompactionMgr();
        compactionMgr.clearPartitions();
        try {
            setCompactionScore(physicalPartitionId, Config.lake_compaction_score_upper_bound + 10.0);
            LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
            TransactionState txnState = prepareTransactionState(listener);
            // past the slowdown delay, so only the upper bound decides
            txnState.setAllowCommitTimeMs(System.currentTimeMillis() - 1);

            LakeTableTxnStateListener commitListener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
            CommitFailedException e = Assertions.assertThrows(CommitFailedException.class,
                    () -> commitListener.preCommit(txnState));
            Assertions.assertTrue(e.getMessage().contains("partition " + physicalPartitionId), e.getMessage());
        } finally {
            compactionMgr.clearPartitions();
        }
    }

    @Test
    public void testCommitThrottledByEachPhysicalPartitionOfLogicalPartition() throws TransactionException {
        new MockUp<LakeTableTxnStateListener>() {
            @Mock
            boolean enableIngestSlowdown() {
                return true;
            }
        };

        LakeTable table = buildLakeTable();
        long subPartitionId = physicalPartitionId + 100;
        long subTabletId = physicalPartitionId + 101;
        MaterializedIndex subIndex = new MaterializedIndex(indexId);
        TabletMeta subTabletMeta = new TabletMeta(dbId, tableId, subPartitionId, 0, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(subTabletId, subTabletMeta);
        subIndex.addTablet(new LakeTablet(subTabletId), subTabletMeta);
        table.getPartition(partitionId).addSubPartition(new PhysicalPartition(subPartitionId, partitionId, subIndex));
        table.addPhysicalPartition(table.getPartition(partitionId).getSubPartition(subPartitionId));

        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        CompactionMgr compactionMgr = GlobalStateMgr.getCurrentState().getCompactionMgr();
        compactionMgr.clearPartitions();
        try {
            // A high score on the logical partition id or on the sibling physical partition does not throttle
            // a load into the sub partition.
            double highScore = Config.lake_ingest_slowdown_threshold + 10.0;
            setCompactionScore(partitionId, highScore);
            setCompactionScore(physicalPartitionId, highScore);
            LakeTableTxnStateListener commitListener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
            commitListener.preCommit(prepareSubPartitionTransactionState(databaseTransactionMgr, table, subTabletId));

            setCompactionScore(subPartitionId, highScore);
            TransactionState txnState = prepareSubPartitionTransactionState(databaseTransactionMgr, table, subTabletId);
            Assertions.assertThrows(CommitRateExceededException.class, () -> commitListener.preCommit(txnState));
            Assertions.assertTrue(txnState.getAllowCommitTimeMs() > txnState.getWriteEndTimeMs());
        } finally {
            compactionMgr.clearPartitions();
        }
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

    private void makeCompactionScoreExceedSlowdownThreshold() {
        long currentTimeMs = System.currentTimeMillis();
        CompactionMgr compactionMgr = GlobalStateMgr.getCurrentState().getCompactionMgr();
        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, physicalPartitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(Config.lake_ingest_slowdown_threshold + 10.0)));
    }

    private void setCompactionScore(long partitionId, double score) {
        GlobalStateMgr.getCurrentState().getCompactionMgr().handleLoadingFinished(
                new PartitionIdentifier(dbId, tableId, partitionId), 3, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(score)));
    }

    private TransactionState prepareSubPartitionTransactionState(DatabaseTransactionMgr databaseTransactionMgr,
                                                                 LakeTable table, long subTabletId)
            throws TransactionException {
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        TransactionState txnState = newTransactionState();
        listener.prePrepared(txnState, Lists.newArrayList(new TabletCommitInfo(subTabletId, 1)),
                Collections.emptyList());
        txnState.setTransactionStatus(TransactionStatus.PREPARED);
        txnState.setPreparedTimeAndTimeout(
                System.currentTimeMillis(), TransactionState.DEFAULT_PREPARED_TIMEOUT_MS);
        listener.preWriteCommitLog(txnState);
        return txnState;
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
