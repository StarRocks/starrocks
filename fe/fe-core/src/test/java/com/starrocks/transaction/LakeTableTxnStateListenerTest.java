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
import org.junit.Assert;
import org.junit.Test;
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
        TransactionCommitFailedException exception = Assert.assertThrows(TransactionCommitFailedException.class, () -> {
            listener.preCommit(newTransactionState(), buildPartialTabletCommitInfo(), Collections.emptyList());
        });
        Assert.assertTrue(exception.getMessage().contains("has unfinished tablets"));
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
        Assert.assertThrows(CommitRateExceededException.class, () -> {
            listener.preCommit(newTransactionState(), buildFullTabletCommitInfo(), Collections.emptyList());
        });
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
                Assert.assertNotNull(node);
                Assert.assertEquals(skipCleanup, request.skipCleanup);
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
        if (!skipCleanup) {
            txnState.setTabletCommitInfos(Collections.singletonList(new TabletCommitInfo(tableId, 10001)));
        }
        listener.postAbort(txnState, Collections.emptyList());
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
                arguments(true, Collections.emptyList()));
    }
}
