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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

// Verifies that a shadow-rewrite transaction is split out at batch formation
// (getReadyToPublishTxnListBatch) so it is never batched with normal version-advancing txns.
public class ShadowRewriteBatchFormationTest {
    private static final long DB_ID = 1000L;
    private static final long TABLE_ID = 20000L;
    private static final long PARTITION_ID = 30000L;

    private DatabaseTransactionMgr newDbTxnMgr() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public com.starrocks.persist.EditLog getEditLog() {
                return null;
            }
        };
        return new DatabaseTransactionMgr(DB_ID, GlobalStateMgr.getCurrentState());
    }

    private TransactionState newState(long txnId, boolean shadowRewrite, long version) {
        TransactionState.LoadJobSourceType sourceType = shadowRewrite
                ? TransactionState.LoadJobSourceType.SHADOW_REWRITE
                : TransactionState.LoadJobSourceType.INSERT_STREAMING;
        TransactionState state = new TransactionState(DB_ID, Lists.newArrayList(TABLE_ID),
                txnId, "label" + txnId, UUIDUtil.genTUniqueId(), sourceType,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "fe1"), 50000L, 60_000L);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(TABLE_ID);
        PartitionCommitInfo pci = new PartitionCommitInfo(PARTITION_ID, version, 0);
        tableCommitInfo.addPartitionCommitInfo(pci);
        state.putIdToTableCommitInfo(TABLE_ID, tableCommitInfo);
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        return state;
    }

    @SuppressWarnings("unchecked")
    private void inject(DatabaseTransactionMgr mgr, TransactionState... states) {
        TransactionGraph graph = new TransactionGraph();
        Map<Long, TransactionState> running =
                (Map<Long, TransactionState>) Deencapsulation.getField(mgr, "idToRunningTransactionState");
        for (TransactionState state : states) {
            running.put(state.getTransactionId(), state);
            graph.add(state.getTransactionId(), Lists.newArrayList(TABLE_ID));
        }
        Deencapsulation.setField(mgr, "transactionGraph", graph);
    }

    private TransactionStateBatch findBatchContaining(List<TransactionStateBatch> batches, long txnId) {
        for (TransactionStateBatch batch : batches) {
            for (TransactionState state : batch.getTransactionStates()) {
                if (state.getTransactionId() == txnId) {
                    return batch;
                }
            }
        }
        return null;
    }

    @Test
    public void testShadowRewriteTxnIsNeverBatchedWithNormalDml() {
        int oldMin = Config.lake_batch_publish_min_version_num;
        int oldMax = Config.lake_batch_publish_max_version_num;
        Config.lake_batch_publish_min_version_num = 1;
        Config.lake_batch_publish_max_version_num = 10;
        try {
            DatabaseTransactionMgr mgr = newDbTxnMgr();
            // A shadow-rewrite txn (sentinel version -1) committed before two adjacent normal DML txns.
            TransactionState shadowRewrite = newState(101, true, -1);
            TransactionState normal1 = newState(102, false, 2);
            TransactionState normal2 = newState(103, false, 3);
            inject(mgr, shadowRewrite, normal1, normal2);

            List<TransactionStateBatch> batches = mgr.getReadyToPublishTxnListBatch();

            TransactionStateBatch shadowBatch = findBatchContaining(batches, 101);
            Assertions.assertNotNull(shadowBatch, "shadow-rewrite txn must still yield a publish work item");
            Assertions.assertEquals(1, shadowBatch.size(), "shadow-rewrite txn must be its own size-1 batch");

            // The shadow-rewrite txn must never share a batch with a normal txn.
            for (TransactionStateBatch batch : batches) {
                boolean hasShadow = false;
                boolean hasNormal = false;
                for (TransactionState state : batch.getTransactionStates()) {
                    if (state.getTransactionId() == 101) {
                        hasShadow = true;
                    } else {
                        hasNormal = true;
                    }
                }
                Assertions.assertFalse(hasShadow && hasNormal,
                        "shadow-rewrite txn must never be batched with a normal txn");
            }
        } finally {
            Config.lake_batch_publish_min_version_num = oldMin;
            Config.lake_batch_publish_max_version_num = oldMax;
        }
    }

    @Test
    public void testNormalDmlBatchesTogetherWithoutShadowRewrite() {
        int oldMin = Config.lake_batch_publish_min_version_num;
        int oldMax = Config.lake_batch_publish_max_version_num;
        Config.lake_batch_publish_min_version_num = 1;
        Config.lake_batch_publish_max_version_num = 10;
        try {
            DatabaseTransactionMgr mgr = newDbTxnMgr();
            // Record loaded index ids so the batch-formation index snapshot stays identical across txns.
            TransactionState normal1 = newState(201, false, 2);
            TransactionState normal2 = newState(202, false, 3);
            normal1.addPartitionLoadedIndexes(TABLE_ID, PARTITION_ID, Lists.newArrayList(1L));
            normal2.addPartitionLoadedIndexes(TABLE_ID, PARTITION_ID, Lists.newArrayList(1L));
            inject(mgr, normal1, normal2);

            List<TransactionStateBatch> batches = mgr.getReadyToPublishTxnListBatch();
            TransactionStateBatch batch = findBatchContaining(batches, 201);
            Assertions.assertNotNull(batch);
            // Two adjacent normal DML txns with consecutive versions batch together.
            Assertions.assertEquals(2, batch.size());
        } finally {
            Config.lake_batch_publish_min_version_num = oldMin;
            Config.lake_batch_publish_max_version_num = oldMax;
        }
    }
}
