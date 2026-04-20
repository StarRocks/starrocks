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

package com.starrocks.http.rest.transaction;

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class BypassWriteTransactionHandlerTest {

    @Test
    public void testRegisterLoadedIndexes() {
        long dbId = 1000L;
        long tableId = 2000L;
        long partitionId1 = 3000L;
        long partitionId2 = 3001L;
        long indexId1 = 4000L;
        long indexId2 = 4001L;
        long tabletId1 = 5000L;
        long tabletId2 = 5001L;
        long tabletId3 = 5002L;

        // Set up tablet inverted index with real tablet metadata
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        invertedIndex.addTablet(tabletId1,
                new TabletMeta(dbId, tableId, partitionId1, indexId1, TStorageMedium.HDD, true));
        invertedIndex.addTablet(tabletId2,
                new TabletMeta(dbId, tableId, partitionId1, indexId2, TStorageMedium.HDD, true));
        invertedIndex.addTablet(tabletId3,
                new TabletMeta(dbId, tableId, partitionId2, indexId1, TStorageMedium.HDD, true));

        // Create transaction state
        TransactionState txnState = new TransactionState(dbId, Lists.newArrayList(tableId),
                9000L, "label_bypass", null,
                LoadJobSourceType.BYPASS_WRITE,
                new TxnCoordinator(TxnSourceType.FE, "127.0.0.1"), 0, 60_000);

        // Build committed tablets
        List<TabletCommitInfo> committedTablets = new ArrayList<>();
        committedTablets.add(new TabletCommitInfo(tabletId1, 1));
        committedTablets.add(new TabletCommitInfo(tabletId2, 1));
        committedTablets.add(new TabletCommitInfo(tabletId3, 1));

        // Call registerLoadedIndexes
        BypassWriteTransactionHandler.registerLoadedIndexes(txnState, committedTablets);

        // Verify: partition1 should have both indexId1 and indexId2 registered
        MaterializedIndex idx1ForP1 = new MaterializedIndex(indexId1);
        MaterializedIndex idx2ForP1 = new MaterializedIndex(indexId2);
        PhysicalPartition pp1 = new PhysicalPartition(partitionId1, partitionId1, idx1ForP1);
        pp1.createRollupIndex(idx2ForP1);

        List<MaterializedIndex> loaded1 = txnState.getPartitionLoadedIndexes(tableId, pp1);
        Assertions.assertEquals(2, loaded1.size());
        List<Long> loadedIds1 = new ArrayList<>();
        for (MaterializedIndex idx : loaded1) {
            loadedIds1.add(idx.getId());
        }
        Assertions.assertTrue(loadedIds1.contains(indexId1));
        Assertions.assertTrue(loadedIds1.contains(indexId2));

        // Verify: partition2 should have indexId1 registered
        MaterializedIndex idx1ForP2 = new MaterializedIndex(indexId1);
        PhysicalPartition pp2 = new PhysicalPartition(partitionId2, partitionId2, idx1ForP2);

        List<MaterializedIndex> loaded2 = txnState.getPartitionLoadedIndexes(tableId, pp2);
        Assertions.assertEquals(1, loaded2.size());
        Assertions.assertEquals(indexId1, loaded2.get(0).getId());
    }

    @Test
    public void testRegisterLoadedIndexesWithNullTablets() {
        TransactionState txnState = new TransactionState(1L, Lists.newArrayList(2L),
                100L, "label", null,
                LoadJobSourceType.BYPASS_WRITE,
                new TxnCoordinator(TxnSourceType.FE, "127.0.0.1"), 0, 60_000);

        // Should not throw with null
        BypassWriteTransactionHandler.registerLoadedIndexes(txnState, null);

        // Should not throw with empty list
        BypassWriteTransactionHandler.registerLoadedIndexes(txnState, new ArrayList<>());
    }
}
