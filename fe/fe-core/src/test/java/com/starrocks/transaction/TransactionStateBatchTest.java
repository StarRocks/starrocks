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
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.system.ComputeNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionStateBatchTest {
    private static String fileName = "./TransactionStateBatchTest";

    @AfterEach
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testPutBeTablets() {
        Long dbId = 1000L;
        Long tableId = 20000L;
        List<TransactionState> transactionStateList = new ArrayList<TransactionState>();
        TransactionState transactionState1 = new TransactionState(dbId, Lists.newArrayList(tableId),
                3000, "label1", UUIDUtil.genTUniqueId(),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);
        TransactionState transactionState2 = new TransactionState(dbId, Lists.newArrayList(tableId),
                3001, "label2", UUIDUtil.genTUniqueId(),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);
        transactionStateList.add(transactionState1);
        transactionStateList.add(transactionState2);
        TransactionStateBatch stateBatch = new TransactionStateBatch(transactionStateList);

        long partitionId1 = 1;
        long partitionId2 = 2;
        Map<ComputeNode, List<Long>> nodeToTablets1 = new HashMap<>();
        ComputeNode node1 = new ComputeNode(1, "host", 9050);
        ComputeNode node2 = new ComputeNode(2, "host", 9050);
        nodeToTablets1.put(node1, Lists.newArrayList(1L, 2L));
        nodeToTablets1.put(node2, Lists.newArrayList(3L, 4L));
        Map<ComputeNode, List<Long>> nodeToTablets2 = new HashMap<>();
        nodeToTablets2.put(node1, Lists.newArrayList(2L, 3L, 4L));

        stateBatch.putBeTablets(partitionId1, nodeToTablets1);
        stateBatch.putBeTablets(partitionId1, nodeToTablets2);
        Assertions.assertEquals(1, stateBatch.getPartitionToTablets().size());
        Assertions.assertEquals(4, stateBatch.getPartitionToTablets().get(partitionId1).get(node1).size());
        Assertions.assertEquals(2, stateBatch.getPartitionToTablets().get(partitionId1).get(node2).size());

        stateBatch.putBeTablets(partitionId2, nodeToTablets2);
        Assertions.assertEquals(2, stateBatch.getPartitionToTablets().size());
    }

}