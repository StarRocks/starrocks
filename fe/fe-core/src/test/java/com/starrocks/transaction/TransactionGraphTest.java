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
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TransactionGraphTest {
    private void expectNextBatch(TransactionGraph graph, List<Long> expected) {
        List<Long> result = graph.getTxnsWithoutDependency();
        Collections.sort(result);
        assertEquals(expected, result);
        for (long txnId : result) {
            graph.remove(txnId);
        }
    }

    @Test
    public void testSimple() {
        TransactionGraph graph = new TransactionGraph();
        graph.add(1, Lists.newArrayList(1L));
        graph.add(2, Lists.newArrayList(2L));
        graph.add(3, Lists.newArrayList(3L));
        graph.add(4, Lists.newArrayList(1L));
        graph.add(5, Lists.newArrayList(2L));
        graph.add(6, Lists.newArrayList(3L));
        assertEquals(graph.size(), 6);
        expectNextBatch(graph, Lists.newArrayList(1L, 2L, 3L));
        assertEquals(graph.size(), 3);
        expectNextBatch(graph, Lists.newArrayList(4L, 5L, 6L));
        assertEquals(graph.size(), 0);
        assertEquals(graph.getTxnsWithoutDependency().size(), 0);
    }

    @Test
    public void testRemoveNodeWithDependency() {
        TransactionGraph graph = new TransactionGraph();
        graph.add(1, Lists.newArrayList(1L));
        graph.add(2, Lists.newArrayList(2L));
        graph.add(3, Lists.newArrayList(1L));
        graph.add(4, Lists.newArrayList(2L));
        graph.add(5, Lists.newArrayList(1L));
        graph.add(6, Lists.newArrayList(2L));
        assertEquals(graph.size(), 6);
        graph.remove(3);
        graph.remove(4);
        assertEquals(graph.size(), 4);
        expectNextBatch(graph, Lists.newArrayList(1L, 2L, 5L, 6L));
        assertEquals(graph.size(), 0);
        assertEquals(graph.getTxnsWithoutDependency().size(), 0);
    }

    @Test
    public void testMultiTableTxn() {
        TransactionGraph graph = new TransactionGraph();
        graph.add(1, Lists.newArrayList(1L));
        graph.add(2, Lists.newArrayList(2L));
        graph.add(3, Lists.newArrayList(1L, 2L));
        graph.add(4, Lists.newArrayList(1L));
        graph.add(5, Lists.newArrayList(2L));
        graph.add(6, Lists.newArrayList(1L, 2L));
        graph.add(7, Lists.newArrayList(3L));
        expectNextBatch(graph, Lists.newArrayList(1L, 2L, 7L));
        expectNextBatch(graph, Lists.newArrayList(3L));
        graph.add(8, Lists.newArrayList(3L));
        expectNextBatch(graph, Lists.newArrayList(4L, 5L, 8L));
        expectNextBatch(graph, Lists.newArrayList(6L));
        assertEquals(graph.size(), 0);
        assertEquals(graph.getTxnsWithoutDependency().size(), 0);
    }

    @Test
    public void testLargeGraph() {
        TransactionGraph graph = new TransactionGraph();
        int nTable = 30;
        int nTxn = 1000;
        int txnPolled = 0;
        Random random = new Random();
        for (int i = 0; i < nTxn; i++) {
            Set<Long> writeTableIds = Sets.newHashSet();
            int nWriteTable = Math.max(1, random.nextInt(10));
            for (int j = 0; j < nWriteTable; j++) {
                while (true) {
                    long tableId = random.nextInt(nTable);
                    if (writeTableIds.contains(tableId)) {
                        continue;
                    }
                    break;
                }
            }
            graph.add(i, new ArrayList<>(writeTableIds));
            if (random.nextInt(10) == 0) {
                List<Long> result = graph.getTxnsWithoutDependency();
                txnPolled += result.size();
                for (long txnId : result) {
                    graph.remove(txnId);
                }
            }
        }
        while (true) {
            List<Long> result = graph.getTxnsWithoutDependency();
            if (result.isEmpty()) {
                break;
            }
            txnPolled += result.size();
            for (long txnId : result) {
                graph.remove(txnId);
            }
        }
        assertEquals(nTxn, txnPolled);
        assertEquals(0, graph.size());
    }

    @Test
    public void testGetTxnsWithTxnDependencyBatch() {
        int maxBatchSize = 5;
        int minBatchSize = 2;

        // TransactionGraph
        // table1: txn1 -> txn4 -> txn6
        // table2: txn2 -> txn5
        // table3: txn3
        // test txns with single table
        TransactionGraph graph = new TransactionGraph();
        graph.add(1, Lists.newArrayList(1L));
        graph.add(2, Lists.newArrayList(2L));
        graph.add(3, Lists.newArrayList(3L));
        graph.add(4, Lists.newArrayList(1L));
        graph.add(5, Lists.newArrayList(2L));
        graph.add(6, Lists.newArrayList(1L));

        List<Long> txnIds = graph.getTxnsWithoutDependency();
        assertEquals(txnIds.size(), 3);
        assertEquals(3, graph.getTxnsWithTxnDependencyBatch(minBatchSize, maxBatchSize, 1).size());
        assertEquals(0, graph.getTxnsWithTxnDependencyBatch(minBatchSize, maxBatchSize, 3).size());

        // test txns with multi tables
        TransactionGraph graph2 = new TransactionGraph();
        graph2.add(1, Lists.newArrayList(1L, 2L));

        txnIds = graph2.getTxnsWithoutDependency();
        assertEquals(txnIds.size(), 1);
        List<Long> batchTxnIds = graph2.getTxnsWithTxnDependencyBatch(1, 5, txnIds.get(0));
        assertEquals(1, batchTxnIds.size());
        assertEquals(txnIds.get(0).longValue(), 1);

        // TransactionGraph
        // table1:  ------------------> txn1 ------------> txn2 --------------> txn3
        // table2:  ------------------> txn1
        graph2.add(2, Lists.newArrayList(1L));
        graph2.add(3, Lists.newArrayList(1L));
        txnIds = graph2.getTxnsWithoutDependency();
        assertEquals(txnIds.size(), 1);
        batchTxnIds = graph2.getTxnsWithTxnDependencyBatch(1, 5, txnIds.get(0));
        assertEquals(batchTxnIds.get(0).longValue(), 1);

        graph2.remove(1);
        batchTxnIds = graph2.getTxnsWithTxnDependencyBatch(1, 5, 2);
        assertEquals(batchTxnIds.size(), 2);
        assertEquals(batchTxnIds.get(0).longValue(), 2);
        assertEquals(batchTxnIds.get(1).longValue(), 3);

        // TransactionGraph
        // table1:  ------------> txn2 -------------> txn3  ----------> txn4 -------> txn5  ------> txn7
        // table2:  --------------------------------------------------> txn4 -------> txn6  ------> txn7
        graph2.add(4, Lists.newArrayList(1L, 2L));
        graph2.add(5, Lists.newArrayList(1L));
        graph2.add(6, Lists.newArrayList(2L));
        graph2.add(7, Lists.newArrayList(1L, 2L));

        txnIds = graph2.getTxnsWithoutDependency();
        assertEquals(txnIds.size(), 1);
        batchTxnIds = graph2.getTxnsWithTxnDependencyBatch(1, 5, 2);
        assertEquals(batchTxnIds.size(), 2);
        graph.remove(2);
        graph.remove(3);

        txnIds = graph2.getTxnsWithoutDependency();
        assertEquals(txnIds.size(), 1);
        batchTxnIds = graph2.getTxnsWithTxnDependencyBatch(1, 5, 4);
        assertEquals(batchTxnIds.size(), 1);
    }

    @Test
    public void testGetTxnsWithTxnDependencyBatchMultiTable() {
        // CDC-like chain: every txn writes the same multi-table set
        TransactionGraph graph = new TransactionGraph();
        for (int i = 1; i <= 6; i++) {
            graph.add(i, Lists.newArrayList(1L, 2L, 3L));
        }
        List<Long> batch = graph.getTxnsWithTxnDependencyBatchMultiTable(1, 10, 1);
        assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L), batch);
        // maxBatchSize caps the batch
        batch = graph.getTxnsWithTxnDependencyBatchMultiTable(1, 4, 1);
        assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L), batch);

        // diamond: txn1 {1,2} -> txn2 {1} / txn3 {2} -> txn4 {1,2}.
        // txn2 and txn3 are independent of each other, so only one of them may chain onto
        // txn1; the other is left out to publish in parallel after the batch finishes.
        TransactionGraph graph2 = new TransactionGraph();
        graph2.add(1, Lists.newArrayList(1L, 2L));
        graph2.add(2, Lists.newArrayList(1L));
        graph2.add(3, Lists.newArrayList(2L));
        graph2.add(4, Lists.newArrayList(1L, 2L));
        batch = graph2.getTxnsWithTxnDependencyBatchMultiTable(1, 10, 1);
        assertEquals(Lists.newArrayList(1L, 2L), batch);

        // a txn introducing a new table still joins when it extends the chain; a txn
        // independent of the chain tail does not, even though its dependencies are in
        // the batch
        TransactionGraph graph3 = new TransactionGraph();
        graph3.add(1, Lists.newArrayList(1L, 2L));
        graph3.add(2, Lists.newArrayList(1L, 3L)); // table 3 is new, depends on txn1 only
        graph3.add(3, Lists.newArrayList(2L));     // depends on txn1 only, independent of txn2
        graph3.add(4, Lists.newArrayList(1L));     // depends on txn2, extends the chain
        batch = graph3.getTxnsWithTxnDependencyBatchMultiTable(1, 10, 1);
        assertEquals(Lists.newArrayList(1L, 2L, 4L), batch);

        // single-table head: a multi-table successor joins as well
        TransactionGraph graph4 = new TransactionGraph();
        graph4.add(1, Lists.newArrayList(1L));
        graph4.add(2, Lists.newArrayList(1L));
        graph4.add(3, Lists.newArrayList(1L, 2L));
        batch = graph4.getTxnsWithTxnDependencyBatchMultiTable(1, 10, 1);
        assertEquals(Lists.newArrayList(1L, 2L, 3L), batch);

        // minBatchSize not reached -> empty result
        batch = graph4.getTxnsWithTxnDependencyBatchMultiTable(4, 10, 1);
        assertEquals(0, batch.size());

        // a txn depending on another dependency-free head outside the batch must not join:
        // publishing it in this batch would jump over that txn
        TransactionGraph graph5 = new TransactionGraph();
        graph5.add(1, Lists.newArrayList(9L));     // independent head on table 9
        graph5.add(2, Lists.newArrayList(1L, 2L)); // head of the walk
        graph5.add(3, Lists.newArrayList(2L, 9L)); // depends on txn2 AND txn1 (outside)
        batch = graph5.getTxnsWithTxnDependencyBatchMultiTable(1, 10, 2);
        assertEquals(Lists.newArrayList(2L), batch);
        // walking from txn1 must not pull txn3 either (txn2 outside that batch)
        batch = graph5.getTxnsWithTxnDependencyBatchMultiTable(1, 10, 1);
        assertEquals(Lists.newArrayList(1L), batch);
    }

    @Test
    public void testPrintGraph() {
        TransactionGraph graph = new TransactionGraph();
        graph.add(1, Lists.newArrayList(1L));
        graph.add(2, Lists.newArrayList(2L));
        graph.add(3, Lists.newArrayList(3L));
        graph.add(4, Lists.newArrayList(1L));
        graph.add(5, Lists.newArrayList(2L));
        graph.add(6, Lists.newArrayList(3L));
        graph.add(7, Lists.newArrayList(1L));
        graph.add(8, Lists.newArrayList(4L));

        String graphPrint = "1->4->7\n" +
                "2->5\n" +
                "3->6\n" +
                "8\n";
        assertEquals(graphPrint, graph.debug());
    }
}
