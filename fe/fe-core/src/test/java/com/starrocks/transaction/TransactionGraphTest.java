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
        // After removing txn3 (mid node of txn1->txn3->txn5) and txn4 (mid node of txn2->txn4->txn6),
        // txn5 is re-linked to depend on txn1, txn6 is re-linked to depend on txn2.
        // So only txn1 and txn2 are roots.
        expectNextBatch(graph, Lists.newArrayList(1L, 2L));
        assertEquals(graph.size(), 2);
        expectNextBatch(graph, Lists.newArrayList(5L, 6L));
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
                    writeTableIds.add(tableId);
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
        graph2.remove(2);
        graph2.remove(3);

        txnIds = graph2.getTxnsWithoutDependency();
        assertEquals(txnIds.size(), 1);
        batchTxnIds = graph2.getTxnsWithTxnDependencyBatch(1, 5, 4);
        assertEquals(batchTxnIds.size(), 1);
    }

    @Test
    public void testRemoveTailBreaksLastTableWriter() {
        // Verify that removing the lastTableWriter (tail node) for a table
        // causes subsequent adds to lose dependency on remaining nodes.
        //
        // Setup chain: txn1 -> txn2 -> txn3, all write table1
        // lastTableWriter[table1] = txn3
        TransactionGraph graph = new TransactionGraph();
        graph.add(1, Lists.newArrayList(1L));
        graph.add(2, Lists.newArrayList(1L));
        graph.add(3, Lists.newArrayList(1L));

        // Only txn1 should be a root
        List<Long> roots = graph.getTxnsWithoutDependency();
        Collections.sort(roots);
        assertEquals(Lists.newArrayList(1L), roots);

        // Remove txn3 (the tail / lastTableWriter for table1)
        // txn1 and txn2 are still in the graph
        graph.remove(3);
        assertEquals(2, graph.size());

        // Now add txn4 writing table1
        // Expected: txn4 should depend on txn2 (the last remaining writer for table1)
        // Actual (BUG): lastTableWriter[table1] was cleared when txn3 was removed,
        //               so txn4 has NO dependency
        graph.add(4, Lists.newArrayList(1L));

        roots = graph.getTxnsWithoutDependency();
        Collections.sort(roots);

        // If correct: roots should be {1} only (txn4 depends on txn2, which depends on txn1)
        // If buggy:   roots will be {1, 4} because txn4 has no dependency
        assertEquals(Lists.newArrayList(1L), roots,
                "txn4 should depend on txn2 since both write table1, " +
                        "but lastTableWriter was incorrectly cleared when txn3 was removed");
    }

    @Test
    public void testRemoveMiddleNodeBreaksDependencyChain() {
        // Verify that removing a middle node doesn't re-link predecessors to successors.
        //
        // Setup chain: txn1 -> txn2 -> txn3, all write table1
        TransactionGraph graph = new TransactionGraph();
        graph.add(1, Lists.newArrayList(1L));
        graph.add(2, Lists.newArrayList(1L));
        graph.add(3, Lists.newArrayList(1L));

        // Remove txn2 (middle node)
        graph.remove(2);
        assertEquals(2, graph.size());

        List<Long> roots = graph.getTxnsWithoutDependency();
        Collections.sort(roots);

        // If correct: roots should be {1} only (txn3 should still depend on txn1
        //             since they both write table1 and txn1 is not yet published)
        // If buggy:   roots will be {1, 3} because the edge txn1->txn3 was never created
        assertEquals(Lists.newArrayList(1L), roots,
                "txn3 should still depend on txn1 after removing txn2, " +
                        "since both write table1 and txn1 is still in the graph");
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
