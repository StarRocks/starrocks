// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

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
}
