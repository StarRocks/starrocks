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

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * store transactions' dependency relationships
 *
 * The dependency graph is modeled as the overlay of one doubly-linked writer
 * chain per table: for every table T a node writes, the node stores its
 * immediate predecessor (tablePrev[T]) and successor (tableNext[T]) on T's
 * chain. Keeping the per-table chain explicit makes add/remove deterministic
 * linked-list operations and avoids reverse-engineering chain membership from
 * an unlabeled dependency set.
 *
 * this class is used in DatabaseTransactionMgr and all methods are protected by mgr's lock
 * so this class does not require additional synchronization
 */
public class TransactionGraph {
    private static final Logger LOG = LogManager.getLogger(TransactionGraph.class);

    static class Node {
        final long txnId;
        final List<Long> writeTableIds;

        // For every table this node writes, the immediate predecessor on that
        // table's chain. An absent key means this node is the head of that
        // table's chain (no predecessor for that table).
        final Map<Long, Node> tablePrev;
        // Symmetric: the immediate successor on each table's chain. An absent
        // key means this node is the tail of that table's chain.
        final Map<Long, Node> tableNext;

        Node(long txnId, List<Long> writeTableIds) {
            this.txnId = txnId;
            this.writeTableIds = writeTableIds;
            this.tablePrev = new HashMap<>(writeTableIds.size());
            this.tableNext = new HashMap<>(writeTableIds.size());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Node node = (Node) o;
            return txnId == node.txnId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(txnId);
        }

        @Override
        public String toString() {
            return Long.toString(txnId);
        }
    }

    private Map<Long, Node> nodes = new HashMap<>();
    private Set<Node> nodesWithoutIns = new HashSet<>();

    // tableId -> tail node of that table's writer chain
    private Map<Long, Node> lastTableWriter = new HashMap<>();

    public TransactionGraph() {
    }

    public int size() {
        return nodes.size();
    }

    public void add(long txnId, List<Long> writeTableIds) {
        if (nodes.containsKey(txnId)) {
            LOG.warn("add an already exist txn:{}", txnId);
            return;
        }
        Node node = new Node(txnId, writeTableIds);
        for (long tableId : writeTableIds) {
            Node prev = lastTableWriter.put(tableId, node);
            if (prev != null) {
                Preconditions.checkState(prev != node, "duplicate node {}", txnId);
                node.tablePrev.put(tableId, prev);
                prev.tableNext.put(tableId, node);
            }
        }
        nodes.put(txnId, node);
        if (node.tablePrev.isEmpty()) {
            nodesWithoutIns.add(node);
        }
    }

    public void remove(long txnId) {
        Node node = nodes.get(txnId);
        if (node == null) {
            return;
        }
        if (!node.tablePrev.isEmpty()) {
            LOG.warn("remove txn " + txnId + " with dependency: " + node.tablePrev.values()
                    + " this may happen during FE upgrading");
        }
        nodes.remove(txnId);
        nodesWithoutIns.remove(node);

        // Splice node out of every table's chain it participated in. Per-table
        // linked-list maintenance keeps predecessor/successor relationships
        // exact and never creates cross-table edges.
        for (long tableId : node.writeTableIds) {
            Node prev = node.tablePrev.get(tableId);   // null => node is head of T
            Node next = node.tableNext.get(tableId);   // null => node is tail of T

            if (prev != null) {
                if (next != null) {
                    prev.tableNext.put(tableId, next);
                } else {
                    prev.tableNext.remove(tableId);
                }
            }
            if (next != null) {
                if (prev != null) {
                    next.tablePrev.put(tableId, prev);
                } else {
                    next.tablePrev.remove(tableId);
                    if (next.tablePrev.isEmpty()) {
                        nodesWithoutIns.add(next);
                    }
                }
            }

            // node was the tail of T iff next == null. In that case the new
            // tail is prev (possibly null, meaning T has no writer left).
            if (next == null) {
                if (prev != null) {
                    lastTableWriter.put(tableId, prev);
                } else {
                    lastTableWriter.remove(tableId);
                }
            }
        }
    }

    public List<Long> getTxnsWithoutDependency() {
        return nodesWithoutIns.stream().map(n -> n.txnId).collect(Collectors.toList());
    }

    // The size of ins of node with txnId must be zero
    public List<Long> getTxnsWithTxnDependencyBatch(int minBatchSize, int maxBatchSize, long txnId) {
        List<Long> txns = new ArrayList<>();
        Node node = nodes.get(txnId);
        if (node == null) {
            return txns;
        }
        if (node.writeTableIds.size() > 1) {
            txns.add(txnId);
            return txns;
        }
        // Walk exactly the writer chain of the single table this node writes.
        long tableId = node.writeTableIds.get(0);
        int count = 0;
        while (count < maxBatchSize && node != null && node.writeTableIds.size() == 1) {
            count++;
            txns.add(node.txnId);
            node = node.tableNext.get(tableId);
        }
        return txns.size() >= minBatchSize ? txns : new ArrayList<>();
    }

    // print the graph for debug
    public String debug() {
        StringBuilder builder = new StringBuilder();
        for (Node node : nodesWithoutIns) {
            List<Long> path = new ArrayList<>();
            travelGraph(node, path, builder);
        }
        return builder.toString();
    }

    // depth-first search
    public void travelGraph(Node node, List<Long> path, StringBuilder builder) {
        if (node == null) {
            return;
        }
        path.add(node.txnId);
        if (node.tableNext.isEmpty()) {
            print(path, builder);
            return;
        }
        // Distinct successor nodes across all tables this node writes.
        Set<Node> outs = new HashSet<>(node.tableNext.values());
        for (Node out : outs) {
            travelGraph(out, path, builder);
            path.remove(path.size() - 1);
        }
    }

    public void print(List<Long> path, StringBuilder builder) {
        for (int i = 0; i < path.size(); i++) {
            builder.append(path.get(i));
            if (i != path.size() - 1) {
                builder.append("->");
            }
        }
        builder.append("\n");
    }
}
