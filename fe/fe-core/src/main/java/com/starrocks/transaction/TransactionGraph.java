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
 * this class is used in DatabaseTransactionMgr and all methods are protected by mgr's lock
 * so this class does not require additional synchronization
 */
public class TransactionGraph {
    private static final Logger LOG = LogManager.getLogger(TransactionGraph.class);

    static class Node {
        long txnId;
        List<Long> writeTableIds;
        // transactions this txn depends
        Set<Node> ins;
        // transactions depending on this txn
        Set<Node> outs;

        Node(long txnId, List<Long> writeTableIds) {
            this.txnId = txnId;
            this.writeTableIds = writeTableIds;
        }

        void addIns(Node in) {
            if (ins == null) {
                ins = new HashSet<>();
            }
            ins.add(in);
        }

        void addOuts(Node out) {
            if (outs == null) {
                outs = new HashSet<>();
            }
            outs.add(out);
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

    // tableid -> txnId that lastly write this table
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
            Node previous = lastTableWriter.put(tableId, node);
            if (previous != null) {
                Preconditions.checkState(previous != node, "duplicate node {}", txnId);
                node.addIns(previous);
                previous.addOuts(node);
            }
        }
        nodes.put(txnId, node);
        if (node.ins == null || node.ins.isEmpty()) {
            nodesWithoutIns.add(node);
        }
    }

    public void remove(long txnId) {
        Node node = nodes.get(txnId);
        if (node == null) {
            return;
        }
        if (node.ins != null && !node.ins.isEmpty()) {
            LOG.warn("remove txn " + txnId + " with dependency: " + node.ins + " this may happen during FE upgrading");
            for (Node dep : node.ins) {
                dep.outs.remove(node);
            }
        }
        nodes.remove(txnId);
        nodesWithoutIns.remove(node);
        for (long tableId : node.writeTableIds) {
            Node holder = lastTableWriter.get(tableId);
            if (holder == node) {
                lastTableWriter.remove(tableId);
            }
        }
        if (node.outs == null) {
            return;
        }
        for (Node next : node.outs) {
            next.ins.remove(node);
            if (next.ins.isEmpty()) {
                nodesWithoutIns.add(next);
            }
        }
    }

    public List<Long> getTxnsWithoutDependency() {
        return nodesWithoutIns.stream().map(n -> n.txnId).collect(Collectors.toList());
    }

    // The size of ins of node with txnId must be zero
    public List<Long> getTxnsWithTxnDependencyBatch(int minBatchSize, int maxBatchSize, long txnId) {
        List<Long> txns = new ArrayList<>();
        if (nodes.containsKey(txnId)) {
            Node node = nodes.get(txnId);
            if (node.writeTableIds.size() > 1) {
                txns.add(txnId);
                return txns;
            }
            int count = 0;
            // can not judge by ins.size()
            // for the ins.size of the txn with multi table can be one
            while (count < maxBatchSize && node != null && (node.writeTableIds.size() == 1)) {
                count++;
                txns.add(node.txnId);

                // the node which size of write table is one, their size of outs can not be greater than two
                if (node.outs != null) {
                    node = node.outs.stream().findAny().orElse(null);
                } else {
                    node = null;
                }
            }
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
        if (node.outs == null) {
            print(path, builder);
            return;
        }

        for (Node out : node.outs) {
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
