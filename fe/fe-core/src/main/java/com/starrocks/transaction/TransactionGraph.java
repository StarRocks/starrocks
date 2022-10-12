// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.transaction;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        Preconditions.checkState(node.ins == null || node.ins.isEmpty(), "only can remove node with no dependency");
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
}
