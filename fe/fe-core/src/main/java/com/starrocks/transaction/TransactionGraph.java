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
import java.util.Comparator;
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
 * <p>
 * Mutual exclusion is not the only thing this class needs from its callers: add() also carries an
 * ordering contract that the mgr lock alone does not provide. See add().
 */
public class TransactionGraph {
    private static final Logger LOG = LogManager.getLogger(TransactionGraph.class);

    static class Node {
        long txnId;
        // position in add order. Each table's writer chain is ordered by seq, not by txnId: txn
        // ids are assigned at begin time but nodes are added at commit time, and txns may commit
        // out of txn id order. See add() for why add order is the order that matters here, and
        // what the caller must do to keep it aligned with partition version order.
        long seq;
        List<Long> writeTableIds;
        // transactions this txn depends
        Set<Node> ins;
        // transactions depending on this txn
        Set<Node> outs;

        Node(long txnId, long seq, List<Long> writeTableIds) {
            this.txnId = txnId;
            this.seq = seq;
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

    private long nextSeq = 0;

    public TransactionGraph() {
    }

    public int size() {
        return nodes.size();
    }

    // Ordering contract: for every table, add() must be called in the order in which the txns'
    // partition versions were allocated. Each table's writer chain, and the seq that identifies
    // positions on it, are defined by the order of these calls -- so if the calls arrive out of
    // version order the chain is built wrong and seq faithfully records the wrong order. Nothing
    // here can detect that.
    //
    // The mgr lock does NOT give this. Version allocation (unprotectedCommitPreparedTransaction)
    // and this add() run in two SEPARATE mgr write-lock windows with an edit-log write between
    // them, and EditLog.logEditGated runs the WAL apply action outside any lock, so two commits
    // can reach add() out of allocation order.
    //
    // What gives it is a WRITE lock on every table the txn writes, held for the whole commit so it
    // spans both windows. Two writers of the same table are therefore serialized end to end, so
    // per table:
    //     version allocation order == add order == seq order
    // Txns that share no table may reorder freely; chains are per table, so that is harmless.
    // Journal replay is single threaded (GlobalStateMgr "replayer" daemon) and replays a given
    // table's txns in that same order, so a follower rebuilds an equivalent chain.
    //
    // That lock is taken by GlobalTransactionMgr.commitTransactionUnderDatabaseWLock and
    // commitPreparedTransactionUnderIntensiveDbLock -- but NOT by the public
    // GlobalTransactionMgr.commitTransaction(dbId, ...), which delegates locking to its callers.
    // Every such caller locks for itself today (CompactionScheduler, BrokerLoadJob, SparkLoadJob,
    // ReplicationJob); a new one that forgets would corrupt the chains here with no failure at the
    // call site. Narrowing the lock scope on any commit path breaks this the same silent way.
    // Keep the lock scope, or give this class its own ordering key.
    public void add(long txnId, List<Long> writeTableIds) {
        if (nodes.containsKey(txnId)) {
            LOG.warn("add an already exist txn:{}", txnId);
            return;
        }
        Node node = new Node(txnId, nextSeq++, writeTableIds);
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
            // Happens when publish readiness is not decided by this graph, e.g. single
            // (partition-version based) publish finishing a txn before its predecessors,
            // or during FE upgrading.
            LOG.warn("remove txn {} with dependency: {}", txnId, node.ins);
        }
        nodes.remove(txnId);
        nodesWithoutIns.remove(node);

        // Removing a node in the middle of a table's writer chain must not lose ordering:
        // splice the chain by linking the immediate predecessor to the immediate successor
        // of every table this node writes, and let lastTableWriter fall back to the
        // predecessor so a later add() of the same table still picks up the dependency.
        // Only same-table edges are added; cross-table shortcuts would break the walk in
        // getTxnsWithTxnDependencyBatch(), which assumes a single-table node has at most
        // one out edge, pointing to the next writer of that table.
        for (long tableId : node.writeTableIds) {
            Node prev = latestWriterAmong(node.ins, tableId);
            Node next = earliestWriterAmong(node.outs, tableId);
            if (prev != null && next != null) {
                prev.addOuts(next);
                next.addIns(prev);
            }
            if (lastTableWriter.get(tableId) == node) {
                if (prev != null) {
                    lastTableWriter.put(tableId, prev);
                } else {
                    lastTableWriter.remove(tableId);
                }
            }
        }

        if (node.ins != null) {
            for (Node in : node.ins) {
                in.outs.remove(node);
            }
        }
        if (node.outs != null) {
            for (Node out : node.outs) {
                out.ins.remove(node);
                if (out.ins.isEmpty()) {
                    nodesWithoutIns.add(out);
                }
            }
        }
    }

    // Among candidates, the writer of tableId added to the graph last. When called with the
    // ins of a node writing tableId, this is that node's immediate predecessor on tableId's
    // writer chain: every writer of tableId in the ins lies on the chain before the node,
    // and the chain is ordered by seq.
    private static Node latestWriterAmong(Set<Node> candidates, long tableId) {
        Node result = null;
        if (candidates != null) {
            for (Node n : candidates) {
                if (n.writeTableIds.contains(tableId) && (result == null || n.seq > result.seq)) {
                    result = n;
                }
            }
        }
        return result;
    }

    // Symmetric to latestWriterAmong: with the outs of a node writing tableId, the writer of
    // tableId added first is that node's immediate successor on tableId's writer chain.
    private static Node earliestWriterAmong(Set<Node> candidates, long tableId) {
        Node result = null;
        if (candidates != null) {
            for (Node n : candidates) {
                if (n.writeTableIds.contains(tableId) && (result == null || n.seq < result.seq)) {
                    result = n;
                }
            }
        }
        return result;
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

    /**
     * Batch selection that also allows multi-table transactions. The batch is a dependency
     * chain: starting from a dependency-free head transaction, it is extended with a
     * successor of the current tail whose dependencies are all inside the batch. Because a
     * successor of the tail depends on the tail directly, every transaction in the batch
     * transitively depends on all transactions before it -- it would have to wait for them
     * even without batching, so grouping them costs nothing. Transactions that are
     * independent of the chain tail (parallel branches hanging off an earlier transaction)
     * are never batched: batch visibility is all-or-nothing, and batching them would couple
     * transactions that could otherwise publish in parallel.
     *
     * The returned list is in chain (hence topological) order, so any prefix cut downstream
     * stays dependency-closed and per-partition commit versions appear in consecutive order.
     *
     * The size of ins of node with txnId must be zero.
     */
    public List<Long> getTxnsWithTxnDependencyBatchMultiTable(int minBatchSize, int maxBatchSize, long txnId) {
        List<Long> txns = new ArrayList<>();
        Node head = nodes.get(txnId);
        if (head == null) {
            return txns;
        }
        Set<Node> inBatch = new HashSet<>();
        txns.add(head.txnId);
        inBatch.add(head);
        Node current = head;
        while (txns.size() < maxBatchSize && current.outs != null) {
            Node nextInChain = null;
            List<Node> successors = current.outs.stream()
                    .sorted(Comparator.comparingLong(n -> n.txnId))
                    .collect(Collectors.toList());
            for (Node next : successors) {
                // A successor of the tail depends on the tail, and joins only when it has
                // no dependency outside the batch: publishing it here would otherwise jump
                // over a txn that has to become visible first.
                if (next.ins == null || inBatch.containsAll(next.ins)) {
                    nextInChain = next;
                    break;
                }
            }
            if (nextInChain == null) {
                break;
            }
            txns.add(nextInChain.txnId);
            inBatch.add(nextInChain);
            current = nextInChain;
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
