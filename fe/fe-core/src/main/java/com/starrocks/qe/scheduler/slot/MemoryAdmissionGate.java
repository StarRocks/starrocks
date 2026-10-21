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

package com.starrocks.qe.scheduler.slot;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The memory admission gate of Query Queue V2: the global variable query_queue_mem_used_pct_limit, which Query Queue
 * V1 applies to the whole cluster, applied to the nodes of one warehouse.
 *
 * <p>The signal is the process memory usage of each alive node, which BE and CN nodes already report with their
 * resource usage, divided by the memory limit of the node from its heartbeat. The process memory also holds what
 * queries do not own, such as loads, compaction, metadata and the page cache, and a query fails as well when the
 * process runs out of memory, so the gate closes earlier than one on query memory alone and keeps a buffer against
 * both limits. Nothing here reads resource groups.
 *
 * <p>The decision is binary: new queries wait while any alive node with a fresh report uses at least the limit, and
 * run again once every such node is below it. Running queries are never touched. A node whose report is stale does not
 * take part, as in Query Queue V1, so that a node that stopped reporting cannot hold the gate closed.
 */
public final class MemoryAdmissionGate {
    private static final Logger LOG = LogManager.getLogger(MemoryAdmissionGate.class);

    private static final int MAX_WARNED_UNBOUNDED = 1024;
    private static final Set<String> WARNED_UNBOUNDED = ConcurrentHashMap.newKeySet();

    public enum State {
        /**
         * No memory usage limit is set for the warehouse.
         */
        DISABLED,
        ALLOW,
        BLOCK
    }

    /**
     * One evaluation of the gate. Immutable.
     */
    public static final class Status {
        private static final Status DISABLED_STATUS = new Status(0, 0, -1, 0, 0, 0, 0, State.DISABLED);

        private final double limit;
        private final double maxMemUsedPct;
        private final long hottestNodeId;
        private final long hottestMemUsedBytes;
        private final long hottestMemLimitBytes;
        private final int numNodes;
        private final int numStaleNodes;
        private final State state;

        private Status(double limit, double maxMemUsedPct, long hottestNodeId, long hottestMemUsedBytes,
                       long hottestMemLimitBytes, int numNodes, int numStaleNodes, State state) {
            this.limit = limit;
            this.maxMemUsedPct = maxMemUsedPct;
            this.hottestNodeId = hottestNodeId;
            this.hottestMemUsedBytes = hottestMemUsedBytes;
            this.hottestMemLimitBytes = hottestMemLimitBytes;
            this.numNodes = numNodes;
            this.numStaleNodes = numStaleNodes;
            this.state = state;
        }

        public static Status disabled() {
            return DISABLED_STATUS;
        }

        /**
         * The memory usage limit, a fraction of the memory limit of a node; 0 when the gate is disabled.
         */
        public double getLimit() {
            return limit;
        }

        /**
         * The highest process memory usage, as a fraction of the node's memory limit, among the alive nodes with a
         * fresh report; 0 when there is none.
         */
        public double getMaxMemUsedPct() {
            return maxMemUsedPct;
        }

        /**
         * The node with the highest memory usage, or -1 when no alive node has a fresh report.
         */
        public long getHottestNodeId() {
            return hottestNodeId;
        }

        public long getHottestMemUsedBytes() {
            return hottestMemUsedBytes;
        }

        public long getHottestMemLimitBytes() {
            return hottestMemLimitBytes;
        }

        /**
         * The alive nodes of the warehouse, whatever their report.
         */
        public int getNumNodes() {
            return numNodes;
        }

        /**
         * The alive nodes whose report is older than the resource usage freshness window, left out of the decision.
         */
        public int getNumStaleNodes() {
            return numStaleNodes;
        }

        public State getState() {
            return state;
        }

        public boolean isEnabled() {
            return state != State.DISABLED;
        }

        public boolean isBlocked() {
            return state == State.BLOCK;
        }

        @Override
        public String toString() {
            if (state == State.DISABLED) {
                return "MemoryGate{DISABLED}";
            }
            return "MemoryGate{state=" + state + ", limit=" + limit + ", maxMemUsedPct=" + maxMemUsedPct +
                    ", hottestNodeId=" + hottestNodeId + ", hottestMemUsedBytes=" + hottestMemUsedBytes +
                    ", hottestMemLimitBytes=" + hottestMemLimitBytes + ", nodes=" + numNodes +
                    ", staleNodes=" + numStaleNodes + "}";
        }
    }

    private MemoryAdmissionGate() {
    }

    /**
     * Evaluate the gate of a warehouse from the latest reports this FE holds. Reports reach only the leader FE,
     * where admission happens; on a follower no node has a fresh report, so the gate never blocks there.
     */
    public static Status evaluate(long warehouseId, BaseSlotManager slotManager) {
        double limit = slotManager.getQueryQueueMemUsedPctLimit(warehouseId);
        // The negated check also treats NaN as unset.
        if (!(limit > 0)) {
            return Status.disabled();
        }
        warnIfUnbounded(warehouseId, limit);
        return evaluate(limit, getWarehouseNodes(warehouseId));
    }

    /**
     * The highest process memory usage, as a fraction of the node's memory limit, among the alive nodes of the
     * warehouse with a fresh report, whether or not a limit is set; 0 when there is none.
     */
    public static double getMaxNodeMemUsedPct(long warehouseId) {
        // A limit no usage reaches, so that only the usage is computed.
        return evaluate(Double.MAX_VALUE, getWarehouseNodes(warehouseId)).getMaxMemUsedPct();
    }

    /**
     * Admission reserves nothing, so the queries admitted in one batch are bounded only by the memory limit of one
     * query on each node, the session variable query_mem_limit. Warn once per warehouse and limit when its global
     * default is not set, though a session can still set its own.
     */
    private static void warnIfUnbounded(long warehouseId, double limit) {
        if (getGlobalQueryMemLimit() > 0) {
            return;
        }
        String key = warehouseId + ":" + limit;
        if (WARNED_UNBOUNDED.size() < MAX_WARNED_UNBOUNDED && WARNED_UNBOUNDED.add(key)) {
            LOG.warn("[Slot] query_queue_mem_used_pct_limit of warehouse {} is {} but the global default of " +
                    "query_mem_limit is not set, so the queries admitted in one batch can overshoot the limit " +
                    "without a per-query bound", warehouseId, limit);
        }
    }

    private static long getGlobalQueryMemLimit() {
        VariableMgr variableMgr = GlobalStateMgr.getCurrentState().getVariableMgr();
        return variableMgr == null ? 0 : variableMgr.getDefaultSessionVariable().getQueryMemLimit();
    }

    @VisibleForTesting
    static Status evaluate(double limit, Collection<? extends ComputeNode> nodes) {
        double maxMemUsedPct = 0;
        long hottestNodeId = -1;
        long hottestMemUsedBytes = 0;
        long hottestMemLimitBytes = 0;
        int numNodes = 0;
        int numStaleNodes = 0;
        for (ComputeNode node : nodes) {
            if (node == null || !node.isAlive()) {
                continue;
            }
            numNodes++;
            if (!node.isResourceUsageFresh()) {
                numStaleNodes++;
                continue;
            }
            double memUsedPct = node.getMemUsedPct();
            if (hottestNodeId < 0 || memUsedPct > maxMemUsedPct) {
                maxMemUsedPct = memUsedPct;
                hottestNodeId = node.getId();
                hottestMemUsedBytes = node.getMemUsedBytes();
                hottestMemLimitBytes = node.getMemLimitBytes();
            }
        }
        // The same comparison as ComputeNode#isResourceOverloaded, which applies the limit under Query Queue V1.
        State state = hottestNodeId >= 0 && maxMemUsedPct >= limit ? State.BLOCK : State.ALLOW;
        return new Status(limit, maxMemUsedPct, hottestNodeId, hottestMemUsedBytes, hottestMemLimitBytes, numNodes,
                numStaleNodes, state);
    }

    /**
     * The nodes a query admitted into the warehouse queue can run on: every backend and compute node in
     * shared-nothing mode, the compute nodes of the warehouse in shared-data mode.
     */
    @VisibleForTesting
    static List<ComputeNode> getWarehouseNodes(long warehouseId) {
        SystemInfoService systemInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        if (RunMode.isSharedNothingMode()) {
            return systemInfo.backendAndComputeNodeStream().collect(Collectors.toList());
        }
        try {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            return warehouseManager.getAllComputeNodeIds(warehouseId).stream()
                    .map(systemInfo::getBackendOrComputeNode)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            // A warehouse that cannot be resolved has no node to hold memory for it. A suspended warehouse is a normal
            // state, and the metric of a warehouse with the query queue enabled is still collected then, so it is not
            // worth a warning.
            if (!(e instanceof ErrorReportException report && report.getErrorCode() == ErrorCode.ERR_WAREHOUSE_SUSPENDED)) {
                LOG.warn("[Slot] failed to resolve the nodes of warehouse {} for the memory gate", warehouseId, e);
            }
            return List.of();
        }
    }
}
