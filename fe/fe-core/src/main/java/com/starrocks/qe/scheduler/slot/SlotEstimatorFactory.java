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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.sql.optimizer.cost.feature.CostPredictor;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.commons.lang3.EnumUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class SlotEstimatorFactory {
    private static final Logger LOG = LogManager.getLogger(SlotEstimatorFactory.class);

    public enum EstimatorPolicy {
        PBE, // parallelism-based slots estimator
        MBE, // memory-cost based slots estimator
        CBE; // CPU-cost based slots estimator

        public static EstimatorPolicy createDefault() {
            return PBE;
        }

        public static EstimatorPolicy create(String value) {
            if ("MAX".equalsIgnoreCase(value) || "MIN".equalsIgnoreCase(value)) {
                return createDefault();
            }
            return EnumUtils.getEnumIgnoreCase(EstimatorPolicy.class, value);
        }

        public SlotEstimator createEstimator() {
            switch (this) {
                case PBE:
                    return new ParallelismBasedSlotsEstimator();
                case MBE:
                    return new MemoryBasedSlotsEstimator();
                case CBE:
                    return new CpuCostBasedSlotsEstimator();
                default:
                    throw new IllegalArgumentException("Unknown EstimatorPolicy: " + this);
            }
        }
    }

    public static SlotEstimator create(QueryQueueOptions opts) {
        if (!opts.isEnableQueryQueueV2()) {
            return new DefaultSlotEstimator();
        }

        EstimatorPolicy policy = getEstimatorPolicy();
        return policy.createEstimator();
    }

    /**
     * Estimate slots for EXPLAIN output when we only have ExecPlan (not DefaultCoordinator).
     * PBE uses fragments from ExecPlan; MBE and CBE use plan costs recorded in the audit event.
     */
    public static int estimateSlotsForExplain(QueryQueueOptions opts, ConnectContext context, ExecPlan execPlan) {
        if (!opts.isEnableQueryQueueV2()) {
            return 1;
        }

        EstimatorPolicy policy = getEstimatorPolicy();
        return estimateSlotsFromExecPlan(opts, context, execPlan, policy);
    }

    static EstimatorPolicy getEstimatorPolicy() {
        EstimatorPolicy policy = EstimatorPolicy.create(Config.query_queue_slots_estimator_strategy);
        if (policy == null) {
            LOG.warn("unknown query_queue_slots_estimator_strategy: {}, fallback to default policy",
                    Config.query_queue_slots_estimator_strategy);
            policy = EstimatorPolicy.createDefault();
        }
        return policy;
    }

    private static int estimateSlotsFromExecPlan(QueryQueueOptions opts, ConnectContext context, ExecPlan execPlan,
                                                  EstimatorPolicy policy) {
        switch (policy) {
            case PBE:
                return estimateParallelismBasedSlotsFromExecPlan(opts, execPlan);
            case MBE:
                return estimateMemoryBasedSlotsFromExecPlan(opts, context, execPlan);
            case CBE:
                return estimateCpuCostBasedSlots(opts, context, execPlan);
            default:
                return estimateParallelismBasedSlotsFromExecPlan(opts, execPlan);
        }
    }

    private static int estimateParallelismBasedSlotsFromExecPlan(QueryQueueOptions opts, ExecPlan execPlan) {
        if (execPlan == null || execPlan.getFragments() == null || execPlan.getFragments().isEmpty()) {
            return 1;
        }
        return estimateParallelismBasedSlotsFromRootFragment(opts, execPlan.getFragments().get(0));
    }

    private static int estimateParallelismBasedSlotsFromRootFragment(QueryQueueOptions opts, PlanFragment rootFragment) {
        if (rootFragment == null || rootFragment.getPlanRoot() == null) {
            return 1;
        }

        Map<PlanFragmentId, FragmentContext> contexts = Maps.newHashMap();
        collectFragmentSourceNodes(rootFragment.getPlanRoot(), contexts);
        int workUnits = contexts.values().stream()
                .flatMap(fragmentContext -> fragmentContext.sourceNodes.stream())
                .mapToInt(sourceNode -> estimateScanWorkUnits(opts, sourceNode))
                .max()
                .orElse(1);
        return normalizeParallelismBasedSlots(opts, workUnits);
    }

    private static int estimateScanWorkUnits(QueryQueueOptions opts, PlanNode sourceNode) {
        if (sourceNode instanceof OlapScanNode) {
            List<TScanRangeLocations> locations = ((OlapScanNode) sourceNode).getScanRangeLocations(0);
            if (locations != null && !locations.isEmpty()) {
                return locations.size();
            }
            return 1;
        }
        // PBE targets a parallelism of number_of_workers; only a very small query (pruned down to a few scan
        // ranges) drops below the worker count. A connector/external scan does not expose its split/file count
        // at estimation time (that listing is deferred to scheduling), so it is treated as a full-parallelism
        // scan (number_of_workers work units) rather than collapsing to a single slot. This intentionally does
        // not use the deprecated query_queue_v2_num_rows_per_slot or the (relative) optimizer cardinality.
        if (sourceNode instanceof ScanNode && ((ScanNode) sourceNode).isConnectorScanNode()) {
            return opts.v2().getNumWorkers();
        }
        // Other non-OLAP source nodes (e.g. exchange nodes) do not carry scan work; the leaf scans they are fed
        // from are collected separately, so they contribute 1.
        return 1;
    }

    private static int normalizeParallelismBasedSlots(QueryQueueOptions opts, int workUnits) {
        int numSlots = Math.min(opts.v2().getNumWorkers(), Math.max(1, workUnits));
        return Math.max(1, Math.min(numSlots, opts.v2().getTotalSlots()));
    }

    private static int normalizeCostBasedSlots(QueryQueueOptions opts, double cost, long costPerSlot,
                                               int maxSlotsByPipelineDop) {
        int maxSlots = Math.max(1, Math.min(opts.v2().getTotalSlots(), maxSlotsByPipelineDop));
        if (Double.isNaN(cost) || cost <= 0 || costPerSlot <= 0) {
            return 1;
        }
        if (Double.isInfinite(cost)) {
            return maxSlots;
        }
        double numSlots = Math.ceil(cost / costPerSlot);
        if (numSlots >= maxSlots) {
            return maxSlots;
        }
        return Math.max(1, (int) numSlots);
    }

    private static int normalizeCostBasedMaxSlots(QueryQueueOptions opts, int pipelineDop) {
        int perWorkerMaxSlots = Math.max(1, pipelineDop / 2);
        double rawMaxSlots = opts.v2().getNumWorkers() * (double) perWorkerMaxSlots;
        int maxSlots = rawMaxSlots >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) rawMaxSlots;
        return Math.max(1, Math.min(opts.v2().getTotalSlots(), maxSlots));
    }

    private static int getMaxPipelineDop(DefaultCoordinator coord) {
        if (coord == null || coord.getExecutionDAG() == null) {
            return 1;
        }
        return coord.getExecutionDAG().getFragmentsInCreatedOrder().stream()
                .map(ExecutionFragment::getPlanFragment)
                .mapToInt(PlanFragment::getPipelineDop)
                .max()
                .orElse(1);
    }

    private static int getMaxPipelineDop(ExecPlan execPlan) {
        if (execPlan == null || execPlan.getFragments() == null || execPlan.getFragments().isEmpty()) {
            return 1;
        }
        return execPlan.getFragments().stream()
                .mapToInt(PlanFragment::getPipelineDop)
                .max()
                .orElse(1);
    }

    private static double getPlanMemCost(ConnectContext context) {
        return context.getAuditEventBuilder().build().planMemCosts;
    }

    private static double getPlanCpuCost(ConnectContext context) {
        return context.getAuditEventBuilder().build().planCpuCosts;
    }

    private static int estimateMemoryBasedSlotsFromExecPlan(QueryQueueOptions opts, ConnectContext context,
                                                            ExecPlan execPlan) {
        return normalizeCostBasedSlots(opts, getPlanMemCost(context), opts.v2().getMemBytesPerSlot(),
                normalizeCostBasedMaxSlots(opts, getMaxPipelineDop(execPlan)));
    }

    private static int estimateCpuCostBasedSlots(QueryQueueOptions opts, ConnectContext context, ExecPlan execPlan) {
        return normalizeCostBasedSlots(opts, getPlanCpuCost(context), opts.v2().getCpuCostsPerSlot(),
                normalizeCostBasedMaxSlots(opts, getMaxPipelineDop(execPlan)));
    }

    private static void collectFragmentSourceNodes(PlanNode node, Map<PlanFragmentId, FragmentContext> contexts) {
        PlanFragmentId fragmentId = node.getFragmentId();
        if (node.getChildren().isEmpty() || !fragmentId.equals(node.getChildren().get(0).getFragmentId())) {
            contexts.computeIfAbsent(fragmentId, k -> new FragmentContext())
                    .sourceNodes.add(node);
        }

        node.getChildren().forEach(child -> collectFragmentSourceNodes(child, contexts));
    }

    private static class FragmentContext {
        private final List<PlanNode> sourceNodes = Lists.newArrayList();
    }

    public static class DefaultSlotEstimator implements SlotEstimator {
        @Override
        public int estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            return 1;
        }
    }

    public static class ParallelismBasedSlotsEstimator implements SlotEstimator {
        @Override
        public int estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            if (coord == null || coord.getExecutionDAG() == null || coord.getExecutionDAG().getRootFragment() == null) {
                return 1;
            }
            PlanFragment rootFragment = coord.getExecutionDAG().getRootFragment().getPlanFragment();
            return estimateParallelismBasedSlotsFromRootFragment(opts, rootFragment);
        }
    }

    public static class MemoryBasedSlotsEstimator implements SlotEstimator {
        @Override
        public int estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            double memCost = getPlanMemCost(context);
            boolean isCostPredictorAvailable = CostPredictor.getServiceBasedCostPredictor().isAvailable();
            if (isCostPredictorAvailable && coord != null && coord.getPredictedCost() > 0) {
                memCost = coord.getPredictedCost();
            }
            return normalizeCostBasedSlots(opts, memCost, opts.v2().getMemBytesPerSlot(),
                    normalizeCostBasedMaxSlots(opts, getMaxPipelineDop(coord)));
        }
    }

    public static class CpuCostBasedSlotsEstimator implements SlotEstimator {
        @Override
        public int estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            return normalizeCostBasedSlots(opts, getPlanCpuCost(context), opts.v2().getCpuCostsPerSlot(),
                    normalizeCostBasedMaxSlots(opts, getMaxPipelineDop(coord)));
        }
    }
}
