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
import com.starrocks.sql.optimizer.cost.feature.CostPredictor;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.commons.lang3.EnumUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.Utils.computeMinGEPower2;

public class SlotEstimatorFactory {
    private static final Logger LOG = LogManager.getLogger(SlotEstimatorFactory.class);

    public enum EstimatorPolicy {
        MBE, // memory-based slots estimator
        PBE, // parallel-based slots estimator
        MAX, // max of memory and parallel based slots estimator
        MIN; // min of memory and parallel based slots estimator

        public static EstimatorPolicy createDefault() {
            return MAX;
        }

        public static EstimatorPolicy create(String value) {
            return EnumUtils.getEnumIgnoreCase(EstimatorPolicy.class, value);
        }

        public SlotEstimator createEstimator() {
            switch (this) {
                case MBE:
                    return new MemoryBasedSlotsEstimator();
                case PBE:
                    return new ParallelismBasedSlotsEstimator();
                case MAX:
                    return new MaxSlotsEstimator(new MemoryBasedSlotsEstimator(), new ParallelismBasedSlotsEstimator());
                case MIN:
                    return new MinSlotsEstimator(new MemoryBasedSlotsEstimator(), new ParallelismBasedSlotsEstimator());
                default:
                    throw new IllegalArgumentException("Unknown EstimatorPolicy: " + this);
            }
        }
    }

    public static SlotEstimator create(QueryQueueOptions opts) {
        if (!opts.isEnableQueryQueueV2()) {
            return new DefaultSlotEstimator();
        }

        EstimatorPolicy policy = EstimatorPolicy.create(Config.query_queue_slots_estimator_strategy);
        if (policy == null) {
            policy = EstimatorPolicy.createDefault();
        }
        return policy.createEstimator();
    }

    /**
     * Estimate slots for EXPLAIN output when we only have ExecPlan (not DefaultCoordinator).
     * This is used for query queue information in EXPLAIN.
     */
    public static int estimateSlotsForExplain(QueryQueueOptions opts, ConnectContext context, ExecPlan execPlan) {
        if (!opts.isEnableQueryQueueV2()) {
            return 1;
        }

        EstimatorPolicy policy = EstimatorPolicy.create(Config.query_queue_slots_estimator_strategy);
        if (policy == null) {
            policy = EstimatorPolicy.createDefault();
        }

        // For explain, we use a simplified estimation based on ExecPlan
        // Memory-based estimation uses audit event costs (fallback path)
        // Parallelism-based estimation uses fragments from ExecPlan
        return estimateSlotsFromExecPlan(opts, context, execPlan, policy);
    }

    private static int estimateSlotsFromExecPlan(QueryQueueOptions opts, ConnectContext context, ExecPlan execPlan,
                                                  EstimatorPolicy policy) {
        int memBasedSlots = estimateMemoryBasedSlotsFromExecPlan(opts, context);
        int parallelBasedSlots = estimateParallelismBasedSlotsFromExecPlan(opts, context, execPlan);

        switch (policy) {
            case MBE:
                return memBasedSlots;
            case PBE:
                return parallelBasedSlots;
            case MIN:
                return Math.min(memBasedSlots, parallelBasedSlots);
            case MAX:
            default:
                return Math.max(memBasedSlots, parallelBasedSlots);
        }
    }

    private static int estimateMemoryBasedSlotsFromExecPlan(QueryQueueOptions opts, ConnectContext context) {
        // Use audit event costs as fallback (same as MemoryBasedSlotsEstimator without predicted cost)
        long memCost = (long) Math.max(context.getAuditEventBuilder().build().planMemCosts,
                context.getAuditEventBuilder().build().planCpuCosts);
        long numSlotsPerWorker = memCost / opts.v2().getMemBytesPerSlot();
        numSlotsPerWorker = Math.max(numSlotsPerWorker, 0);
        numSlotsPerWorker = computeMinGEPower2((int) numSlotsPerWorker);

        long numSlots = numSlotsPerWorker * opts.v2().getNumWorkers();
        numSlots = Math.max(numSlots, 1);
        numSlots = Math.min(numSlots, opts.v2().getTotalSlots());

        return (int) numSlots;
    }

    private static int estimateParallelismBasedSlotsFromExecPlan(QueryQueueOptions opts, ConnectContext context,
                                                                  ExecPlan execPlan) {
        List<PlanFragment> fragments = execPlan.getFragments();
        if (fragments == null || fragments.isEmpty()) {
            return 1;
        }

        PlanFragment rootFragment = fragments.get(0);
        PlanNode rootNode = rootFragment.getPlanRoot();
        Map<PlanFragmentId, FragmentContext> contexts = Maps.newHashMap();
        collectFragmentSourceNodes(rootNode, contexts);
        calculateFragmentWorkers(opts, rootFragment, contexts);

        int numSlots = contexts.values().stream()
                .mapToInt(fragmentContext -> estimateFragmentSlots(opts, fragmentContext))
                .max().orElse(1);

        // Apply CPU-cost clamp like ParallelismBasedSlotsEstimator does
        final int numWorkers = contexts.values().stream()
                .mapToInt(fragmentContext -> fragmentContext.numWorkers)
                .max().orElse(1);
        final long planCpuCosts = (long) context.getAuditEventBuilder().build().planCpuCosts;
        int numSlotsByCpuCosts = (int) (planCpuCosts / opts.v2().getCpuCostsPerSlot());
        numSlotsByCpuCosts = Math.max(1, numSlotsByCpuCosts / numWorkers) * numWorkers;

        // Restrict numSlotsByCpuCosts to be within [numSlots / 2, numSlots].
        return Math.min(Math.max(numSlots / 2, numSlotsByCpuCosts), numSlots);
    }

    private static int estimateFragmentSlots(QueryQueueOptions opts, FragmentContext context) {
        int numSlots = 1;
        for (PlanNode sourceNode : context.sourceNodes) {
            int curNumSlots = estimateNumSlotsBySourceNode(opts, sourceNode);

            curNumSlots /= context.numWorkers;
            curNumSlots = Math.max(curNumSlots, 1);
            curNumSlots = Math.min(curNumSlots, context.fragment.getPipelineDop());

            curNumSlots *= context.numWorkers;
            curNumSlots = Math.min(curNumSlots, opts.v2().getTotalSlots());

            numSlots = Math.max(numSlots, curNumSlots);
        }
        return numSlots;
    }

    private static int estimateNumSlotsBySourceNode(QueryQueueOptions opts, PlanNode sourceNode) {
        if (sourceNode instanceof OlapScanNode) {
            OlapScanNode olapScanNode = (OlapScanNode) sourceNode;
            List<TScanRangeLocations> locations = olapScanNode.getScanRangeLocations(0);
            if (locations != null) {
                return locations.size();
            }
        }

        return (int) (sourceNode.getCardinality() / opts.v2().getNumRowsPerSlot());
    }

    private static void collectFragmentSourceNodes(PlanNode node, Map<PlanFragmentId, FragmentContext> contexts) {
        PlanFragmentId fragmentId = node.getFragmentId();
        if (node.getChildren().isEmpty() || !fragmentId.equals(node.getChildren().get(0).getFragmentId())) {
            contexts.computeIfAbsent(fragmentId, k -> new FragmentContext(node.getFragment()))
                    .sourceNodes.add(node);
        }

        node.getChildren().forEach(child -> collectFragmentSourceNodes(child, contexts));
    }

    private static void calculateFragmentWorkers(QueryQueueOptions opts, PlanFragment fragment,
                                                 Map<PlanFragmentId, FragmentContext> contexts) {
        fragment.getChildren().forEach(child -> calculateFragmentWorkers(opts, child, contexts));

        FragmentContext context = contexts.get(fragment.getFragmentId());
        if (context == null) {
            return;
        }

        PlanNode leftMostNode = fragment.getLeftMostNode();
        if (leftMostNode instanceof OlapScanNode) {
            OlapScanNode scanNode = (OlapScanNode) leftMostNode;
            context.numWorkers = scanNode.getScanRangeLocations(0).stream()
                    .flatMap(locations -> locations.getLocations().stream())
                    .map(TScanRangeLocation::getBackend_id)
                    .collect(Collectors.toSet())
                    .size();
        } else if (leftMostNode instanceof ScanNode && ((ScanNode) leftMostNode).isConnectorScanNode()) {
            // TODO: get the actual number of files for connector scan nodes.
            int numWorkers = (int) leftMostNode.getCardinality() / opts.v2().getNumRowsPerSlot();
            context.numWorkers = Math.max(1, Math.min(numWorkers, opts.v2().getNumWorkers()));
        } else if (fragment.isGatherFragment()) {
            context.numWorkers = 1;
        } else {
            context.numWorkers = fragment.getChildren().stream()
                    .mapToInt(child -> contexts.get(child.getFragmentId()).numWorkers)
                    .max().orElse(1);
        }
    }

    private static class FragmentContext {
        private final PlanFragment fragment;
        private final List<PlanNode> sourceNodes = Lists.newArrayList();
        private int numWorkers = 1;

        public FragmentContext(PlanFragment fragment) {
            this.fragment = fragment;
        }
    }

    public static class DefaultSlotEstimator implements SlotEstimator {
        @Override
        public SlotEstimate estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            return new SlotEstimate(1, 1);
        }
    }

    public static class MemoryBasedSlotsEstimator implements SlotEstimator {
        @Override
        public SlotEstimate estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            long memCost;
            boolean isCostPredictorAvailable = CostPredictor.getServiceBasedCostPredictor().isAvailable();
            if (isCostPredictorAvailable && coord.getPredictedCost() > 0) {
                memCost = coord.getPredictedCost();
            } else {
                if (isCostPredictorAvailable) {
                    LOG.warn("Cost predictor is available, but predicted cost is not set. " +
                            "Using planCpuCosts as the fallback.");
                }
                // The estimate of planMemCosts is typically an underestimation, often several orders of magnitude smaller than
                // the actual memory usage, whereas planCpuCosts tends to be relatively larger.
                // Therefore, the maximum value between the two is used as the estimate for memory.
                memCost = (long) Math.max(context.getAuditEventBuilder().build().planMemCosts,
                        context.getAuditEventBuilder().build().planCpuCosts);
            }
            long numSlotsPerWorker = memCost / opts.v2().getMemBytesPerSlot();
            numSlotsPerWorker = Math.max(numSlotsPerWorker, 0);
            numSlotsPerWorker = computeMinGEPower2((int) numSlotsPerWorker);

            long numSlots = numSlotsPerWorker * opts.v2().getNumWorkers();
            numSlots = Math.max(numSlots, 1);
            int rawSlots = (int) Math.min(numSlots, Integer.MAX_VALUE);
            int clampedSlots = (int) Math.min(numSlots, opts.v2().getTotalSlots());

            return new SlotEstimate(rawSlots, clampedSlots);
        }
    }

    public static class ParallelismBasedSlotsEstimator implements SlotEstimator {
        @Override
        public SlotEstimate estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            Map<PlanFragmentId, FragmentContext> fragmentContexts = collectFragmentContexts(opts, coord);
            // rawMax is the max per-fragment demand BEFORE the totalSlots cap is applied.
            int rawMax = fragmentContexts.values().stream()
                    .mapToInt(fragmentContext -> estimateFragmentSlotsRaw(opts, fragmentContext))
                    .max().orElse(1);
            int totalSlots = opts.v2().getTotalSlots();
            int numSlots = Math.min(rawMax, totalSlots);

            final int numWorkers = fragmentContexts.values().stream()
                    .mapToInt(fragmentContext -> fragmentContext.numWorkers)
                    .max().orElse(1);
            final long planCpuCosts = (long) context.getAuditEventBuilder().build().planCpuCosts;
            int numSlotsByCpuCosts = (int) (planCpuCosts / opts.v2().getCpuCostsPerSlot());
            numSlotsByCpuCosts = Math.max(1, numSlotsByCpuCosts / numWorkers) * numWorkers;

            // Restrict numSlotsByCpuCosts to be within [numSlots / 2, numSlots].
            int clamped = Math.min(Math.max(numSlots / 2, numSlotsByCpuCosts), numSlots);

            // Invariant: clamped <= numSlots = min(rawMax, totalSlots) <= rawMax, so rawMax >= clamped.
            return new SlotEstimate(rawMax, clamped);
        }

        private static int estimateFragmentSlotsRaw(QueryQueueOptions opts, FragmentContext context) {
            int numSlots = 1;
            for (PlanNode sourceNode : context.sourceNodes) {
                int curNumSlots = estimateNumSlotsBySourceNode(opts, sourceNode);

                curNumSlots /= context.numWorkers;
                curNumSlots = Math.max(curNumSlots, 1);
                curNumSlots = Math.min(curNumSlots, context.fragment.getPipelineDop());

                curNumSlots *= context.numWorkers;
                // NOTE: deliberately omits the per-fragment `min(curNumSlots, totalSlots)` clamp.
                // estimateSlots applies the totalSlots cap once at the top level after taking the
                // max across fragments, so the pre-clamp demand survives here.

                numSlots = Math.max(numSlots, curNumSlots);
            }
            return numSlots;
        }

        private static int estimateNumSlotsBySourceNode(QueryQueueOptions opts, PlanNode sourceNode) {
            if (sourceNode instanceof OlapScanNode) {
                OlapScanNode olapScanNode = (OlapScanNode) sourceNode;
                List<TScanRangeLocations> locations = olapScanNode.getScanRangeLocations(0);
                if (locations != null) {
                    return locations.size();
                }
            }

            return (int) (sourceNode.getCardinality() / opts.v2().getNumRowsPerSlot());
        }

        private static Map<PlanFragmentId, FragmentContext> collectFragmentContexts(QueryQueueOptions opts,
                                                                                    DefaultCoordinator coord) {
            PlanFragment rootFragment = coord.getExecutionDAG().getRootFragment().getPlanFragment();
            PlanNode rootNode = rootFragment.getPlanRoot();

            Map<PlanFragmentId, FragmentContext> contexts = Maps.newHashMap();
            collectFragmentSourceNodes(rootNode, contexts);
            calculateFragmentWorkers(opts, rootFragment, contexts);

            return contexts;
        }

        private static void collectFragmentSourceNodes(PlanNode node, Map<PlanFragmentId, FragmentContext> contexts) {
            PlanFragmentId fragmentId = node.getFragmentId();
            if (node.getChildren().isEmpty() || !fragmentId.equals(node.getChildren().get(0).getFragmentId())) {
                contexts.computeIfAbsent(fragmentId, k -> new FragmentContext(node.getFragment()))
                        .sourceNodes.add(node);
            }

            node.getChildren().forEach(child -> collectFragmentSourceNodes(child, contexts));
        }

        private static void calculateFragmentWorkers(QueryQueueOptions opts, PlanFragment fragment,
                                                     Map<PlanFragmentId, FragmentContext> contexts) {
            fragment.getChildren().forEach(child -> calculateFragmentWorkers(opts, child, contexts));

            FragmentContext context = contexts.get(fragment.getFragmentId());
            if (context == null) {
                return;
            }

            PlanNode leftMostNode = fragment.getLeftMostNode();
            if (leftMostNode instanceof OlapScanNode) {
                OlapScanNode scanNode = (OlapScanNode) leftMostNode;
                context.numWorkers = scanNode.getScanRangeLocations(0).stream()
                        .flatMap(locations -> locations.getLocations().stream())
                        .map(TScanRangeLocation::getBackend_id)
                        .collect(Collectors.toSet())
                        .size();
            } else if (leftMostNode instanceof ScanNode && ((ScanNode) leftMostNode).isConnectorScanNode()) {
                // TODO: get the actual number of files for connector scan nodes.
                int numWorkers = (int) leftMostNode.getCardinality() / opts.v2().getNumRowsPerSlot();
                context.numWorkers = Math.max(1, Math.min(numWorkers, opts.v2().getNumWorkers()));
            } else if (fragment.isGatherFragment()) {
                context.numWorkers = 1;
            } else {
                context.numWorkers = fragment.getChildren().stream()
                        .mapToInt(child -> contexts.get(child.getFragmentId()).numWorkers)
                        .max().orElse(1);
            }
        }

        private static class FragmentContext {
            private final PlanFragment fragment;
            private final List<PlanNode> sourceNodes = Lists.newArrayList();
            private int numWorkers = 1;

            public FragmentContext(PlanFragment fragment) {
                this.fragment = fragment;
            }
        }
    }

    public static class MaxSlotsEstimator implements SlotEstimator {
        private final SlotEstimator[] estimators;

        public MaxSlotsEstimator(SlotEstimator... estimators) {
            this.estimators = estimators;
        }

        @Override
        public SlotEstimate estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            // Aggregate component-wise: child estimates each satisfy rawSlots >= clampedSlots,
            // and for the i* that achieves max(c), c_{i*} <= r_{i*} <= max(r), so the invariant
            // max(r) >= max(c) carries through.
            int maxRaw = 1;
            int maxClamped = 1;
            for (SlotEstimator estimator : estimators) {
                SlotEstimate est = estimator.estimateSlots(opts, context, coord);
                maxRaw = Math.max(maxRaw, est.rawSlots());
                maxClamped = Math.max(maxClamped, est.clampedSlots());
            }
            return new SlotEstimate(maxRaw, maxClamped);
        }
    }

    public static class MinSlotsEstimator implements SlotEstimator {
        private final SlotEstimator[] estimators;

        public MinSlotsEstimator(SlotEstimator... estimators) {
            this.estimators = estimators;
        }

        @Override
        public SlotEstimate estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            // Aggregate component-wise: for each i, r_i >= c_i, so min(c_i) <= c_i <= r_i for every i,
            // which means min(c_i) <= min(r_i). The invariant rawSlots >= clampedSlots carries through.
            int minRaw = Integer.MAX_VALUE;
            int minClamped = Integer.MAX_VALUE;
            for (SlotEstimator estimator : estimators) {
                SlotEstimate est = estimator.estimateSlots(opts, context, coord);
                minRaw = Math.min(minRaw, est.rawSlots());
                minClamped = Math.min(minClamped, est.clampedSlots());
            }
            if (minRaw == Integer.MAX_VALUE) {
                minRaw = 1;
                minClamped = 1;
            }
            return new SlotEstimate(minRaw, minClamped);
        }
    }
}
