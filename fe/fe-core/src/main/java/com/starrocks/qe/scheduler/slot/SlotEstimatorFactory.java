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
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.sql.optimizer.cost.feature.CostPredictor;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.Utils.computeMinGEPower2;

public class SlotEstimatorFactory {
    public static SlotEstimator create(QueryQueueOptions opts) {
        if (!opts.isEnableQueryQueueV2()) {
            return new DefaultSlotEstimator();
        }
        return new MaxSlotsEstimator(new MemoryBasedSlotsEstimator(), new ParallelismBasedSlotsEstimator());
    }

    public static class DefaultSlotEstimator implements SlotEstimator {
        @Override
        public int estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            return 1;
        }
    }

    public static class MemoryBasedSlotsEstimator implements SlotEstimator {
        @Override
        public int estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            long memCost;
            if (CostPredictor.getServiceBasedCostPredictor().isAvailable() && coord.getPredictedCost() > 0) {
                memCost = coord.getPredictedCost();
            } else {
                memCost = (long) context.getAuditEventBuilder().build().planMemCosts;
            }
            long numSlotsPerWorker = memCost / opts.v2().getMemBytesPerSlot();
            numSlotsPerWorker = Math.max(numSlotsPerWorker, 0);
            numSlotsPerWorker = computeMinGEPower2((int) numSlotsPerWorker);

            long numSlots = numSlotsPerWorker * opts.v2().getNumWorkers();
            numSlots = Math.max(numSlots, 1);
            numSlots = Math.min(numSlots, opts.v2().getTotalSlots());

            return (int) numSlots;
        }
    }

    public static class ParallelismBasedSlotsEstimator implements SlotEstimator {
        @Override
        public int estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            Map<PlanFragmentId, FragmentContext> fragmentContexts = collectFragmentContexts(coord);
            int numSlots = fragmentContexts.values().stream()
                    .mapToInt(fragmentContext -> estimateFragmentSlots(opts, fragmentContext))
                    .max().orElse(1);

            final int numWorkers = fragmentContexts.values().stream()
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

        private static Map<PlanFragmentId, FragmentContext> collectFragmentContexts(DefaultCoordinator coord) {
            PlanFragment rootFragment = coord.getExecutionDAG().getRootFragment().getPlanFragment();
            PlanNode rootNode = rootFragment.getPlanRoot();

            Map<PlanFragmentId, FragmentContext> contexts = Maps.newHashMap();
            collectFragmentSourceNodes(rootNode, contexts);
            calculateFragmentWorkers(rootFragment, contexts);

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

        private static void calculateFragmentWorkers(PlanFragment fragment, Map<PlanFragmentId, FragmentContext> contexts) {
            fragment.getChildren().forEach(child -> calculateFragmentWorkers(child, contexts));

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
        public int estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
            return Arrays.stream(estimators)
                    .mapToInt(estimator -> estimator.estimateSlots(opts, context, coord))
                    .max()
                    .orElse(1);
        }
    }
}
