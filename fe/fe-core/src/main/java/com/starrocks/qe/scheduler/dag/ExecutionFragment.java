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

package com.starrocks.qe.scheduler.dag;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ColocatedBackendSelector;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.FragmentScanRangeAssignment;
import com.starrocks.qe.scheduler.ExplainBuilder;
import com.starrocks.thrift.TPlanFragmentDestination;
import com.starrocks.thrift.TRuntimeFilterParams;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExecutionFragment {
    private final ExecutionDAG executionDAG;
    private final PlanFragment planFragment;
    private final Map<PlanNodeId, ScanNode> scanNodes;

    private final List<TPlanFragmentDestination> destinations;
    private final Map<Integer, Integer> numSendersPerExchange;

    private final List<FragmentInstance> instances;

    private final FragmentScanRangeAssignment scanRangeAssignment;
    private ColocatedBackendSelector.Assignment colocatedAssignment = null;

    private List<Integer> bucketSeqToInstance = null;

    private final TRuntimeFilterParams runtimeFilterParams = new TRuntimeFilterParams();

    private Boolean cachedIsColocated = null;
    private Boolean cachedIsReplicated = null;
    private Boolean cachedIsBucketShuffleJoin = null;

    private boolean isRightOrFullBucketShuffle = false;

    public ExecutionFragment(ExecutionDAG executionDAG, PlanFragment planFragment) {
        this.executionDAG = executionDAG;
        this.planFragment = planFragment;
        this.scanNodes = planFragment.collectScanNodes();

        this.destinations = Lists.newArrayList();
        this.numSendersPerExchange = Maps.newHashMap();

        this.instances = Lists.newArrayList();
        this.scanRangeAssignment = new FragmentScanRangeAssignment();
    }

    public String getExplainString(int level) {
        ExplainBuilder builder = new ExplainBuilder(level);

        builder.addSimpleKVItem("DOP", planFragment.getPipelineDop());

        builder.addMultipleChildren("INSTANCES",
                () -> instances.forEach(instance -> instance.buildExplainString(builder)));

        return builder.build();
    }

    public PlanFragment getPlanFragment() {
        return planFragment;
    }

    public PlanFragmentId getFragmentId() {
        return planFragment.getFragmentId();
    }

    public Collection<ScanNode> getScanNodes() {
        return scanNodes.values();
    }

    public ScanNode getScanNode(PlanNodeId scanId) {
        return scanNodes.get(scanId);
    }

    public List<FragmentInstance> getInstances() {
        return instances;
    }

    public Map<Long, List<FragmentInstance>> geWorkerIdToInstances() {
        return instances.stream()
                .collect(Collectors.groupingBy(
                        FragmentInstance::getWorkerId,
                        Collectors.mapping(Function.identity(), Collectors.toList())
                ));
    }

    public ExecutionDAG getExecutionDAG() {
        return executionDAG;
    }

    public FragmentScanRangeAssignment getScanRangeAssignment() {
        return scanRangeAssignment;
    }

    public ColocatedBackendSelector.Assignment getColocatedAssignment() {
        return colocatedAssignment;
    }

    public ColocatedBackendSelector.Assignment getOrCreateColocatedAssignment(OlapScanNode scanNode) {
        if (colocatedAssignment == null) {
            colocatedAssignment = new ColocatedBackendSelector.Assignment(scanNode);
        }
        return colocatedAssignment;
    }

    public int getBucketNum() {
        if (colocatedAssignment == null) {
            return 0;
        }
        return colocatedAssignment.getBucketNum();
    }

    public List<Integer> getBucketSeqToInstance() {
        Preconditions.checkNotNull(colocatedAssignment);

        if (this.bucketSeqToInstance != null) {
            return this.bucketSeqToInstance;
        }

        int numBuckets = getBucketNum();
        Integer[] bucketSeqToInstance = new Integer[numBuckets];
        // some buckets are pruned, so set the corresponding instance ordinal to BUCKET_ABSENT to indicate
        // absence of buckets.
        Arrays.fill(bucketSeqToInstance, CoordinatorPreprocessor.BUCKET_ABSENT);
        for (FragmentInstance instance : instances) {
            for (Integer bucketSeq : instance.getBucketSeqs()) {
                Preconditions.checkState(bucketSeq < numBuckets,
                        "bucketSeq exceeds bucketNum in colocate Fragment");
                bucketSeqToInstance[bucketSeq] = instance.getIndexInFragment();
            }
        }

        this.bucketSeqToInstance = Arrays.asList(bucketSeqToInstance);
        return this.bucketSeqToInstance;
    }

    public void addDestination(TPlanFragmentDestination destination) {
        destinations.add(destination);
    }

    public List<TPlanFragmentDestination> getDestinations() {
        return destinations;
    }

    public int childrenSize() {
        return planFragment.getChildren().size();
    }

    public ExecutionFragment getChild(int i) {
        return executionDAG.getFragment(planFragment.getChild(i).getFragmentId());
    }

    public void addInstance(FragmentInstance instance) {
        instance.setIndexInFragment(instances.size());
        instances.add(instance);
    }

    public void shuffleInstances(Random random) {
        Collections.shuffle(instances, random);
        for (int i = 0; i < instances.size(); i++) {
            instances.get(i).setIndexInFragment(i);
        }
    }

    public PlanNode getLeftMostNode() {
        PlanNode node = planFragment.getPlanRoot();
        while (node.getChildren().size() != 0 && !(node instanceof ExchangeNode)) {
            node = node.getChild(0);
        }
        return node;
    }

    public Map<Integer, Integer> getNumSendersPerExchange() {
        return numSendersPerExchange;
    }

    public TRuntimeFilterParams getRuntimeFilterParams() {
        return runtimeFilterParams;
    }

    public boolean isColocated() {
        if (cachedIsColocated != null) {
            return cachedIsColocated;
        }

        cachedIsColocated = isColocated(planFragment.getPlanRoot());
        return cachedIsColocated;
    }

    public boolean isReplicated() {
        if (cachedIsReplicated != null) {
            return cachedIsReplicated;
        }

        cachedIsReplicated = isReplicated(planFragment.getPlanRoot());
        return cachedIsReplicated;
    }

    public boolean isBucketShuffleJoin() {
        if (cachedIsBucketShuffleJoin != null) {
            return cachedIsBucketShuffleJoin;
        }

        cachedIsBucketShuffleJoin = isBucketShuffleJoin(planFragment.getPlanRoot());
        return cachedIsBucketShuffleJoin;
    }

    public boolean isRightOrFullBucketShuffle() {
        return isRightOrFullBucketShuffle;
    }

    private boolean isColocated(PlanNode root) {
        if (root instanceof ExchangeNode) {
            return false;
        }

        if (root.isColocate()) {
            return true;
        }

        if (root.isReplicated()) {
            // Only check left if node is replicate join
            return isColocated(root.getChild(0));
        } else {
            return root.getChildren().stream().anyMatch(this::isColocated);
        }
    }

    private boolean isReplicated(PlanNode root) {
        if (root instanceof ExchangeNode) {
            return false;
        }

        if (root.isReplicated()) {
            return true;
        }

        return root.getChildren().stream().anyMatch(PlanNode::isReplicated);
    }

    private boolean isBucketShuffleJoin(PlanNode root) {
        if (root instanceof ExchangeNode) {
            return false;
        }

        if (root instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) root;
            if (joinNode.isLocalHashBucket()) {
                isRightOrFullBucketShuffle =
                        joinNode.getJoinOp().isFullOuterJoin() || joinNode.getJoinOp().isRightJoin();
                return true;
            }
        }

        boolean childHasBucketShuffle = false;
        for (PlanNode child : root.getChildren()) {
            childHasBucketShuffle |= isBucketShuffleJoin(child);
        }

        return childHasBucketShuffle;
    }
}
