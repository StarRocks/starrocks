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

package com.starrocks.qe.scheduler.assignment;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.TINY_SCALE_ROWS_LIMIT;

/**
 * The assignment strategy for fragments whose left most node is not a scan node.
 */
public class RemoteFragmentAssignmentStrategy implements FragmentAssignmentStrategy {

    private final ConnectContext connectContext;
    private final WorkerProvider workerProvider;
    private final boolean isGatherOutput;
    private final boolean usePipeline;
    private final boolean enableDopAdaption;

    private final Random random;

    public RemoteFragmentAssignmentStrategy(ConnectContext connectContext, WorkerProvider workerProvider,
                                            boolean usePipeline, boolean isGatherOutput, Random random) {
        this.connectContext = connectContext;
        this.workerProvider = workerProvider;
        this.usePipeline = usePipeline;
        this.enableDopAdaption = usePipeline && connectContext.getSessionVariable().isEnablePipelineAdaptiveDop();
        this.isGatherOutput = isGatherOutput;
        this.random = random;
    }

    @Override
    public void assignFragmentToWorker(ExecutionFragment execFragment) throws StarRocksException {
        final PlanFragment fragment = execFragment.getPlanFragment();

        // If left child is MultiCastDataFragment(only support left now), will keep same instance with child.
        boolean isCTEConsumerFragment =
                !fragment.getChildren().isEmpty() && fragment.getChild(0) instanceof MultiCastPlanFragment;
        if (isCTEConsumerFragment) {
            assignCTEConsumerFragmentToWorker(execFragment);
            return;
        }

        if (fragment.isGatherFragment()) {
            assignGatherFragmentToWorker(execFragment);
            return;
        }

        assignRemoteFragmentToWorker(execFragment);
    }

    private void assignCTEConsumerFragmentToWorker(ExecutionFragment execFragment) {
        ExecutionFragment childFragment = execFragment.getChild(0);
        for (FragmentInstance childInstance : childFragment.getInstances()) {
            execFragment.addInstance(new FragmentInstance(childInstance.getWorker(), execFragment));
        }
    }

    private void assignGatherFragmentToWorker(ExecutionFragment execFragment) throws StarRocksException {
        long workerId = workerProvider.selectNextWorker();
        FragmentInstance instance = new FragmentInstance(workerProvider.getWorkerById(workerId), execFragment);
        execFragment.addInstance(instance);
    }

    private void assignRemoteFragmentToWorker(ExecutionFragment execFragment) {
        final PlanFragment fragment = execFragment.getPlanFragment();

        // hostSet contains target backends to whom fragment instances of the current PlanFragment will be
        // delivered. when pipeline parallelization is adopted, the number of instances should be the size
        // of hostSet, that it to say, each backend has exactly one fragment.
        Set<Long> workerIdSet = Sets.newHashSet();
        int maxParallelism = 0;

        List<Long> selectedComputedNodes = workerProvider.selectAllComputeNodes();
        if (workerProvider.isPreferComputeNode() && !selectedComputedNodes.isEmpty()) {
            workerIdSet = adaptiveChooseNodes(fragment, selectedComputedNodes, Sets.newHashSet(selectedComputedNodes));
            // make olapScan maxParallelism equals prefer compute node number
            maxParallelism = workerIdSet.size() * fragment.getParallelExecNum();
        } else if (fragment.isUnionFragment() && isGatherOutput) {
            // union fragment use all children's host
            // if output fragment isn't gather, all fragment must keep 1 instance
            for (int i = 0; i < execFragment.childrenSize(); i++) {
                ExecutionFragment childExecFragment = execFragment.getChild(i);
                childExecFragment.getInstances().stream()
                        .map(FragmentInstance::getWorkerId)
                        .forEach(workerIdSet::add);
            }
            maxParallelism = workerIdSet.size() * fragment.getParallelExecNum();
        } else {
            // there is no leftmost scan; we assign the same hosts as those of our
            // input fragment which has a higher instance_number
            int maxIndex = 0;
            for (int i = 0; i < execFragment.childrenSize(); i++) {
                int curInputFragmentParallelism = execFragment.getChild(i).getInstances().size();
                // when dop adaptation enabled, numInstances * pipelineDop is equivalent to numInstances in
                // non-pipeline engine and pipeline engine(dop adaptation disabled).
                if (enableDopAdaption) {
                    curInputFragmentParallelism *= fragment.getChild(i).getPipelineDop();
                }

                if (curInputFragmentParallelism > maxParallelism) {
                    maxParallelism = curInputFragmentParallelism;
                    maxIndex = i;
                }
            }

            ExecutionFragment maxFragment = execFragment.getChild(maxIndex);
            Set<Long> childUsedHost = maxFragment.getInstances().stream()
                    .map(FragmentInstance::getWorkerId)
                    .collect(Collectors.toSet());
            workerIdSet = adaptiveChooseNodes(fragment, workerProvider.getAllAvailableNodes(), childUsedHost);

            // The adaptive choose nodes process may change the selected nodes number.
            // When enable pipeline engine but dop is not 0, We have to change the maxParallelism value
            // to ensure it keeps equal with the size of workerIdSet.
            if (usePipeline) {
                maxParallelism = workerIdSet.size();
            }
        }

        if (enableDopAdaption) {
            maxParallelism = workerIdSet.size();
        }

        int exchangeInstances = -1;
        if (connectContext != null && connectContext.getSessionVariable() != null) {
            exchangeInstances = connectContext.getSessionVariable().getExchangeInstanceParallel();
        }
        List<Long> workerIds;
        if (exchangeInstances > 0 && maxParallelism > exchangeInstances) {
            // random select some instance
            // get distinct host, when parallel_fragment_exec_instance_num > 1, single host may execute several instances
            maxParallelism = exchangeInstances;
            workerIds = Lists.newArrayList(workerIdSet);
            Collections.shuffle(workerIds, random);
        } else {
            workerIds = Lists.newArrayList(workerIdSet);
        }

        for (int i = 0; i < maxParallelism; i++) {
            Long workerId = workerIds.get(i % workerIds.size());
            FragmentInstance instance = new FragmentInstance(workerProvider.getWorkerById(workerId), execFragment);
            execFragment.addInstance(instance);
        }

        // When group by cardinality is smaller than number of backend, only some backends always
        // process while other has no data to process.
        // So we shuffle instances to make different backends handle different queries.
        execFragment.shuffleInstances(random);

        // TODO: switch to unpartitioned/coord execution if our input fragment
        // is executed that way (could have been downgraded from distributed)
    }

    private Set<Long> adaptiveChooseNodes(PlanFragment fragment, List<Long> candidates,
                                          Set<Long> childUsedHosts) {
        List<Long> childHosts = Lists.newArrayList(childUsedHosts);

        // sometimes we may reverse the fragment order like SHUFFLE_HASH_BUCKET plan, so we need sort
        // the list to ensure the most left child is at the 0 index position.
        List<PlanFragment> sortedFragments = fragment.getChildren().stream()
                .sorted(Comparator.comparing(e -> e.getPlanRoot().getId().asInt()))
                .collect(Collectors.toList());

        long maxOutputOfRightChild = sortedFragments.stream().skip(1)
                .map(e -> e.getPlanRoot().getCardinality()).reduce(Long::max)
                .orElse(fragment.getChild(0).getPlanRoot().getCardinality());
        long outputOfMostLeftChild = sortedFragments.get(0).getPlanRoot().getCardinality();

        long nodeNums = getOptimalNodeNums(outputOfMostLeftChild, maxOutputOfRightChild, fragment.getPipelineDop(),
                candidates.size());

        SessionVariableConstants.ChooseInstancesMode mode = connectContext.getSessionVariable()
                .getChooseExecuteInstancesMode();
        if (mode.enableIncreaseInstance() && nodeNums > childUsedHosts.size()) {
            for (Long id : candidates) {
                if (!childUsedHosts.contains(id)) {
                    childHosts.add(id);
                    workerProvider.selectWorkerUnchecked(id);
                    if (childHosts.size() == nodeNums) {
                        break;
                    }
                }
            }
            return Sets.newHashSet(childHosts);
        } else if (mode.enableDecreaseInstance() && nodeNums < childUsedHosts.size()
                && candidates.size() >= Config.adaptive_choose_instances_threshold) {
            Collections.shuffle(childHosts, random);
            return childHosts.stream().limit(nodeNums).collect(Collectors.toSet());
        } else {
            return Sets.newHashSet(childHosts);
        }
    }

    public static long getOptimalNodeNums(long outputOfMostLeftChild, long maxOutputOfRightChild, int dop, int candidateSize) {
        long baseNodeNums = (long) Math.ceil((double) maxOutputOfRightChild / TINY_SCALE_ROWS_LIMIT / dop);
        double base = Math.max(Math.E, baseNodeNums);

        long amplifyFactor = Math.round(Math.max(1,
                Math.log(outputOfMostLeftChild / TINY_SCALE_ROWS_LIMIT / dop) / Math.log(base)));

        return Math.min(amplifyFactor * baseNodeNums, candidateSize);
    }

}
