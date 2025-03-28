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
import com.google.common.collect.Sets;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.UnionFind;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.rpc.RpcException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TUniqueId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PhasedExecutionSchedule implements ExecutionSchedule {
    private static class PackedExecutionFragment {
        PackedExecutionFragment(ExecutionFragment fragment) {
            fragments.add(fragment);
        }

        PackedExecutionFragment(Collection<ExecutionFragment> fragments) {
            this.fragments.addAll(fragments);
        }

        private final List<ExecutionFragment> fragments = Lists.newArrayList();

        List<ExecutionFragment> getFragments() {
            return fragments;
        }
    }

    private Coordinator coordinator;
    private Deployer deployer;
    private ExecutionDAG dag;

    private volatile boolean cancelled = false;

    public PhasedExecutionSchedule(ConnectContext context) {
        this.connectContext = context;
        this.maxScheduleConcurrency = context.getSessionVariable().getPhasedSchedulerMaxConcurrency();
    }

    private final Stack<PackedExecutionFragment> stack = new Stack<>();

    // Maximum scheduling concurrency.
    private final int maxScheduleConcurrency;
    // fragment id -> in-degree > 1 children (cte producer fragments)
    //       A(E)
    //       │
    //       │
    //       │
    // ┌─────┼─────┐
    // │     │     │
    // ▼     ▼     ▼
    // B(E)  C     D(E)
    // │           │
    // │           │
    // └─────┬─────┘
    //       │
    //       ▼
    //       E
    private final Map<PlanFragmentId, Set<PlanFragmentId>> commonChildrens = Maps.newHashMap();
    private Map<PlanFragmentId, Integer> fragmentInDegrees = Maps.newHashMap();
    // the same with fragmentInDegrees.
    private Map<PlanFragmentId, Integer> scheduleFragmentInDegrees = Maps.newHashMap();
    // Constructing equivalence classes. Having the same child is in an equivalent class.
    private final UnionFind<PlanFragmentId> mergedUnionFind = new UnionFind<>();
    // Controls that only one thread can call scheduleNext.
    private final AtomicInteger inputScheduleTaskNums = new AtomicInteger();
    private final ConnectContext connectContext;
    private final Set<PlanFragmentId> waitScheduleFragments = Sets.newHashSet();

    // TODO: coordinate in BE
    private final Map<PlanFragmentId, AtomicInteger> schedulingFragmentInstances = Maps.newConcurrentMap();

    // fragment schedule queue
    private final Queue<List<DeployState>> deployStates = Lists.newLinkedList();

    // fragment id -> fragment child schedule order
    private Map<PlanFragmentId, FragmentSequence> sequenceMap = Maps.newHashMap();

    public void prepareSchedule(Coordinator coordinator, Deployer deployer, ExecutionDAG dag) {
        this.coordinator = coordinator;
        this.deployer = deployer;
        this.dag = dag;
        ExecutionFragment rootFragment = dag.getRootFragment();
        this.stack.push(new PackedExecutionFragment(rootFragment));
        // build in-degrees
        fragmentInDegrees = buildChildInDegrees(rootFragment);
        scheduleFragmentInDegrees = Maps.newHashMap(fragmentInDegrees);
        // build common children
        buildCommonChildren(rootFragment);

        buildMergedSets(rootFragment, Sets.newHashSet());

        sequenceMap = FragmentSequence.Builder.build(dag, rootFragment);
    }

    private Map<PlanFragmentId, Integer> buildChildInDegrees(ExecutionFragment root) {
        Queue<ExecutionFragment> queue = Lists.newLinkedList();
        Map<PlanFragmentId, Integer> inDegrees = Maps.newHashMap();
        inDegrees.put(root.getFragmentId(), 0);
        queue.add(root);

        while (!queue.isEmpty()) {
            final ExecutionFragment fragment = queue.poll();
            for (int i = 0; i < fragment.childrenSize(); i++) {
                ExecutionFragment child = fragment.getChild(i);
                PlanFragmentId cid = fragment.getChild(i).getFragmentId();
                Integer v = inDegrees.get(cid);
                if (v != null) {
                    inDegrees.put(cid, v + 1);
                } else {
                    inDegrees.put(cid, 1);
                    queue.add(child);
                }
            }
        }

        return inDegrees;
    }

    private Set<PlanFragmentId> buildCommonChildren(ExecutionFragment fragment) {
        final PlanFragmentId fragmentId = fragment.getFragmentId();
        final Set<PlanFragmentId> planFragmentIds = commonChildrens.get(fragmentId);

        if (planFragmentIds != null) {
            return planFragmentIds;
        }

        final Set<PlanFragmentId> fragmentCommonChild = Sets.newHashSet();

        if (fragmentInDegrees.get(fragmentId) > 1) {
            fragmentCommonChild.add(fragmentId);
        }

        for (int i = 0; i < fragment.childrenSize(); i++) {
            final ExecutionFragment child = fragment.getChild(i);
            final Set<PlanFragmentId> childCommonChildren = buildCommonChildren(child);
            fragmentCommonChild.addAll(childCommonChildren);
        }

        commonChildrens.put(fragmentId, fragmentCommonChild);
        return fragmentCommonChild;
    }

    private void buildMergedSets(ExecutionFragment node, Set<PlanFragmentId> accessed) {
        final PlanFragmentId fragmentId = node.getFragmentId();

        if (accessed.contains(fragmentId)) {
            return;
        }
        accessed.add(fragmentId);

        final Set<PlanFragmentId> planFragmentIds = commonChildrens.get(fragmentId);
        for (PlanFragmentId planFragmentId : planFragmentIds) {
            mergedUnionFind.union(fragmentId, planFragmentId);
        }
        for (int i = 0; i < node.childrenSize(); i++) {
            final ExecutionFragment child = node.getChild(i);
            buildMergedSets(child, accessed);
        }
    }

    // build all deploy states
    private void buildDeployStates() {
        while (!isFinished()) {
            final List<DeployState> nextTurnSchedule = getNextTurnSchedule();
            deployStates.add(nextTurnSchedule);
        }
    }

    // schedule next
    public void schedule(Coordinator.ScheduleOption option) throws RpcException, StarRocksException {
        buildDeployStates();
        final int oldTaskCnt = inputScheduleTaskNums.getAndIncrement();
        if (oldTaskCnt == 0) {
            int dec = 0;
            do {
                doDeploy();
                dec = inputScheduleTaskNums.getAndDecrement();
            } while (dec > 1);
        }
    }

    public void cancel() {
        cancelled = true;
    }

    private void doDeploy() throws RpcException, StarRocksException {
        if (deployStates.isEmpty()) {
            return;
        }
        List<DeployState> deployState = deployStates.poll();
        Preconditions.checkState(deployState != null);
        for (DeployState state : deployState) {
            deployer.deployFragments(state);
        }

        while (true) {
            deployState = coordinator.assignIncrementalScanRangesToDeployStates(deployer, deployState);
            if (deployState.isEmpty()) {
                break;
            }
            for (DeployState state : deployState) {
                deployer.deployFragments(state);
            }
        }
    }

    public void tryScheduleNextTurn(TUniqueId fragmentInstanceId) throws RpcException, StarRocksException {
        if (cancelled) {
            return;
        }
        final FragmentInstance instance = dag.getInstanceByInstanceId(fragmentInstanceId);
        final PlanFragmentId fragmentId = instance.getFragmentId();
        final AtomicInteger countDowns = schedulingFragmentInstances.get(fragmentId);
        if (countDowns.decrementAndGet() != 0) {
            return;
        }
        try (var guard = connectContext.bindScope()) {
            final int oldTaskCnt = inputScheduleTaskNums.getAndIncrement();
            if (oldTaskCnt == 0) {
                int dec = 0;
                do {
                    doDeploy();
                    dec = inputScheduleTaskNums.getAndDecrement();
                } while (dec > 1);
            }
        }
    }

    // inner data structure (ExecutionDag::executors) should be protected by Coordinator lock
    private List<DeployState> getNextTurnSchedule() {
        if (isFinished()) {
            return Collections.emptyList();
        }
        List<List<ExecutionFragment>> scheduleFragments = Lists.newArrayList();
        int currentScheduleConcurrency = 0;
        while (currentScheduleConcurrency < maxScheduleConcurrency) {
            if (isFinished()) {
                break;
            }
            if (scheduleNext(scheduleFragments) && waitScheduleFragments.isEmpty()) {
                currentScheduleConcurrency++;
            }
        }

        // merge and build fragment
        final List<List<ExecutionFragment>> fragments = buildScheduleOrder(scheduleFragments);

        // build deploy states
        List<DeployState> deployStates = Lists.newArrayList();
        for (List<ExecutionFragment> fragment : fragments) {
            for (ExecutionFragment executionFragment : fragment) {
                final PlanFragmentId fragmentId = executionFragment.getFragmentId();
                int instanceNums = executionFragment.getInstances().size();
                schedulingFragmentInstances.put(fragmentId, new AtomicInteger(instanceNums));
            }
            final DeployState deployState = deployer.createFragmentExecStates(fragment);
            deployStates.add(deployState);
        }
        return deployStates;
    }

    // Get the sequence of fragments for the next dispatch.
    // 1. Get the top-of-stack fragments and add them to the sequence to be scheduled.
    // 2. Iterate through each fragment.
    //   2.1 Iterate over the children of each fragment. lower their in-degree.
    //   2.2 Stack each child in turn. If any of the fragment's children belong to the same equivalence
    //   class, then the children are scheduled together. They are stacked together.
    private boolean scheduleNext(List<List<ExecutionFragment>> scheduleFragments) {
        List<ExecutionFragment> currentScheduleFragments = Lists.newArrayList();

        PackedExecutionFragment packedExecutionFragment = stack.pop();
        currentScheduleFragments.addAll(packedExecutionFragment.getFragments());

        Preconditions.checkState(!currentScheduleFragments.isEmpty());

        final Iterator<ExecutionFragment> iterator = currentScheduleFragments.iterator();
        while (iterator.hasNext()) {
            final ExecutionFragment fragment = iterator.next();
            if (fragment.isScheduled()) {
                iterator.remove();
                continue;
            }
            fragment.setIsScheduled(true);

            // keep the order
            final Map<Integer, LinkedHashSet<ExecutionFragment>> groups = Maps.newHashMap();
            final Set<PlanFragmentId> processedFragments = Sets.newHashSet();

            for (int i = fragment.childrenSize() - 1; i >= 0; i--) {
                final ExecutionFragment child = sequenceMap.get(fragment.getFragmentId()).getAt(i);
                final PlanFragmentId childFragmentId = child.getFragmentId();
                final int groupId = mergedUnionFind.getGroupIdOrAdd(childFragmentId);
                Set<ExecutionFragment> groupFragments = groups.computeIfAbsent(groupId, k -> Sets.newLinkedHashSet());
                int degree =
                        scheduleFragmentInDegrees.compute(childFragmentId, (k, v) -> Preconditions.checkNotNull(v) - 1);
                // we won't schedule the fragment that in-degree neq 0
                if (degree == 0) {
                    groupFragments.add(child);
                } else {
                    waitScheduleFragments.add(child.getFragmentId());
                }
            }

            if (groups.size() != fragment.childrenSize()) {
                List<PackedExecutionFragment> fragments = Lists.newArrayList();
                for (int i = fragment.childrenSize() - 1; i >= 0; i--) {
                    final ExecutionFragment child = sequenceMap.get(fragment.getFragmentId()).getAt(i);
                    final PlanFragmentId childFragmentId = child.getFragmentId();
                    final int groupId = mergedUnionFind.getGroupIdOrAdd(childFragmentId);
                    final Set<ExecutionFragment> groupFragments = groups.get(groupId);
                    // The current fragment is in the same group as the prev fragment and
                    // does not need to be processed.
                    if (processedFragments.contains(childFragmentId)) {
                        continue;
                    }

                    fragments.add(new PackedExecutionFragment(groupFragments));

                    for (ExecutionFragment groupFragment : groupFragments) {
                        processedFragments.add(groupFragment.getFragmentId());
                    }

                    // child that index > 0 needs to schedule the next round of fragments.
                    // eg: each node is a fragment
                    //           Union
                    //            │
                    //            │
                    //            │
                    //  ┌─────────┼─────────┐
                    //  │         │         │
                    //  ▼         ▼         ▼
                    // AGG(1)    AGG(2)    AGG(3)
                    //  │         │         │
                    //  │         │         │
                    //  ▼         ▼         ▼
                    //SCAN(1)   SCAN(2)   SCAN(3)
                    //
                    // in this case when AGG2/AGG3 fragment finished we should trigger next schedule
                    if (i != 0) {
                        child.setNeedReportFragmentFinish(true);
                    }
                }
                for (int i = fragments.size() - 1; i >= 0; i--) {
                    stack.push(fragments.get(i));
                }
            } else {
                for (int i = 0; i < fragment.childrenSize(); i++) {
                    final ExecutionFragment child = sequenceMap.get(fragment.getFragmentId()).getAt(i);
                    if (!scheduleFragmentInDegrees.get(child.getFragmentId()).equals(0)) {
                        continue;
                    }
                    if (i != 0) {
                        child.setNeedReportFragmentFinish(true);
                    }
                    stack.push(new PackedExecutionFragment(child));
                }
            }
        }

        if (!currentScheduleFragments.isEmpty()) {
            for (ExecutionFragment currentScheduleFragment : currentScheduleFragments) {
                waitScheduleFragments.remove(currentScheduleFragment.getFragmentId());
            }
            scheduleFragments.add(currentScheduleFragments);
            for (ExecutionFragment currentScheduleFragment : currentScheduleFragments) {
                if (currentScheduleFragment.childrenSize() == 0) {
                    return true;
                }
            }
        }

        return false;
    }

    // Input: set of N fragments
    // Output: a fragment scheduling sequence by topological ordering
    private List<List<ExecutionFragment>> buildScheduleOrder(List<List<ExecutionFragment>> scheduleFragments) {
        List<List<ExecutionFragment>> groups = Lists.newArrayList();
        // collect zero in-degree nodes
        Queue<ExecutionFragment> queue = Lists.newLinkedList();

        final Set<ExecutionFragment> fragments =
                scheduleFragments.stream().flatMap(Collection::stream).collect(Collectors.toUnmodifiableSet());

        for (ExecutionFragment fragment : fragments) {
            if (fragmentInDegrees.get(fragment.getFragmentId()).equals(0)) {
                queue.add(fragment);
            }
        }

        int totalFragments = fragments.size();

        final ExecutionFragment captureVersionFragment = dag.getCaptureVersionFragment();
        if (captureVersionFragment != null && !captureVersionFragment.isScheduled()) {
            queue.add(captureVersionFragment);
            totalFragments++;
        }

        // topological-sort for input fragments
        int scheduleFragmentNums = 0;
        while (!queue.isEmpty()) {
            int groupSize = queue.size();
            List<ExecutionFragment> group = new ArrayList<>(groupSize);
            // The next `groupSize` fragments can be delivered concurrently, because zero in-degree indicates that
            // they don't depend on each other and all the fragments depending on them have been delivered.
            for (int i = 0; i < groupSize; ++i) {
                ExecutionFragment fragment = Preconditions.checkNotNull(queue.poll());
                fragment.setIsScheduled(true);
                group.add(fragment);

                for (int j = 0; j < fragment.childrenSize(); j++) {
                    ExecutionFragment child = fragment.getChild(j);
                    final PlanFragmentId fragmentId = child.getFragmentId();
                    int degree = fragmentInDegrees.compute(fragmentId, (k, v) -> Preconditions.checkNotNull(v) - 1);
                    if (degree == 0 && fragments.contains(child)) {
                        queue.add(child);
                    }
                }
            }
            groups.add(group);
            scheduleFragmentNums += group.size();
        }

        if (scheduleFragmentNums != totalFragments) {
            throw new StarRocksPlannerException("invalid schedule plan",
                    ErrorType.INTERNAL_ERROR);
        }

        return groups;
    }

    public boolean isFinished() {
        return stack.isEmpty();
    }

}