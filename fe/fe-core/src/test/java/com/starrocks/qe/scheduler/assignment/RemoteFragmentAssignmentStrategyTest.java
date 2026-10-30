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

import com.starrocks.common.StarRocksException;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.system.ComputeNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteFragmentAssignmentStrategyTest {

    private ConnectContext connectContext;
    private SessionVariable sessionVariable;
    private WorkerProvider workerProvider;

    private ExecutionFragment gatherExecFragment;
    private PlanFragment gatherPlanFragment;

    private ExecutionFragment childExecFragment;
    private ExecutionDAG executionDAG;

    @BeforeEach
    void setUp() {
        connectContext = mock(ConnectContext.class);
        sessionVariable = mock(SessionVariable.class);
        workerProvider = mock(WorkerProvider.class);
        gatherExecFragment = mock(ExecutionFragment.class);
        gatherPlanFragment = mock(PlanFragment.class);
        childExecFragment = mock(ExecutionFragment.class);
        executionDAG = mock(ExecutionDAG.class);

        when(connectContext.getSessionVariable()).thenReturn(sessionVariable);

        // Default: both optimizations off
        when(sessionVariable.isEnableGatherFragmentLocalityOptimization()).thenReturn(false);
        when(sessionVariable.isEnableGatherFragmentChildAffinity()).thenReturn(false);

        // Gather PlanFragment: no children (so isCTEConsumerFragment=false), isGatherFragment=true
        when(gatherExecFragment.getPlanFragment()).thenReturn(gatherPlanFragment);
        when(gatherPlanFragment.getChildren()).thenReturn(new ArrayList<>());
        when(gatherPlanFragment.isGatherFragment()).thenReturn(true);

        // DAG: contains gather + child fragment (used by findCommonWorkerIdForOtherFragments)
        when(gatherExecFragment.getExecutionDAG()).thenReturn(executionDAG);
        when(executionDAG.getFragmentsInCreatedOrder())
                .thenReturn(List.of(gatherExecFragment, childExecFragment));

        // Gather has one child execution fragment
        when(gatherExecFragment.childrenSize()).thenReturn(1);
        when(gatherExecFragment.getChild(0)).thenReturn(childExecFragment);

        // Default: child fragment has no instances (prevents NPE in findCommonWorkerIdForOtherFragments)
        when(childExecFragment.getInstances()).thenReturn(Collections.emptyList());

        // WorkerProvider: getWorkerById(id) returns a ComputeNode stub whose getId() == id
        when(workerProvider.getWorkerById(anyLong())).thenAnswer(inv -> {
            ComputeNode node = mock(ComputeNode.class);
            when(node.getId()).thenReturn((Long) inv.getArgument(0));
            return node;
        });
    }

    private RemoteFragmentAssignmentStrategy strategy(Random random) {
        return new RemoteFragmentAssignmentStrategy(
                connectContext, workerProvider, /*usePipeline=*/true, /*isGatherOutput=*/true, random);
    }

    /**
     * Stubs the child fragment to have instances on the given worker IDs.
     * Used both for {@code selectWorkerConstrainedToChildren} (via childrenSize/getChild)
     * and for {@code findCommonWorkerIdForOtherFragments} (via getInstances).
     */
    private void withChildWorkers(long... workerIds) {
        List<FragmentInstance> instances = new ArrayList<>();
        for (long id : workerIds) {
            FragmentInstance fi = mock(FragmentInstance.class);
            when(fi.getWorkerId()).thenReturn(id);
            instances.add(fi);
        }
        when(childExecFragment.getInstances()).thenReturn(instances);
    }

    // -------------------------------------------------------------------------
    // Test 1: child affinity picks a worker from the child fragment's worker set
    // -------------------------------------------------------------------------

    @Test
    void testChildAffinity_picksFromChildWorkers() throws StarRocksException {
        when(sessionVariable.isEnableGatherFragmentChildAffinity()).thenReturn(true);
        // Child fragment spans 3 workers — findCommonWorkerIdForOtherFragments returns null (>1 unique)
        withChildWorkers(1L, 2L, 3L);

        ArgumentCaptor<FragmentInstance> captor = ArgumentCaptor.forClass(FragmentInstance.class);
        strategy(new Random(0)).assignFragmentToWorker(gatherExecFragment);

        verify(gatherExecFragment).addInstance(captor.capture());
        long assignedId = captor.getValue().getWorkerId();
        assertTrue(Set.of(1L, 2L, 3L).contains(assignedId),
                "Gather fragment must land on one of the child workers, got: " + assignedId);
        verify(workerProvider).selectWorkerUnchecked(assignedId);
        verify(workerProvider, never()).selectNextWorker();
    }

    // -------------------------------------------------------------------------
    // Test 2: child affinity disabled → falls back to global round-robin
    // -------------------------------------------------------------------------

    @Test
    void testChildAffinity_disabled_fallsBackToRoundRobin() throws StarRocksException {
        when(sessionVariable.isEnableGatherFragmentChildAffinity()).thenReturn(false);
        when(workerProvider.selectNextWorker()).thenReturn(99L);
        withChildWorkers(1L, 2L, 3L);

        ArgumentCaptor<FragmentInstance> captor = ArgumentCaptor.forClass(FragmentInstance.class);
        strategy(new Random(0)).assignFragmentToWorker(gatherExecFragment);

        verify(gatherExecFragment).addInstance(captor.capture());
        assertEquals(99L, captor.getValue().getWorkerId());
        verify(workerProvider).selectNextWorker();
        verify(workerProvider, never()).selectWorkerUnchecked(anyLong());
    }

    // -------------------------------------------------------------------------
    // Test 3: child affinity enabled but no child instances → falls back to round-robin
    // -------------------------------------------------------------------------

    @Test
    void testChildAffinity_noChildInstances_fallsBackToRoundRobin() throws StarRocksException {
        when(sessionVariable.isEnableGatherFragmentChildAffinity()).thenReturn(true);
        // childrenSize = 0 → selectWorkerConstrainedToChildren finds no workers
        when(gatherExecFragment.childrenSize()).thenReturn(0);
        when(workerProvider.selectNextWorker()).thenReturn(99L);

        ArgumentCaptor<FragmentInstance> captor = ArgumentCaptor.forClass(FragmentInstance.class);
        strategy(new Random(0)).assignFragmentToWorker(gatherExecFragment);

        verify(gatherExecFragment).addInstance(captor.capture());
        assertEquals(99L, captor.getValue().getWorkerId());
        verify(workerProvider).selectNextWorker();
    }

    // -------------------------------------------------------------------------
    // Test 4: locality optimization wins when all other fragments share one worker
    // -------------------------------------------------------------------------

    @Test
    void testLocalityOptimization_singleCommonWorker_takesOverChildAffinity() throws StarRocksException {
        when(sessionVariable.isEnableGatherFragmentLocalityOptimization()).thenReturn(true);
        when(sessionVariable.isEnableGatherFragmentChildAffinity()).thenReturn(true);
        // All child instances on worker 5 → findCommonWorkerIdForOtherFragments returns 5L
        withChildWorkers(5L, 5L);

        ArgumentCaptor<FragmentInstance> captor = ArgumentCaptor.forClass(FragmentInstance.class);
        strategy(new Random(0)).assignFragmentToWorker(gatherExecFragment);

        verify(gatherExecFragment).addInstance(captor.capture());
        assertEquals(5L, captor.getValue().getWorkerId(),
                "Locality optimization should pin gather to the single common worker");
        // Common-worker path does not call selectWorkerUnchecked or selectNextWorker
        verify(workerProvider, never()).selectWorkerUnchecked(anyLong());
        verify(workerProvider, never()).selectNextWorker();
    }

    // -------------------------------------------------------------------------
    // Test 5: locality optimization on, no common worker → falls through to child affinity
    // -------------------------------------------------------------------------

    @Test
    void testLocalityOptimization_noCommonWorker_fallsThroughToChildAffinity() throws StarRocksException {
        when(sessionVariable.isEnableGatherFragmentLocalityOptimization()).thenReturn(true);
        when(sessionVariable.isEnableGatherFragmentChildAffinity()).thenReturn(true);
        // 2 distinct workers → findCommonWorkerIdForOtherFragments returns null
        withChildWorkers(1L, 2L);

        ArgumentCaptor<FragmentInstance> captor = ArgumentCaptor.forClass(FragmentInstance.class);
        strategy(new Random(0)).assignFragmentToWorker(gatherExecFragment);

        verify(gatherExecFragment).addInstance(captor.capture());
        long assignedId = captor.getValue().getWorkerId();
        assertTrue(Set.of(1L, 2L).contains(assignedId),
                "Should fall through to child affinity when no common worker, got: " + assignedId);
        verify(workerProvider).selectWorkerUnchecked(assignedId);
        verify(workerProvider, never()).selectNextWorker();
    }

    // -------------------------------------------------------------------------
    // Test 6: locality optimization on, no common worker, child affinity off
    // -> falls back to round-robin (nested else-branch inside enableOptimization)
    // -------------------------------------------------------------------------

    @Test
    void testLocalityOptimization_noCommonWorker_childAffinityDisabled_fallsBackToRoundRobin()
            throws StarRocksException {
        when(sessionVariable.isEnableGatherFragmentLocalityOptimization()).thenReturn(true);
        when(sessionVariable.isEnableGatherFragmentChildAffinity()).thenReturn(false);
        // 2 distinct workers → findCommonWorkerIdForOtherFragments returns null
        withChildWorkers(1L, 2L);
        when(workerProvider.selectNextWorker()).thenReturn(99L);

        ArgumentCaptor<FragmentInstance> captor = ArgumentCaptor.forClass(FragmentInstance.class);
        strategy(new Random(0)).assignFragmentToWorker(gatherExecFragment);

        verify(gatherExecFragment).addInstance(captor.capture());
        assertEquals(99L, captor.getValue().getWorkerId());
        verify(workerProvider).selectNextWorker();
        verify(workerProvider, never()).selectWorkerUnchecked(anyLong());
    }
}
