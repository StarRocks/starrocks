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

package com.starrocks.qe.scheduler;

import com.starrocks.planner.ScanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.thrift.TUniqueId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

public class RegisterLateInstanceTest extends SchedulerTestBase {

    private static ExecutionFragment findScanFragment(ExecutionDAG dag) {
        return dag.getFragmentsInPostorder().stream()
                .filter(fragment -> !fragment.getScanNodes().isEmpty())
                .filter(fragment -> fragment.getScanNodes().stream().map(ScanNode::getId).findAny().isPresent())
                .findFirst()
                .orElseThrow();
    }

    @Test
    public void testRegisterLateInstanceInvariants() throws Exception {
        DefaultCoordinator coordinator = startScheduling("select L_ORDERKEY from lineitem");
        ExecutionDAG dag = coordinator.getExecutionDAG();
        ExecutionFragment scanFragment = findScanFragment(dag);

        int totalInstances = dag.getInstanceIds().size();
        int fragmentInstances = scanFragment.getInstances().size();
        Set<TUniqueId> existingIds = new HashSet<>(dag.getInstanceIds());
        int workerInstancesBefore = countInstancesOfWorker(dag, backend2.getId());

        FragmentInstance late = dag.registerLateInstance(scanFragment, backend2);

        // Dense indexInJob continues after the last finalized instance.
        Assertions.assertEquals(totalInstances, late.getIndexInJob());
        // Appended to the fragment without reordering existing instances.
        Assertions.assertEquals(fragmentInstances, late.getIndexInFragment());
        Assertions.assertEquals(fragmentInstances + 1, scanFragment.getInstances().size());
        Assertions.assertSame(late, scanFragment.getInstances().get(fragmentInstances));
        // Fresh, collision-free instance id, registered for lookups.
        Assertions.assertFalse(existingIds.contains(late.getInstanceId()));
        Assertions.assertSame(late, dag.getInstanceByInstanceId(late.getInstanceId()));
        // Worker instance accounting includes the late instance.
        Assertions.assertEquals(workerInstancesBefore + 1, dag.getNumInstancesOfWorkerId(backend2.getId()));

        // A second registration keeps the sequence dense.
        FragmentInstance late2 = dag.registerLateInstance(scanFragment, backend3);
        Assertions.assertEquals(totalInstances + 1, late2.getIndexInJob());
        Assertions.assertEquals(fragmentInstances + 1, late2.getIndexInFragment());
        Assertions.assertNotEquals(late.getInstanceId(), late2.getInstanceId());

        // A reserved index that is never used just leaves a gap in the sequence.
        int reserved = dag.reserveLateIndexInJob();
        Assertions.assertEquals(totalInstances + 2, reserved);
        FragmentInstance late3 = dag.registerLateInstance(scanFragment, backend2);
        Assertions.assertEquals(totalInstances + 3, late3.getIndexInJob());
    }

    @Test
    public void testExistingInstancesUntouchedByLateRegistration() throws Exception {
        DefaultCoordinator coordinator = startScheduling("select L_ORDERKEY from lineitem");
        ExecutionDAG dag = coordinator.getExecutionDAG();
        ExecutionFragment scanFragment = findScanFragment(dag);

        int[] indexInJobBefore = scanFragment.getInstances().stream()
                .mapToInt(FragmentInstance::getIndexInJob).toArray();
        int[] indexInFragmentBefore = scanFragment.getInstances().stream()
                .mapToInt(FragmentInstance::getIndexInFragment).toArray();

        dag.registerLateInstance(scanFragment, backend2);

        // GRF component ordinals depend on the original ordering: it must be unchanged.
        for (int i = 0; i < indexInJobBefore.length; i++) {
            Assertions.assertEquals(indexInJobBefore[i], scanFragment.getInstances().get(i).getIndexInJob());
            Assertions.assertEquals(indexInFragmentBefore[i],
                    scanFragment.getInstances().get(i).getIndexInFragment());
        }
    }

    @Test
    public void testRejectLateInstanceOnRuntimeFilterBuilder() throws Exception {
        // A hash join builds a global runtime filter; its build fragment sizes the RF merge layout at
        // plan time and cannot safely grow the builder set mid-query.
        DefaultCoordinator coordinator =
                startScheduling("select count(*) from lineitem join orders on l_orderkey = o_orderkey");
        ExecutionDAG dag = coordinator.getExecutionDAG();
        ExecutionFragment rfBuilder = dag.getFragmentsInPostorder().stream()
                .filter(fragment -> !fragment.getPlanFragment().getBuildRuntimeFilters().isEmpty())
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected a runtime-filter build fragment"));

        Assertions.assertThrows(IllegalStateException.class,
                () -> dag.registerLateInstance(rfBuilder, backend2));
    }

    private static int countInstancesOfWorker(ExecutionDAG dag, long workerId) {
        return (int) dag.getInstances().stream().filter(instance -> instance.getWorkerId() == workerId).count();
    }
}
