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

import com.google.api.client.util.Lists;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import mockit.Mock;
import mockit.MockUp;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class PhasedScheduleTest extends SchedulerTestBase {
    @Test
    public void testSchedule() throws Exception {
        connectContext.getSessionVariable().setEnablePhasedScheduler(true);
        connectContext.getSessionVariable().setPhasedSchedulerMaxConcurrency(1);

        String sql = "select count(1) from lineitem " +
                "UNION ALL select count(1) from lineitem " +
                "UNION ALL select count(1) from lineitem";

        Set<FragmentInstanceExecState> dispatched = Sets.newHashSet();
        // deploy
        new MockUp<Deployer>() {
            @Mock
            public void deployFragments(DeployState deployState) {
                final List<List<FragmentInstanceExecState>> state =
                        deployState.getThreeStageExecutionsToDeploy();
                for (List<FragmentInstanceExecState> execStates : state) {
                    dispatched.addAll(execStates);
                }
            }
        };

        // firstly schedule
        final DefaultCoordinator coordinator = startScheduling(sql);
        final ExecutionDAG executionDAG = coordinator.getExecutionDAG();
        reportScan(Sets.newHashSet(dispatched), executionDAG, coordinator);

        Set<FragmentInstanceExecState> noDispatched = Sets.newHashSet();
        for (FragmentInstanceExecState execution : executionDAG.getExecutions()) {
            if (!dispatched.contains(execution)) {
                noDispatched.add(execution);
            }
        }
        Assert.assertFalse(noDispatched.isEmpty());
        reportScan(noDispatched, executionDAG, coordinator);

    }

    @Test
    public void testPhasedScheduleWithCTE() throws Exception {
        connectContext.getSessionVariable().setEnablePhasedScheduler(true);
        connectContext.getSessionVariable().setPhasedSchedulerMaxConcurrency(1);

        String sql =
                "with a as (select count(*) from lineitem) " +
                        "select /*+SET_VAR(cbo_cte_reuse_rate=0) */ * from a union all select * from a;";

        // firstly schedule
        final DefaultCoordinator coordinator = startScheduling(sql);
        final ExecutionDAG executionDAG = coordinator.getExecutionDAG();
        final int size = executionDAG.getExecutions().size();
        Assert.assertEquals(size, 11);
    }

    @Test
    public void testScheduleWithParallelReport() throws Exception {
        connectContext.getSessionVariable().setEnablePhasedScheduler(true);
        connectContext.getSessionVariable().setPhasedSchedulerMaxConcurrency(1);

        String sql = "select count(1) from lineitem " +
                "UNION ALL select count(1) from lineitem " +
                "UNION ALL select count(1) from lineitem";

        Set<FragmentInstanceExecState> dispatched = Sets.newHashSet();
        // deploy
        new MockUp<Deployer>() {
            @Mock
            public void deployFragments(DeployState deployState) {
                final List<List<FragmentInstanceExecState>> state =
                        deployState.getThreeStageExecutionsToDeploy();
                for (List<FragmentInstanceExecState> execStates : state) {
                    dispatched.addAll(execStates);
                }
            }
        };

        // firstly schedule
        final DefaultCoordinator coordinator = startScheduling(sql);
        final ExecutionDAG executionDAG = coordinator.getExecutionDAG();

        parallelReport(Sets.newHashSet(dispatched), executionDAG, coordinator);

        Set<FragmentInstanceExecState> noDispatched = Sets.newHashSet();
        for (FragmentInstanceExecState execution : executionDAG.getExecutions()) {
            if (!dispatched.contains(execution)) {
                noDispatched.add(execution);
            }
        }
        Assert.assertFalse(noDispatched.isEmpty());

        parallelReport(noDispatched, executionDAG, coordinator);

    }

    @Test
    public void testScheduleWithException() throws Exception {
        connectContext.getSessionVariable().setEnablePhasedScheduler(true);
        connectContext.getSessionVariable().setPhasedSchedulerMaxConcurrency(1);

        String sql = "select count(1) from lineitem " +
                "UNION ALL select count(1) from lineitem " +
                "UNION ALL select count(1) from lineitem";

        Set<FragmentInstanceExecState> dispatched = Sets.newHashSet();
        // deploy
        new MockUp<FragmentInstanceExecState>() {
            @Mock
            public void deployAsync() {

            }

            @Mock
            public FragmentInstanceExecState.DeploymentResult waitForDeploymentCompletion(long deployTimeoutMs) {
                return new FragmentInstanceExecState.DeploymentResult(TStatusCode.CANCELLED,
                        "QueryFinished", null);
            }
        };

        // firstly schedule
        final DefaultCoordinator coordinator = startScheduling(sql);
        final ExecutionDAG executionDAG = coordinator.getExecutionDAG();

        parallelReport(Sets.newHashSet(dispatched), executionDAG, coordinator);

        executionDAG.getExecutions();

    }

    private void reportScan(Collection<FragmentInstanceExecState> instances, ExecutionDAG dag, Coordinator coordinator)
            throws Exception {
        for (FragmentInstanceExecState execution : instances) {
            final FragmentInstance instance =
                    dag.getInstanceByInstanceId(execution.getInstanceId());
            boolean isleaf = !instance.getExecFragment().getScanNodes().isEmpty();
            if (isleaf) {
                coordinator.scheduleNextTurn(execution.getInstanceId());
            }
        }
    }

    private void parallelReport(Collection<FragmentInstanceExecState> instances, ExecutionDAG dag,
                                Coordinator coordinator) throws Exception {
        final List<TUniqueId> uniqueIds = Lists.newArrayList();
        for (FragmentInstanceExecState execution : instances) {
            final FragmentInstance instance =
                    dag.getInstanceByInstanceId(execution.getInstanceId());
            boolean isleaf = !instance.getExecFragment().getScanNodes().isEmpty();
            if (isleaf) {
                uniqueIds.add(execution.getInstanceId());
            }
        }
        uniqueIds.stream().parallel().forEach(i -> {
            try {
                coordinator.scheduleNextTurn(i);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }
}
