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

package com.starrocks.qe;

import com.starrocks.common.ConfigBase;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.sql.plan.DistributedEnvPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUniqueId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.stream.Collectors;

public class AdaptiveChooseNodesTest extends DistributedEnvPlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testDecreaseNodesInPipeline() throws Exception {
        ConfigBase.setMutableConfig("adaptive_choose_instances_threshold", "3");
        connectContext.getSessionVariable().setChooseExecuteInstancesMode("auto");
        connectContext.getSessionVariable().setPipelineDop(2);
        connectContext.setExecutionId(new TUniqueId(0x33, 0x0));
        String sql = "select count(*) from lineorder_new_l group by P_SIZE";
        ExecPlan execPlan = getExecPlan(sql);
        CoordinatorPreprocessor prepare = new CoordinatorPreprocessor(execPlan.getFragments(), execPlan.getScanNodes(),
                connectContext);
        prepare.computeFragmentInstances();

        ExecutionDAG dag = prepare.getExecutionDAG();

        // bottom fragment use 3 nodes
        Assert.assertEquals(3, dag.getFragmentsInCreatedOrder().get(2).getInstances().size());

        // remote fragment use 1 nodes
        Assert.assertEquals(1, dag.getFragmentsInCreatedOrder().get(1).getInstances().size());
        Assert.assertEquals(1, dag.getFragmentsInCreatedOrder().get(1).getNumSendersPerExchange().size());

        connectContext.getSessionVariable().setEnablePipelineEngine(false);
        execPlan = getExecPlan(sql);
        prepare = new CoordinatorPreprocessor(execPlan.getFragments(), execPlan.getScanNodes(), connectContext);
        prepare.computeFragmentInstances();

        dag = prepare.getExecutionDAG();

        // bottom fragment use 3 nodes
        Assert.assertEquals(3, dag.getFragmentsInCreatedOrder().get(2).getInstances().size());

        // not in pipeline engine, decrease nodes to 1. The instance exec param size also be 3.
        Assert.assertEquals(3, dag.getFragmentsInCreatedOrder().get(1).getInstances().size());
        Assert.assertEquals(1, dag.getFragmentsInCreatedOrder().get(1).getInstances()
                .stream().map(e -> e.getWorkerId()).collect(Collectors.toSet()).size());
        Assert.assertEquals(1, dag.getFragmentsInCreatedOrder().get(1).getNumSendersPerExchange().size());
    }

    @Test
    public void testIncreaseNodes() throws Exception {
        ConfigBase.setMutableConfig("adaptive_choose_instances_threshold", "3");
        connectContext.getSessionVariable().setChooseExecuteInstancesMode("auto");
        connectContext.getSessionVariable().setPipelineDop(2);
        connectContext.setExecutionId(new TUniqueId(0x33, 0x0));
        String sql = "select * from (select * from skew_table where id = 1) t " +
                "join (select abs(id) abs from skew_table where id = 1) tt on t.id = tt.abs";
        ExecPlan execPlan = getExecPlan(sql);
        CoordinatorPreprocessor prepare = new CoordinatorPreprocessor(execPlan.getFragments(), execPlan.getScanNodes(),
                connectContext);
        prepare.computeFragmentInstances();

        ExecutionDAG dag = prepare.getExecutionDAG();
        // scan fragment only hit one tablet use 1 nodes
        Assert.assertEquals(1, dag.getFragmentsInCreatedOrder().get(2).getInstances().size());
        Assert.assertEquals(1, dag.getFragmentsInCreatedOrder().get(3).getInstances().size());

        // join fragment use 3 nodes
        Assert.assertEquals(3, dag.getFragmentsInCreatedOrder().get(1).getInstances().size());
    }
}
