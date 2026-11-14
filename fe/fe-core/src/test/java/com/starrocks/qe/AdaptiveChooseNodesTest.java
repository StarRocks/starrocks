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

import com.google.api.client.util.Lists;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.scheduler.assignment.RemoteFragmentAssignmentStrategy;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.sql.plan.DistributedEnvPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUniqueId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AdaptiveChooseNodesTest extends DistributedEnvPlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @Test
    @Order(1)
    void testIncreaseNodes() throws Exception {
        ConfigBase.setMutableConfig("adaptive_choose_instances_threshold", "3", false, "");
        connectContext.getSessionVariable().setChooseExecuteInstancesMode("auto");
        connectContext.getSessionVariable().setPipelineDop(2);
        connectContext.setExecutionId(new TUniqueId(0x33, 0x0));
        connectContext.getSessionVariable().setPreferComputeNode(true);
        String sql = "select * from (select * from skew_table where id = 1) t " +
                "join (select abs(id) abs from skew_table where id = 1) tt on t.id = tt.abs";
        ExecPlan execPlan = getExecPlan(sql);
        CoordinatorPreprocessor prepare = new CoordinatorPreprocessor(execPlan.getFragments(), execPlan.getScanNodes(),
                connectContext);
        prepare.computeFragmentInstances();

        ExecutionDAG dag = prepare.getExecutionDAG();
        // scan fragment only hit one tablet use 1 nodes
        Assertions.assertEquals(1, dag.getFragmentsInCreatedOrder().get(2).getInstances().size());
        Assertions.assertEquals(1, dag.getFragmentsInCreatedOrder().get(3).getInstances().size());

        // join fragment only use compute nodes
        Assertions.assertEquals(1, dag.getFragmentsInCreatedOrder().get(1).getInstances().size());
        Assertions.assertEquals(10004L, (long) dag.getFragmentsInCreatedOrder().get(1).getInstances().get(0).getWorkerId());

        connectContext.getSessionVariable().setPreferComputeNode(false);
        execPlan = getExecPlan(sql);
        prepare = new CoordinatorPreprocessor(execPlan.getFragments(), execPlan.getScanNodes(),
                connectContext);
        prepare.computeFragmentInstances();

        dag = prepare.getExecutionDAG();
        // scan fragment only hit one tablet use 1 nodes
        Assertions.assertEquals(1, dag.getFragmentsInCreatedOrder().get(2).getInstances().size());
        Assertions.assertEquals(1, dag.getFragmentsInCreatedOrder().get(3).getInstances().size());

        // join fragment use all be nodes
        Assertions.assertEquals(3, dag.getFragmentsInCreatedOrder().get(1).getInstances().size());
    }

    @Test
    @Order(2)
    void testDecreaseNodesInPipeline() throws Exception {
        ConfigBase.setMutableConfig("adaptive_choose_instances_threshold", "3", false, "");
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
        Assertions.assertEquals(3, dag.getFragmentsInCreatedOrder().get(2).getInstances().size());

        // remote fragment use 1 nodes
        Assertions.assertEquals(1, dag.getFragmentsInCreatedOrder().get(1).getInstances().size());
        Assertions.assertEquals(1, dag.getFragmentsInCreatedOrder().get(1).getNumSendersPerExchange().size());

        connectContext.getSessionVariable().setEnablePipelineEngine(false);
        execPlan = getExecPlan(sql);
        prepare = new CoordinatorPreprocessor(execPlan.getFragments(), execPlan.getScanNodes(), connectContext);
        prepare.computeFragmentInstances();

        dag = prepare.getExecutionDAG();

        // bottom fragment use 3 nodes
        Assertions.assertEquals(3, dag.getFragmentsInCreatedOrder().get(2).getInstances().size());

        // not in pipeline engine, decrease nodes to 1. The instance exec param size also be 3.
        Assertions.assertEquals(3, dag.getFragmentsInCreatedOrder().get(1).getInstances().size());
        Assertions.assertEquals(1, dag.getFragmentsInCreatedOrder().get(1).getInstances()
                .stream().map(e -> e.getWorkerId()).collect(Collectors.toSet()).size());
        Assertions.assertEquals(1, dag.getFragmentsInCreatedOrder().get(1).getNumSendersPerExchange().size());
    }

    @ParameterizedTest
    @MethodSource("getCases")
    public void testNumberEstimateFormula(long outputOfMostLeftChild, long maxOutputOfRightChild, int dop,
                                          int candidateSize, long expected) {
        Assertions.assertEquals(expected,
                RemoteFragmentAssignmentStrategy.getOptimalNodeNums(outputOfMostLeftChild,
                        maxOutputOfRightChild, dop, candidateSize));
    }

    /**
     * A fragment always runs on at least one node. Zero nodes means zero instances, and deployment
     * asserts on that, so the query dies with a bare IllegalStateException instead of running.
     * <p>
     * Zero is what the formula produced whenever the right child's cardinality was not positive -
     * including the -1 that PlanNode carries for "unknown", which is what information_schema scans
     * report because they never estimate a row count. That is why joining two information_schema
     * tables could fail outright under an instance-decreasing mode.
     */
    @ParameterizedTest
    @MethodSource("getNonPositiveRightCardinalityCases")
    public void testNeverChoosesZeroNodes(long outputOfMostLeftChild, long maxOutputOfRightChild, int dop,
                                          int candidateSize) {
        long nodeNums = RemoteFragmentAssignmentStrategy.getOptimalNodeNums(outputOfMostLeftChild,
                maxOutputOfRightChild, dop, candidateSize);
        Assertions.assertTrue(nodeNums >= 1,
                "a fragment must run on at least one node, got " + nodeNums + " for right cardinality "
                        + maxOutputOfRightChild);
    }

    private static Stream<Arguments> getNonPositiveRightCardinalityCases() {
        List<Arguments> cases = Lists.newArrayList();

        // Unknown cardinality: what an information_schema scan reports. -1 / 50000 / dop lands in
        // (-1, 0), and Math.ceil of that is -0.0, which narrows to 0.
        cases.add(Arguments.of(1000L, -1L, 1, 16));
        cases.add(Arguments.of(1000000L, -1L, 16, 128));

        // Both children unknown - the shape of the reported information_schema join.
        cases.add(Arguments.of(-1L, -1L, 8, 32));

        // An empty right child: a genuine zero, not an unknown.
        cases.add(Arguments.of(1000000L, 0L, 16, 16));

        return cases.stream();
    }

    private static Stream<Arguments> getCases() {
        List<Arguments> cases = Lists.newArrayList();

        // very small data only need one node
        cases.add(Arguments.of(10000L, 10000L, 8, 16, 1L));

        // left table 1 million rows, right table 10000 rows, 16 dop, need one node
        cases.add(Arguments.of(1000000L, 10000L, 16, 16, 1));

        // left table 1 million rows, right table 1 million rows, 16 dop, need 2 nodes
        cases.add(Arguments.of(1000000L, 1000000L, 16, 16, 2));

        // left table 1 million rows, right table 100 million rows, 16 dop, need 125 nodes
        cases.add(Arguments.of(1000000L, 100000000L, 16, 128, 125));

        // left table 100 million rows, right table 1 million rows, 16 dop, need 10 nodes
        cases.add(Arguments.of(100000000L, 1000000L, 16, 16, 10));

        // left table 10 billion rows, right table 1 million rows, 16 dop, need 18 nodes
        cases.add(Arguments.of(10000000000L, 1000000L, 16, 32, 18));

        return cases.stream();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setChooseExecuteInstancesMode("locality");
        ConfigBase.setMutableConfig("adaptive_choose_instances_threshold", "32", false, "");
    }


}
