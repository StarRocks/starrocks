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

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.thrift.TPlanNode;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// Verifies that the propagated group-by NDV estimate is sharded by the fragment instance
// count at schedule time, but only when the aggregation input is hash-shuffled on the
// grouping key. The FE divides by the instance count here; the BE further divides by the
// per-instance pipeline DOP, yielding a per-driver reserve of NDV / (instances * dop).
// See ExecutionFragment#scaleAggReserveEstimateByInstanceCount and
// AggregationNode#scaleReserveEstimateByInstances.
public class AggReserveInstanceShardTest extends PlanWithCostTestBase {
    private static final long T0_ROWS = 1_000_000_000L;
    private static final int NUM_INSTANCES = 4;

    @BeforeEach
    public void before() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        setTableStatistics(t0, T0_ROWS);

        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                // v1: high NDV -> agg cardinality well above the min-reserve threshold.
                ss.getColumnStatistic(t0, "v1");
                result = new ColumnStatistic(1, T0_ROWS, 0, 8, T0_ROWS / 2.0);
                minTimes = 0;
                // v2: aggregated column, arbitrary known stat.
                ss.getColumnStatistic(t0, "v2");
                result = new ColumnStatistic(1, 1000, 0, 8, 1000);
                minTimes = 0;
            }
        };
    }

    @Test
    public void shardsEstimateWhenHashShuffled() throws Exception {
        AggregationNode aggNode = planEstimateAgg();
        long rawEstimate = serializedEstimate(aggNode);
        // Hash-shuffled input: each instance owns a disjoint key shard.
        DataPartition hashShuffle = DataPartition.hashPartitioned(aggNode.getAggInfo().getGroupingExprs());
        Assertions.assertEquals(rawEstimate / NUM_INSTANCES,
                scaleAndSerialize(aggNode, hashShuffle, NUM_INSTANCES),
                "hash-shuffled aggregation estimate must be sharded by the instance count");
    }

    @Test
    public void keepsFullEstimateWhenColocated() throws Exception {
        AggregationNode aggNode = planEstimateAgg();
        long rawEstimate = serializedEstimate(aggNode);
        // Co-located (non-hash-partitioned) input: every instance can see the full keyspace.
        Assertions.assertEquals(rawEstimate,
                scaleAndSerialize(aggNode, DataPartition.RANDOM, NUM_INSTANCES),
                "co-located aggregation must keep the full cluster-wide estimate");
    }

    @Test
    public void keepsFullEstimateForSingleInstance() throws Exception {
        AggregationNode aggNode = planEstimateAgg();
        long rawEstimate = serializedEstimate(aggNode);
        // A single instance owns the whole keyspace even under a hash shuffle.
        DataPartition hashShuffle = DataPartition.hashPartitioned(aggNode.getAggInfo().getGroupingExprs());
        Assertions.assertEquals(rawEstimate, scaleAndSerialize(aggNode, hashShuffle, 1),
                "a single-instance aggregation must keep the full estimate");
    }

    // Plans a high-NDV aggregation and returns the AggregationNode carrying the NDV estimate.
    private AggregationNode planEstimateAgg() throws Exception {
        ExecPlan plan = getExecPlan("select v1, count(v2) from t0 group by v1");
        for (PlanFragment fragment : plan.getFragments()) {
            for (PlanNode node : fragment.collectNodes()) {
                if (node instanceof AggregationNode && serializedEstimate(node) > 0) {
                    return (AggregationNode) node;
                }
            }
        }
        throw new AssertionError("expected an aggregation carrying an NDV estimate");
    }

    // Wraps the agg node in a synthetic fragment with the given input partition, attaches
    // numInstances instances, runs the schedule-time sharding, and returns the estimate the
    // node would serialize to the BE.
    private static long scaleAndSerialize(AggregationNode aggNode, DataPartition inputPartition, int numInstances) {
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(900), aggNode, inputPartition);
        ExecutionFragment execFragment = new ExecutionFragment(null, fragment, 0);
        for (int i = 0; i < numInstances; i++) {
            execFragment.addInstance(new FragmentInstance(null, execFragment));
        }
        execFragment.scaleAggReserveEstimateByInstanceCount();
        return serializedEstimate(aggNode);
    }

    private static long serializedEstimate(PlanNode aggNode) {
        for (TPlanNode tnode : aggNode.treeToThrift().getNodes()) {
            if (tnode.isSetAgg_node() && tnode.getAgg_node().isSetEstimated_cardinality()) {
                return tnode.getAgg_node().getEstimated_cardinality();
            }
        }
        return -1;
    }
}
