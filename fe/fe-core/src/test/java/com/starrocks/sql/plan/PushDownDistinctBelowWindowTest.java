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

import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PushDownDistinctAggregateRewriter pre-aggregates the argument of a window function below the window
 * operator. The projections it rewrites on the way must not keep reading the argument column afterwards,
 * because the aggregation it inserted no longer produces it.
 * <p>
 * The resulting invalid projection is not visible in the final plan -- the column pruning this rule
 * schedules right after removes it -- but statistics derivation runs in between, in the
 * Utils.calculateStatistics that pushDownAggregation reaches through logicalJoinReorder, and fails there
 * with "missing statistic of col". That exception is swallowed, so these tests observe it at the throw
 * site instead of expecting the query to fail.
 */
public class PushDownDistinctBelowWindowTest extends PlanTestBase {

    private final List<String> missingStatistics = new ArrayList<>();

    @BeforeEach
    public void setUpPushDownDistinct() {
        missingStatistics.clear();
        // PlanTestBase turns aggregate push-down off, but the rewrite under test only runs together with
        // it, and it is on by default in production.
        connectContext.getSessionVariable().setCboPushDownAggregateMode(0);
        new MockUp<Statistics>() {
            @Mock
            public ColumnStatistic getColumnStatistic(Invocation invocation, ColumnRefOperator column) {
                try {
                    return invocation.proceed(column);
                } catch (StarRocksPlannerException e) {
                    missingStatistics.add(e.getMessage());
                    throw e;
                }
            }
        };
    }

    @AfterEach
    public void tearDownPushDownDistinct() {
        connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
    }

    private void assertStatisticsDerived(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertTrue(missingStatistics.isEmpty(),
                "statistics derivation failed with " + missingStatistics + " for: " + sql + "\n" + plan);
    }

    @Test
    public void testWindowArgumentIsNotKeptInProjection() throws Exception {
        assertStatisticsDerived("select distinct v1, sum(v3) over () from t0");
        assertStatisticsDerived("select distinct sum(v3) over () as s, v1 from t0 order by 1 limit 30, 1");
        assertStatisticsDerived("select distinct v1, v2, sum(v3) over (partition by v1 order by v2) from t0");
        assertStatisticsDerived("select distinct v1, sum(v3) over () as s, sum(v2) over () as t from t0");
        assertStatisticsDerived("select distinct t1a, sum(t1f) over (partition by t1b) from test_all_type");
    }

    /**
     * Nothing but the window function is selected here, so the projection that reads the dropped column
     * sits below the window and no window function is visible in it at all. This is the shape cluster
     * fuzzing reports as
     * "LogicalProjectOperator [1: c0, 3: c2, 15: sum] child size 1" over an aggregation of {c2, sum}.
     */
    @Test
    public void testOnlyTheWindowFunctionIsSelected() throws Exception {
        assertStatisticsDerived("select distinct sum(v1) over (partition by v3) from t0 limit 100");
        assertStatisticsDerived("select distinct sum(v1) over (partition by v3) from t0");
    }

    /**
     * The window argument is selected as well here, so it is a grouping key of the pushed-down
     * aggregation and does stay available; the projection must keep emitting it.
     */
    @Test
    public void testWindowArgumentThatIsAlsoSelectedIsKept() throws Exception {
        assertStatisticsDerived("select distinct v1, v3, sum(v3) over () from t0");
        assertStatisticsDerived("select distinct v1, v3, sum(v3) over (partition by v1) from t0");
    }

    /**
     * The rewrite itself must still happen: an aggregation is inserted below the window function.
     */
    @Test
    public void testDistinctIsStillPushedBelowWindow() throws Exception {
        String plan = getFragmentPlan("select distinct v1, sum(v3) over () from t0");
        assertTrue(plan.indexOf("ANALYTIC") < plan.lastIndexOf("AGGREGATE"),
                "expected an aggregation below the analytic node:\n" + plan);
    }
}
