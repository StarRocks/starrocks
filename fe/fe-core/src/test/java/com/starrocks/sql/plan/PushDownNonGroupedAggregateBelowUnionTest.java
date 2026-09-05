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

import com.starrocks.common.Config;
import com.starrocks.qe.SessionVariableConstants;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PushDownNonGroupedAggregateBelowUnionTest extends PlanTestBase {
    private static final long UNION_INPUT_ROW_COUNT = 100000000L;
    private static final String UNION_ALL_INPUT =
            "(SELECT v1 AS v FROM t0 UNION ALL SELECT v4 AS v FROM t1)";

    private void setUnionInputStatistics(long rowCount) {
        setTableStatistics(getOlapTable("t0"), rowCount);
        setTableStatistics(getOlapTable("t1"), rowCount);
        setTableStatistics(getOlapTable("t2"), rowCount);
    }

    private String getPlan(String sql, boolean enable) throws Exception {
        return getPlan(sql, enable, SessionVariableConstants.AggregationStage.TWO_STAGE.ordinal());
    }

    private String getPlan(String sql, boolean enable, int aggStage) throws Exception {
        int oldAggStage = connectContext.getSessionVariable().getNewPlannerAggStage();
        boolean oldEnable = Config.push_down_non_grouped_aggregate_below_union;
        try {
            connectContext.getSessionVariable().setNewPlanerAggStage(aggStage);
            Config.push_down_non_grouped_aggregate_below_union = enable;
            setUnionInputStatistics(UNION_INPUT_ROW_COUNT);
            return getVerboseExplain(sql);
        } finally {
            Config.push_down_non_grouped_aggregate_below_union = oldEnable;
            setUnionInputStatistics(0);
            connectContext.getSessionVariable().setNewPlanerAggStage(oldAggStage);
        }
    }

    private void assertPushDownPlanShape(String plan) {
        assertContains(plan,
                "AGGREGATE (merge finalize)\n" +
                        "  |  aggregate:",
                "AGGREGATE (merge serialize)\n" +
                        "  |  aggregate:",
                "0:UNION\n" +
                        "  |  output exprs:\n" +
                        "  |      [",
                "|----",
                "EXCHANGE\n" +
                        "  |       distribution type: ROUND_ROBIN",
                "AGGREGATE (update serialize)\n" +
                        "  |  aggregate:",
                "OlapScanNode\n" +
                        "     table:");
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "AGGREGATE (merge serialize)"), plan);
        Assertions.assertEquals(2, StringUtils.countMatches(plan, "AGGREGATE (update serialize)"), plan);
        assertPlanOrder(plan, "AGGREGATE (merge finalize)", "AGGREGATE (merge serialize)",
                ":UNION", ":EXCHANGE", "AGGREGATE (update serialize)", "OlapScanNode");

        int exchangeBelowUnion = plan.indexOf(":EXCHANGE", plan.indexOf(":UNION"));
        Assertions.assertTrue(exchangeBelowUnion > 0 &&
                exchangeBelowUnion < plan.indexOf("AGGREGATE (update serialize)"), plan);
    }

    private void assertPlanOrder(String plan, String... fragments) {
        int previous = -1;
        for (String fragment : fragments) {
            int current = plan.indexOf(fragment, previous + 1);
            Assertions.assertTrue(current > previous, plan);
            previous = current;
        }
    }

    private String assertNotRewritten(String sql) throws Exception {
        return assertNotRewritten(sql, SessionVariableConstants.AggregationStage.TWO_STAGE.ordinal());
    }

    private String assertNotRewritten(String sql, int aggStage) throws Exception {
        String disabledPlan = getPlan(sql, false, aggStage);
        String enabledPlan = getPlan(sql, true, aggStage);
        Assertions.assertEquals(disabledPlan, enabledPlan);
        return enabledPlan;
    }

    @Test
    public void testPushDownTypicalStatisticAggregatesBelowUnionAll() throws Exception {
        String sql = "SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), NDV(v), " +
                "HLL_CARDINALITY(HLL_RAW(v)), BITMAP_COUNT(BITMAP_UNION(TO_BITMAP(v))), " +
                "PERCENTILE_APPROX(v, 0.5, 1000) FROM " + UNION_ALL_INPUT + " u";
        String plan = getPlan(sql, true);
        assertPushDownPlanShape(plan);
        assertContains(plan,
                "AGGREGATE (merge serialize)\n" +
                        "  |  aggregate: count",
                "AGGREGATE (update serialize)\n" +
                        "  |  aggregate: count",
                "sum", "min", "max", "ndv", "hll_raw", "bitmap_agg", "percentile_approx", "0.5", "1000");
    }

    @Test
    public void testPushDownAggregateArgumentsWithSimpleExpressions() throws Exception {
        String sql = "SELECT SUM(CHAR_LENGTH(CAST(v AS VARCHAR))), " +
                "MAX(LEFT(CAST(v AS VARCHAR), 3)), MIN(LEFT(CAST(v AS VARCHAR), 3)), " +
                "SUM(CAST(v AS DECIMAL(18, 2))) FROM " + UNION_ALL_INPUT + " u";
        String plan = getPlan(sql, true);
        assertPushDownPlanShape(plan);
        assertContains(plan,
                "AGGREGATE (merge serialize)\n" +
                        "  |  aggregate: sum",
                "AGGREGATE (update serialize)\n" +
                        "  |  aggregate: sum",
                "char_length", "left", "cast", "DECIMAL");
    }

    @Test
    public void testPushDownAggregateWithBranchProjection() throws Exception {
        String sql = "SELECT SUM(v), AVG(v) " +
                "FROM (SELECT v1 + 1 AS v FROM t0 UNION ALL SELECT v4 + 1 AS v FROM t1) u";
        String plan = getPlan(sql, true);
        assertPushDownPlanShape(plan);
        assertContains(plan,
                "AGGREGATE (merge serialize)\n" +
                        "  |  aggregate: sum",
                "AGGREGATE (update serialize)\n" +
                        "  |  aggregate: sum",
                "avg", "+ 1");
    }

    @Test
    public void testPushDownAggregateWithCommonSubExpression() throws Exception {
        String sql = "SELECT SUM((v + 1) * (v + 1)), MAX((v + 1) * (v + 1)) " +
                "FROM " + UNION_ALL_INPUT + " u";
        String plan = getPlan(sql, true);
        assertPushDownPlanShape(plan);
        assertContains(plan,
                "Project\n" +
                        "  |  output columns:",
                "common expressions:",
                "AGGREGATE (update serialize)\n" +
                        "  |  aggregate: sum",
                "*", "+ 1");
    }

    @Test
    public void testPushDownAdjacentNonGroupedAggregateBelowUnionPatterns() throws Exception {
        String sql = "SELECT SUM(cnt), SUM(total) FROM (" +
                "SELECT COUNT(*) AS cnt, SUM(v) AS total FROM " +
                "(SELECT v1 AS v FROM t0 UNION ALL SELECT v4 AS v FROM t1) u1 " +
                "UNION ALL " +
                "SELECT COUNT(*) AS cnt, SUM(v) AS total FROM " +
                "(SELECT v7 AS v FROM t2 UNION ALL SELECT v1 AS v FROM t0) u2" +
                ") outer_u";

        String disabledPlan = getPlan(sql, false);
        Assertions.assertEquals(0, StringUtils.countMatches(disabledPlan, "AGGREGATE (merge serialize)"),
                disabledPlan);
        Assertions.assertEquals(3, StringUtils.countMatches(disabledPlan, "AGGREGATE (update serialize)"),
                disabledPlan);

        String enabledPlan = getPlan(sql, true);
        Assertions.assertEquals(3, StringUtils.countMatches(enabledPlan, "AGGREGATE (merge serialize)"),
                enabledPlan);
        Assertions.assertEquals(6, StringUtils.countMatches(enabledPlan, "AGGREGATE (update serialize)"),
                enabledPlan);

        assertPlanOrder(enabledPlan, ":UNION", "AGGREGATE (update serialize)");
    }

    @Test
    public void testPushDownMultiArgumentAggregateBelowUnionAll() throws Exception {
        String sql = "SELECT INTERSECT_COUNT(TO_BITMAP(id), tag, 10) " +
                "FROM (SELECT v1 AS id, v1 AS tag FROM t0 UNION ALL SELECT v4 AS id, v4 AS tag FROM t1) u";
        String plan = getPlan(sql, true);
        assertPushDownPlanShape(plan);
    }

    @Test
    public void testPushDownDataSketchAggregateFunctions() throws Exception {
        String[] sqls = {
                "SELECT APPROX_COUNT_DISTINCT(v) FROM " + UNION_ALL_INPUT + " u",
                "SELECT APPROX_COUNT_DISTINCT_HLL_SKETCH(v) FROM " + UNION_ALL_INPUT + " u",
                "SELECT DS_HLL_COUNT_DISTINCT(v) FROM " + UNION_ALL_INPUT + " u",
                "SELECT DS_THETA_COUNT_DISTINCT(v) FROM " + UNION_ALL_INPUT + " u"
        };
        for (String sql : sqls) {
            assertPushDownPlanShape(getPlan(sql, true));
        }
    }

    @Test
    public void testDisablePushDownNonGroupedAggregateBelowUnion() throws Exception {
        String sql = "SELECT COUNT(*), SUM(CHAR_LENGTH(CAST(v AS VARCHAR))) FROM " + UNION_ALL_INPUT + " u";
        String plan = getPlan(sql, false);
        assertNotContains(plan, "AGGREGATE (merge serialize)");
        assertContains(plan,
                "AGGREGATE (merge finalize)\n" +
                        "  |  aggregate:",
                "AGGREGATE (update serialize)\n" +
                        "  |  aggregate:",
                "0:UNION\n" +
                        "  |  output exprs:");
        assertPlanOrder(plan, "AGGREGATE (merge finalize)", "AGGREGATE (update serialize)", ":UNION");
    }

    @Test
    public void testNotPushDownGroupByDistinctAndMultiDistinctAggregates() throws Exception {
        String[] sqls = {
                "SELECT v, COUNT(*) FROM " + UNION_ALL_INPUT + " u GROUP BY v",
                "SELECT COUNT(DISTINCT v) FROM " + UNION_ALL_INPUT + " u",
                "SELECT MULTI_DISTINCT_COUNT(v) FROM " + UNION_ALL_INPUT + " u",
                "SELECT MULTI_DISTINCT_SUM(v) FROM " + UNION_ALL_INPUT + " u",
                "SELECT ARRAY_AGG(DISTINCT v) FROM " + UNION_ALL_INPUT + " u"
        };
        for (String sql : sqls) {
            assertNotRewritten(sql);
        }
    }

    @Test
    public void testNotPushDownUnsupportedAggregateFunctions() throws Exception {
        String[] sqls = {
                "SELECT GROUP_CONCAT(CAST(v AS VARCHAR)) FROM " + UNION_ALL_INPUT + " u",
                "SELECT ARRAY_AGG(v) FROM " + UNION_ALL_INPUT + " u",
                "SELECT ARRAY_AGG_DISTINCT(v) FROM " + UNION_ALL_INPUT + " u",
                "SELECT ARRAY_UNIQUE_AGG(a) FROM " +
                        "(SELECT [v1] AS a FROM t0 UNION ALL SELECT [v4] AS a FROM t1) u"
        };
        for (String sql : sqls) {
            assertNotRewritten(sql);
        }
    }

    @Test
    public void testNotPushDownUnionDistinctOrOneStageAggregate() throws Exception {
        assertNotRewritten("SELECT COUNT(*) FROM (SELECT v1 AS v FROM t0 UNION SELECT v4 AS v FROM t1) u");

        String sql = "SELECT COUNT(*) FROM " + UNION_ALL_INPUT + " u";
        assertNotRewritten(sql, SessionVariableConstants.AggregationStage.ONE_STAGE.ordinal());
    }

    @Test
    public void testNotPushDownWhenAggInputContainsJoinLimitOrWindow() throws Exception {
        assertNotRewritten("SELECT SUM(x) FROM (SELECT u.v + t2.v7 AS x FROM " + UNION_ALL_INPUT +
                " u JOIN t2 ON u.v = t2.v7) x");

        assertNotRewritten("SELECT COUNT(*) FROM (SELECT v FROM " + UNION_ALL_INPUT + " u LIMIT 10) x");

        assertNotRewritten("SELECT SUM(rn) FROM " +
                "(SELECT v, ROW_NUMBER() OVER (ORDER BY v) rn FROM " + UNION_ALL_INPUT + " u) x");
    }

    @Test
    public void testPushDownWhenFilterHasBeenPushedBelowUnionAll() throws Exception {
        String plan = getPlan("SELECT COUNT(*) FROM (SELECT v FROM " + UNION_ALL_INPUT +
                " u WHERE rand() > 0.1) x", true);
        assertPushDownPlanShape(plan);
        assertContains(plan, "Predicates: rand", "AGGREGATE (update serialize)");
    }

    @Test
    public void testNotPushDownCteReuse() throws Exception {
        double oldCteReuseRatio = connectContext.getSessionVariable().getCboCTERuseRatio();
        try {
            connectContext.getSessionVariable().setCboCTERuseRatio(0);
            String sql = "WITH c AS " + UNION_ALL_INPUT + " " +
                    "SELECT COUNT(*) FROM c UNION ALL SELECT SUM(v) FROM c";
            assertNotRewritten(sql);
        } finally {
            connectContext.getSessionVariable().setCboCTERuseRatio(oldCteReuseRatio);
        }
    }
}
