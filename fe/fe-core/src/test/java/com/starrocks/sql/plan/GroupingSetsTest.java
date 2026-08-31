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
// limitations under the License.package com.starrocks.sql.plan;
package com.starrocks.sql.plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ibm.icu.impl.Assert;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.PushDownAggregateGroupingSetsRule;
import com.starrocks.type.Type;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GroupingSetsTest extends PlanTestBase {
    private static final int NUM_TABLE0_ROWS = 10000;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        Config.alter_scheduler_interval_millisecond = 1;
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        setTableStatistics(t0, NUM_TABLE0_ROWS);
        FeConstants.runningUnitTest = true;
    }

    @BeforeEach
    public void before() {
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testRepeatNodeWithUnionAllRewriteDuplicateGroupingKey() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        try {
            // ROLLUP(v1, v2, v1) expands to the grouping sets (), (v1), (v1, v2), (v1, v2, v1). The rewrite
            // turns each set into an aggregation's group-by keys; the last set must not carry v1 twice, or the
            // FE emits one output slot fewer than group-by expressions and the BE crashes building the chunk.
            String[] sqls = {
                    "select v1, v2, sum(v3) from t0 group by rollup(v1, v2, v1)",
                    "select v1, v2, sum(v3) from t0 group by cube(v1, v2, v1)",
                    "select v1, v2, sum(v3) from t0 group by grouping sets((v1, v2), (v1, v2, v1), (v1, v1))",
            };
            for (String sql : sqls) {
                String plan = getFragmentPlan(sql);
                assertContains(plan, "1:UNION");
                for (String line : plan.split("\n")) {
                    int pos = line.indexOf("group by:");
                    if (pos < 0) {
                        continue;
                    }
                    List<String> keys = Arrays.stream(line.substring(pos + "group by:".length()).split(","))
                            .map(String::trim).filter(k -> !k.isEmpty()).collect(Collectors.toList());
                    Assertions.assertEquals(keys.size(), new HashSet<>(keys).size(),
                            "duplicated group-by key in " + sql + ":\n" + plan);
                }
            }

            // Every grouping set still becomes its own union branch: 4 sets for ROLLUP(v1, v2, v1).
            String plan = getFragmentPlan(sqls[0]).replaceAll(" ", "");
            Assertions.assertTrue(Pattern.compile("1:UNION\n\\|\n(\\|----\\d+:EXCHANGE\n\\|\n){3}\\d+:EXCHANGE\n")
                    .matcher(plan).find(), plan);
        } finally {
            connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
        }
    }

    @Test
    public void testRepeatNodeWithUnionAllRewrite1() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, v2, SUM(v3) from t0 group by rollup(v1, v2)";
        String plan = getFragmentPlan(sql).replaceAll(" ", "");
        Assertions.assertTrue(plan.contains("1:UNION\n" +
                "|\n" +
                "|----15:EXCHANGE\n" +
                "|\n" +
                "|----21:EXCHANGE\n" +
                "|\n" +
                "8:EXCHANGE\n"));

        sql = "select v1, SUM(v3) from t0 group by rollup(v1)";
        plan = getFragmentPlan(sql).replaceAll(" ", "");
        Assertions.assertTrue(plan.contains("1:UNION\n" +
                "|\n" +
                "|----14:EXCHANGE\n" +
                "|\n" +
                "8:EXCHANGE\n"));

        sql = "select SUM(v3) from t0 group by grouping sets(())";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  3:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: GROUPING_ID\n" +
                "\n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 5: GROUPING_ID\n" +
                "  |  \n" +
                "  1:REPEAT_NODE"));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testGroupingSetsToUnionRewrite1() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, grouping(v1) as b, sum(v3) " +
                "   from t0 group by grouping sets((), (v1)) order by v1, b";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("14:Project\n" +
                "  |  <slot 12> : 12: v1\n" +
                "  |  <slot 14> : 14: sum\n" +
                "  |  <slot 16> : 0\n" +
                "  |  \n" +
                "  13:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(14: sum)\n" +
                "  |  group by: 12: v1"));
        Assertions.assertTrue(plan.contains("  7:Project\n" +
                "  |  <slot 8> : 8: sum\n" +
                "  |  <slot 9> : NULL\n" +
                "  |  <slot 11> : 1\n" +
                "  |  \n" +
                "  6:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(8: sum)\n" +
                "  |  group by: "));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testGroupingSetsToUnionRewrite2() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, v2, grouping_id(v1, v2) as b, sum(v3) " +
                "from t0 group by grouping sets((), (v1, v2)) order by v1, b";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("14:Project\n" +
                "  |  <slot 13> : 13: v1\n" +
                "  |  <slot 14> : 14: v2\n" +
                "  |  <slot 16> : 16: sum\n" +
                "  |  <slot 18> : 0\n" +
                "  |  \n" +
                "  13:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(16: sum)\n" +
                "  |  group by: 13: v1, 14: v2"));
        Assertions.assertTrue(plan.contains("14:Project\n" +
                "  |  <slot 13> : 13: v1\n" +
                "  |  <slot 14> : 14: v2\n" +
                "  |  <slot 16> : 16: sum\n" +
                "  |  <slot 18> : 0\n" +
                "  |  \n" +
                "  13:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(16: sum)\n" +
                "  |  group by: 13: v1, 14: v2"));
        Assertions.assertTrue(plan.contains("7:Project\n" +
                "  |  <slot 8> : 8: sum\n" +
                "  |  <slot 9> : NULL\n" +
                "  |  <slot 10> : NULL\n" +
                "  |  <slot 12> : 3\n" +
                "  |  \n" +
                "  6:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(8: sum)\n" +
                "  |  group by: "));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testGroupingSetsToUnionRewrite3() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, v2, sum(v3) " +
                "from t0 group by grouping sets((), (v1, v2)) order by v1, v2";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  7:Project\n" +
                "  |  <slot 7> : 7: sum\n" +
                "  |  <slot 8> : NULL\n" +
                "  |  <slot 9> : NULL\n" +
                "  |  \n" +
                "  6:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(7: sum)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  5:EXCHANGE"));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testRollupToUnionRewrite1() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, grouping(v1) as b, sum(v3) " +
                "   from t0 group by rollup(v1, v2) order by v1, b";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("21:Project\n" +
                "  |  <slot 19> : 19: v1\n" +
                "  |  <slot 22> : 22: sum\n" +
                "  |  <slot 24> : 0"));
        Assertions.assertTrue(plan.contains("14:Project\n" +
                "  |  <slot 13> : 13: v1\n" +
                "  |  <slot 15> : 15: sum\n" +
                "  |  <slot 18> : 0"));
        Assertions.assertTrue(plan.contains("  7:Project\n" +
                "  |  <slot 8> : 8: sum\n" +
                "  |  <slot 9> : NULL\n" +
                "  |  <slot 12> : 1"));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testCubeUnionRewrite1() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, grouping_id(v1) as b, count(1) " +
                "   from t0 group by rollup(v1, v2, v3) order by v1, b";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  1:UNION\n" +
                "  |  \n" +
                "  |----15:EXCHANGE\n" +
                "  |    \n" +
                "  |----22:EXCHANGE\n" +
                "  |    \n" +
                "  |----29:EXCHANGE\n" +
                "  |    \n" +
                "  8:EXCHANGE"));
        Assertions.assertTrue(plan.contains("  28:Project\n" +
                "  |  <slot 26> : 26: v1\n" +
                "  |  <slot 29> : 29: count\n" +
                "  |  <slot 31> : 0\n"));
        Assertions.assertTrue(plan.contains("  21:Project\n" +
                "  |  <slot 20> : 20: v1\n" +
                "  |  <slot 22> : 22: count\n" +
                "  |  <slot 25> : 0\n"));
        Assertions.assertTrue(plan.contains("  14:Project\n" +
                "  |  <slot 14> : 14: v1\n" +
                "  |  <slot 15> : 15: count\n" +
                "  |  <slot 19> : 0\n"));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testPushDownGroupingSetNormal() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            String sql = "select t1b, t1c, t1d, sum(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "    HASH_PARTITIONED: 2: t1b, 3: t1c, 4: t1d\n" +
                    "\n" +
                    "  1:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  output: sum(7: t1g)\n" +
                    "  |  group by: 2: t1b, 3: t1c, 4: t1d");
            assertContains(plan, "  7:REPEAT_NODE\n" +
                    "  |  repeat: repeat 2 lines [[], [14], [14, 15]]");

            sql = "select t1b, t1c, t1d, GROUPING_ID(t1c), GROUPING(t1d), sum(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            plan = getVerboseExplain(sql);
            assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  aggregate: sum[([7: t1g, BIGINT, true]); " +
                    "args: BIGINT; result: BIGINT; args nullable: true; result nullable: true]\n" +
                    "  |  group by: [2: t1b, SMALLINT, true], [3: t1c, INT, true], [4: t1d, BIGINT, true]\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
            assertContains(plan, "  15:Project\n" +
                    "  |  output columns:\n" +
                    "  |  23 <-> [23: sum, BIGINT, true]\n" +
                    "  |  24 <-> [24: t1b, SMALLINT, true]\n" +
                    "  |  25 <-> [25: t1c, INT, true]\n" +
                    "  |  26 <-> [26: t1d, BIGINT, true]\n" +
                    "  |  28 <-> 0\n" +
                    "  |  29 <-> 0");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetAvg() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            String sql = "select t1b, t1c, t1d, avg(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            String plan = getFragmentPlan(sql);
            // finest grain computes sum/count instead of avg, so coarser rollup levels can be
            // re-aggregated correctly (re-summing, not re-averaging); the BIGINT arg is summed as
            // DOUBLE to match avg's own double accumulator and avoid overflow a plain BIGINT sum risks
            assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  output: sum(CAST(7: t1g AS DOUBLE)), count(7: t1g)\n" +
                    "  |  group by: 2: t1b, 3: t1c, 4: t1d");
            // REPEAT_NODE now runs on the already-aggregated (t1b, t1c) result, not on raw rows
            assertContains(plan, "  7:REPEAT_NODE\n" +
                    "  |  repeat: repeat 2 lines [[], [17], [17, 18]]");
            // coarser rollup levels re-sum the sum/count columns, never re-average them
            assertContains(plan, "  3:AGGREGATE (merge finalize)\n" +
                    "  |  output: sum(13: sum), count(14: count)\n" +
                    "  |  group by: 2: t1b, 3: t1c, 4: t1d");
            assertContains(plan, "  10:AGGREGATE (merge finalize)\n" +
                    "  |  output: sum(20: sum), sum(21: count)\n" +
                    "  |  group by: 17: t1b, 18: t1c, 19: GROUPING_ID");
            // avg is recovered via division wherever it's consumed; sum is already DOUBLE, so only
            // count needs the cast
            assertContains(plan, "20: sum / CAST(21: count AS DOUBLE)");
            assertContains(plan, "24: sum / CAST(25: count AS DOUBLE)");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetAvgWithHavingNotPushedDown() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            // a HAVING predicate directly on the avg() output can't be safely re-evaluated against the
            // pre-divide sum/count columns of a re-aggregated rollup level, so push down must not fire
            String sql = "select t1b, t1c, t1d, avg(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d) having avg(t1g) > 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:REPEAT_NODE\n" +
                    "  |  repeat: repeat 3 lines [[], [2], [2, 3], [2, 3, 4]]\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    private static int countMatches(String haystack, String needle) {
        int n = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            n++;
        }
        return n;
    }

    @Test
    public void testPushDownGroupingSetAvgReusesExistingAggregations() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            // avg(x) is computed as sum(x)/count(x). When the query already asks for count(x), the
            // decomposition must bind to that existing output column instead of adding a second,
            // identical count to the CTE - and the coarser level must then re-aggregate it once
            // rather than once per consumer.
            String withSiblingCount = "select t1b, t1c, t1d, avg(t1g), count(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            String plan = getFragmentPlan(withSiblingCount);
            Assertions.assertEquals(1, countMatches(plan, "count(7: t1g)"),
                    "avg's count(t1g) should reuse the query's own count(t1g), plan:\n" + plan);

            // two avg() over the same argument likewise need only one sum/count pair
            String twoAvgs = "select t1b, t1c, t1d, avg(t1g), avg(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            String twoAvgsPlan = getFragmentPlan(twoAvgs);
            Assertions.assertEquals(1, countMatches(twoAvgsPlan, "count(7: t1g)"),
                    "duplicate avg() should share one count, plan:\n" + twoAvgsPlan);
            Assertions.assertEquals(1, countMatches(twoAvgsPlan, "sum(CAST(7: t1g AS DOUBLE))"),
                    "duplicate avg() should share one sum, plan:\n" + twoAvgsPlan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetUnsupportedFn() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            // SUPPORT_AGGREGATE_FUNCTIONS is a whitelist of functions that can be recombined across
            // rollup levels. Widening it (as AVG/COUNT did) must not turn it into an allow-everything:
            // a function outside the list still has to fall back to REPEAT over the raw rows.
            String unsupportedFn = "select t1b, t1c, t1d, stddev(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            assertContains(getFragmentPlan(unsupportedFn), "  1:REPEAT_NODE\n" +
                    "  |  repeat: repeat 3 lines [[], [2], [2, 3], [2, 3, 4]]\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");

            // DISTINCT is excluded for every whitelisted function too: count(distinct x) cannot be
            // recombined by re-summing partial counts, since the same value may repeat across the
            // finer groups being rolled up.
            String distinctCount = "select t1b, t1c, t1d, count(distinct t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            assertContains(getFragmentPlan(distinctCount), "  1:REPEAT_NODE\n" +
                    "  |  repeat: repeat 3 lines [[], [2], [2, 3], [2, 3, 4]]\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetAvgDecimal() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            // avg(DECIMAL) decomposes into sum(DECIMAL)/count(DECIMAL); the synthesized sum must be
            // rectified to the argument's concrete precision/scale rather than keeping the wildcard
            // decimal128 return type registered on the builtin SUM signature
            String sql = "select t1b, t1c, t1d, avg(id_decimal) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            String plan = getVerboseExplain(sql);
            // id_decimal is DECIMAL64(10,2); the synthesized sum must widen its result to
            // DECIMAL128(38, 2) - the BE's registered decimal_sum accumulator width - not stay
            // narrowed to the argument's own DECIMAL64 precision
            assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  aggregate: sum[([10: id_decimal, DECIMAL64(10,2), true]); args: DECIMAL64; " +
                    "result: DECIMAL128(38,2); args nullable: true; result nullable: true], " +
                    "count[([10: id_decimal, DECIMAL64(10,2), true]); args: DECIMAL64; result: BIGINT; " +
                    "args nullable: true; result nullable: false]\n" +
                    "  |  group by: [2: t1b, SMALLINT, true], [3: t1c, INT, true], [4: t1d, BIGINT, true]");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetAvgDecimal256() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        starRocksAssert.withTable("CREATE TABLE test_decimal256_avg (\n" +
                "  `k1` int NULL,\n" +
                "  `k2` int NULL,\n" +
                "  `k3` int NULL,\n" +
                "  `k4` int NULL,\n" +
                "  `v` decimal(76, 10) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        try {
            // v is DECIMAL256(76,10); the BE registers a dedicated decimal256->decimal256 decimal_sum
            // mapping (unlike decimal32/64/128, which all widen to DECIMAL128), so the synthesized sum
            // must stay in DECIMAL256, not get force-widened down to DECIMAL128
            String sql = "select k1, k2, k3, k4, avg(v) " +
                    "   from test_decimal256_avg group by rollup(k1, k2, k3, k4)";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: sum[([5: v, DECIMAL256(76,10), true]); args: DECIMAL256; " +
                    "result: DECIMAL256(76,10); args nullable: true; result nullable: true], " +
                    "count[([5: v, DECIMAL256(76,10), true]); args: DECIMAL256; result: BIGINT; " +
                    "args nullable: true; result nullable: false]");
            // the rolled-up sum must also stay in DECIMAL256, never narrow to DECIMAL128
            assertContains(plan, "sum[([16: sum, DECIMAL256(76,10), true]); args: DECIMAL256; " +
                    "result: DECIMAL256(76,10); args nullable: true; result nullable: true]");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
            starRocksAssert.dropTable("test_decimal256_avg");
        }
    }

    @Test
    public void testPushDownGroupingSetCount() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            String sql = "select t1b, t1c, t1d, count(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            String plan = getFragmentPlan(sql);
            // finest grain: an ordinary count, nothing to recombine yet
            assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  output: count(7: t1g)\n" +
                    "  |  group by: 2: t1b, 3: t1c, 4: t1d");
            // REPEAT now runs on the already-aggregated (t1b, t1c) result, not on raw rows
            assertContains(plan, "  7:REPEAT_NODE\n" +
                    "  |  repeat: repeat 2 lines [[], [14], [14, 15]]");
            // coarser rollup levels re-SUM the finer levels' partial counts, never re-COUNT them
            assertContains(plan, "  8:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  output: sum(13: count)\n" +
                    "  |  group by: 14: t1b, 15: t1c, 16: GROUPING_ID");
            assertContains(plan, "  10:AGGREGATE (merge finalize)\n" +
                    "  |  output: sum(17: count)\n" +
                    "  |  group by: 14: t1b, 15: t1c, 16: GROUPING_ID");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetCountWithHaving() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            // unlike avg, a re-summed count IS directly the correct final value (no division needed),
            // so HAVING count(t1g) > 10 can safely push down and filter on the re-summed column.
            String sql = "select t1b, t1c, t1d, count(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d) having count(t1g) > 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  10:AGGREGATE (merge finalize)\n" +
                    "  |  output: sum(17: count)\n" +
                    "  |  group by: 14: t1b, 15: t1c, 16: GROUPING_ID\n" +
                    "  |  having: 17: count > 10");
            assertContains(plan, "  15:SELECT\n" +
                    "  |  predicates: 19: count > 10");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetErrorKeys() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            String sql = "select t1b, t1c, sum(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:REPEAT_NODE\n" +
                    "  |  repeat: repeat 2 lines [[], [2], [2, 3]]\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetErrorGroup() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            String sql = "select t1b, t1c, t1d, id_date, count(t1g) " +
                    "   from test_all_type " +
                    "   group by grouping sets(" +
                    "   (t1b)," +
                    "   (t1c, id_date)," +
                    "   (t1b, t1c, t1d)" +
                    "   )";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:REPEAT_NODE\n" +
                    "  |  repeat: repeat 2 lines [[2], [3, 9], [2, 3, 4]]\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetSomeGroupKey() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            String sql = "select distinct t1b, x1, x2 from ( " +
                    "   select t1b, t1c, grouping_id(t1b) x1, grouping_id(t1c, t1d) x2 " +
                    "   from test_all_type " +
                    "   group by rollup(t1b, t1c, t1d, id_date) ) xxx";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  group by: 2: t1b\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
            assertContains(plan, "  7:REPEAT_NODE\n" +
                    "  |  repeat: repeat 3 lines [[], [14], [14], [14]]\n");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetDecimal() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            String sql = "select t1b, t1c, t1d, sum(id_decimal) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            String plan = getCostExplain(sql);
            assertContains(plan, "  8:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  aggregate: sum[([13: sum, DECIMAL128(38,2), true]); args: DECIMAL128; " +
                    "result: DECIMAL128(38,2); args nullable: true; result nullable: true]");
            assertContains(plan, "  10:AGGREGATE (merge finalize)\n" +
                    "  |  aggregate: sum[([17: sum, DECIMAL128(38,2), true]); args: DECIMAL128; " +
                    "result: DECIMAL128(38,2); args nullable: true; result nullable: true]");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingID() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            String sql = "select * from (" +
                    "   select grouping(t1b, t1c) as aa, t1b, t1c, t1d, sum(id_decimal) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)) tt" +
                    "   where aa = 'aa';";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  6:REPEAT_NODE\n" +
                    "  |  repeat: repeat 2 lines [[], [15], [16, 15]]\n" +
                    "  |  PREDICATES: CAST(18: GROUPING AS VARCHAR(2147482624)) = 'aa'");
            assertNotContains(plan, "UNION");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testNotEliminateConstantGroupByColumnInGroupingSets() throws Exception {
        String sql = "select v1, v2,v3, cnt\n" +
                "from(\n" +
                "select v1, v2, v3, count(*) as cnt\n" +
                "from  (\n" +
                "select v1, 1 as v2, v3, 1 as metric\n" +
                "from t0\n" +
                ") t2\n" +
                "group by cube(v1,v2,v3)\n" +
                ")t3;";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  6:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 6> : 6: v2\n" +
                "  |  <slot 7> : 7: count\n" +
                "  |  \n" +
                "  5:AGGREGATE (merge finalize)\n" +
                "  |  output: count(7: count)\n" +
                "  |  group by: 1: v1, 6: v2, 3: v3, 8: GROUPING_ID"), plan);
    }

    @Test
    public void testPushDownGroupingSetHaving() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            String sql = "select t1b, t1c, t1d, sum(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d) " +
                    "   having t1b is null and (t1c is null or t1d is null)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "predicates: 20: t1b IS NULL, (21: t1c IS NULL) OR (22: t1d IS NULL)");
            assertContains(plan, "PREDICATES: 14: t1b IS NULL\n"
                    + "  |");

            sql = "select t1b, t1c, t1d, sum(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d) " +
                    "   having t1b is null and (t1c is null or t1d is null) and sum(t1g) > 10";
            plan = getFragmentPlan(sql);
            assertContains(plan, "predicates: 20: t1b IS NULL, "
                    + "(21: t1c IS NULL) OR (22: t1d IS NULL), 19: sum > 10");
            assertContains(plan, "having: 17: sum > 10");
            assertContains(plan, "PREDICATES: 14: t1b IS NULL");
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
        }
    }

    @Test
    public void testPushDownGroupingSetHavingWithPlanValidate() {
        new MockUp<PushDownAggregateGroupingSetsRule>() {
            @Mock
            public OptExpression buildSubRepeatConsume(ColumnRefFactory factory,
                                                       Map<ColumnRefOperator, ColumnRefOperator> outputs,
                                                       LogicalAggregationOperator aggregate, LogicalRepeatOperator repeat,
                                                       int cteId,
                                                       Map<ColumnRefOperator, PushDownAggregateGroupingSetsRule.AvgDecomposition>
                                                               avgDecompositions) {
                int subGroups = repeat.getRepeatColumnRef().size() - 1;
                List<ColumnRefOperator> nullRefs = Lists.newArrayList(repeat.getRepeatColumnRef().get(subGroups));
                repeat.getRepeatColumnRef().stream().limit(subGroups).forEach(nullRefs::removeAll);

                // consume
                Map<ColumnRefOperator, ColumnRefOperator> cteColumnRefs = Maps.newHashMap();
                for (ColumnRefOperator input : aggregate.getAggregations().keySet()) {
                    ColumnRefOperator cteOutput = factory.create(input, input.getType(), input.isNullable());
                    cteColumnRefs.put(cteOutput, input);
                    outputs.put(input, cteOutput);
                }
                for (ColumnRefOperator input : aggregate.getGroupingKeys()) {
                    if (!repeat.getOutputGrouping().contains(input) && !nullRefs.contains(input)) {
                        ColumnRefOperator cteOutput = factory.create(input, input.getType(), input.isNullable());
                        cteColumnRefs.put(cteOutput, input);
                        outputs.put(input, cteOutput);
                    }
                }

                LogicalCTEConsumeOperator consume = new LogicalCTEConsumeOperator(cteId, cteColumnRefs);

                // repeat
                List<ColumnRefOperator> outputGrouping = Lists.newArrayList();
                repeat.getOutputGrouping().forEach(k -> {
                    ColumnRefOperator x = factory.create(k, k.getType(), k.isNullable());
                    outputs.put(k, x);
                    outputGrouping.add(x);
                });

                List<List<ColumnRefOperator>> repeatRefs = repeat.getRepeatColumnRef().stream().limit(subGroups)
                        .map(l -> l.stream().map(outputs::get).filter(Objects::nonNull).collect(Collectors.toList()))
                        .collect(Collectors.toList());

                List<List<Long>> groupingIds = repeat.getGroupingIds().stream()
                        .map(s -> s.subList(0, subGroups)).collect(Collectors.toList());

                ScalarOperator predicate = null;
                if (null != repeat.getPredicate()) {
                    ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(outputs);
                    predicate = rewriter.rewrite(repeat.getPredicate());
                }

                LogicalRepeatOperator newRepeat = LogicalRepeatOperator.builder()
                        .setOutputGrouping(outputGrouping)
                        .setRepeatColumnRefList(repeatRefs)
                        .setGroupingIds(groupingIds)
                        .setHasPushDown(true)
                        .setPredicate(predicate)
                        .build();

                // aggregate
                Map<ColumnRefOperator, CallOperator> aggregations = Maps.newHashMap();
                aggregate.getAggregations().forEach((k, v) -> {
                    ColumnRefOperator x = factory.create(k, k.getType(), k.isNullable());
                    Function aggFunc = ExprUtils.getBuiltinFunction(v.getFnName(), new Type[] {k.getType()},
                            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

                    Preconditions.checkState(aggFunc instanceof AggregateFunction);
                    if (k.getType().isDecimalOfAnyVersion()) {
                        aggFunc = DecimalV3FunctionAnalyzer.rectifyAggregationFunction((AggregateFunction) aggFunc, k.getType(),
                                v.getType());
                    }

                    aggregations.put(x,
                            new CallOperator(v.getFnName(), k.getType(), Lists.newArrayList(outputs.get(k)), aggFunc));
                    outputs.put(k, x);
                });

                List<ColumnRefOperator> groupings = aggregate.getGroupingKeys().stream()
                        .filter(c -> !nullRefs.contains(c)).map(outputs::get).collect(Collectors.toList());

                if (null != aggregate.getPredicate()) {
                    Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap(outputs);
                    nullRefs.forEach(c -> replaceMap.put(c, ConstantOperator.createNull(c.getType())));
                    ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(replaceMap);
                    predicate = rewriter.rewrite(aggregate.getPredicate());
                }
                LogicalAggregationOperator newAggregate = LogicalAggregationOperator.builder()
                        .setAggregations(aggregations)
                        .setGroupingKeys(groupings)
                        .setType(AggType.GLOBAL)
                        .setPredicate(predicate)
                        .setPartitionByColumns(groupings)
                        .build();

                // project
                Map<ColumnRefOperator, ScalarOperator> projection = Maps.newHashMap();
                aggregations.keySet().forEach(k -> projection.put(k, k));
                groupings.forEach(k -> projection.put(k, k));

                for (ColumnRefOperator nullRef : nullRefs) {
                    ColumnRefOperator m = factory.create(nullRef, nullRef.getType(), true);
                    projection.put(m, ConstantOperator.createNull(nullRef.getType()));
                    outputs.put(nullRef, m);
                }
                LogicalProjectOperator projectOperator = new LogicalProjectOperator(projection);

                return OptExpression.create(projectOperator,
                        OptExpression.create(newAggregate, OptExpression.create(newRepeat, OptExpression.create(consume))));
            }
        };

        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        connectContext.getSessionVariable().setEnableOptimizerRuleDebug(true);
        try {
            String sql = "select t1b, t1c, t1d, sum(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d) " +
                    "   having t1b is null and (t1c is null or t1d is null)";
            try {
                getFragmentPlan(sql);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                String errMsg = e.getMessage();
                assertContains(errMsg, "Optimizer rule debug: Plan validation failed after applying rule " +
                        "[TF_PUSHDOWN_AGG_GROUPING_SET].");
                assertContains(errMsg, "The required cols {4} cannot obtain from input cols {13,14,15}");
                assertContains(errMsg, "Input dependency cols check failed");
            }
        } finally {
            connectContext.getSessionVariable().setCboPushDownGroupingSet(false);
            connectContext.getSessionVariable().setEnableOptimizerRuleDebug(false);
        }
    }
}
