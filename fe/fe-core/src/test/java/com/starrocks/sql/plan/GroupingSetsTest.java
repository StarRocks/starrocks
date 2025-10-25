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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
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
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    public void testPushDownGroupingSetErrorFn() throws Exception {
        connectContext.getSessionVariable().setCboPushDownGroupingSet(true);
        try {
            String sql = "select t1b, t1c, t1d, count(t1g) " +
                    "   from test_all_type group by rollup(t1b, t1c, t1d)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:REPEAT_NODE\n" +
                    "  |  repeat: repeat 3 lines [[], [2], [2, 3], [2, 3, 4]]\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
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
                    "  |  PREDICATES: CAST(18: GROUPING AS VARCHAR(1048576)) = 'aa'");
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
                                                       int cteId) {
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
                    Function aggFunc = Expr.getBuiltinFunction(v.getFnName(), new Type[] {k.getType()},
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
