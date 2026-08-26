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

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AggregatePushDownTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        starRocksAssert.withDatabase("test_window_db");
        starRocksAssert.withTable("CREATE TABLE if not exists trans\n" +
                "(\n" +
                "region VARCHAR(128)  NULL,\n" +
                "order_date DATE NOT NULL,\n" +
                "income DECIMAL128(10, 2) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`region`, `order_date`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`region`, `order_date`) BUCKETS 128\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ")");
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS t_json_a (\n" +
                "  c0 INT NULL,\n" +
                "  c1 BIGINT NULL,\n" +
                "  c2 DATE NULL,\n" +
                "  c3 JSON NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(c0, c1)\n" +
                "DISTRIBUTED BY HASH(c0) BUCKETS 3\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(false);
        connectContext.getSessionVariable().setEnableEliminateAgg(false);
    }

    @Test
    public void testCountNotPushedIntoConstantKeyUnionBranch() throws Exception {
        // A group-by key can be erased without ever crossing a join: a UNION branch projecting a constant
        // (`select 1 as k ... group by k`) leaves the pushed context with a column-less group-by, so the
        // partial aggregate lands ungrouped in every branch and each emits a phantom row. With all inputs
        // empty the query would then return one `(1, 0)` group instead of no rows at all.
        String constKeyUnion = getFragmentPlan(
                "select k, count(*) from (select 1 as k from t0 union all select 1 from t1) u group by k");
        Assertions.assertEquals(1, StringUtils.countMatches(constKeyUnion, ":AGGREGATE "), constKeyUnion);
        assertNotContains(constKeyUnion, "group by: \n");

        // Sanity: a UNION whose key is a real column still pushes a count into both branches.
        String colKeyUnion = getFragmentPlan(
                "select k, count(*) from (select v1 as k from t0 union all select v4 from t1) u group by k");
        Assertions.assertEquals(3, StringUtils.countMatches(colKeyUnion, ":AGGREGATE "), colKeyUnion);
    }

    @Test
    public void testCountNotPushedBelowKeylessJoin() throws Exception {
        // A count() pushed down with an empty group-by set degenerates into a scalar aggregate
        // that always emits exactly one row, even when its side has zero input rows. Re-joining
        // that phantom row through a keyless join (CROSS JOIN, or an INNER JOIN whose condition
        // contributes no columns) would corrupt the join's cardinality instead of correctly
        // producing no rows/groups, so both the collector and the rewriter must refuse to push count()
        // in that shape (PushDownAggregateUtils#isUngroupedCountPush).
        String crossJoinPlan = getFragmentPlan("select t1.v4, count(*) from t0, t1 group by t1.v4");
        Assertions.assertEquals(1, StringUtils.countMatches(crossJoinPlan, ":AGGREGATE "));
        assertNotContains(crossJoinPlan, "group by: \n");

        String constCondPlan =
                getFragmentPlan("select t1.v4, count(*) from t0 inner join t1 on 1 = 1 group by t1.v4");
        Assertions.assertEquals(1, StringUtils.countMatches(constCondPlan, ":AGGREGATE "));
        assertNotContains(constCondPlan, "group by: \n");

        // Grouping on a constant is ungrouped too, even though the group-by map is not empty: the entry
        // carries an expression that uses no column, so the pushed count would again be a scalar
        // aggregate emitting a phantom row for an empty child.
        String constGroupByPlan = getFragmentPlan("select 1 + 1, count(*) from t0, t1 group by 1 + 1");
        Assertions.assertEquals(1, StringUtils.countMatches(constGroupByPlan, ":AGGREGATE "), constGroupByPlan);
        assertNotContains(constGroupByPlan, "group by: \n");

        // Same constant group-by over a real equi-join still pushes: the on-predicate column supplies the
        // grouping key the constant does not.
        String constGroupByEquiPlan =
                getFragmentPlan("select 1 + 1, count(*) from t0 join t1 on t0.v1 = t1.v4 group by 1 + 1");
        Assertions.assertEquals(2, StringUtils.countMatches(constGroupByEquiPlan, ":AGGREGATE "), constGroupByEquiPlan);

        // A group-by expression spanning both sides of a keyless join is NOT ungrouped: the rewriter
        // flattens `t0.v1 + t1.v4` into its component columns before reaching the join, so child 0 keeps
        // `v1` as its grouping key and the pushed count stays correct (an empty t0 produces no rows).
        String derivedGroupByPlan =
                getFragmentPlan("select t0.v1 + t1.v4, count(*) from t0, t1 group by t0.v1 + t1.v4");
        Assertions.assertEquals(2, StringUtils.countMatches(derivedGroupByPlan, ":AGGREGATE "), derivedGroupByPlan);
        assertContains(derivedGroupByPlan, "output: count(*)\n  |  group by: 1: v1");

        // Sanity: a real equi-join still gets count() pushed below it (the two-stage aggregate
        // this whole feature exists for), since the join column populates a non-empty group-by
        // set on the pushed side.
        String realJoinPlan =
                getFragmentPlan("select t0.v1, count(*) from t0 join t1 on t0.v1 = t1.v4 group by t0.v1");
        Assertions.assertEquals(2, StringUtils.countMatches(realJoinPlan, ":AGGREGATE "));
    }

    @Test
    public void testCountPushedBelowSemiAntiJoin() throws Exception {
        // A semi/anti join only filters its preserved side: it neither duplicates those rows nor pads them
        // with NULLs, and it decides row by row on the on-predicate columns, which the pushed group-by set
        // always contains. sum(partial_count) over the surviving groups is therefore the true count.
        //
        // Rejecting these would not merely forgo a count push down. A count that cannot land on a side
        // aborts the whole push down for that side, so it would also cancel the sum/min/max push downs that
        // already worked below semi joins -- which is what regressed TPC-DS Q14 (its `ss_item_sk IN (...)`
        // becomes a LEFT SEMI JOIN).
        String semiPlan = getFragmentPlan(
                "select t0.v1, count(*) from t0 where t0.v1 in (select t1.v4 from t1) group by t0.v1");
        Assertions.assertEquals(2, StringUtils.countMatches(semiPlan, ":AGGREGATE "), semiPlan);

        String antiPlan = getFragmentPlan(
                "select t0.v1, count(*) from t0 where t0.v1 not in (select t1.v4 from t1) group by t0.v1");
        Assertions.assertEquals(2, StringUtils.countMatches(antiPlan, ":AGGREGATE "), antiPlan);

        String mixedPlan = getFragmentPlan(
                "select t0.v1, sum(t0.v2), count(*) from t0 where t0.v1 in (select t1.v4 from t1) group by t0.v1");
        Assertions.assertEquals(2, StringUtils.countMatches(mixedPlan, ":AGGREGATE "), mixedPlan);
    }

    @Test
    public void testCountNotLeftAboveJoinWhenOtherAggPushed() throws Exception {
        // Pushing ANY aggregation to a join child collapses that child to one row per group key.
        // count(*) is the only whitelisted function sensitive to that collapse, so if count() is
        // stripped from a side (because it may only land on one side of the join) the remaining
        // aggregations must not be pushed either -- otherwise the count() left above the join
        // counts the collapsed rows instead of the real join rows.
        //
        // t0 = {(1,10),(1,20)}, t1 = {(1),(1),(1)} joins to 6 rows: sum = 90, count = 6.
        // Collapsing t0 to a single (v1=1, sum=30) row would yield count = 3.
        String plan = getFragmentPlan(
                "select t0.v1, sum(t0.v2), count(*) from t0 join t1 on t0.v1 = t1.v4 group by t0.v1");
        Assertions.assertEquals(1, StringUtils.countMatches(plan, ":AGGREGATE "), plan);

        String leftJoinPlan = getFragmentPlan(
                "select t0.v1, sum(t1.v5), count(*) from t0 left join t1 on t0.v1 = t1.v4 group by t0.v1");
        Assertions.assertEquals(1, StringUtils.countMatches(leftJoinPlan, ":AGGREGATE "), leftJoinPlan);

        // Sanity: sum() alone is insensitive to the collapse, so it is still pushed below the join.
        String sumOnlyPlan = getFragmentPlan(
                "select t0.v1, sum(t0.v2) from t0 join t1 on t0.v1 = t1.v4 group by t0.v1");
        Assertions.assertEquals(2, StringUtils.countMatches(sumOnlyPlan, ":AGGREGATE "), sumOnlyPlan);
    }

    @Test
    public void testPushDownDisableOnBroadcastJoin() {
        connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(false);
        try {
            runFileUnitTest("optimized-plan/agg-pushdown-disable_on_broadcast_join");
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(true);
        }
    }

    @Test
    public void testPushDownEnableOnBroadcastJoin() {
        runFileUnitTest("optimized-plan/agg-pushdown-enable_on_broadcast_join");
    }

    @Test
    public void testPushDownPreAggDisableOnBroadcastJoin() {
        connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(false);
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            runFileUnitTest("optimized-plan/preagg-pushdown-disable_on_broadcast_join");
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
            connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(true);
        }
    }

    @Test
    public void testPushDownPreAggEnableOnBroadcastJoin() {
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            runFileUnitTest("optimized-plan/preagg-pushdown-enable_on_broadcast_join");
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }

    @Test
    public void testPushDownDistinctAggBelowWindow()
            throws Exception {
        String q1 = "SELECT DISTINCT \n" +
                "  COALESCE(region, 'Other') AS region, \n" +
                "  order_date, \n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   order_date) AS gp_income,  \n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   MONTH(order_date) ORDER BY order_date) AS gp_income_MTD,\n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   YEAR (order_date), QUARTER(order_date) ORDER BY order_date) AS gp_income_QTD,\n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   YEAR (order_date) ORDER BY order_date) AS gp_income_YTD  \n" +
                "FROM  trans\n" +
                "where month(order_date)=1\n" +
                "order by region, order_date";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, q1);
        Assertions.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: sum[([3: income, DECIMAL128(10,2), false]); args: DECIMAL128; " +
                "result: DECIMAL128(38,2); args nullable: false; result nullable: true]\n" +
                "  |  group by: [1: region, VARCHAR, true], [2: order_date, DATE, false]\n"), plan);

        Assertions.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     table: trans, rollup: trans\n" +
                "     preAggregation: on\n" +
                "     Predicates: month[([2: order_date, DATE, false]); args: DATE; result: TINYINT; " +
                "args nullable: false; result nullable: false] = 1\n" +
                ""));
    }

    @Test
    public void testNotPushdownWithJsonType() throws Exception {
        String sql = "select /*+ SET_VAR(cbo_push_down_aggregate_mode=1) */ distinct " +
                "cast(json_query(a.c3, '$.\"14\"') as varchar) as v0 " +
                "from t_json_a a " +
                "where a.c0 = 1 and a.c1 in (" +
                "  select distinct v1 from t0 where v1 in (4)" +
                ")";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        assertContains(plan, "|----4:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1\n" +
                "  |    \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  2 <-> [2: c1, BIGINT, true]\n" +
                "  |  10 <-> json_query[([4: c3, JSON, true], '$.\"14\"'); args: JSON,VARCHAR; result: JSON; args nullable: " +
                "true; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: t_json_a, rollup: t_json_a");
    }

    @Test
    public void testPushDownDistinctAggBelowWindow_1() throws Exception {
        // unsupported window func ref cols from partition by cols
        String sql = "select distinct t1d from (select *, sum(t1e) over (partition by t1d) as cnt from test_all_type ) " +
                "t where cnt > 1 limit 10;";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "5:SELECT\n" +
                "  |  predicates: 11: sum(5: t1e) > 1.0\n" +
                "  |  \n" +
                "  4:ANALYTIC\n" +
                "  |  functions: [, sum(12: sum), ]\n" +
                "  |  partition by: 4: t1d\n" +
                "  |  \n" +
                "  3:SORT\n" +
                "  |  order by: <slot 4> 4: t1d ASC\n" +
                "  |  analytic partition by: 4: t1d\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(5: t1e)\n" +
                "  |  group by: 4: t1d\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testPushDownDistinctAggBelowWindow_2() throws Exception {
        // unsupported window func ref cols from partition by cols
        String sql = "select distinct t1d from (select *, sum(t1d) over (partition by t1d, t1e) as cnt from " +
                "test_all_type ) t where cnt > 1 limit 10;";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:ANALYTIC\n" +
                "  |  functions: [, sum(12: sum), ]\n" +
                "  |  partition by: 4: t1d, 5: t1e\n" +
                "  |  \n" +
                "  3:SORT\n" +
                "  |  order by: <slot 4> 4: t1d ASC, <slot 5> 5: t1e ASC\n" +
                "  |  analytic partition by: 4: t1d, 5: t1e\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: t1d)\n" +
                "  |  group by: 4: t1d, 5: t1e\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testPushDownDistinctAggBelowWindow_3() throws Exception {
        // unsupported window func ref cols from partition by cols
        String sql = "select distinct t1c from (select *, sum(t1d) over (partition by t1e order by t1d) as cnt from " +
                "test_all_type ) t where cnt > 1 limit 10;";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: t1d)\n" +
                "  |  group by: 3: t1c, 4: t1d, 5: t1e\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testNotPushDownDistinctAggBelowWindow_1() throws Exception {
        // unsupported count function
        String sql = "select distinct t1d from (select *, count(1) over (partition by t1d) as cnt from test_all_type ) " +
                "t where cnt > 1 limit 10;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:SELECT\n" +
                "  |  predicates: 11: count(1) > 1\n" +
                "  |  \n" +
                "  3:ANALYTIC\n" +
                "  |  functions: [, count(1), ]\n" +
                "  |  partition by: 4: t1d\n" +
                "  |  \n" +
                "  2:SORT\n" +
                "  |  order by: <slot 4> 4: t1d ASC\n" +
                "  |  analytic partition by: 4: t1d\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testPruneColsAfterPushdownAgg_1() throws Exception {
        String sql = "select L_PARTKEY from lineitem_partition where L_SHIPDATE >= '1992-01-01' and L_SHIPDATE < '1993-01-01'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 2> : 2: L_PARTKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lineitem_partition\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/7");
    }

    @Test
    public void testPruneColsAfterPushdownAgg_2() throws Exception {
        String sql = "select max(L_ORDERKEY), sum(2), L_PARTKEY from lineitem_partition " +
                "join t0 on L_PARTKEY = v1 " +
                "where L_SHIPDATE >= '1992-01-01' and L_SHIPDATE < '1993-01-01' group by L_PARTKEY";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "1:Project\n" +
                "  |  <slot 1> : 1: L_ORDERKEY\n" +
                "  |  <slot 2> : 2: L_PARTKEY\n" +
                "  |  <slot 26> : 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lineitem_partition\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/7",
                "7:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 23: cast = 18: v1\n" +
                        "  |  \n" +
                        "  |----6:EXCHANGE\n" +
                        "  |    \n" +
                        "  4:Project\n" +
                        "  |  <slot 2> : 2: L_PARTKEY\n" +
                        "  |  <slot 23> : CAST(2: L_PARTKEY AS BIGINT)\n" +
                        "  |  <slot 24> : 24: max\n" +
                        "  |  <slot 25> : 25: sum\n" +
                        "  |  \n" +
                        "  3:AGGREGATE (update finalize)\n" +
                        "  |  output: sum(26: expr), max(1: L_ORDERKEY)\n" +
                        "  |  group by: 2: L_PARTKEY\n" +
                        "  |  \n" +
                        "  2:EXCHANGE");
    }

    @Test
    public void testPruneDistinctWindow() throws Exception {
        String sql = "select distinct t1c, t1d, t1g, amount " +
                " from (" +
                " select  t1b, t1c, t1d, t1g, id_date, \n" +
                "     sum(id_decimal)over(partition by t1c) as amount\n" +
                "from test_all_type_not_null) tt";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  5:ANALYTIC\n" +
                "  |  functions: [, sum[([12: sum, DECIMAL128(38,2), true]);" +
                " args: DECIMAL128; result: DECIMAL128(38,2); args nullable: true; result nullable: true], ]");
        assertContains(plan, "2:AGGREGATE (update finalize)");
    }

    @Test
    public void testPushDownWithNestedCaseWhenIfs() throws Exception {
        String sql = """
                WITH cte1 AS (
                  SELECT
                    t.t1d AS fk,
                    t.t1a AS cat,
                    CASE WHEN t.t1b = 1 THEN t.t1e ELSE t.t1f END AS cval
                  FROM test_all_type t
                ),
                cte2 AS (
                  SELECT a.cval, a.fk, a.cat
                  FROM cte1 a
                  LEFT JOIN t1 ON a.fk = t1.v4
                ),
                cte3 AS (
                  SELECT CASE WHEN c.cat THEN c.cval ELSE NULL END gval, c.fk
                  FROM cte2 c
                )
                SELECT SUM(gval)
                FROM cte3
                GROUP BY fk;
                """;
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  aggregate: sum[([21: cast, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result" +
                " nullable: true], sum[([6: t1f, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result" +
                " nullable: true]\n" +
                "  |  group by: [1: t1a, VARCHAR, true], [2: t1b, SMALLINT, true], [4: t1d, BIGINT, true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: t1a, VARCHAR, true]\n" +
                "  |  2 <-> [2: t1b, SMALLINT, true]\n" +
                "  |  4 <-> [4: t1d, BIGINT, true]\n" +
                "  |  6 <-> [6: t1f, DOUBLE, true]\n" +
                "  |  21 <-> cast([5: t1e, FLOAT, true] as DOUBLE)\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: test_all_type, rollup: test_all_type\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                "     tabletList=10140,10142,10144\n" +
                "     actualRows=0, avgRowSize=6.0\n" +
                "     cardinality: 1");

    }

    @Test
    public void testRewriterSharedMutationWithCaseWhen() throws Exception {
        // Bug: PushDownAggregateRewriter.rewriteProject() mutates shared CaseWhenOperator
        // in-place via setThenClause(). When two aggregations (SUM + MIN) reference the same
        // CASE WHEN column, the first aggregation's processing corrupts the CaseWhenOperator,
        // causing the second aggregation to see pushed-down column refs instead of original columns.
        String sql = "SELECT SUM(sub.cval), MIN(sub.cval), sub.fk " +
                "FROM ( " +
                "    SELECT t1d AS fk, " +
                "           CASE WHEN t1b = 1 THEN t1e ELSE NULL END AS cval " +
                "    FROM test_all_type " +
                ") sub " +
                "JOIN t0 ON sub.fk = t0.v1 " +
                "GROUP BY sub.fk";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "sum");
        assertContains(plan, "min");
    }

    @Test
    public void testRewriterSharedMutationWithIf() throws Exception {
        // Bug: PushDownAggregateRewriter.rewriteProject() mutates shared CallOperator (IF)
        // in-place via setChild(). Same root cause as the CaseWhen bug but on the IF path.
        String sql = "SELECT SUM(sub.cval), MIN(sub.cval), sub.fk " +
                "FROM ( " +
                "    SELECT t1d AS fk, " +
                "           IF(t1b = 1, t1e, NULL) AS cval " +
                "    FROM test_all_type " +
                ") sub " +
                "JOIN t0 ON sub.fk = t0.v1 " +
                "GROUP BY sub.fk";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "sum");
        assertContains(plan, "min");
    }
}
