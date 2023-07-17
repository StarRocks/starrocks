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
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ScanTest extends PlanTestBase {
    @Test
    public void testScan() throws Exception {
        String sql = "select * from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains(" OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3\n"
                + "  PARTITION: RANDOM"));
    }

    @Test
    public void testInColumnPredicate() throws Exception {
        String sql = "select v1 from t0 where v1 in (v1 + v2, sin(v2))";
        String thriftPlan = getThriftPlan(sql);
        Assert.assertFalse(thriftPlan.contains("FILTER_IN"));
    }

    @Test
    public void testOlapScanSelectedIndex() throws Exception {
        String sql = "select v1 from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: t0"));
    }

    @Test
    public void testSingleTabletOutput() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        FeConstants.runningUnitTest = true;
        String sql = "select S_COMMENT from supplier;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:7: S_COMMENT\n"
                + "  PARTITION: RANDOM\n"
                + "\n"
                + "  RESULT SINK\n"
                + "\n"
                + "  0:OlapScanNode\n"
                + "     TABLE: supplier"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSingleTabletOutput2() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        FeConstants.runningUnitTest = true;
        String sql = "select SUM(S_NATIONKEY) from supplier;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:9: sum\n"
                + "  PARTITION: UNPARTITIONED\n"
                + "\n"
                + "  RESULT SINK\n"
                + "\n"
                + "  3:AGGREGATE (merge finalize)\n"
                + "  |  output: sum(9: sum)\n"
                + "  |  group by: \n"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testPreAggregation() throws Exception {
        String sql = "select k1 from t0 inner join baseall on v1 = cast(k8 as int) group by k1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 4> : 4: k1\n" +
                "  |  <slot 15> : CAST(CAST(13: k8 AS INT) AS BIGINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Predicates include the value column\n");

        sql = "select 0 from baseall inner join t0 on v1 = k1 group by (v2 + k2),k1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Group columns isn't bound table baseall");
    }

    @Test
    public void testInformationSchema() throws Exception {
        String sql = "select column_name from information_schema.columns limit 1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  RESULT SINK\n" +
                "\n" +
                "  0:SCAN SCHEMA\n" +
                "     limit: 1\n");
        ;
    }

    @Test
    public void testInformationSchema1() throws Exception {
        String sql = "select column_name, UPPER(DATA_TYPE) from information_schema.columns;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n"
                + "  |  <slot 4> : 4: COLUMN_NAME\n"
                + "  |  <slot 25> : upper(8: DATA_TYPE)\n"
                + "  |  \n"
                + "  0:SCAN SCHEMA\n"));
    }

    @Test
    public void testProject() throws Exception {
        String sql = "select v1 from t0";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PLAN FRAGMENT 0\n"
                + " OUTPUT EXPRS:1: v1\n"
                + "  PARTITION: RANDOM\n"
                + "\n"
                + "  RESULT SINK\n"
                + "\n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=0/1");
    }

    @Test
    public void testEmptySet() throws Exception {
        String queryStr = "select * from test.colocate1 t1, test.colocate2 t2 " +
                "where NOT NULL IS NULL";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  0:EMPTYSET\n"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 where FALSE";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testSingleNodeExecPlan() throws Exception {
        String sql = "select v1,v2,v3 from t0";
        connectContext.getSessionVariable().setSingleNodeExecPlan(true);
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n");
        connectContext.getSessionVariable().setSingleNodeExecPlan(false);
    }

    @Test
    public void testSchemaScan() throws Exception {
        String sql = "select * from information_schema.columns";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:SCAN SCHEMA\n"));
    }

    @Test
    public void testPreAggregationWithJoin() throws Exception {
        FeConstants.runningUnitTest = true;
        // check left agg table with pre-aggregation
        String sql = "select k2, sum(k9) from baseall join join2 on k1 = id group by k2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));

        // check right agg table with pre-agg
        sql = "select k2, sum(k9) from join2 join [broadcast] baseall on k1 = id group by k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));

        // check two agg tables only one agg table can pre-aggregation
        sql = "select t1.k2, sum(t1.k9) from baseall t1 join baseall t2 on t1.k1 = t2.k1 group by t1.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));
        Assert.assertTrue(plan.contains("1:OlapScanNode\n" +
                "  |       TABLE: baseall\n" +
                "  |       PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));

        sql = "select t2.k2, sum(t2.k9) from baseall t1 join [broadcast] baseall t2 on t1.k1 = t2.k1 group by t2.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));
        Assert.assertTrue(plan.contains("1:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));

        // check multi tables only one agg table can pre-aggregation
        sql = "select t1.k2, sum(t1.k9) from baseall t1 " +
                "join join2 t2 on t1.k1 = t2.id join baseall t3 on t1.k1 = t3.k1 group by t1.k2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON\n");
        assertContains(plan, "  2:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n");

        sql = "select t3.k2, sum(t3.k9) from baseall t1 " +
                "join [broadcast] join2 t2 on t1.k1 = t2.id join [broadcast] baseall t3 " +
                "on t1.k1 = t3.k1 group by t3.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("6:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));

        // check join predicate with non key columns
        sql = "select t1.k2, sum(t1.k9) from baseall t1 join baseall t2 on t1.k9 = t2.k9 group by t1.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Predicates include the value column"));

        sql =
                "select t1.k2, sum(t1.k9) from baseall t1 " +
                        "join baseall t2 on t1.k1 = t2.k1 where t1.k9 + t2.k9 = 1 group by t1.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Predicates include the value column"));

        // check group by two tables columns
        sql = "select t1.k2, t2.k2, sum(t1.k9) from baseall t1 join baseall t2 on t1.k1 = t2.k1 group by t1.k2, t2.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));

        // check aggregate two table columns
        sql =
                "select t1.k2, t2.k2, sum(t1.k9), sum(t2.k9) from baseall t1 " +
                        "join baseall t2 on t1.k1 = t2.k1 group by t1.k2, t2.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testPreAggregateForCrossJoin() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select join1.id from join1, join2 group by join1.id";
        String plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON"));
        Assert.assertTrue(plan.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON"));

        // AGGREGATE KEY table PREAGGREGATION should be off
        sql = "select join2.id from baseall, join2 group by join2.id";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON"));
        Assert.assertTrue(plan.contains("  1:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSetVar() throws Exception {
        String sql = "select * from db1.tbl3 as t1 JOIN db1.tbl4 as t2 ON t1.c2 = t2.c2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (BROADCAST)"));

        sql = "select /*+ SET_VAR(broadcast_row_limit=0) */ * from db1.tbl3 as t1 JOIN db1.tbl4 as t2 ON t1.c2 = t2.c2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testFilter() throws Exception {
        String sql = "select v1 from t0 where v2 > 1";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 2: v2 > 1"));
    }

    @Test
    public void testMergeTwoFilters() throws Exception {
        String sql = "select v1 from t0 where v2 < null group by v1 HAVING NULL IS NULL;";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "  1:AGGREGATE (update finalize)\n"
                + "  |  group by: 1: v1");

        Assert.assertTrue(planFragment.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testScalarReuseIsNull() throws Exception {
        String sql =
                getFragmentPlan("SELECT (abs(1) IS NULL) = true AND ((abs(1) IS NULL) IS NOT NULL) as count FROM t1;");
        Assert.assertTrue(sql.contains("1:Project\n"
                + "  |  <slot 4> : (6: expr = TRUE) AND (6: expr IS NOT NULL)\n"
                + "  |  common expressions:\n"
                + "  |  <slot 5> : abs(1)\n"
                + "  |  <slot 6> : 5: abs IS NULL"));
    }

    @Test
    public void testProjectFilterRewrite() throws Exception {
        String queryStr = "select 1 as b, MIN(v1) from t0 having (b + 1) != b;";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  1:AGGREGATE (update finalize)\n"
                + "  |  output: min(1: v1)\n"
                + "  |  group by: \n"));
    }

    @Test
    public void testMergeProject() throws Exception {
        String sql = "select case when v1 then 2 else 2 end from (select v1, case when true then v1 else v1 end as c2"
                + " from t0 limit 1) as x where c2 > 2 limit 2;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 4> : 2\n" +
                "  |  limit: 2\n" +
                "  |  \n" +
                "  2:SELECT\n" +
                "  |  predicates: 1: v1 > 2\n" +
                "  |  limit: 2\n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     limit: 1\n" +
                "\n" +
                "PLAN FRAGMENT 1");
    }

    @Test
    public void testProjectReuse() throws Exception {
        String sql = "select nullif(v1, v1) + (0) as a , nullif(v1, v1) + (1 - 1) as b from t0;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("<slot 4> : nullif(1: v1, 1: v1) + 0"));
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:4: expr | 4: expr"));
    }

    @Test
    public void testSchemaScanWithWhere() throws Exception {
        String sql = "select column_name, table_name from information_schema.columns" +
                " where table_schema = 'information_schema' and table_name = 'columns'";
        ExecPlan plan = getExecPlan(sql);
        Assert.assertTrue(((SchemaScanNode) plan.getScanNodes().get(0)).getSchemaDb().equals("information_schema"));
        Assert.assertTrue(((SchemaScanNode) plan.getScanNodes().get(0)).getSchemaTable().equals("columns"));
    }

    @Test
    public void testSchemaScanWithWhereConstantFunction() throws Exception {
        String sql = "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, " +
                "IF(TABLE_TYPE='BASE TABLE' or TABLE_TYPE='SYSTEM VERSIONED', 'TABLE', TABLE_TYPE) as TABLE_TYPE, " +
                "TABLE_COMMENT REMARKS, NULL TYPE_CAT, NULL TYPE_SCHEM, NULL TYPE_NAME, NULL SELF_REFERENCING_COL_NAME, " +
                "NULL REF_GENERATION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = database() AND  " +
                "TABLE_TYPE IN ('BASE TABLE','SYSTEM VERSIONED','PARTITIONED TABLE','VIEW','FOREIGN TABLE'," +
                "'MATERIALIZED VIEW','EXTERNAL TABLE') ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME";
        getExecPlan(sql);
    }

    @Test
    public void testPushDownExternalTableMissNot() throws Exception {
        String sql = "select * from ods_order where order_no not like \"%hehe%\"";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: NOT (order_no LIKE '%hehe%')");

        sql = "select * from ods_order where order_no in (1,2,3)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FROM `ods_order` WHERE (order_no IN ('1', '2', '3'))");

        sql = "select * from ods_order where order_no not in (1,2,3)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FROM `ods_order` WHERE (order_no NOT IN ('1', '2', '3'))");
    }

    @Test
    public void testMetaScanWithCount() throws Exception {
        String sql = "select count(*),count(),count(t1a),count(t1b),count(t1c) from test_all_type[_META_]";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  output: sum(count_t1a), sum(count_t1a), sum(count_t1a), sum(count_t1a)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:MetaScan\n" +
                "     Table: test_all_type\n" +
                "     <id 16> : count_t1a");
        // compatibility test
        // we should keep nullable attribute of columns consistent with previous version,
        // see more detail in the description of https://github.com/StarRocks/starrocks/pull/17619
        // without count, all columns should be not null
        sql = "select min(t1a),max(t1a),dict_merge(t1a) from test_all_type_not_null[_META_]";
        plan = getVerboseExplain(sql);
        assertContains(plan, "aggregate: " +
                "min[([min_t1a, VARCHAR, false]); args: VARCHAR; result: VARCHAR; " +
                "args nullable: false; result nullable: true], " +
                "max[([max_t1a, VARCHAR, false]); args: VARCHAR; result: VARCHAR; " +
                "args nullable: false; result nullable: true], " +
                "dict_merge[([dict_merge_t1a, VARCHAR, false]); args: INVALID_TYPE; " +
                "result: VARCHAR; args nullable: false; result nullable: true]");

        // with count, all columns should be nullable
        sql = "select min(t1a),max(t1a),dict_merge(t1a),count() from test_all_type_not_null[_META_]";
        plan = getVerboseExplain(sql);
        assertContains(plan, "min[([min_t1a, VARCHAR, true]); args: VARCHAR; result: VARCHAR; " +
                "args nullable: true; result nullable: true], " +
                "max[([max_t1a, VARCHAR, true]); args: VARCHAR; result: VARCHAR; " +
                "args nullable: true; result nullable: true], " +
                "dict_merge[([dict_merge_t1a, VARCHAR, true]); args: INVALID_TYPE; result: VARCHAR; " +
                "args nullable: true; result nullable: true], " +
                "sum[([count_t1a, VARCHAR, true]); args: BIGINT; result: BIGINT; " +
                "args nullable: true; result nullable: true]");
    }

    @Test
    public void testImplicitCast() throws Exception {
        String sql = "select count(distinct v1||v2) from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count((CAST(1: v1 AS BOOLEAN)) OR (CAST(2: v2 AS BOOLEAN)))");
    }

    @Test
    public void testPruneColumnTest() throws Exception {
        String[] sqlString = {
                "select count(*) from lineitem_partition",
                // for olap, partition key is not partition column.
                "select count(*) from lineitem_partition where l_shipdate = '1996-01-01'"
        };
        boolean[] expexted = {true, false};
        Assert.assertEquals(sqlString.length, expexted.length);
        for (int i = 0; i < sqlString.length; i++) {
            String sql = sqlString[i];
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assert.assertEquals(scanNodeList.get(0).getCanUseAnyColumn(), expexted[i]);
        }
    }

    @Test
    public void testLabelMinMaxCountTest() throws Exception {
        String[] sqlString = {
                "select count(l_orderkey) from lineitem_partition", "true",
                // for olap, partition key is not partition column.
                "select count(l_orderkey) from lineitem_partition where l_shipdate = '1996-01-01'", "false",
                "select count(distinct l_orderkey) from lineitem_partition", "false",
                "select count(l_orderkey), min(l_partkey) from lineitem_partition", "true",
                "select count(l_orderkey) from lineitem_partition group by l_partkey", "false",
                "select count(l_orderkey) from lineitem_partition limit 10", "true",
                "select count(l_orderkey), max(l_partkey), avg(l_partkey) from lineitem_partition", "false",
                "select count(l_orderkey), max(l_partkey), min(l_partkey) from lineitem_partition", "true",
        };
        Assert.assertTrue(sqlString.length % 2 == 0);
        for (int i = 0; i < sqlString.length; i += 2) {
            String sql = sqlString[i];
            boolean expexted = Boolean.valueOf(sqlString[i + 1]);
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assert.assertEquals(expexted, scanNodeList.get(0).getCanUseMinMaxCountOpt());
        }
    }
}
