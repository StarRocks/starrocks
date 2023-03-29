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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/SelectStmtTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SelectStmtTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr = "create table db1.tbl1 (\n"
                + "    k1 varchar(32),\n"
                + "    k2 varchar(32),\n"
                + "    k3 varchar(32),\n"
                + "    k4 int,\n"
                + "    k5 varchar(32),\n"
                + "    k6 varchar(32)\n"
                + ") AGGREGATE KEY(k1, k2, k3, k4, k5, k6) \n"
                + "distributed by hash(k1) buckets 3 \n"
                + "properties('replication_num' = '1');\n";

        String createTblStmtStr2 = "create table db1.tbl2 (\n"
                + "    v1 varchar(32),\n"
                + "    v2 varchar(32),\n"
                + "    v3 int,\n"
                + "    v4 int\n"
                + ") DUPLICATE KEY(v1, v2, v3, v4) \n"
                + "distributed by hash(v1) buckets 3 \n"
                + "properties('replication_num' = '1');";

        String createBaseAllStmtStr = "create table db1.baseall(k1 int) distributed by hash(k1) "
                + "buckets 3 properties('replication_num' = '1');";
        String createPratitionTableStr = "CREATE TABLE db1.partition_table (\n" +
                "datekey int(11) NULL COMMENT \"datekey\",\n" +
                "poi_id bigint(20) NULL COMMENT \"poi_id\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(datekey, poi_id)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(datekey)\n" +
                "(PARTITION p20200727 VALUES [(\"20200726\"), (\"20200727\")),\n" +
                "PARTITION p20200728 VALUES [(\"20200727\"), (\"20200728\")))\n" +
                "DISTRIBUTED BY HASH(poi_id) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"storage_type\" = \"COLUMN\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";

        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr)
                .withTable(createTblStmtStr2)
                .withTable(createBaseAllStmtStr)
                .withTable(createPratitionTableStr);

    }

    @Before
    public void init() {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(TimeUnit.MINUTES.toMillis(20L));
    }

    @Test
    public void testGroupByConstantExpression() throws Exception {
        String sql = "SELECT k1 - 4*60*60 FROM baseall GROUP BY k1 - 4*60*60";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    public void testWithWithoutDatabase() throws Exception {
        String sql = "with tmp as (select count(*) from db1.tbl1) select * from tmp;";
        starRocksAssert.withoutUseDatabase();
        starRocksAssert.query(sql).explainQuery();

        sql = "with tmp as (select * from db1.tbl1) " +
                "select a.k1, b.k2, a.k3 from (select k1, k3 from tmp) a " +
                "left join (select k1, k2 from tmp) b on a.k1 = b.k1;";
        starRocksAssert.withoutUseDatabase();
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    public void testDataGripSupport() throws Exception {
        String sql = "select schema();";
        starRocksAssert.query(sql).explainQuery();
        sql = "select\n" +
                "collation_name,\n" +
                "character_set_name,\n" +
                "is_default collate utf8_general_ci = 'Yes' as is_default\n" +
                "from information_schema.collations";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    public void testEqualExprNotMonotonic() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "select k1 from db1.baseall where (k1=10) = true";
        String expectString =
                "[TPlanNode(node_id:0, node_type:OLAP_SCAN_NODE, num_children:0, limit:-1, row_tuples:[0], " +
                        "nullable_tuples:[false], conjuncts:[TExpr(nodes:[TExprNode(node_type:BINARY_PRED, " +
                        "type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:BOOLEAN))]), " +
                        "opcode:EQ, num_children:2, output_scale:-1, vector_opcode:INVALID_OPCODE, child_type:BOOLEAN, " +
                        "has_nullable_child:true, is_nullable:true, is_monotonic:false)";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));
    }

    @Test
    public void testCurrentUserFunSupport() throws Exception {
        String sql = "select current_user()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_user";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    public void testTimeFunSupport() throws Exception {
        String sql = "select current_timestamp()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_timestamp";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_time()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_time";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_date()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_date";
        starRocksAssert.query(sql).explainQuery();
        sql = "select localtime()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select localtime";
        starRocksAssert.query(sql).explainQuery();
        sql = "select localtimestamp()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select localtimestamp";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    public void testDateTruncUpperCase() throws Exception {
        String sql = "select date_trunc('MONTH', CAST('2020-11-04 11:12:13' AS DATE));";
        ConnectContext ctx = starRocksAssert.getCtx();
        UtFrameUtils.parseStmtWithNewParser(sql, ctx);
    }

    @Test
    public void testSelectFromTabletIds() throws Exception {
        FeConstants.runningUnitTest = true;
        ShowResultSet tablets = starRocksAssert.showTablet("db1", "partition_table");
        List<String> tabletIds = tablets.getResultRows().stream().map(r -> r.get(0)).collect(Collectors.toList());
        Assert.assertEquals(tabletIds.size(), 4);
        String tabletCsv = String.join(",", tabletIds);
        String sql = String.format("select count(1) from db1.partition_table tablet (%s)", tabletCsv);
        String explain = starRocksAssert.query(sql).explainQuery();
        Assert.assertTrue(explain.contains(tabletCsv));

        String invalidTabletCsv = tabletIds.stream().map(id -> id + "0").collect(Collectors.joining(","));
        String invalidSql = String.format("select count(1) from db1.partition_table tablet (%s)", invalidTabletCsv);
        try {
            starRocksAssert.query(invalidSql).explainQuery();
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getMessage().contains("Invalid tablet"));
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testNegateEqualForNullInWhereClause() throws Exception {
        String[] queryList = {
                "select * from db1.tbl1 where not(k1 <=> NULL)",
                "select * from db1.tbl1 where not(k1 <=> k2)",
                "select * from db1.tbl1 where not(k1 <=> 'abc-def')",
        };
        Pattern re = Pattern.compile("PREDICATES: NOT.*<=>.*");
        for (String q : queryList) {
            String s = starRocksAssert.query(q).explainQuery();
            Assert.assertTrue(re.matcher(s).find());
        }
    }

    @Test
    public void testSimplifiedPredicateRuleApplyToNegateEuqualForNull() throws Exception {
        String[] queryList = {
                "select not(k1 <=> NULL) from db1.tbl1",
                "select not(NULL <=> k1) from db1.tbl1",
                "select not(k1 <=> 'abc-def') from db1.tbl1",
        };
        Pattern re = Pattern.compile("NOT.*<=>.*");
        for (String q : queryList) {
            String s = starRocksAssert.query(q).explainQuery();
            Assert.assertTrue(re.matcher(s).find());
        }
    }

    private void assertNoCastStringAsStringInPlan(String sql) throws Exception {
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second;
        List<ScalarOperator> operators = execPlan.getPhysicalPlan().getInputs().stream().flatMap(input ->
                input.getOp().getProjection().getColumnRefMap().values().stream()).collect(Collectors.toList());
        Assert.assertTrue(operators.stream().noneMatch(op -> (op instanceof CastOperator) &&
                op.getType().isStringType() &&
                op.getChild(0).getType().isStringType()));
    }

    @Test
    public void testFoldCastOfChildExprsOfSetOperation() throws Exception {
        String sql0 = "select cast('abcdefg' as varchar(2)) a, cast('abc' as  varchar(3)) b\n" +
                "intersect\n" +
                "select cast('aa123456789' as varchar) a, cast('abcd' as varchar(4)) b";

        String sql1 = "select k1, group_concat(k2) as k2 from db1.tbl1 group by k1 \n" +
                "except\n" +
                "select k1, cast(k4 as varchar(255)) from db1.tbl1";

        String sql2 = "select k1, k2 from db1.tbl1\n" +
                "union all\n" +
                "select cast(concat(k1, 'abc') as varchar(256)) as k1, cast(concat(k2, 'abc') as varchar(256)) as k2 " +
                "from db1.tbl1\n" +
                "union all\n" +
                "select cast('abcdef' as varchar) k1, cast('deadbeef' as varchar(1999)) k2";
        for (String sql : Arrays.asList(sql0, sql1, sql2)) {
            assertNoCastStringAsStringInPlan(sql);
        }
    }

    @Test
    public void testCatalogFunSupport() throws Exception {
        String sql = "select current_catalog()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_catalog";
        starRocksAssert.query(sql).explainQuery();

    }

    @Test
    public void testBanSubqueryAppearsInLeftSideChildOfInPredicates()
            throws Exception {
        String sql = "select k1, count(k2) from db1.tbl1 group by k1 " +
                "having (exists (select k1 from db1.tbl1 where NULL)) in (select k1 from db1.tbl1 where NULL);";
        try {
            UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Subquery in left-side child of in-predicate is not supported"));
        }
    }

    @Test
    public void testGroupByCountDistinctWithSkewHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select cast(k1 as int), count(distinct [skew] cast(k2 as int)) from db1.tbl1 group by cast(k1 as int)";
        String s = starRocksAssert.query(sql).explainQuery();
        Assert.assertTrue(s, s.contains("  3:Project\n" +
                "  |  <slot 7> : 7: cast\n" +
                "  |  <slot 8> : 8: cast\n" +
                "  |  <slot 10> : CAST(murmur_hash3_32(CAST(8: cast AS VARCHAR)) % 512 AS SMALLINT)"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testGroupByMultiColumnCountDistinctWithSkewHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select cast(k1 as int), k3, count(distinct [skew] cast(k2 as int)) from db1.tbl1 group by cast(k1 as int), k3";
        String s = starRocksAssert.query(sql).explainQuery();
        Assert.assertTrue(s, s.contains("  3:Project\n" +
                "  |  <slot 3> : 3: k3\n" +
                "  |  <slot 7> : 7: cast\n" +
                "  |  <slot 8> : 8: cast\n" +
                "  |  <slot 10> : CAST(murmur_hash3_32(CAST(8: cast AS VARCHAR)) % 512 AS SMALLINT)"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testGroupByMultiColumnMultiCountDistinctWithSkewHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select k1, k3, count(distinct [skew] k2), count(distinct k4) from db1.tbl1 group by k1, k3";
        String s = starRocksAssert.query(sql).explainQuery();
        Assert.assertTrue(s, s.contains("  4:Project\n" +
                "  |  <slot 9> : 9: k1\n" +
                "  |  <slot 10> : 10: k2\n" +
                "  |  <slot 11> : 11: k3\n" +
                "  |  <slot 15> : CAST(murmur_hash3_32(10: k2) % 512 AS SMALLINT)"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testGroupByCountDistinctUseTheSameColumn()
            throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select k3, count(distinct [skew] k3) from db1.tbl1 group by k3";
        String s = starRocksAssert.query(sql).explainQuery();
        Assert.assertFalse(s, s.contains("murmur_hash3_32"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testGroupByCountDistinctWithSkewHintExtTestCase01() throws Exception {
        FeConstants.runningUnitTest = true;

        String sql = "SELECT\n"
                + "  SUM(tbl1.k5) / COUNT(DISTINCT [skew] tbl1.k3),\n"
                + "  SUM(tbl1.k6),\n"
                + "  COUNT(DISTINCT [skew] tbl1.k2),\n"
                + "  tbl1.k1 AS k1,\n"
                + "  tbl2.v1 AS v1\n"
                + "FROM db1.tbl1 as tbl1 \n"
                + "LEFT JOIN db1.tbl2 as tbl2 ON tbl1.k4 = tbl2.v4\n"
                + "GROUP BY\n"
                + "  tbl1.k1,\n"
                + "  tbl2.v1\n"
                + "LIMIT 100";
        String explain = starRocksAssert.query(sql).explainQuery();

        Assert.assertTrue(explain, explain.contains("  9:Project\n" +
                "  |  <slot 16> : 16: k1\n" +
                "  |  <slot 17> : 17: k3\n" +
                "  |  <slot 18> : 18: v1\n" +
                "  |  <slot 26> : CAST(murmur_hash3_32(17: k3) % 512 AS SMALLINT)"));

        Assert.assertTrue(explain, explain.contains("  18:Project\n" +
                "  |  <slot 19> : 19: k1\n" +
                "  |  <slot 20> : 20: k2\n" +
                "  |  <slot 21> : 21: v1\n" +
                "  |  <slot 27> : CAST(murmur_hash3_32(20: k2) % 512 AS SMALLINT)"));

        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testGroupByCountDistinctWithSkewHintExtTestCase02() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setForceDistinctColumnBucketization(true);

        String sql = "SELECT\n"
                + "  SUM(tbl1.k5) / COUNT(DISTINCT tbl1.k3),\n"
                + "  SUM(tbl1.k6),\n"
                + "  COUNT(DISTINCT tbl1.k2),\n"
                + "  tbl1.k1 AS k1,\n"
                + "  tbl2.v1 AS v1\n"
                + "FROM db1.tbl1 as tbl1 \n"
                + "LEFT JOIN db1.tbl2 as tbl2 ON tbl1.k4 = tbl2.v4\n"
                + "GROUP BY\n"
                + "  tbl1.k1,\n"
                + "  tbl2.v1\n"
                + "LIMIT 100";
        String explain = starRocksAssert.query(sql).explainQuery();

        Assert.assertTrue(explain, explain.contains("  9:Project\n" +
                "  |  <slot 16> : 16: k1\n" +
                "  |  <slot 17> : 17: k3\n" +
                "  |  <slot 18> : 18: v1\n" +
                "  |  <slot 26> : CAST(murmur_hash3_32(17: k3) % 512 AS SMALLINT)"));

        Assert.assertTrue(explain, explain.contains("  18:Project\n" +
                "  |  <slot 19> : 19: k1\n" +
                "  |  <slot 20> : 20: k2\n" +
                "  |  <slot 21> : 21: v1\n" +
                "  |  <slot 27> : CAST(murmur_hash3_32(20: k2) % 512 AS SMALLINT)"));

        connectContext.getSessionVariable().setForceDistinctColumnBucketization(false);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testGroupByCountDistinctWithSkewHintExtTestCase03() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setNewPlanerAggStage(1);

        String sql = "SELECT \n"
                + "    COUNT(DISTINCT [skew] tbl1.k2),"
                + "    SUM( cast(tbl1.k3 as int) ) / COUNT(DISTINCT [skew] tbl1.k2),\n"
                + "    tbl1.k1 AS k1,\n"
                + "    tbl2.v1 AS v1\n"
                + "FROM db1.tbl1 as tbl1 \n"
                + "LEFT JOIN db1.tbl2 as tbl2 ON tbl1.k4 = tbl2.v4\n"
                + "GROUP BY\n"
                + "  tbl1.k1,\n"
                + "  tbl2.v1\n"
                + "LIMIT 100";
        String explain = starRocksAssert.query(sql).explainQuery();

        Assert.assertTrue(explain, explain.contains("  7:AGGREGATE (update finalize)\n"
                + "  |  output: multi_distinct_count(2: k2), sum(11: cast)\n"
                + "  |  group by: 1: k1, 7: v1\n"
                + "  |  limit: 100"));

        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        FeConstants.runningUnitTest = false;
    }


    @Test
    public void testScalarCorrelatedSubquery() {
        try {
            String sql = "select *, (select [a.k1,a.k2] from db1.tbl1 a where a.k4 = b.k1) as r from db1.baseall b;";
            UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.fail("Must throw an exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(),
                    e.getMessage().contains("NOT support scalar correlated sub-query of type ARRAY<varchar(32)>"));
        }

        try {
            String sql = "select *, (select a.k1 from db1.tbl1 a where a.k4 = b.k1) as r from db1.baseall b;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.assertTrue(plan, plan.contains("assert_true[((9: countRows IS NULL) OR (9: countRows <= 1)"));
        } catch (Exception e) {
            Assert.fail("Should not throw an exception");
        }
    }
}
