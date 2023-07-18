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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SelectStmtTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        String createBaseAllStmtStr = "create table db1.baseall(k1 int) distributed by hash(k1) "
                + "buckets 3 properties('replication_num' = '1');";
        String createDateTblStmtStr = "create table db1.t(k1 int, dt date) "
                + "DUPLICATE KEY(k1) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
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

        String createTable1 = "CREATE TABLE `t0` (\n" +
                "  `c0` varchar(24) NOT NULL COMMENT \"\",\n" +
                "  `c1` decimal128(24, 5) NOT NULL COMMENT \"\",\n" +
                "  `c2` decimal128(24, 2) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`c0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c0`) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                "); ";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr)
                .withTable(createBaseAllStmtStr)
                .withTable(createDateTblStmtStr)
                .withTable(createPratitionTableStr)
                .withTable(createTable1);
    }

    @Test
    void testGroupByConstantExpression() throws Exception {
        String sql = "SELECT k1 - 4*60*60 FROM baseall GROUP BY k1 - 4*60*60";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    void testWithWithoutDatabase() throws Exception {
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
    void testDataGripSupport() throws Exception {
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
    void testEqualExprNotMonotonic() throws Exception {
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
    void testCurrentUserFunSupport() throws Exception {
        String sql = "select current_user()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_user";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    void testTimeFunSupport() throws Exception {
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
    void testDateTruncUpperCase() throws Exception {
        String sql = "select date_trunc('MONTH', CAST('2020-11-04 11:12:13' AS DATE));";
        ConnectContext ctx = starRocksAssert.getCtx();
        UtFrameUtils.parseStmtWithNewParser(sql, ctx);
    }

    @Test
    void testSelectFromTabletIds() throws Exception {
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
    void testNegateEqualForNullInWhereClause() throws Exception {
        String[] queryList = {
                "select * from db1.tbl1 where not(k1 <=> NULL)",
                "select * from db1.tbl1 where not(k1 <=> k2)",
                "select * from db1.tbl1 where not(k1 <=> 'abc-def')",
        };
        Pattern re = Pattern.compile("PREDICATES: NOT.*<=>.*");
        for (String q: queryList) {
            String s = starRocksAssert.query(q).explainQuery();
            Assert.assertTrue(re.matcher(s).find());
        }
    }

    @Test
    void testSimplifiedPredicateRuleApplyToNegateEuqualForNull() throws Exception {
        String[] queryList = {
                "select not(k1 <=> NULL) from db1.tbl1",
                "select not(NULL <=> k1) from db1.tbl1",
                "select not(k1 <=> 'abc-def') from db1.tbl1",
        };
        Pattern re = Pattern.compile("NOT.*<=>.*");
        for (String q: queryList) {
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
    void testFoldCastOfChildExprsOfSetOperation() throws Exception {
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
    void testCatalogFunSupport() throws Exception {
        String sql = "select catalog()";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    void testBanSubqueryAppearsInLeftSideChildOfInPredicates() {
        String sql = "select k1, count(k2) from db1.tbl1 group by k1 " +
                "having (exists (select k1 from db1.tbl1 where NULL)) in (select k1 from db1.tbl1 where NULL);";
        try {
            UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Subquery in left-side child of in-predicate is not supported"));
        }
    }

    @Test
    void testGroupByCountDistinctWithSkewHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select cast(k1 as int), count(distinct [skew] cast(k2 as int)) from db1.tbl1 group by cast(k1 as int)";
        String s = starRocksAssert.query(sql).explainQuery();
        Assert.assertTrue(s, s.contains("  3:Project\n" +
                "  |  <slot 5> : 5: cast\n" +
                "  |  <slot 6> : 6: cast\n" +
                "  |  <slot 8> : CAST(murmur_hash3_32(CAST(6: cast AS VARCHAR)) % 512 AS SMALLINT)"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testGroupByCountDistinctArrayWithSkewHint() throws Exception {
        FeConstants.runningUnitTest = true;
        // array is not supported now
        String sql = "select b1, count(distinct [skew] a1) as cnt from (select split('a,b,c', ',') as a1, 'aaa' as b1) t1 group by b1";
        String s = starRocksAssert.query(sql).explainQuery();
        Assert.assertTrue(s, s.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:3: expr | 4: count\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  5:AGGREGATE (merge finalize)\n" +
                "  |  output: count(4: count)\n" +
                "  |  group by: 3: expr\n" +
                "  |  \n" +
                "  4:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(2: split)\n" +
                "  |  group by: 3: expr\n" +
                "  |  \n" +
                "  3:Project\n" +
                "  |  <slot 2> : 2: split\n" +
                "  |  <slot 3> : 'aaa'\n" +
                "  |  \n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  group by: 2: split\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 2> : split('a,b,c', ',')\n" +
                "  |  <slot 3> : 'aaa'\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testGroupByMultiColumnCountDistinctWithSkewHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select cast(k1 as int), k3, count(distinct [skew] cast(k2 as int)) from db1.tbl1 group by cast(k1 as int), k3";
        String s = starRocksAssert.query(sql).explainQuery();
        Assert.assertTrue(s, s.contains("  3:Project\n" +
                "  |  <slot 3> : 3: k3\n" +
                "  |  <slot 5> : 5: cast\n" +
                "  |  <slot 6> : 6: cast\n" +
                "  |  <slot 8> : CAST(murmur_hash3_32(CAST(6: cast AS VARCHAR)) % 512 AS SMALLINT)"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testGroupByMultiColumnMultiCountDistinctWithSkewHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select k1, k3, count(distinct [skew] k2), count(distinct k4) from db1.tbl1 group by k1, k3";
        String s = starRocksAssert.query(sql).explainQuery();
        Assert.assertTrue(s, s.contains("  4:Project\n" +
                "  |  <slot 7> : 7: k1\n" +
                "  |  <slot 8> : 8: k2\n" +
                "  |  <slot 9> : 9: k3\n" +
                "  |  <slot 13> : CAST(murmur_hash3_32(8: k2) % 512 AS SMALLINT)"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testGroupByCountDistinctUseTheSameColumn()
            throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select k3, count(distinct [skew] k3) from db1.tbl1 group by k3";
        String s = starRocksAssert.query(sql).explainQuery();
        Assert.assertFalse(s, s.contains("murmur_hash3_32"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testScalarCorrelatedSubquery() {
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
            Assert.assertTrue(plan, plan.contains("assert_true[((7: countRows IS NULL) OR (7: countRows <= 1)"));
        } catch (Exception e) {
            Assert.fail("Should not throw an exception");
        }
    }

    @ParameterizedTest
    @MethodSource("multiDistinctMultiColumnWithLimitSqls")
    void testMultiDistinctMultiColumnWithLimit(String sql, String pattern) throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(30000000);
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
        System.out.println(plan);
        Assert.assertTrue(plan, plan.contains(pattern));
    }


    private static Stream<Arguments> multiDistinctMultiColumnWithLimitSqls() {
        String[][] sqlList = {
                {"select count(distinct k1, k2), count(distinct k3) from db1.tbl1 limit 1",
                        "18:NESTLOOP JOIN\n" +
                                "  |  join op: CROSS JOIN\n" +
                                "  |  colocate: false, reason: \n" +
                                "  |  limit: 1\n" +
                                "  |  \n" +
                                "  |----17:EXCHANGE"},
                {"select * from (select count(distinct k1, k2), count(distinct k3) from db1.tbl1) t1 limit 1",
                     "18:NESTLOOP JOIN\n" +
                             "  |  join op: CROSS JOIN\n" +
                             "  |  colocate: false, reason: \n" +
                             "  |  limit: 1\n" +
                             "  |  \n" +
                             "  |----17:EXCHANGE"
                },
                {"with t1 as (select count(distinct k1, k2) as a, count(distinct k3) as b from db1.tbl1) " +
                        "select * from t1 limit 1",
                        "18:NESTLOOP JOIN\n" +
                                "  |  join op: CROSS JOIN\n" +
                                "  |  colocate: false, reason: \n" +
                                "  |  limit: 1\n" +
                                "  |  \n" +
                                "  |----17:EXCHANGE"
                },
                {"select count(distinct k1, k2), count(distinct k3) from db1.tbl1 group by k4 limit 1",
                    "14:Project\n" +
                            "  |  <slot 5> : 5: count\n" +
                            "  |  <slot 6> : 6: count\n" +
                            "  |  limit: 1\n" +
                            "  |  \n" +
                            "  13:HASH JOIN\n" +
                            "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                            "  |  colocate: false, reason: \n" +
                            "  |  equal join conjunct: 9: k4 <=> 11: k4\n" +
                            "  |  limit: "
                },
                {"select * from (select count(distinct k1, k2), count(distinct k3) from db1.tbl1 group by k4, k3) t1" +
                        " limit 1",
                       "14:Project\n" +
                               "  |  <slot 5> : 5: count\n" +
                               "  |  <slot 6> : 6: count\n" +
                               "  |  limit: 1\n" +
                               "  |  \n" +
                               "  13:HASH JOIN\n" +
                               "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                               "  |  colocate: false, reason: \n" +
                               "  |  equal join conjunct: 10: k4 <=> 12: k4\n" +
                               "  |  equal join conjunct: 9: k3 <=> 11: k3\n" +
                               "  |  limit: 1"
                },
                {"with t1 as (select count(distinct k1, k2) as a, count(distinct k3) as b from db1.tbl1 " +
                        "group by k2, k3, k4) select * from t1 limit 1",
                        "14:Project\n" +
                                "  |  <slot 11> : 11: count\n" +
                                "  |  <slot 12> : 12: count\n" +
                                "  |  limit: 1\n" +
                                "  |  \n" +
                                "  13:HASH JOIN\n" +
                                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                                "  |  colocate: false, reason: \n" +
                                "  |  equal join conjunct: 14: k2 <=> 17: k2\n" +
                                "  |  equal join conjunct: 15: k3 <=> 18: k3\n" +
                                "  |  equal join conjunct: 16: k4 <=> 19: k4\n" +
                                "  |  limit: 1"
                }
        };
        return Arrays.stream(sqlList).map(e -> Arguments.of(e[0], e[1]));
    }

    @Test
    void testSubstringConstantFolding() {
        try {
            String sql = "select * from db1.t where dt = \"2022-01-02\" or dt = cast(substring(\"2022-01-03\", 1, 10) as date);";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.assertTrue(plan, plan.contains("dt IN ('2022-01-02', '2022-01-03')"));
        } catch (Exception e) {
            Assert.fail("Should not throw an exception");
        }
    }

    @Test
    void testAnalyzeDecimalArithmeticExprIdempotently()
            throws Exception {
        {
            String sql = "select c0, sum(c2/(1+c1)) as a, sum(c2/(1+c1)) as b from t0 group by c0;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.assertTrue(plan, plan.contains("PLAN FRAGMENT 0(F00)\n" +
                    "  Output Exprs:1: c0 | 5: sum | 5: sum\n" +
                    "  Input Partition: RANDOM\n" +
                    "  RESULT SINK\n" +
                    "\n" +
                    "  2:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: sum[([4: expr, DECIMAL128(38,8), true]); " +
                    "args: DECIMAL128; result: DECIMAL128(38,8); args nullable: true; " +
                    "result nullable: true]\n" +
                    "  |  group by: [1: c0, VARCHAR, false]\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: c0, VARCHAR, false]\n" +
                    "  |  4 <-> [3: c2, DECIMAL128(24,2), false] / 1 + [2: c1, DECIMAL128(24,5), false]\n" +
                    "  |  cardinality: 1"));
        }

        {
            String sql = " select c0, sum(1/(1+cast(substr('1.12',1,4) as decimal(24,4)))) as a, " +
                    "sum(1/(1+cast(substr('1.12',1,4) as decimal(24,4)))) as b from t0 group by c0;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.assertTrue(plan, plan.contains("  Output Exprs:1: c0 | 4: sum | 4: sum\n" +
                    "  Input Partition: RANDOM\n" +
                    "  RESULT SINK\n" +
                    "\n" +
                    "  1:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: sum[(1 / 2.12); args: DECIMAL128; result: DECIMAL128(38,6);" +
                    " args nullable: true; result nullable: true]\n" +
                    "  |  group by: [1: c0, VARCHAR, false]\n" +
                    "  |  cardinality: 1"));
        }

        {
            String sql = "select c0, sum(cast(c2 as decimal(38,19))/(1+c1)) as a, " +
                    "sum(cast(c2 as decimal(38,19))/(1+c1)) as b from t0 group by c0;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.assertTrue(plan, plan.contains("PLAN FRAGMENT 0(F00)\n" +
                    "  Output Exprs:1: c0 | 5: sum | 5: sum\n" +
                    "  Input Partition: RANDOM\n" +
                    "  RESULT SINK\n" +
                    "\n" +
                    "  2:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: sum[(cast([4: expr, DECIMAL128(38,19), true] as DECIMAL128(38,18))); " +
                    "args: DECIMAL128; result: DECIMAL128(38,18); args nullable: true; result nullable: true]\n" +
                    "  |  group by: [1: c0, VARCHAR, false]\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: c0, VARCHAR, false]\n" +
                    "  |  4 <-> cast([3: c2, DECIMAL128(24,2), false] as DECIMAL128(38,19)) / 1 + " +
                    "[2: c1, DECIMAL128(24,5), false]\n" +
                    "  |  cardinality: 1"));
        }
    }
}
