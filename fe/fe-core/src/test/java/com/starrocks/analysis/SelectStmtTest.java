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
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SelectStmtTest {
    private static StarRocksAssert starRocksAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
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

        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr)
                .withTable(createBaseAllStmtStr)
                .withTable(createPratitionTableStr);
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
        for (String q: queryList) {
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
                "  |  <slot 5> : 5: cast\n" +
                "  |  <slot 6> : 6: cast\n" +
                "  |  <slot 8> : CAST(murmur_hash3_32(CAST(6: cast AS VARCHAR)) % 512 AS SMALLINT)"));
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
}
