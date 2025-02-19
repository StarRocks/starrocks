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
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.statistic.MockHistogramStatisticStorage;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class SkewJoinTest extends PlanTestBase {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, temp.newFolder().toURI().toString());

        int scale = 100;
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockHistogramStatisticStorage(scale));
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        connectContext.getSessionVariable().setEnableStatsToOptimizeSkewJoin(true);

        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("region");
        setTableStatistics(t0, 5);

        OlapTable t5 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("nation");
        setTableStatistics(t5, 25);

        OlapTable t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("supplier");
        setTableStatistics(t1, 10000 * scale);

        OlapTable t4 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("customer");
        setTableStatistics(t4, 150000 * scale);

        OlapTable t6 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("part");
        setTableStatistics(t6, 200000 * scale);

        OlapTable t2 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("partsupp");
        setTableStatistics(t2, 800000 * scale);

        OlapTable t3 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("orders");
        setTableStatistics(t3, 1500000 * scale);

        OlapTable t7 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("lineitem");
        setTableStatistics(t7, 6000000 * scale);

        starRocksAssert.withTable("create table struct_tbl(c0 INT, " +
                "c1 struct<a int, b array<struct<a int, b int>>>," +
                "c2 struct<a int, b int>," +
                "c3 struct<a int, b int, c struct<a int, b int>, d array<int>>) " +
                "duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
    }

    @Test
    public void testSkewJoin() throws Exception {
        String sql = "select v2, v5 from t0 join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, " equal join conjunct: 7: rand_col = 14: rand_col\n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");
        assertCContains(sqlPlan, "  |  <slot 10> : 10: unnest\n" +
                "  |  <slot 11> : 0\n" +
                "  |  <slot 12> : 1000");

        int oldSkewRange = connectContext.getSessionVariable().getSkewJoinRandRange();
        connectContext.getSessionVariable().setSkewJoinRandRange(10);
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "<slot 10> : 10: unnest\n" +
                "  |  <slot 11> : 0\n" +
                "  |  <slot 12> : 10");

        connectContext.getSessionVariable().setSkewJoinRandRange(oldSkewRange);
    }

    @Test
    public void testSkewJoinWithLeftJoin() throws Exception {
        String sql = "select v2, v5 from t0 left join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, " join op: LEFT OUTER JOIN (PARTITIONED)");

        sql = "select v2 from t0 left semi join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "LEFT SEMI JOIN (PARTITIONED)");

        sql = "select v2 from t0 left anti join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "LEFT ANTI JOIN (PARTITIONED)");
    }

    @Test
    public void testSkewJoinWithException1() throws Exception {
        String sql = "select v2, v5 from t0 right join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        expectedException.expect(StarRocksPlannerException.class);
        expectedException.expectMessage("RIGHT JOIN does not support SKEW JOIN optimize");
        getFragmentPlan(sql);
    }

    @Test
    public void testSkewJoinWithException2() throws Exception {
        String sql = "select v2, v5 from t0 right semi join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        expectedException.expect(StarRocksPlannerException.class);
        expectedException.expectMessage("RIGHT JOIN does not support SKEW JOIN optimize");
        getFragmentPlan(sql);
    }

    @Test
    public void testSkewJoinWithException3() throws Exception {
        String sql = "select v2, v5 from t0 right anti join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        expectedException.expect(StarRocksPlannerException.class);
        expectedException.expectMessage("RIGHT JOIN does not support SKEW JOIN optimize");
        getFragmentPlan(sql);
    }

    @Test
    public void testSkewJoinWithException4() throws Exception {
        String sql = "select v2, v5 from t0 cross join[skew|t0.v1(1,2)] t1";
        expectedException.expect(StarRocksPlannerException.class);
        expectedException.expectMessage("CROSS JOIN does not support SKEW JOIN optimize");
        getFragmentPlan(sql);
    }

    @Test
    public void testSkewJoinWithException5() throws Exception {
        String sql = "select v2, v5 from t0 join[skew|t0.v1(1,2)] t1";
        expectedException.expect(StarRocksPlannerException.class);
        expectedException.expectMessage("CROSS JOIN does not support SKEW JOIN optimize");
        getFragmentPlan(sql);
    }

    @Test
    public void testSkewJoinWithException6() throws Exception {
        String sql = "select v2, v5 from t0 left join[skew|abs(t0.v1)(1,2)] t1 on v1 = v4 ";
        expectedException.expect(StarRocksPlannerException.class);
        expectedException.expectMessage("Skew join column must be a column reference");
        getFragmentPlan(sql);
    }

    @Test
    public void testSkewJoinWithException7() throws Exception {
        String sql = "select t1.c2, t3.c3 from hive0.partitioned_db.t1 join[skew] hive0.partitioned_db.t3" +
                " on t1.c1 = t3.c1";
        expectedException.expect(StarRocksPlannerException.class);
        expectedException.expectMessage("Skew join column must be specified");
        getFragmentPlan(sql);
    }

    @Test
    public void testSkewJoinWithHiveTable() throws Exception {
        String sql = "select t1.c2, t3.c3 from hive0.partitioned_db.t1 join[skew|t1.c1(1,2)] hive0.partitioned_db.t3" +
                " on t1.c1 = t3.c1";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, " equal join conjunct: 9: rand_col = 16: rand_col\n" +
                "  |  equal join conjunct: 1: c1 = 5: c1");
        assertCContains(sqlPlan, " 5:Project\n" +
                "  |  <slot 11> : [1,2]");

        sql = "select t1.c2, t3.c3 from hive0.partitioned_db.t1 join[skew|t1.c2('xx','abc','skew')] hive0.partitioned_db.t3" +
                " on t1.c2 = t3.c2";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "5:Project\n" +
                "  |  <slot 11> : ['xx','abc','skew']");
    }

    @Test
    public void testSkewJoinWithStructType() throws Exception {
        String sql = "select struct_tbl.c0, struct_tbl.c2.a, t3.c2 from default_catalog.test.struct_tbl " +
                "join[skew|test.struct_tbl.c1.a(1,2)] hive0.partitioned_db.t3 on c1.a = t3.c1 ";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 11: rand_col = 18: rand_col\n" +
                "  |  equal join conjunct: 10: expr = 5: c1");
        // exchange join on predicate child
        sql = "select struct_tbl.c0, struct_tbl.c2.a, t3.c2 from default_catalog.test.struct_tbl " +
                "join[skew|test.struct_tbl.c1.a(1,2)] hive0.partitioned_db.t3 on t3.c1 = c1.a";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 11: rand_col = 18: rand_col\n" +
                "  |  equal join conjunct: 10: expr = 5: c1");
        // will add cast at the predicate
        sql = "select struct_tbl.c0, struct_tbl.c2.a, t0.v2 from default_catalog.test.struct_tbl " +
                "join[skew|test.struct_tbl.c1.a(1,2)] test.t0 on c1.a = t0.v1 ";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: rand_col = 17: rand_col\n" +
                "  |  equal join conjunct: 9: cast = 5: v1");

        sql = "select struct_tbl.c0, struct_tbl.c2.a, t0.v2 from default_catalog.test.struct_tbl " +
                "join[skew|test.struct_tbl.c1.a(1,2)] test.t0 on t0.v1 = c1.a ";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: rand_col = 17: rand_col\n" +
                "  |  equal join conjunct: 9: cast = 5: v1",
                "<slot 10> : CASE WHEN 2: c1.a[true] IS NULL THEN 23: round WHEN 2: c1.a[true] IN (1, 2) THEN " +
                        "23: round ELSE 0 END");
    }

    @Test
    public void testSkewJoinWithStructTypeStats() throws Exception {
        String sql = "select col_struct.c0, col_struct.c1.c11 from hive0.subfield_db.subfield join hive0.partitioned_db.t3 " +
                "on subfield.col_struct.c1.c11 = t3.c1";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: rand_col = 17: rand_col\n" +
                "  |  equal join conjunct: 9: expr = 3: c1");
    }

    @Test
    public void testSkewJoinWithMultiJoin() throws Exception {
        String sql = "select t1.c2, t3.c3 from hive0.partitioned_db.t1 join[skew|t1.c1(1,2)] hive0.partitioned_db.t3" +
                " on t1.c1 = t3.c1 join hive0.partitioned_db2.t2 on t1.c2 = t2.c2";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "equal join conjunct: 2: c2 = 10: c2");

        sql = "select t1.c2, t3.c3 from hive0.partitioned_db.t1 join[skew|t1.c1(1,2)] hive0.partitioned_db.t3" +
                " on t1.c1 = t3.c1 join[skew|t1.c2('a','b','c')] hive0.partitioned_db2.t2 on t1.c2 = t2.c2";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "equal join conjunct: 21: rand_col = 28: rand_col\n" +
                "  |  equal join conjunct: 1: c1 = 5: c1");
        assertCContains(sqlPlan, "equal join conjunct: 13: rand_col = 20: rand_col\n" +
                "  |  equal join conjunct: 2: c2 = 10: c2");
    }

    @Test
    public void testSkewJoinWithStats() throws Exception {
        String sql = "select * from test.customer join test.part on c_mktsegment = p_name and c_custkey = p_partkey ";
        String sqlPlan = getFragmentPlan(sql);
        // rewrite success
        assertCContains(sqlPlan, "equal join conjunct: 20: rand_col = 27: rand_col\n" +
                "  |  equal join conjunct: 7: C_MKTSEGMENT = 11: P_NAME\n" +
                "  |  equal join conjunct: 1: C_CUSTKEY = 10: P_PARTKEY");

        int skewJoinUseMCVCount = connectContext.getSessionVariable().getSkewJoinOptimizeUseMCVCount();
        double skewDataThreshold = connectContext.getSessionVariable().getSkewJoinDataSkewThreshold();
        connectContext.getSessionVariable().setSkewJoinOptimizeUseMCVCount(1);
        connectContext.getSessionVariable().setSkewJoinDataSkewThreshold(0.3);
        sqlPlan = getFragmentPlan(sql);
        // not rewrite because of the skewJoinUseMCVCount and skewDataThreshold
        assertCContains(sqlPlan, "equal join conjunct: 7: C_MKTSEGMENT = 11: P_NAME\n" +
                "  |  equal join conjunct: 1: C_CUSTKEY = 10: P_PARTKEY");
        connectContext.getSessionVariable().setSkewJoinDataSkewThreshold(skewDataThreshold);
        connectContext.getSessionVariable().setSkewJoinOptimizeUseMCVCount(skewJoinUseMCVCount);

        sql = "select * from test.customer join test.part on trim(c_mktsegment) = p_name and c_custkey = p_partkey ";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "equal join conjunct: 20: trim = 11: P_NAME\n" +
                "  |  equal join conjunct: 1: C_CUSTKEY = 10: P_PARTKEY");

        sql = "select c_custkey, sum(p_retailprice) from (select c_custkey, c_mktsegment from test.customer) c" +
                " join (select p_name,p_retailprice from test.part) p on c.c_mktsegment = p.p_name group by c_custkey";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "equal join conjunct: 21: rand_col = 28: rand_col\n" +
                "  |  equal join conjunct: 7: C_MKTSEGMENT = 11: P_NAME");

        sql = "select * from test.customer join test.part on p_name = c_mktsegment and p_partkey = c_custkey";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "equal join conjunct: 20: rand_col = 27: rand_col\n" +
                "  |  equal join conjunct: 7: C_MKTSEGMENT = 11: P_NAME\n" +
                "  |  equal join conjunct: 1: C_CUSTKEY = 10: P_PARTKEY");
    }

    @Test
    public void testSkewJoinWithStats2() throws Exception {
        // test hive partitioned table
        String sql = "select l_returnflag, t3.c3 from hive0.partitioned_db.lineitem_par join hive0.partitioned_db.t3" +
                " on l_returnflag = t3.c3";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "equal join conjunct: 21: rand_col = 28: rand_col\n" +
                "  |  equal join conjunct: 9: l_returnflag = 19: c3", "cardinality=540034112");
    }

    @Test
    public void testIntSkewColumnVarchar() throws Exception {
        connectContext.getSessionVariable().setSkewJoinDataSkewThreshold(0);
        ((MockHistogramStatisticStorage) connectContext.getGlobalStateMgr()
                .getStatisticStorage()).addHistogramStatistis("c_nationkey", Type.INT, 100);

        String sql = "select * from test.customer join test.part on P_SIZE = C_NATIONKEY and p_partkey = c_custkey";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "C_NATIONKEY IN (22, 23, 24, 10, 11)");
    }
}
