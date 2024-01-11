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
        assertCContains(sqlPlan, " equal join conjunct: 7: rand_col = 15: cast\n" +
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
    public void testSkewJoinWithHiveTable() throws Exception {
        String sql = "select t1.c2, t3.c3 from hive0.partitioned_db.t1 join[skew|t1.c1(1,2)] hive0.partitioned_db.t3" +
                " on t1.c1 = t3.c1";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, " equal join conjunct: 9: rand_col = 17: cast\n" +
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
        assertCContains(sqlPlan, "1:Project\n" +
                "  |  <slot 1> : 1: c0\n" +
                "  |  <slot 10> : CASE WHEN 2: c1.a IS NULL THEN round(rand() * 1000.0) WHEN 2: c1.a IN (1, 2) " +
                "THEN round(rand() * 1000.0) ELSE 0 END\n" +
                "  |  <slot 19> : 2: c1.a\n" +
                "  |  <slot 21> : 3: c2.a");
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
        assertCContains(sqlPlan, "equal join conjunct: 21: rand_col = 30: cast\n" +
                "  |  equal join conjunct: 1: c1 = 5: c1");
        assertCContains(sqlPlan, "equal join conjunct: 13: rand_col = 29: cast\n" +
                "  |  equal join conjunct: 2: c2 = 10: c2");
    }
}
