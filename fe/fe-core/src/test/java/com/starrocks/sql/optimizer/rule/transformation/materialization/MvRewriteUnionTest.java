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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class MvRewriteUnionTest extends MvRewriteTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();

        starRocksAssert.withTable(cluster, "depts");
        starRocksAssert.withTable(cluster, "emps");
        starRocksAssert.withTable(cluster, "test_base_part");
        starRocksAssert.withTable(cluster, "test_all_type");

        starRocksAssert.withTable("create table emps2 (\n" +
                        "    empid int not null,\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null,\n" +
                        "    salary double\n" +
                        ")\n" +
                        "distributed by hash(`empid`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("create table depts2 (\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null\n" +
                        ")\n" +
                        "distributed by hash(`deptno`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");

        cluster.runSql("test", "insert into emps2 values(1, 1, \"emp_name1\", 100);");
        cluster.runSql("test", "insert into emps2 values(2, 1, \"emp_name1\", 120);");
        cluster.runSql("test", "insert into emps2 values(3, 1, \"emp_name1\", 150);");
        cluster.runSql("test", "insert into depts2 values(1, \"dept_name1\")");
        cluster.runSql("test", "insert into depts2 values(2, \"dept_name2\")");
        cluster.runSql("test", "insert into depts2 values(3, \"dept_name3\")");

        starRocksAssert.withTable("CREATE TABLE `test_all_type2` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `t02` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ");");
        cluster.runSql("test", "insert into t02 values(1, 2, 3)");
        cluster.runSql("test", "insert into test_all_type2 values(" +
                "\"value1\", 1, 2, 3, 4.0, 5.0, 6, \"2022-11-11 10:00:01\", \"2022-11-11\", 10.12)");

        cluster.runSql("test", "insert into test_base_part values(1, 1, 2, 3)");
        cluster.runSql("test", "insert into test_base_part values(100, 1, 2, 3)");
        cluster.runSql("test", "insert into test_base_part values(200, 1, 2, 3)");
        cluster.runSql("test", "insert into test_base_part values(1000, 1, 2, 3)");
        cluster.runSql("test", "insert into test_base_part values(2000, 1, 2, 3)");
        cluster.runSql("test", "insert into test_base_part values(2500, 1, 2, 3)");


        Table emps2 = getTable("test", "emps2");
        PlanTestBase.setTableStatistics((OlapTable) emps2, 1000000);
        Table depts2 = getTable("test", "depts2");
        PlanTestBase.setTableStatistics((OlapTable) depts2, 1000000);

        Table t0Table = getTable("test", "t02");
        PlanTestBase.setTableStatistics((OlapTable) t0Table, 1000000);
        Table testAllTypeTable = getTable("test", "test_all_type2");
        PlanTestBase.setTableStatistics((OlapTable) testAllTypeTable, 1000000);

        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
    }

    @Test
    public void testUnionRewrite1() throws Exception {
        // single table union
        createAndRefreshMv("create materialized view union_mv_1" +
                " distributed by hash(empid)  as select empid, deptno, name, salary from emps2 where empid < 3");
        MaterializedView mv1 = getMv("test", "union_mv_1");
        PlanTestBase.setTableStatistics(mv1, 10);
        String query1 = "select empid, deptno, name, salary from emps2 where empid < 5";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "0:UNION\n" +
                "  |  \n" +
                "  |----5:EXCHANGE");
        PlanTestBase.assertContains(plan1, "  3:OlapScanNode\n" +
                "     TABLE: union_mv_1");
        PlanTestBase.assertContains(plan1, "TABLE: emps2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 9: empid < 5, 9: empid >= 3");

        String query7 = "select deptno, empid from emps2 where empid < 5";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "union_mv_1");
        OptExpression optExpression7 = getOptimizedPlan(query7, connectContext);
        List<PhysicalScanOperator> scanOperators = getScanOperators(optExpression7, "union_mv_1");
        Assert.assertEquals(1, scanOperators.size());
        Assert.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("name"));
        Assert.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("salary"));

        dropMv("test", "union_mv_1");
    }

    @Test
    public void testUnionRewrite2() throws Exception {
        // multi tables query
        createAndRefreshMv("create materialized view join_union_mv_1" +
                " distributed by hash(empid)" +
                " as" +
                " select emps2.empid, emps2.salary, depts2.deptno, depts2.name" +
                " from emps2 join depts2 using (deptno) where depts2.deptno < 100");
        MaterializedView mv2 = getMv("test", "join_union_mv_1");
        PlanTestBase.setTableStatistics(mv2, 1);
        String query2 = "select emps2.empid, emps2.salary, depts2.deptno, depts2.name" +
                " from emps2 join depts2 using (deptno) where depts2.deptno < 120";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "join_union_mv_1");
        PlanTestBase.assertContains(plan2, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 15: deptno = 12: deptno\n" +
                "  |  \n" +
                "  |----3:EXCHANGE");
        dropMv("test", "join_union_mv_1");
    }

    @Test
    public void testUnionRewrite3() throws Exception {
        // multi tables query
        createAndRefreshMv("create materialized view join_union_mv_1" +
                " distributed by hash(empid)" +
                " as" +
                " select emps2.empid, emps2.salary, d1.deptno, d1.name name1, d2.name name2" +
                " from emps2 join depts2 d1 on emps2.deptno = d1.deptno" +
                " join depts2 d2 on emps2.deptno = d2.deptno where d1.deptno < 100");
        MaterializedView mv2 = getMv("test", "join_union_mv_1");
        PlanTestBase.setTableStatistics(mv2, 1);
        String query2 = "select emps2.empid, emps2.salary, d1.deptno, d1.name name1, d2.name name2" +
                " from emps2 join depts2 d1 on emps2.deptno = d1.deptno" +
                " join depts2 d2 on emps2.deptno = d2.deptno where d1.deptno < 120";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "join_union_mv_1");
        PlanTestBase.assertContains(plan2, "6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 15: deptno = 20: deptno");
        PlanTestBase.assertContains(plan2, "2:OlapScanNode\n" +
                "     TABLE: emps2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 15: deptno < 120, 15: deptno >= 100\n" +
                "     partitions=1/1");
        PlanTestBase.assertContains(plan2, "1:OlapScanNode\n" +
                "     TABLE: depts2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 18: deptno < 120, 18: deptno >= 100");
    }

    @Test
    public void testUnionRewrite4() throws Exception {
        createAndRefreshMv("create materialized view join_agg_union_mv_1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t02.v1 as v1, test_all_type2.t1d," +
                " sum(test_all_type2.t1c) as total_sum, count(test_all_type2.t1c) as total_num" +
                " from t02 join test_all_type2" +
                " on t02.v1 = test_all_type2.t1d" +
                " where t02.v1 < 100" +
                " group by v1, test_all_type2.t1d");
        MaterializedView mv3 = getMv("test", "join_agg_union_mv_1");
        PlanTestBase.setTableStatistics(mv3, 1);

        String query3 = " SELECT t02.v1 as v1, test_all_type2.t1d," +
                " sum(test_all_type2.t1c) as total_sum, count(test_all_type2.t1c) as total_num" +
                " from t02 join test_all_type2" +
                " on t02.v1 = test_all_type2.t1d" +
                " where t02.v1 < 120" +
                " group by v1, test_all_type2.t1d";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "join_agg_union_mv_1");
        dropMv("test", "join_agg_union_mv_1");

    }

    @Test
    public void testUnionRewrite5() throws Exception {
        createAndRefreshMv("create materialized view join_agg_union_mv_2" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t02.v1 as v1, test_all_type2.t1d," +
                " sum(test_all_type2.t1c) as total_sum, count(test_all_type2.t1c) as total_num" +
                " from t02 left join test_all_type2" +
                " on t02.v1 = test_all_type2.t1d" +
                " where t02.v1 < 100" +
                " group by v1, test_all_type2.t1d");
        MaterializedView mv4 = getMv("test", "join_agg_union_mv_2");
        PlanTestBase.setTableStatistics(mv4, 1);

        String query8 = " SELECT t02.v1 as v1, test_all_type2.t1d," +
                " sum(test_all_type2.t1c) as total_sum, count(test_all_type2.t1c) as total_num" +
                " from t02 left join test_all_type2" +
                " on t02.v1 = test_all_type2.t1d" +
                " where t02.v1 < 120" +
                " group by v1, test_all_type2.t1d";
        String plan8 = getFragmentPlan(query8);

        PlanTestBase.assertContains(plan8, "join_agg_union_mv_2");
        PlanTestBase.assertContains(plan8, "4:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 20: v1 = 24: t1d\n" +
                "  |  \n" +
                "  |----3:EXCHANGE");
        PlanTestBase.assertContains(plan8, "2:OlapScanNode\n" +
                "     TABLE: test_all_type2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 24: t1d >= 100, 24: t1d < 120");
        PlanTestBase.assertContains(plan8, "1:OlapScanNode\n" +
                "     TABLE: t02\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 20: v1 >= 100, 20: v1 < 120");
        dropMv("test", "join_agg_union_mv_2");
    }

    @Test
    public void testUnionRewrite6() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW `ttl_union_mv_1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`c3`)\n" +
                "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\",\n" +
                "\"partition_ttl_number\" = \"3\"\n" +
                ")\n" +
                "AS SELECT `test_base_part`.`c1`, `test_base_part`.`c3`, sum(`test_base_part`.`c2`) AS `c2`\n" +
                "FROM `test_base_part`\n" +
                "GROUP BY `test_base_part`.`c1`, `test_base_part`.`c3`;");

        MaterializedView ttlMv1 = getMv("test", "ttl_union_mv_1");
        Assert.assertNotNull(ttlMv1);
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
        Assert.assertEquals(3, ttlMv1.getPartitions().size());

        String query4 = "select c3, sum(c2) from test_base_part group by c3";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertContains(plan4, "ttl_union_mv_1", "UNION", "test_base_part");
        dropMv("test", "ttl_union_mv_1");
    }

    @Test
    public void testUnionRewrite7() throws Exception {
        starRocksAssert.withTable("CREATE TABLE multi_mv_table (\n" +
                "                    k1 INT,\n" +
                "                    v1 INT,\n" +
                "                    v2 INT)\n" +
                "                DUPLICATE KEY(k1)\n" +
                "                PARTITION BY RANGE(`k1`)\n" +
                "                (\n" +
                "                PARTITION `p1` VALUES LESS THAN ('3'),\n" +
                "                PARTITION `p2` VALUES LESS THAN ('6'),\n" +
                "                PARTITION `p3` VALUES LESS THAN ('9'),\n" +
                "                PARTITION `p4` VALUES LESS THAN ('12'),\n" +
                "                PARTITION `p5` VALUES LESS THAN ('15'),\n" +
                "                PARTITION `p6` VALUES LESS THAN ('18')\n" +
                "                )\n" +
                "                DISTRIBUTED BY HASH(k1) properties('replication_num'='1');");
        cluster.runSql("test", "insert into multi_mv_table values (1,1,1),(2,1,1),(3,1,1),\n" +
                "                                      (4,1,1),(5,1,1),(6,1,1),\n" +
                "                                      (7,1,1),(8,1,1),(9,1,1),\n" +
                "                                      (10,1,1),(11,1,1);");
        createAndRefreshMv("CREATE MATERIALIZED VIEW multi_mv_1" +
                " DISTRIBUTED BY HASH(k1) AS SELECT k1,v1,v2 from multi_mv_table where k1>1;");
        createAndRefreshMv("CREATE MATERIALIZED VIEW multi_mv_2" +
                " DISTRIBUTED BY HASH(k1) AS SELECT k1,v1,v2 from multi_mv_1 where k1>2;");
        createAndRefreshMv("CREATE MATERIALIZED VIEW multi_mv_3" +
                " DISTRIBUTED BY HASH(k1) AS SELECT k1,v1,v2 from multi_mv_2 where k1>3;");

        String query5 = "select * from multi_mv_1";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "multi_mv_1", "multi_mv_2", "UNION");
        dropMv("test", "multi_mv_1");
        dropMv("test", "multi_mv_2");
        dropMv("test", "multi_mv_3");
        starRocksAssert.dropTable("multi_mv_table");

        createAndRefreshMv("CREATE MATERIALIZED VIEW `mv_agg_1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "DISTRIBUTED BY HASH(`name`) BUCKETS 2\n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `emps`.`deptno`, `emps`.`name`, sum(`emps`.`salary`) AS `salary`\n" +
                "FROM `emps`\n" +
                "WHERE `emps`.`empid` < 5\n" +
                "GROUP BY `emps`.`deptno`, `emps`.`name`;");
        String query6 =
                "SELECT `emps`.`deptno`, `emps`.`name`, sum(salary) as salary FROM `emps` group by deptno, name;";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "mv_agg_1", "emps", "UNION");
        dropMv("test", "mv_agg_1");
    }

    @Test
    public void testUnionAllRewriteWithExtraPredicates() {
        starRocksAssert.withTable(new MTable("mt1", "k1",
                        ImmutableList.of(
                                "k1 INT",
                                "k2 string",
                                "v1 INT",
                                "v2 INT"
                        ),
                        "k1",
                        ImmutableList.of(
                                "PARTITION `p1` VALUES LESS THAN ('3')",
                                "PARTITION `p2` VALUES LESS THAN ('6')",
                                "PARTITION `p3` VALUES LESS THAN ('9')"
                        )
                ),
                () -> {
                    cluster.runSql("test", "insert into mt1 values (1,1,1,1), (4,2,1,1);");
                    starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW union_mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " AS SELECT k1,k2, v1,v2 from mt1;");
                    starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW union_mv0 \n" +
                            "PARTITION START ('%s') END ('%s')", "1", "3"));
                    MaterializedView mv1 = getMv("test", "union_mv0");
                    Set<String> mvNames = mv1.getPartitionNames();
                    Assert.assertEquals("[p1]", mvNames.toString());

                    {
                        String[] sqls = {
                                "SELECT k1,k2, v1,v2 from mt1 where k1=1",
                                "SELECT k1,k2, v1,v2 from mt1 where k1<3",
                                "SELECT k1,k2, v1,v2 from mt1 where k1<2",
                        };
                        for (String query : sqls) {
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertNotContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "union_mv0");
                        }
                    }
                    {
                        String query = "SELECT k1,k2, v1,v2 from mt1 where k1<6";
                        String plan = getFragmentPlan(query);
                        PlanTestBase.assertContains(plan, ":UNION");
                        PlanTestBase.assertContains(plan, "union_mv0");
                    }

                    {
                        List<Pair<String, String>> sqls = ImmutableList.of(
                                Pair.create("SELECT k1,k2, v1,v2 from mt1 where k1<6 and k2 like 'a%'",
                                        "1:OlapScanNode\n" +
                                                "     TABLE: mt1\n" +
                                                "     PREAGGREGATION: ON\n" +
                                                "     PREDICATES: 10: k2 LIKE 'a%'"),
                                // TODO: remove redundant predicates
                                Pair.create("SELECT k1,k2, v1,v2 from mt1 where k1 != 3 and k2 like 'a%'",
                                        "TABLE: mt1\n" +
                                                "     PREAGGREGATION: ON\n" +
                                                "     PREDICATES: 9: k1 != 3, (9: k1 < 3) OR (9: k1 > 3), 10: k2 LIKE 'a%'\n" +
                                                "     partitions=2/3")
                        );
                        for (Pair<String, String> p : sqls) {
                            String query = p.first;
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContainsIgnoreColRefs(plan, "union_mv0", p.second);
                        }
                    }
                    {
                        String query = "SELECT k1,k2, v1,v2 from mt1 where k1 > 0 and k2 like 'a%'";
                        String plan = getFragmentPlan(query);
                        PlanTestBase.assertNotContains(plan, ":UNION", "union_mv0");
                    }
                    {
                        List<Pair<String, String>> sqls = ImmutableList.of(
                                Pair.create("SELECT k1,k2, v1,v2 from mt1 where k1>1 and k2 like 'a%'",
                                        "1:OlapScanNode\n" +
                                                "     TABLE: mt1\n" +
                                                "     PREAGGREGATION: ON\n" +
                                                "     PREDICATES: 9: k1 > 1, (9: k1 >= 3) OR (9: k1 IS NULL), 10: k2 LIKE 'a%'"),
                                Pair.create("SELECT k1,k2, v1,v2 from mt1 where k1>1 and k2 like 'a%'",
                                        "1:OlapScanNode\n" +
                                                "     TABLE: mt1\n" +
                                                "     PREAGGREGATION: ON\n" +
                                                "     PREDICATES: 9: k1 > 1, 10: k2 LIKE 'a%', (9: k1 >= 3) OR (9: k1 IS NULL)"),
                                Pair.create("SELECT k1,k2, v1,v2 from mt1 where k1>0 and k2 like 'a%'",
                                        "1:OlapScanNode\n" +
                                                "     TABLE: mt1\n" +
                                                "     PREAGGREGATION: ON\n" +
                                                "     PREDICATES: 9: k1 > 0, 10: k2 LIKE 'a%', (9: k1 >= 3) OR (9: k1 IS NULL)")
                                );
                        QueryDebugOptions debugOptions = new QueryDebugOptions();
                        debugOptions.setEnableMVEagerUnionAllRewrite(true);
                        connectContext.getSessionVariable().setQueryDebugOptions(debugOptions.toString());
                        for (Pair<String, String> p : sqls) {
                            String query = p.first;
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContainsIgnoreColRefs(plan, "union_mv0", p.second);
                        }
                    }
                    starRocksAssert.dropMaterializedView("union_mv0");
                }
        );
    }
    @Test
    public void testAssertContainsIgnoreColRefs() {
        String p1 = "7:SELECT\n" +
                "  |  predicates: 1: k1 > 1, 2: k2 LIKE 'a%'";
        String p =
                "7:SELECT\n" +
                "  |  predicates: 2: k2 LIKE 'a%', 1: k1 > 1";
        PlanTestBase.assertContainsIgnoreColRefs(p1, p);
    }
}
