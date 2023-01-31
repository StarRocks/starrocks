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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.TimeoutException;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvRewriteOptimizationTest {
    private static ConnectContext connectContext;
    private static PseudoCluster cluster;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        cluster = PseudoCluster.getInstance();
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");

        Config.enable_experimental_mv = true;

        starRocksAssert.withTable("create table emps (\n" +
                "    empid int not null,\n" +
                "    deptno int not null,\n" +
                "    name varchar(25) not null,\n" +
                "    salary double\n" +
                ")\n" +
                "distributed by hash(`empid`) buckets 10\n" +
                "properties (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");")
                .withTable("create table depts (\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null\n" +
                        ")\n" +
                        "distributed by hash(`deptno`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("create table dependents (\n" +
                        "    empid int not null,\n" +
                        "    name varchar(25) not null\n" +
                        ")\n" +
                        "distributed by hash(`empid`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("create table locations (\n" +
                        "    empid int not null,\n" +
                        "    name varchar(25) not null\n" +
                        ")\n" +
                        "distributed by hash(`empid`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");
        starRocksAssert.withTable("CREATE TABLE `test_all_type` (\n" +
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
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `table_with_partition` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`,`id_date`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`id_date`)\n" +
                "(PARTITION p1991 VALUES [('1991-01-01'), ('1992-01-01')),\n" +
                "PARTITION p1992 VALUES [('1992-01-01'), ('1993-01-01')),\n" +
                "PARTITION p1993 VALUES [('1993-01-01'), ('1994-01-01')))" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `table_with_day_partition` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`,`id_date`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`id_date`)\n" +
                "(PARTITION p19910330 VALUES [('1991-03-30'), ('1991-03-31')),\n" +
                "PARTITION p19910331 VALUES [('1991-03-31'), ('1991-04-01')),\n" +
                "PARTITION p19910401 VALUES [('1991-04-01'), ('1991-04-02')),\n" +
                "PARTITION p19910402 VALUES [('1991-04-02'), ('1991-04-03')))" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("create table test_base_part(c1 int, c2 bigint, c3 bigint, c4 bigint)" +
                " partition by range(c3) (" +
                " partition p1 values less than (\"100\")," +
                " partition p2 values less than (\"200\")," +
                " partition p3 values less than (\"1000\")," +
                " PARTITION p4 values less than (\"2000\")," +
                " PARTITION p5 values less than (\"3000\"))" +
                " distributed by hash(c1)" +
                " properties (\"replication_num\"=\"1\");");

        cluster.runSql("test", "insert into emps values(1, 1, \"emp_name1\", 100);");
        cluster.runSql("test", "insert into emps values(2, 1, \"emp_name1\", 120);");
        cluster.runSql("test", "insert into emps values(3, 1, \"emp_name1\", 150);");
        cluster.runSql("test", "insert into depts values(1, \"dept_name1\")");
        cluster.runSql("test", "insert into depts values(2, \"dept_name2\")");
        cluster.runSql("test", "insert into depts values(3, \"dept_name3\")");
        cluster.runSql("test", "insert into dependents values(1, \"dependent_name1\")");
        cluster.runSql("test", "insert into locations values(1, \"location1\")");
        cluster.runSql("test", "insert into t0 values(1, 2, 3)");
        cluster.runSql("test", "insert into test_all_type values(" +
                "\"value1\", 1, 2, 3, 4.0, 5.0, 6, \"2022-11-11 10:00:01\", \"2022-11-11\", 10.12)");

        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `k1` int(11) NULL COMMENT \"\",\n" +
                "  `v1` int(11) NULL COMMENT \"\",\n" +
                "  `v2` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p1 VALUES [(\"-2147483648\"), (\"2\")),\n" +
                "PARTITION p2 VALUES [(\"2\"), (\"3\")),\n" +
                "PARTITION p3 VALUES [(\"3\"), (\"4\")),\n" +
                "PARTITION p4 VALUES [(\"4\"), (\"5\")),\n" +
                "PARTITION p5 VALUES [(\"5\"), (\"6\")),\n" +
                "PARTITION p6 VALUES [(\"6\"), (\"7\")))\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 6\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        cluster.runSql("test", "insert into t1 values (1,1,1),(1,1,2),(1,1,3),(1,2,1),(1,2,2),(1,2,3)," +
                " (1,3,1),(1,3,2),(1,3,3)\n" +
                " ,(2,1,1),(2,1,2),(2,1,3),(2,2,1),(2,2,2),(2,2,3),(2,3,1),(2,3,2),(2,3,3)\n" +
                " ,(3,1,1),(3,1,2),(3,1,3),(3,2,1),(3,2,2),(3,2,3),(3,3,1),(3,3,2),(3,3,3)");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testSingleTableRewrite() throws Exception {
        testSingleTableEqualPredicateRewrite();
        testSingleTableRangePredicateRewrite();
        testMultiMvsForSingleTable();
        testNestedMvOnSingleTable();
        testSingleTableResidualPredicateRewrite();
    }

    public void testSingleTableEqualPredicateRewrite() throws Exception {
        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid = 5");
        String query = "select empid, deptno, name, salary from emps where empid = 5";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 5: empid\n" +
                "  |  <slot 2> : 6: deptno\n" +
                "  |  <slot 3> : 7: name\n" +
                "  |  <slot 4> : 8: salary\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv_1");
        PlanTestBase.assertContains(plan, "tabletRatio=6/6");

        String query2 = "select empid, deptno, name, salary from emps where empid = 6";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "mv_1");

        String query3 = "select empid, deptno, name, salary from emps where empid > 5";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "mv_1");

        String query4 = "select empid, deptno, name, salary from emps where empid < 5";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "mv_1");

        String query5 = "select empid, length(name), (salary + 1) * 2 from emps where empid = 5";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "1:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv_1");

        String query7 = "select empid, deptno from emps where empid = 5";
        String plan7 = getCostsFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "mv_1");
        // column prune
        PlanTestBase.assertNotContains(plan7, "name-->");
        PlanTestBase.assertNotContains(plan7, "salary-->");

        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        String query6 = "select empid, deptno, name, salary from emps where empid = 5";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertNotContains(plan6, "mv_1");

        dropMv("test", "mv_1");
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(true);
    }

    @Test
    public void testHiveSingleTableEqualPredicateRewrite() throws Exception {
        createAndRefreshMv("test", "hive_mv_1",
                "create materialized view hive_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey = 5");
        String query = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey = 5";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_mv_1");

        String query2 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey = 6";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "hive_mv_1");

        String query3 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey > 5";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "hive_mv_1");

        String query4 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "hive_mv_1");

        String query5 = "select s_suppkey, s_name, s_address, (s_acctbal + 1) * 2 from hive0.tpch.supplier where s_suppkey = 5";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "hive_mv_1");

        dropMv("test", "hive_mv_1");
    }

    public void testSingleTableRangePredicateRewrite() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        String query = "select empid, deptno, name, salary from emps where empid < 5";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 5: empid\n" +
                "  |  <slot 2> : 6: deptno\n" +
                "  |  <slot 3> : 7: name\n" +
                "  |  <slot 4> : 8: salary\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv_1");

        String query2 = "select empid, deptno, name, salary from emps where empid < 4";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "1:Project\n" +
                "  |  <slot 1> : 5: empid\n" +
                "  |  <slot 2> : 6: deptno\n" +
                "  |  <slot 3> : 7: name\n" +
                "  |  <slot 4> : 8: salary\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: empid <= 3");

        String query3 = "select empid, deptno, name, salary from emps where empid <= 5";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "mv_1");

        String query4 = "select empid, deptno, name, salary from emps where empid > 5";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "mv_1");

        String query5 = "select empid, length(name), (salary + 1) * 2 from emps where empid = 4";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "1:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 7: empid = 4");

        String query6 = "select empid, length(name), (salary + 1) * 2 from emps where empid between 3 and 4";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "1:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 7: empid <= 4, 7: empid >= 3");

        String query7 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 and salary > 100";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "1:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 10: salary > 100.0");
        dropMv("test", "mv_1");

        createAndRefreshMv("test", "mv_2",
                "create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5 and salary > 100");
        String query8 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertNotContains(plan8, "mv_2");

        String query9 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 and salary > 90";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertNotContains(plan9, "mv_2");
        dropMv("test", "mv_2");

        createAndRefreshMv("test", "mv_3",
                "create materialized view mv_3 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5 or salary > 100");
        String query10 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "mv_3");

        String query11 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 or salary > 100";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "mv_3");

        String query12 = "select empid, length(name), (salary + 1) * 2 from emps" +
                " where empid < 5 or salary > 100 or salary < 10";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertNotContains(plan12, "mv_3");
        dropMv("test", "mv_3");
    }

    @Test
    public void testHiveSingleTableRangePredicateRewrite() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        createAndRefreshMv("test", "hive_mv_1",
                "create materialized view hive_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
        String query = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_mv_1");

        String query2 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 4";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "hive_mv_1");

        String query3 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey <= 5";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "hive_mv_1");

        String query4 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey > 5";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "hive_mv_1");

        String query5 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey = 4";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "hive_mv_1");

        String query6 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey between 3 and 4";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "hive_mv_1");

        String query7 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5 and " +
                "s_acctbal > 100.0";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "hive_mv_1");

        dropMv("test", "hive_mv_1");
    }

    public void testSingleTableResidualPredicateRewrite() throws Exception {
        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where name like \"%abc%\" and salary * deptno > 100");
        String query = "select empid, deptno, name, salary from emps where salary * deptno > 100 and name like \"%abc%\"";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_1");
        dropMv("test", "mv_1");
    }

    @Test
    public void testHiveSingleTableResidualPredicateRewrite() throws Exception {
        createAndRefreshMv("test", "hive_mv_1",
                "create materialized view hive_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where " +
                        "s_suppkey * s_acctbal > 100 and s_name like \"%abc%\"");
        String query = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where " +
                "s_suppkey * s_acctbal > 100 and s_name like \"%abc%\"";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_mv_1");
        dropMv("test", "hive_mv_1");
    }

    public void testMultiMvsForSingleTable() throws Exception {
        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        createAndRefreshMv("test", "mv_2",
                "create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 6 and salary > 100");
        String query = "select empid, length(name), (salary + 1) * 2 from emps where empid < 3 and salary > 110";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_");
        dropMv("test", "mv_1");
        dropMv("test", "mv_2");

        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        createAndRefreshMv("test", "mv_2",
                "create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5 and salary > 100");
        String query1 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 and salary > 110";
        String plan2 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan2, "mv_");
        dropMv("test", "mv_1");
        dropMv("test", "mv_2");

        createAndRefreshMv("test", "agg_mv_1",
                "create materialized view agg_mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, sum(salary) as total_salary from emps" +
                        " where empid < 5 group by empid, deptno");
        createAndRefreshMv("test", "agg_mv_2",
                "create materialized view agg_mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, sum(salary) as total_salary from emps" +
                        " where empid < 10 group by empid, deptno");
        String query2 = "select empid, sum(salary) from emps where empid < 5 group by empid";
        String plan3 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan3, "agg_mv_");
        dropMv("test", "agg_mv_1");
        dropMv("test", "agg_mv_2");
    }

    public void testNestedMvOnSingleTable() throws Exception {
        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        createAndRefreshMv("test", "mv_2",
                "create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, salary from mv_1 where salary > 100");
        String query = "select empid, deptno, (salary + 1) * 2 from emps where empid < 5 and salary > 110";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_2");
        dropMv("test", "mv_1");
        dropMv("test", "mv_2");
    }

    @Test
    public void testJoinMvRewrite() throws Exception {
        createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100");

        String query1 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "join_mv_1");
        String costPlan1 = getCostsFragmentPlan(query1);
        // column prune
        PlanTestBase.assertNotContains(costPlan1, "t1d-->");

        // t1e is not the output of mv
        String query2 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c, test_all_type.t1e" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "join_mv_1");

        String query3 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 99";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "1:Project\n" +
                "  |  <slot 6> : 17: t1c\n" +
                "  |  <slot 14> : 15: v1 + 1 * 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 15: v1 = 99");

        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        String query4 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 101";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "join_mv_1");

        String query5 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100 and t0.v1 > 10";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "join_mv_1");

        String query6 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertNotContains(plan6, "join_mv_1");

        dropMv("test", "join_mv_1");

        createAndRefreshMv("test", "join_mv_2", "create materialized view join_mv_2" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 <= 100");

        // test on equivalence classes for output and predicates
        String query7 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d < 100";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "join_mv_2");

        String query8 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d < 10";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContains(plan8, "1:Project\n" +
                "  |  <slot 6> : 16: t1c\n" +
                "  |  <slot 14> : 15: v1 + 1 * 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 15: v1 <= 9");
        String query9 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d = 100";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertContains(plan9, "join_mv_2");

        String query10 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 between 1 and 10";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "join_mv_2");

        dropMv("test", "join_mv_2");

        createAndRefreshMv("test", "join_mv_3", "create materialized view join_mv_3" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid, depts.deptno, depts.name from emps join depts using (deptno)");
        String query11 = "select empid, depts.deptno from emps join depts using (deptno) where empid = 1";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "join_mv_3");
        String costPlan2 = getFragmentPlan(query11);
        PlanTestBase.assertContains(costPlan2, "join_mv_3");
        PlanTestBase.assertNotContains(costPlan2, "name-->");
        String newQuery11 = "select depts.deptno from emps join depts using (deptno) where empid = 1";
        String costPlan3 = getCostsFragmentPlan(newQuery11);
        PlanTestBase.assertContains(costPlan3, "join_mv_3");
        // empid is kept for predicate
        PlanTestBase.assertContains(costPlan3, "empid-->", "deptno-->");
        // column prune
        PlanTestBase.assertNotContains(costPlan3, "name-->");

        String newQuery12 = "select depts.name from emps join depts using (deptno)";
        String costPlan4 = getCostsFragmentPlan(newQuery12);
        PlanTestBase.assertContains(costPlan4, "join_mv_3");
        PlanTestBase.assertContains(costPlan4, "name-->");
        // deptno is not projected
        PlanTestBase.assertNotContains(costPlan4, "deptno-->");

        // join to scan with projection
        String newQuery13 = "select upper(depts.name) from emps join depts using (deptno)";
        String costPlan5 = getCostsFragmentPlan(newQuery13);
        PlanTestBase.assertContains(costPlan5, "join_mv_3");
        PlanTestBase.assertContains(costPlan5, "name-->");
        // deptno is not projected
        PlanTestBase.assertNotContains(costPlan4, "deptno-->");

        // output on equivalence classes
        String query12 = "select empid, emps.deptno from emps join depts using (deptno) where empid = 1";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertContains(plan12, "join_mv_3");

        String query13 = "select empid, emps.deptno from emps join depts using (deptno) where empid > 1";
        String plan13 = getFragmentPlan(query13);
        PlanTestBase.assertContains(plan13, "join_mv_3");

        String query14 = "select empid, emps.deptno from emps join depts using (deptno) where empid < 1";
        String plan14 = getFragmentPlan(query14);
        PlanTestBase.assertContains(plan14, "join_mv_3");

        // query delta(query has three tables and view has two tabels) is supported
        // depts.name should be in the output of mv
        String query15 = "select emps.empid from emps join depts using (deptno)" +
                " join dependents on (depts.name = dependents.name)";
        String plan15 = getFragmentPlan(query15);
        PlanTestBase.assertContains(plan15, "join_mv_3");
        String costPlan15 = getCostsFragmentPlan(query15);
        PlanTestBase.assertContains(costPlan15, "join_mv_3");
        PlanTestBase.assertContains(costPlan15, "name-->");
        // column prune
        PlanTestBase.assertNotContains(costPlan15, "deptno-->");

        // query delta depends on join reorder
        String query16 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join emps on (emps.deptno = depts.deptno)";
        String plan16 = getFragmentPlan(query16);
        PlanTestBase.assertContains(plan16, "join_mv_3");
        String costPlan16 = getCostsFragmentPlan(query16);
        PlanTestBase.assertContains(costPlan16, "join_mv_3");
        PlanTestBase.assertContains(costPlan16, "name-->");
        // column prune
        PlanTestBase.assertNotContains(costPlan16, "deptno-->");

        String query23 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join emps on (emps.deptno = depts.deptno) where emps.deptno = 1";
        String costPlan23 = getCostsFragmentPlan(query23);
        PlanTestBase.assertContains(costPlan23, "join_mv_3");
        PlanTestBase.assertContains(costPlan23, "name-->");
        // deptno is kept for predicate
        PlanTestBase.assertContains(costPlan23, "deptno-->");

        // more tables
        String query17 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join locations on (locations.name = dependents.name) join emps on (emps.deptno = depts.deptno)";
        String plan17 = getFragmentPlan(query17);
        PlanTestBase.assertContains(plan17, "join_mv_3");

        dropMv("test", "join_mv_3");

        createAndRefreshMv("test", "join_mv_4", "create materialized view join_mv_4" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid, emps.name as name1, emps.deptno, depts.name as name2 from emps join depts using (deptno)" +
                " where (depts.name is not null and emps.name ='a')" +
                " or (depts.name is not null and emps.name = 'b')" +
                " or (depts.name is not null and emps.name = 'c')");

        // TODO: support in predicate rewrite
        String query18 = "select depts.deptno, depts.name from emps join depts using (deptno)" +
                " where (depts.name is not null and emps.name = 'a')" +
                " or (depts.name is not null and emps.name = 'b')";
        String plan18 = getFragmentPlan(query18);
        PlanTestBase.assertNotContains(plan18, "join_mv_4");
        dropMv("test", "join_mv_4");

        createAndRefreshMv("test", "join_mv_5", "create materialized view join_mv_5" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid, emps.name as name1, emps.deptno, depts.name as name2 from emps join depts using (deptno)" +
                " where emps.name = 'a'");

        createAndRefreshMv("test", "join_mv_6", "create materialized view join_mv_6" +
                        " distributed by hash(empid)" +
                        " as " +
                        " select empid, deptno, name2 from join_mv_5 where name2 like \"%abc%\"");

        String query19 = "select emps.deptno, depts.name from emps join depts using (deptno)" +
                " where emps.name = 'a' and depts.name like \"%abc%\"";
        String plan19 = getFragmentPlan(query19);
        // the nested rewrite succeed, but the result depends on cost
        PlanTestBase.assertContains(plan19, "join_mv_");

        dropMv("test", "join_mv_5");
        dropMv("test", "join_mv_6");

        createAndRefreshMv("test", "join_mv_7", "create materialized view join_mv_7" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid from emps join depts using (deptno)");

        // TODO: rewrite on subquery
        String query20 = "select emps.empid from emps where deptno in (select deptno from depts)";
        String plan20 = getFragmentPlan(query20);
        PlanTestBase.assertNotContains(plan20, "join_mv_7");
        dropMv("test", "join_mv_7");

        // multi relations test
        createAndRefreshMv("test", "join_mv_8", "create materialized view join_mv_8" +
                " distributed by hash(empid)" +
                " as" +
                " select emps1.empid, emps2.name from emps emps1 join emps emps2 on (emps1.empid = emps2.empid)");
        String query21 = "select emps1.name, emps2.empid from emps emps1 join emps emps2 on (emps1.empid = emps2.empid)";
        String plan21 = getFragmentPlan(query21);
        PlanTestBase.assertContains(plan21, "join_mv_8");
        dropMv("test", "join_mv_8");

        createAndRefreshMv("test", "join_mv_9", "create materialized view join_mv_9" +
                " distributed by hash(empid)" +
                " as" +
                " select emps1.empid, emps2.name as name1, depts.name as name2 from emps emps1 join depts using (deptno)" +
                " join emps emps2 on (emps1.empid = emps2.empid)");
        String query22 = "select emps2.empid, emps1.name as name1, depts.name as name2" +
                " from emps emps2 join depts using (deptno)" +
                " join emps emps1 on (emps1.empid = emps2.empid)";
        String plan22 = getFragmentPlan(query22);
        PlanTestBase.assertContains(plan22, "join_mv_9");
        dropMv("test", "join_mv_9");

    }

    @Test
    public void testHiveJoinMvRewrite() throws Exception {
        createAndRefreshMv("test", "hive_join_mv_1", "create materialized view hive_join_mv_1" +
                " distributed by hash(s_suppkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_suppkey , s_name, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_suppkey < 100");

        String query1 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey < 100";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "hive_join_mv_1");

        String query2 = "SELECT (s_suppkey + 1) * 2, n_name, n_comment" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey < 100";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "hive_join_mv_1");

        String query3 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey = 99";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "hive_join_mv_1");

        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        String query4 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey < 101";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "hive_join_mv_1");

        String query5 = "SELECT (s_suppkey + 1) * 2, n_name from hive0.tpch.supplier join hive0.tpch.nation on " +
                "s_nationkey = n_nationkey where s_suppkey < 100 and s_suppkey > 10";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "hive_join_mv_1");

        String query6 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertNotContains(plan6, "hive_join_mv_1");

        dropMv("test", "hive_join_mv_1");

        createAndRefreshMv("test", "hive_join_mv_2", "create materialized view hive_join_mv_2" +
                " distributed by hash(s_nationkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_nationkey , s_name, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey <= 100");

        // test on equivalence classes for output and predicates
        String query7 = "SELECT (n_nationkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where n_nationkey < 100";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "hive_join_mv_2");

        String query8 = "SELECT (n_nationkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where n_nationkey < 10";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContains(plan8, "hive_join_mv_2");

        String query9 = "SELECT (n_nationkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where n_nationkey = 100";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertContains(plan9, "hive_join_mv_2");

        String query10 = "SELECT (n_nationkey + 1) * 2, n_name from hive0.tpch.supplier join hive0.tpch.nation on " +
                "s_nationkey = n_nationkey where n_nationkey between 10 and 20";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "hive_join_mv_2");

        dropMv("test", "hive_join_mv_2");
    }


    @Test
    public void testAggregateMvRewrite() throws Exception {
        createAndRefreshMv("test", "agg_join_mv_1", "create materialized view agg_join_mv_1" +
                " distributed by hash(v1) as SELECT t0.v1 as v1," +
                " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1d");

        String query1 = "SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 = 1" +
                " group by v1, test_all_type.t1d";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 7> : 16: v1\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 = 1");

        String query2 = "SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1d";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 7> : 16: v1\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1");

        String query3 = "SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1, test_all_type.t1d";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 7> : 16: v1\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 <= 98");

        String query4 = "SELECT t0.v1 as v1, " +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertContains(plan4, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 <= 98");

        // test group key not equal
        String query5 = "SELECT t0.v1 + 1 as alias, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by alias, test_all_type.t1d";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertNotContains(plan5, "agg_join_mv_1");

        dropMv("test", "agg_join_mv_1");

        createAndRefreshMv("test", "agg_join_mv_2", "create materialized view agg_join_mv_2" +
                " distributed by hash(v1) as SELECT t0.v1 as v1," +
                " test_all_type.t1b, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1b");
        String query6 = "SELECT t0.v1 as v1, " +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1";
        // rollup test
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(18: total_sum), sum(19: total_num)\n" +
                "  |  group by: 16: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 <= 98");
        dropMv("test", "agg_join_mv_2");

        createAndRefreshMv("test", "agg_join_mv_3", "create materialized view agg_join_mv_3" +
                " distributed by hash(v1) as SELECT t0.v1 as v1," +
                " test_all_type.t1b, sum(test_all_type.t1c) * 2 as total_sum," +
                " count(distinct test_all_type.t1c) + 1 as total_num" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1b");

        // rollup with distinct
        String query7 = "SELECT t0.v1 as v1, " +
                " (sum(test_all_type.t1c)  * 2) + (count(distinct test_all_type.t1c) + 1) as total_sum," +
                " (count(distinct test_all_type.t1c) + 1) * 2 as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertNotContains(plan7, "agg_join_mv_3");

        // distinct rewrite without rollup
        String query8 = "SELECT t0.v1, test_all_type.t1b," +
                " (sum(test_all_type.t1c) * 2) + 1 as total_sum, (count(distinct test_all_type.t1c) + 1) * 2 as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1, test_all_type.t1b";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContains(plan8, "agg_join_mv_3");

        // test group by keys order change
        String query9 = "SELECT t0.v1, test_all_type.t1b," +
                " (sum(test_all_type.t1c) * 2) + 1 as total_sum, (count(distinct test_all_type.t1c) + 1) * 2 as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by test_all_type.t1b, v1";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertContains(plan9, "agg_join_mv_3");

        dropMv("test", "agg_join_mv_3");

        createAndRefreshMv("test", "agg_join_mv_4", "create materialized view agg_join_mv_4" +
                " distributed by hash(`deptno`) as SELECT deptno, count(*) as num from emps group by deptno");
        String query10 = "select deptno, count(*) from emps group by deptno";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "agg_join_mv_4");
        String costPlan10 = getCostsFragmentPlan(query10);
        PlanTestBase.assertContains(costPlan10, "agg_join_mv_4", "deptno-->", "num-->");

        String query11 = "select count(*) from emps";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "agg_join_mv_4");
        String costPlan11 = getCostsFragmentPlan(query11);
        PlanTestBase.assertContains(costPlan11, "agg_join_mv_4");
        // column prune
        PlanTestBase.assertNotContains(costPlan11, "deptno-->");
        dropMv("test", "agg_join_mv_4");

        createAndRefreshMv("test", "agg_join_mv_5", "create materialized view agg_join_mv_5" +
                " distributed by hash(`deptno`) as SELECT deptno, count(1) as num from emps group by deptno");
        String query12 = "select deptno, count(1) from emps group by deptno";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertContains(plan12, "agg_join_mv_5");

        dropMv("test", "agg_join_mv_5");

        // test aggregate with projection
        createAndRefreshMv("test", "agg_mv_6", "create materialized view agg_mv_6" +
                " distributed by hash(`empid`) as select empid, abs(empid) as abs_empid, avg(salary) as total" +
                " from emps group by empid");

        String query13 = "select empid, abs(empid), avg(salary) from emps group by empid";
        String plan13 = getFragmentPlan(query13);
        PlanTestBase.assertContains(plan13, "agg_mv_6");


        String query14 = "select empid, avg(salary) from emps group by empid";
        String plan14 = getCostsFragmentPlan(query14);
        PlanTestBase.assertContains(plan14, "agg_mv_6");
        // column prune
        PlanTestBase.assertNotContains(plan14, "abs_empid");

        String query15 = "select abs(empid), avg(salary) from emps group by empid";
        String plan15 = getFragmentPlan(query15);
        PlanTestBase.assertContains(plan15, "agg_mv_6");

        // avg can not be rolled up
        String query16 = "select avg(salary) from emps";
        String plan16 = getFragmentPlan(query16);
        PlanTestBase.assertNotContains(plan16, "agg_mv_6");
        dropMv("test", "agg_mv_6");

        createAndRefreshMv("test", "agg_mv_7", "create materialized view agg_mv_7" +
                " distributed by hash(`empid`) as select empid, abs(empid) as abs_empid," +
                " sum(salary) as total, count(salary) as cnt" +
                " from emps group by empid");

        String query17 = "select empid, abs(empid), sum(salary), count(salary) from emps group by empid";
        String plan17 = getFragmentPlan(query17);
        PlanTestBase.assertContains(plan17, "agg_mv_7");

        String query18 = "select empid, sum(salary), count(salary) + 1 from emps group by empid";
        String plan18 = getCostsFragmentPlan(query18);
        PlanTestBase.assertContains(plan18, "agg_mv_7", "cnt-->", "total-->");

        String query19 = "select abs(empid), sum(salary), count(salary) from emps group by empid";
        String plan19 = getFragmentPlan(query19);
        PlanTestBase.assertContains(plan19, "agg_mv_7");

        String query20 = "select sum(salary), count(salary) from emps";
        String plan20 = getCostsFragmentPlan(query20);
        PlanTestBase.assertContains(plan20, "agg_mv_7");
        PlanTestBase.assertNotContains(plan20, "empid-->");
        PlanTestBase.assertNotContains(plan20, "abs_empid-->");

        String query27 = "select sum(salary), count(salary) from emps";
        String plan27 = getCostsFragmentPlan(query27);
        PlanTestBase.assertContains(plan27, "agg_mv_7");
        PlanTestBase.assertNotContains(plan27, "empid-->");
        PlanTestBase.assertNotContains(plan27, "abs_empid-->");

        dropMv("test", "agg_mv_7");

        createAndRefreshMv("test", "agg_mv_8", "create materialized view agg_mv_8" +
                " distributed by hash(`empid`) as select empid, deptno," +
                " sum(salary) as total, count(salary) + 1 as cnt" +
                " from emps group by empid, deptno");

        // abs(empid) can not be rewritten
        String query21 = "select abs(empid), sum(salary) from emps group by empid";
        String plan21 = getFragmentPlan(query21);
        PlanTestBase.assertNotContains(plan21, "agg_mv_8");

        // count(salary) + 1 cannot be rewritten
        String query22 = "select sum(salary), count(salary) + 1 from emps";
        String plan22 = getFragmentPlan(query22);
        PlanTestBase.assertNotContains(plan22, "agg_mv_8");

        String query23 = "select sum(salary) from emps";
        String plan23 = getFragmentPlan(query23);
        PlanTestBase.assertContains(plan23, "agg_mv_8");

        String query24 = "select empid, sum(salary) from emps group by empid";
        String plan24 = getFragmentPlan(query24);
        PlanTestBase.assertContains(plan24, "agg_mv_8");

        dropMv("test", "agg_mv_8");

        createAndRefreshMv("test", "agg_mv_9", "create materialized view agg_mv_9" +
                " distributed by hash(`deptno`) as select deptno," +
                " count(distinct empid) as num" +
                " from emps group by deptno");

        String query25 = "select deptno, count(distinct empid) from emps group by deptno";
        String plan25 = getFragmentPlan(query25);
        PlanTestBase.assertContains(plan25, "agg_mv_9");
        dropMv("test", "agg_mv_9");

        starRocksAssert.withTable("CREATE TABLE `test_table_1` (\n" +
                "  `dt` date NULL COMMENT \"\",\n" +
                "  `experiment_id` bigint(20) NULL COMMENT \"\",\n" +
                "  `hour` varchar(65533) NULL COMMENT \"\",\n" +
                "  `player_id` varchar(65533) NULL COMMENT \"\",\n" +
                "  `metric_value` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`dt`, `experiment_id`, `hour`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p202207 VALUES [(\"2022-07-01\"), (\"2022-08-01\")),\n" +
                "PARTITION p202208 VALUES [(\"2022-08-01\"), (\"2022-09-01\")),\n" +
                "PARTITION p202209 VALUES [(\"2022-09-01\"), (\"2022-10-01\")),\n" +
                "PARTITION p202210 VALUES [(\"2022-10-01\"), (\"2022-11-01\")))\n" +
                "DISTRIBUTED BY HASH(`player_id`) BUCKETS 160 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");
        cluster.runSql("test", "insert into test_table_1 values('2022-07-01', 1, '08:00:00', 'player_id_1', 20.0)");
        cluster.runSql("test", "insert into test_table_1 values('2022-08-01', 1, '08:00:00', 'player_id_1', 20.0)");
        cluster.runSql("test", "insert into test_table_1 values('2022-09-01', 1, '08:00:00', 'player_id_1', 20.0)");
        cluster.runSql("test", "insert into test_table_1 values('2022-10-01', 1, '08:00:00', 'player_id_1', 20.0)");

        createAndRefreshMv("test", "agg_mv_10", "create materialized view agg_mv_10" +
                " distributed by hash(experiment_id)\n" +
                " refresh manual\n" +
                " as\n" +
                " SELECT `dt`, `experiment_id`," +
                "       count(DISTINCT `player_id`) AS `count_distinct_player_id`\n" +
                " FROM `test_table_1`\n" +
                " GROUP BY `dt`, `experiment_id`;");

        String query26 = "SELECT `dt`, `experiment_id`," +
                " count(DISTINCT `player_id`) AS `count_distinct_player_id`\n" +
                "FROM `test_table_1`\n" +
                "GROUP BY `dt`, `experiment_id`";
        String plan26 = getFragmentPlan(query26);
        PlanTestBase.assertContains(plan26, "agg_mv_10");
        dropMv("test", "agg_mv_10");
        starRocksAssert.dropTable("test_table_1");
    }

    @Test
    public void testHiveAggregateMvRewrite() throws Exception {
        createAndRefreshMv("test", "hive_agg_join_mv_1", "create materialized view hive_agg_join_mv_1" +
                " distributed by hash(s_nationkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_nationkey , n_name, sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey < 100 " +
                "group by s_nationkey , n_name");

        String query1 = " SELECT s_nationkey , n_name, sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey = 1 " +
                "group by s_nationkey , n_name";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "hive_agg_join_mv_1");

        String query2 = " SELECT s_nationkey , n_name, sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey < 100 " +
                "group by s_nationkey , n_name";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "hive_agg_join_mv_1");

        String query3 = " SELECT s_nationkey , sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey < 99 " +
                "group by s_nationkey";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "hive_agg_join_mv_1");
    }

    @Test
    public void testHiveUnionRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        createAndRefreshMv("test", "hive_union_mv_1",
                "create materialized view hive_union_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
        String query1 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 10";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "0:UNION");
        PlanTestBase.assertContains(plan1, "hive_union_mv_1");
        PlanTestBase.assertContains(plan1, "1:HdfsScanNode\n" +
                "     TABLE: supplier\n" +
                "     NON-PARTITION PREDICATES: 13: s_suppkey < 10, 13: s_suppkey > 4");

        dropMv("test", "hive_union_mv_1");
    }

    @Test
    public void testUnionRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);

        Table emps = getTable("test", "emps");
        PlanTestBase.setTableStatistics((OlapTable) emps, 1000000);
        Table depts = getTable("test", "depts");
        PlanTestBase.setTableStatistics((OlapTable) depts, 1000000);

        // single table union
        createAndRefreshMv("test", "union_mv_1", "create materialized view union_mv_1" +
                " distributed by hash(empid)  as select empid, deptno, name, salary from emps where empid < 3");
        MaterializedView mv1 = getMv("test", "union_mv_1");
        PlanTestBase.setTableStatistics(mv1, 10);
        String query1 = "select empid, deptno, name, salary from emps where empid < 5";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "0:UNION\n" +
                "  |  \n" +
                "  |----5:EXCHANGE");
        PlanTestBase.assertContains(plan1, "4:Project\n" +
                "  |  <slot 13> : 5: empid\n" +
                "  |  <slot 14> : 6: deptno\n" +
                "  |  <slot 15> : 7: name\n" +
                "  |  <slot 16> : 8: salary\n" +
                "  |  \n" +
                "  3:OlapScanNode\n" +
                "     TABLE: union_mv_1");
        PlanTestBase.assertContains(plan1, "1:OlapScanNode\n" +
                "     TABLE: emps\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 9: empid < 5, 9: empid > 2");

        String query7 = "select deptno, empid from emps where empid < 5";
        String plan7 = getCostsFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "union_mv_1", "empid-->", "deptno-->");
        PlanTestBase.assertNotContains(plan7, "name-->");
        PlanTestBase.assertNotContains(plan7, "salary-->");

        dropMv("test", "union_mv_1");

        // multi tables query
        createAndRefreshMv("test", "join_union_mv_1", "create materialized view join_union_mv_1" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid, emps.salary, depts.deptno, depts.name" +
                " from emps join depts using (deptno) where depts.deptno < 100");
        MaterializedView mv2 = getMv("test", "join_union_mv_1");
        PlanTestBase.setTableStatistics(mv2, 1);
        String query2 = "select emps.empid, emps.salary, depts.deptno, depts.name" +
                " from emps join depts using (deptno) where depts.deptno < 120";
        getFragmentPlan(query2);
        dropMv("test", "join_union_mv_1");

        // aggregate querys
        createAndRefreshMv("test", "join_agg_union_mv_1", "create materialized view join_agg_union_mv_1" +
                        " distributed by hash(v1)" +
                        " as " +
                        " SELECT t0.v1 as v1, test_all_type.t1d," +
                        " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                        " from t0 join test_all_type" +
                        " on t0.v1 = test_all_type.t1d" +
                        " where t0.v1 < 100" +
                        " group by v1, test_all_type.t1d");

        String query3 = " SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 120" +
                " group by v1, test_all_type.t1d";
        getFragmentPlan(query3);
        dropMv("test", "join_agg_union_mv_1");

        cluster.runSql("test", "insert into test_base_part values(1, 1, 2, 3)");
        cluster.runSql("test", "insert into test_base_part values(100, 1, 2, 3)");
        cluster.runSql("test", "insert into test_base_part values(200, 1, 2, 3)");
        cluster.runSql("test", "insert into test_base_part values(1000, 1, 2, 3)");
        cluster.runSql("test", "insert into test_base_part values(2000, 1, 2, 3)");
        cluster.runSql("test", "insert into test_base_part values(2500, 1, 2, 3)");
        createAndRefreshMv("test", "ttl_union_mv_1", "CREATE MATERIALIZED VIEW `ttl_union_mv_1`\n" +
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
        waitTtl(ttlMv1, 3, 200);
        String query4 = "select c3, sum(c2) from test_base_part group by c3";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertContains(plan4, "ttl_union_mv_1", "UNION", "test_base_part");
        dropMv("test", "ttl_union_mv_1");

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
        createAndRefreshMv("test", "multi_mv_1", "CREATE MATERIALIZED VIEW multi_mv_1" +
                " DISTRIBUTED BY HASH(k1) AS SELECT k1,v1,v2 from multi_mv_table where k1>1;");
        createAndRefreshMv("test", "multi_mv_2", "CREATE MATERIALIZED VIEW multi_mv_2" +
                " DISTRIBUTED BY HASH(k1) AS SELECT k1,v1,v2 from multi_mv_1 where k1>2;");
        createAndRefreshMv("test", "multi_mv_3", "CREATE MATERIALIZED VIEW multi_mv_3" +
                " DISTRIBUTED BY HASH(k1) AS SELECT k1,v1,v2 from multi_mv_2 where k1>3;");

        String query5 = "select * from multi_mv_1";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "multi_mv_1", "multi_mv_2", "multi_mv_3", "UNION");
        dropMv("test", "multi_mv_1");
        dropMv("test", "multi_mv_2");
        dropMv("test", "multi_mv_3");
        starRocksAssert.dropTable("multi_mv_table");

        createAndRefreshMv("test", "mv_agg_1", "CREATE MATERIALIZED VIEW `mv_agg_1`\n" +
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
        String query6 = "SELECT `emps`.`deptno`, `emps`.`name`, sum(salary) as salary FROM `emps` group by deptno, name;";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertNotContains(plan6, "mv_agg_1");
        PlanTestBase.assertContains(plan6, "emps");
        dropMv("test", "mv_agg_1");
    }

    @Test
    public void testNestedMv() throws Exception {
        starRocksAssert.withTable("CREATE TABLE nest_base_table_1 (\n" +
                "    k1 INT,\n" +
                "    v1 INT,\n" +
                "    v2 INT)\n" +
                "DUPLICATE KEY(k1)\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(\n" +
                "PARTITION `p1` VALUES LESS THAN ('2'),\n" +
                "PARTITION `p2` VALUES LESS THAN ('3'),\n" +
                "PARTITION `p3` VALUES LESS THAN ('4'),\n" +
                "PARTITION `p4` VALUES LESS THAN ('5'),\n" +
                "PARTITION `p5` VALUES LESS THAN ('6'),\n" +
                "PARTITION `p6` VALUES LESS THAN ('7')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");
        cluster.runSql("test", "insert into t1 values (1,1,1),(1,1,2),(1,1,3),(1,2,1),(1,2,2),(1,2,3)," +
                " (1,3,1),(1,3,2),(1,3,3)\n" +
                " ,(2,1,1),(2,1,2),(2,1,3),(2,2,1),(2,2,2),(2,2,3),(2,3,1),(2,3,2),(2,3,3)\n" +
                " ,(3,1,1),(3,1,2),(3,1,3),(3,2,1),(3,2,2),(3,2,3),(3,3,1),(3,3,2),(3,3,3);");
        createAndRefreshMv("test", "nested_mv_1", "CREATE MATERIALIZED VIEW nested_mv_1" +
                " PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL AS SELECT k1, v1 as k2, v2 as k3 from t1;");
        createAndRefreshMv("test", "nested_mv_1", "CREATE MATERIALIZED VIEW nested_mv_2 " +
                "PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL AS SELECT k1, count(k2) as count_k2, sum(k3) as sum_k3 from nested_mv_1 group by k1;");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW nested_mv_3 DISTRIBUTED BY HASH(k1)\n" +
                "REFRESH MANUAL AS SELECT k1, count_k2, sum_k3 from nested_mv_2 where k1 >1;");
        cluster.runSql("test", "insert into t1 values (4,1,1);");
        refreshMaterializedView("test", "nested_mv_1");
        refreshMaterializedView("test", "nested_mv_2");
        String query1 = "SELECT k1, count(v1), sum(v2) from t1 where k1 >1 group by k1";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertNotContains(plan1, "nested_mv_3");

        dropMv("test", "nested_mv_1");
        dropMv("test", "nested_mv_2");
        dropMv("test", "nested_mv_3");
        starRocksAssert.dropTable("nest_base_table_1");
    }

    private void waitTtl(MaterializedView mv, int number, int maxRound) throws InterruptedException, TimeoutException {
        int round = 0;
        while (true) {
            if (mv.getPartitions().size() == number) {
                break;
            }
            if (round >= maxRound) {
                throw new TimeoutException("wait ttl timeout");
            }
            Thread.sleep(1000);
            round++;
        }
    }

    @Test
    public void testPartialPartition() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);

        cluster.runSql("test", "insert into table_with_partition values(\"varchar1\", '1991-02-01', 1, 1, 1)");
        cluster.runSql("test", "insert into table_with_partition values(\"varchar2\", '1992-02-01', 2, 1, 1)");
        cluster.runSql("test", "insert into table_with_partition values(\"varchar3\", '1993-02-01', 3, 1, 1)");

        createAndRefreshMv("test", "partial_mv",
                "create materialized view partial_mv" +
                        " partition by id_date" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date, t1b from table_with_partition");
        // modify p1991 and make it outdated
        // so p1992 and p1993 are updated
        cluster.runSql("test", "insert into table_with_partition partition(p1991)" +
                " values(\"varchar12\", '1991-03-01', 2, 1, 1)");

        String query = "select t1a, id_date, t1b from table_with_partition" +
                " where id_date >= '1993-02-01' and id_date < '1993-05-01'";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "partial_mv");

        String query2 = "select t1a, id_date, t1b from table_with_partition" +
                " where id_date >= '1992-02-01' and id_date < '1993-05-01'";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "partial_mv");

        dropMv("test", "partial_mv");

        createAndRefreshMv("test", "partial_mv_2",
                "create materialized view partial_mv_2" +
                        " partition by id_date" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date, t1b from table_with_partition where t1b > 100");
        cluster.runSql("test", "insert into table_with_partition partition(p1991)" +
                " values(\"varchar12\", '1991-03-01', 2, 1, 1)");
        String query4 = "select t1a, id_date, t1b from table_with_partition" +
                " where t1b > 110 and id_date >= '1993-02-01' and id_date < '1993-05-01'";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertContains(plan4, "partial_mv_2");
        dropMv("test", "partial_mv_2");

        cluster.runSql("test", "insert into table_with_day_partition values(\"varchar1\", '1991-03-30', 1, 1, 1)");
        cluster.runSql("test", "insert into table_with_day_partition values(\"varchar2\", '1991-03-31', 2, 1, 1)");
        cluster.runSql("test", "insert into table_with_day_partition values(\"varchar3\", '1991-04-01', 3, 1, 1)");
        cluster.runSql("test", "insert into table_with_day_partition values(\"varchar3\", '1991-04-02', 4, 1, 1)");

        createAndRefreshMv("test", "partial_mv_3",
                "create materialized view partial_mv_3" +
                        " partition by date_trunc('month', new_date)" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date as new_date, t1b from table_with_day_partition");
        cluster.runSql("test", "insert into table_with_day_partition partition(p19910331)" +
                " values(\"varchar12\", '1991-03-31', 2, 2, 1)");
        String query5 = "select t1a, id_date, t1b from table_with_day_partition" +
                " where id_date >= '1991-04-01' and id_date < '1991-04-03'";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "partial_mv_3");
        dropMv("test", "partial_mv_3");

        cluster.runSql("test", "insert into table_with_day_partition values(\"varchar1\", '1991-03-30', 1, 1, 1)");
        cluster.runSql("test", "insert into table_with_day_partition values(\"varchar2\", '1991-03-31', 2, 1, 1)");
        cluster.runSql("test", "insert into table_with_day_partition values(\"varchar3\", '1991-04-01', 3, 1, 1)");
        cluster.runSql("test", "insert into table_with_day_partition values(\"varchar3\", '1991-04-02', 4, 1, 1)");

        createAndRefreshMv("test", "partial_mv_3",
                "create materialized view partial_mv_3" +
                        " partition by new_date" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, date_trunc('month', id_date) as new_date, t1b from table_with_day_partition");
        cluster.runSql("test", "insert into table_with_day_partition partition(p19910331)" +
                " values(\"varchar12\", '1991-03-31', 2, 2, 1)");
        String query6 = "select t1a, date_trunc('month', id_date), t1b from table_with_day_partition" +
                " where id_date >= '1991-04-01' and id_date < '1991-04-03'";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "partial_mv_3");
        dropMv("test", "partial_mv_3");

        cluster.runSql("test", "insert into table_with_partition values(\"varchar1\", '1991-02-01', 1, 1, 1)");
        cluster.runSql("test", "insert into table_with_partition values(\"varchar2\", '1992-02-01', 2, 1, 1)");
        cluster.runSql("test", "insert into table_with_partition values(\"varchar3\", '1993-02-01', 3, 1, 1)");
        createAndRefreshMv("test", "partial_mv_4",
                "create materialized view partial_mv_4" +
                        " partition by new_name" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date as new_name, t1b from table_with_partition");
        cluster.runSql("test", "insert into table_with_partition partition(p1991)" +
                " values(\"varchar12\", '1991-03-01', 2, 1, 1)");
        String query7 = "select t1a, id_date, t1b from table_with_partition" +
                " where id_date >= '1993-02-01' and id_date < '1993-05-01'";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "partial_mv_4");

        dropMv("test", "partial_mv_4");

        cluster.runSql("test", "insert into test_base_part values (1, 1, 1, 1);");
        createAndRefreshMv("test", "partial_mv_5", "create materialized view partial_mv_5" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, sum(c2) as c2 from test_base_part group by c1, c3;");
        cluster.runSql("test", "alter table test_base_part add partition p6 values less than (\"4000\")");
        cluster.runSql("test", "insert into test_base_part partition(p6) values (1, 2, 4500, 4)");
        String query8 = "select c3, sum(c2) from test_base_part group by c3";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContains(plan8, "partial_mv_5");
        PlanTestBase.assertContains(plan8, "UNION");
        PlanTestBase.assertNotContains(plan8, "c3 < -9223372036854775808");

        String query9 = "select sum(c3) from test_base_part";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertNotContains(plan9, "partial_mv_5");
        dropMv("test", "partial_mv_5");

        // test partition prune
        createAndRefreshMv("test", "partial_mv_6", "create materialized view partial_mv_6" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c3 < 2000;");

        String query10 = "select c1, c3, c2 from test_base_part";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "partial_mv_6", "UNION", "c3 > 1999");

        String query12 = "select c1, c3, c2 from test_base_part where c3 < 2000";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertContains(plan12, "partial_mv_6");

        String query13 = "select c1, c3, c2 from test_base_part where c3 < 1000";
        String plan13 = getFragmentPlan(query13);
        PlanTestBase.assertContains(plan13, "partial_mv_6", "c3 <= 999");

        dropMv("test", "partial_mv_6");

        // test bucket prune
        createAndRefreshMv("test", "partial_mv_7", "create materialized view partial_mv_7" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c3 < 2000 and c1 = 1;");
        String query11 = "select c1, c3, c2 from test_base_part";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "partial_mv_7", "UNION", "c3 > 1999");
        dropMv("test", "partial_mv_7");

        createAndRefreshMv("test", "partial_mv_8", "create materialized view partial_mv_8" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c3 < 1000;");
        String query14 = "select c1, c3, c2 from test_base_part where c3 < 1000";
        String plan14 = getFragmentPlan(query14);
        PlanTestBase.assertContains(plan14, "partial_mv_8");
        dropMv("test", "partial_mv_8");

        createAndRefreshMv("test", "partial_mv_9", "CREATE MATERIALIZED VIEW partial_mv_9" +
                " PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL AS SELECT k1, v1 as k2, v2 as k3 from t1;");
        // create nested mv based on partial_mv_9
        createAndRefreshMv("test", "partial_mv_10", "CREATE MATERIALIZED VIEW partial_mv_10" +
                " PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL AS SELECT k1, count(k2) as count_k2, sum(k3) as sum_k3 from partial_mv_9 group by k1;");
        cluster.runSql("test", "insert into t1 values (4,1,1);");

        // first refresh nest mv partial_mv_10, will do nothing
        refreshMaterializedView("test", "partial_mv_10");
        // then refresh mv partial_mv_9
        refreshMaterializedView("test", "partial_mv_9");
        String query15 = "SELECT k1, count(v1), sum(v2) from t1 group by k1";
        String plan15 = getFragmentPlan(query15);
        // it should be union
        PlanTestBase.assertContains(plan15, "partial_mv_9");
        PlanTestBase.assertNotContains(plan15, "partial_mv_10");
        dropMv("test", "partial_mv_9");
        dropMv("test", "partial_mv_10");

        starRocksAssert.withTable("CREATE TABLE ttl_base_table (\n" +
                "                            k1 INT,\n" +
                "                            v1 INT,\n" +
                "                            v2 INT)\n" +
                "                        DUPLICATE KEY(k1)\n" +
                "                        PARTITION BY RANGE(`k1`)\n" +
                "                        (\n" +
                "                        PARTITION `p1` VALUES LESS THAN ('2'),\n" +
                "                        PARTITION `p2` VALUES LESS THAN ('3'),\n" +
                "                        PARTITION `p3` VALUES LESS THAN ('4'),\n" +
                "                        PARTITION `p4` VALUES LESS THAN ('5'),\n" +
                "                        PARTITION `p5` VALUES LESS THAN ('6'),\n" +
                "                        PARTITION `p6` VALUES LESS THAN ('7')\n" +
                "                        )\n" +
                "                        DISTRIBUTED BY HASH(k1) properties('replication_num'='1');");
        cluster.runSql("test", "insert into ttl_base_table values (1,1,1),(1,1,2),(1,2,1),(1,2,2),\n" +
                "                                              (2,1,1),(2,1,2),(2,2,1),(2,2,2),\n" +
                "                                              (3,1,1),(3,1,2),(3,2,1),(3,2,2);");
        createAndRefreshMv("test", "ttl_mv_2", "CREATE MATERIALIZED VIEW ttl_mv_2\n" +
                "               PARTITION BY k1\n" +
                "               DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "               REFRESH ASYNC\n" +
                "               PROPERTIES(\n" +
                "               \"partition_ttl_number\"=\"4\"\n" +
                "               )\n" +
                "               AS SELECT k1, sum(v1) as sum_v1 FROM ttl_base_table group by k1;");
        MaterializedView ttlMv2 = getMv("test", "ttl_mv_2");
        waitTtl(ttlMv2, 4, 100);

        String query16 = "select k1, sum(v1) FROM ttl_base_table where k1=3 group by k1";
        String plan16 = getFragmentPlan(query16);
        PlanTestBase.assertContains(plan16, "ttl_mv_2");
        dropMv("test", "ttl_mv_2");
        starRocksAssert.dropTable("ttl_base_table");

        starRocksAssert.withTable("CREATE TABLE ttl_base_table_2 (\n" +
                "                            k1 date,\n" +
                "                            v1 INT,\n" +
                "                            v2 INT)\n" +
                "                        DUPLICATE KEY(k1)\n" +
                "                        PARTITION BY RANGE(`k1`)\n" +
                "                        (\n" +
                "                        PARTITION `p1` VALUES LESS THAN ('2020-01-01'),\n" +
                "                        PARTITION `p2` VALUES LESS THAN ('2020-02-01'),\n" +
                "                        PARTITION `p3` VALUES LESS THAN ('2020-03-01'),\n" +
                "                        PARTITION `p4` VALUES LESS THAN ('2020-04-01'),\n" +
                "                        PARTITION `p5` VALUES LESS THAN ('2020-05-01'),\n" +
                "                        PARTITION `p6` VALUES LESS THAN ('2020-06-01')\n" +
                "                        )\n" +
                "                        DISTRIBUTED BY HASH(k1) properties('replication_num'='1');");
        cluster.runSql("test", "insert into ttl_base_table_2 values " +
                " (\"2019-01-01\",1,1),(\"2019-01-01\",1,2),(\"2019-01-01\",2,1),(\"2019-01-01\",2,2),\n" +
                " (\"2020-01-11\",1,1),(\"2020-01-11\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2),\n" +
                " (\"2020-02-11\",1,1),(\"2020-02-11\",1,2),(\"2020-02-11\",2,1),(\"2020-02-11\",2,2);");
        createAndRefreshMv("test", "ttl_mv_3", "CREATE MATERIALIZED VIEW ttl_mv_3\n" +
                "               PARTITION BY k1\n" +
                "               DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "               REFRESH MANUAL\n" +
                "               AS SELECT k1, sum(v1) as sum_v1 FROM ttl_base_table_2 group by k1;");
        String query17 = "select k1, sum(v1) FROM ttl_base_table_2 where k1 = '2020-02-11' group by k1";
        String plan17 = getFragmentPlan(query17);
        PlanTestBase.assertContains(plan17, "ttl_mv_3", "k1 = '2020-02-11'");
        dropMv("test", "ttl_mv_3");
        starRocksAssert.dropTable("ttl_base_table_2");
    }

    public String getFragmentPlan(String sql) throws Exception {
        String s = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.NORMAL);
        return s;
    }

    public String getCostsFragmentPlan(String sql) throws Exception {
        String s = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.COSTS);
        return s;
    }

    private Table getTable(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(mvName);
        Assert.assertNotNull(table);
        return table;
    }

    private MaterializedView getMv(String dbName, String mvName) {
        Table table = getTable(dbName, mvName);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        return mv;
    }

    private void refreshMaterializedView(String dbName, String mvName) throws Exception {
        MaterializedView mv = getMv(dbName, mvName);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        final String mvTaskName = TaskBuilder.getMvTaskName(mv.getId());
        if (!taskManager.containTask(mvTaskName)) {
            Task task = TaskBuilder.buildMvTask(mv, "test");
            TaskBuilder.updateTaskInfo(task, mv);
            taskManager.createTask(task, false);
        }
        taskManager.executeTaskSync(mvTaskName);
    }

    private void createAndRefreshMv(String dbName, String mvName, String sql) throws Exception {
        starRocksAssert.withMaterializedView(sql);
        refreshMaterializedView(dbName, mvName);
    }

    private void dropMv(String dbName, String mvName) throws Exception {
        starRocksAssert.dropMaterializedView(mvName);
    }
}
