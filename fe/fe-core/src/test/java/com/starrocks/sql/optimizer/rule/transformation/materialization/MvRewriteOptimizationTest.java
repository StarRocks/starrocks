// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.common.Config;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
<<<<<<< HEAD:fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvRewriteOptimizationTest.java
import org.junit.BeforeClass;
=======
import org.junit.Ignore;
>>>>>>> 656e543e84 ([BugFix] fix mv rewrite for join predicate pushdown (#27632)):fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvRewriteTest.java
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class MvRewriteOptimizationTest {
    private static ConnectContext connectContext;
    private static PseudoCluster cluster;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.dynamic_partition_check_interval_seconds = 10000;
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
                .withTable("create table emps_par (\n" +
                        "    empid int not null,\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null,\n" +
                        "    salary double\n" +
                        ")\n" +
                        "PARTITION BY RANGE(`deptno`)\n" +
                        "(PARTITION p1 VALUES [(\"-2147483648\"), (\"2\")),\n" +
                        "PARTITION p2 VALUES [(\"2\"), (\"3\")),\n" +
                        "PARTITION p3 VALUES [(\"3\"), (\"4\")))\n" +
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

        starRocksAssert.withTable("CREATE TABLE `json_tbl` (\n" +
                "  `p_dt` date NULL COMMENT \"\",\n" +
                "  `d_user` json NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`p_dt`)\n" +
                "DISTRIBUTED BY HASH(p_dt) BUCKETS 2\n" +
                "PROPERTIES ( \"replication_num\" = \"1\");");
        cluster.runSql("test", "insert into json_tbl values('2020-01-01', '{\"a\": 1, \"gender\": \"man\"}') ");
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
        PlanTestBase.assertContains(plan, "tabletRatio=1/6");

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
                "     rollup: mv_1\n" +
                "     tabletRatio=1/6");

        String query7 = "select empid, deptno from emps where empid = 5";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "mv_1");
        OptExpression optimizedPlan7 = getOptimizedPlan(query7, connectContext);
        List<PhysicalScanOperator> scanOperators = getScanOperators(optimizedPlan7, "mv_1");
        Assert.assertEquals(1, scanOperators.size());
        // column prune
        Assert.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("name"));
        Assert.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("salary"));

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

        String query5 =
                "select s_suppkey, s_name, s_address, (s_acctbal + 1) * 2 from hive0.tpch.supplier where s_suppkey = 5";
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
                "     rollup: mv_1\n" +
                "     tabletRatio=6/6");

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
                "     PREAGGREGATION: ON\n");

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

        String query6 =
                "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey between 3 and 4";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "hive_mv_1");

        String query7 =
                "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5 and " +
                        "s_acctbal > 100.0";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "hive_mv_1");

        dropMv("test", "hive_mv_1");
    }

    public void testSingleTableResidualPredicateRewrite() throws Exception {
        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where name like \"%abc%\" and salary * deptno > 100");
        String query =
                "select empid, deptno, name, salary from emps where salary * deptno > 100 and name like \"%abc%\"";
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
        PlanTestBase.assertContains(plan, "TABLE: mv_2");
        dropMv("test", "mv_1");
        dropMv("test", "mv_2");
    }

    @Test
    public void testViewBasedMv() throws Exception {
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                    " from t0 join test_all_type" +
                    " on t0.v1 = test_all_type.t1d" +
                    " where t0.v1 < 100");

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
                    " distributed by hash(v1)" +
                    " as " +
                    " SELECT v1, t1d, t1c from view1");
            {
                String query = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                        " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            {
                String query = "SELECT (t1d + 1) * 2, t1c from view1 where v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }
            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
        }

        // nested views
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT v1 from t0");

            starRocksAssert.withView("create view view2 as " +
                    " SELECT t1d, t1c from test_all_type");

            starRocksAssert.withView("create view view3 as " +
                    " SELECT view1.v1, view2.t1d, view2.t1c" +
                    " from view1 join view2" +
                    " on view1.v1 = view2.t1d" +
                    " where view1.v1 < 100");

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
                    " distributed by hash(v1)" +
                    " as " +
                    " SELECT v1, t1d, t1c from view3");
            {
                String query = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                        " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            {
                String query = "SELECT (t1d + 1) * 2, t1c" +
                        " from view3 where v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            starRocksAssert.dropView("view2");
            starRocksAssert.dropView("view3");
            dropMv("test", "join_mv_1");
        }

        // duplicate views
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT v1 from t0");

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
                    " distributed by hash(v11)" +
                    " as " +
                    " SELECT vv1.v1 v11, vv2.v1 v12 from view1 vv1 join view1 vv2 on vv1.v1 = vv2.v1");
            {
                String query = "SELECT vv1.v1, vv2.v1 from view1 vv1 join view1 vv2 on vv1.v1 = vv2.v1";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
        }

        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT emps_par.deptno as deptno1, depts.deptno as deptno2, emps_par.empid, emps_par.name" +
                    " from emps_par join depts" +
                    " on emps_par.deptno = depts.deptno");

            createAndRefreshMv("test", "join_mv_2", "create materialized view join_mv_2" +
                    " distributed by hash(deptno2)" +
                    " partition by deptno1" +
                    " as " +
                    " SELECT deptno1, deptno2, empid, name from view1 union SELECT deptno1, deptno2, empid, name from view1");

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
                    " distributed by hash(deptno2)" +
                    " partition by deptno1" +
                    " as " +
                    " SELECT deptno1, deptno2, empid, name from view1");

            {
                String query = "SELECT deptno1, deptno2, empid, name from view1";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
            dropMv("test", "join_mv_2");
        }
    }

    @Test
    public void testJoinMvRewrite() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000000);
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
        OptExpression optExpression1 = getOptimizedPlan(query1, connectContext);
        List<PhysicalScanOperator> scanOperators = getScanOperators(optExpression1, "join_mv_1");
        Assert.assertEquals(1, scanOperators.size());
        // column prune
        Assert.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("t1d"));

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
        String newPlan11 = getFragmentPlan(newQuery11);
        PlanTestBase.assertContains(newPlan11, "join_mv_3");
        OptExpression optExpression11 = getOptimizedPlan(newQuery11, connectContext);
        List<PhysicalScanOperator> scanOperators11 = getScanOperators(optExpression11, "join_mv_3");
        Assert.assertEquals(1, scanOperators11.size());
        // column prune
        Assert.assertFalse(scanOperators11.get(0).getColRefToColumnMetaMap().keySet().toString().contains("name"));

        String newQuery12 = "select depts.name from emps join depts using (deptno)";
        OptExpression optExpression12 = getOptimizedPlan(newQuery12, connectContext);
        List<PhysicalScanOperator> scanOperators12 = getScanOperators(optExpression12, "join_mv_3");
        Assert.assertEquals(1, scanOperators12.size());
        // deptno is not projected
        Assert.assertFalse(scanOperators12.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        // join to scan with projection
        String newQuery13 = "select upper(depts.name) from emps join depts using (deptno)";
        OptExpression optExpression13 = getOptimizedPlan(newQuery13, connectContext);
        List<PhysicalScanOperator> scanOperators13 = getScanOperators(optExpression13, "join_mv_3");
        Assert.assertEquals(1, scanOperators13.size());
        // deptno is not projected
        Assert.assertFalse(scanOperators13.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

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
        OptExpression optExpression15 = getOptimizedPlan(query15, connectContext);
        List<PhysicalScanOperator> scanOperators15 = getScanOperators(optExpression15, "join_mv_3");
        Assert.assertEquals(1, scanOperators15.size());
        // column prune
        Assert.assertFalse(scanOperators15.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        // query delta depends on join reorder
        String query16 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join emps on (emps.deptno = depts.deptno)";
        String plan16 = getFragmentPlan(query16);
        PlanTestBase.assertContains(plan16, "join_mv_3");
        OptExpression optExpression16 = getOptimizedPlan(query16, connectContext);
        List<PhysicalScanOperator> scanOperators16 = getScanOperators(optExpression16, "join_mv_3");
        Assert.assertEquals(1, scanOperators16.size());
        // column prune
        Assert.assertFalse(scanOperators16.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        String query23 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join emps on (emps.deptno = depts.deptno) where emps.deptno = 1";
        String plan23 = getFragmentPlan(query23);
        PlanTestBase.assertContains(plan23, "join_mv_3");

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
        String query21 =
                "select emps1.name, emps2.empid from emps emps1 join emps emps2 on (emps1.empid = emps2.empid)";
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
    public void testCrossJoin() throws Exception {
        createAndRefreshMv("test", "cross_join_mv1", "create materialized view cross_join_mv1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " where t0.v1 < 100");

        String query1 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
        String plan1 = getFragmentPlan(query1);
        System.out.println(plan1);
        PlanTestBase.assertContains(plan1, "cross_join_mv1");
        dropMv("test", "cross_join_mv1");

        createAndRefreshMv("test", "cross_join_mv2", "create materialized view cross_join_mv2" +
                " distributed by hash(empid)" +
                " as" +
                " select emps1.empid, emps2.name as name1, depts.name as name2 from emps emps1 join depts" +
                " join emps emps2 on (emps1.empid = emps2.empid)");
        String query22 = "select depts.name as name2" +
                " from emps emps2 join depts" +
                " join emps emps1 on (emps1.empid = emps2.empid)";
        String plan22 = getFragmentPlan(query22);
        PlanTestBase.assertContains(plan22, "cross_join_mv2");
        dropMv("test", "cross_join_mv2");
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
        PlanTestBase.assertContains(plan5, "agg_join_mv_1");
        PlanTestBase.assertContains(plan5, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(19: total_sum), sum(20: total_num)\n" +
                "  |  group by: 23: add, 17: v1");
        PlanTestBase.assertContains(plan5, "  1:Project\n" +
                "  |  <slot 17> : 17: v1\n" +
                "  |  <slot 18> : 18: t1d\n" +
                "  |  <slot 19> : 19: total_sum\n" +
                "  |  <slot 20> : 20: total_num\n" +
                "  |  <slot 23> : 17: v1 + 1");

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

        String query11 = "select count(*) from emps";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "agg_join_mv_4");
        OptExpression optExpression11 = getOptimizedPlan(query11, connectContext);
        List<PhysicalScanOperator> scanOperators11 = getScanOperators(optExpression11, "agg_join_mv_4");
        Assert.assertEquals(1, scanOperators11.size());
        // column prune
        Assert.assertFalse(scanOperators11.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));
        dropMv("test", "agg_join_mv_4");

        createAndRefreshMv("test", "agg_join_mv_5", "create materialized view agg_join_mv_5" +
                " distributed by hash(`deptno`) as SELECT deptno, count(1) as num from emps group by deptno");
        String query12 = "select deptno, count(1) from emps group by deptno";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertContains(plan12, "agg_join_mv_5");

        dropMv("test", "agg_join_mv_5");

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

        // test aggregate with projection
        createAndRefreshMv("test", "agg_mv_6", "create materialized view agg_mv_6" +
                " distributed by hash(`empid`) as select empid, abs(empid) as abs_empid, avg(salary) as total" +
                " from emps group by empid");

        MaterializedView mv = getMv("test", "agg_mv_6");
        Column idColumn = mv.getColumn("empid");
        Assert.assertFalse(idColumn.isAllowNull());
        Column totalColumn = mv.getColumn("total");
        Assert.assertTrue(totalColumn.isAllowNull());

        String query13 = "select empid, abs(empid), avg(salary) from emps group by empid";
        String plan13 = getFragmentPlan(query13);
        PlanTestBase.assertContains(plan13, "agg_mv_6");

        String query14 = "select empid, avg(salary) from emps group by empid";
        OptExpression optExpression14 = getOptimizedPlan(query14, connectContext);
        List<PhysicalScanOperator> scanOperators14 = getScanOperators(optExpression14, "agg_mv_6");
        Assert.assertEquals(1, scanOperators14.size());
        // column prune
        Assert.assertFalse(scanOperators14.get(0).getColRefToColumnMetaMap().keySet().toString().contains("abs_empid"));

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

        String query19 = "select abs(empid), sum(salary), count(salary) from emps group by empid";
        String plan19 = getFragmentPlan(query19);
        PlanTestBase.assertContains(plan19, "agg_mv_7");

        String query20 = "select sum(salary), count(salary) from emps";
        OptExpression optExpression20 = getOptimizedPlan(query20, connectContext);
        List<PhysicalScanOperator> scanOperators20 = getScanOperators(optExpression20, "agg_mv_7");
        Assert.assertEquals(1, scanOperators20.size());
        // column prune
        Assert.assertFalse(scanOperators20.get(0).getColRefToColumnMetaMap().keySet().toString().contains("empid"));
        Assert.assertFalse(scanOperators20.get(0).getColRefToColumnMetaMap().keySet().toString().contains("abs_empid"));

        String query27 = "select sum(salary), count(salary) from emps";
        OptExpression optExpression27 = getOptimizedPlan(query27, connectContext);
        List<PhysicalScanOperator> scanOperators27 = getScanOperators(optExpression27, "agg_mv_7");
        Assert.assertEquals(1, scanOperators27.size());
        // column prune
        Assert.assertFalse(scanOperators27.get(0).getColRefToColumnMetaMap().keySet().toString().contains("empid"));
        Assert.assertFalse(scanOperators27.get(0).getColRefToColumnMetaMap().keySet().toString().contains("abs_empid"));

        dropMv("test", "agg_mv_7");

        createAndRefreshMv("test", "agg_mv_8", "create materialized view agg_mv_8" +
                " distributed by hash(`empid`) as select empid, deptno," +
                " sum(salary) as total, count(salary) + 1 as cnt" +
                " from emps group by empid, deptno");

        // abs(empid) can not be rewritten
        String query21 = "select abs(empid), sum(salary) from emps group by empid";
        String plan21 = getFragmentPlan(query21);
        PlanTestBase.assertContains(plan21, "agg_mv_8");

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
    }

    @Test
    public void testAggExprRewrite() throws Exception {
        {
            // Group by Cast Expr
            String mvName = "mv_q15";
            createAndRefreshMv("test", mvName, "CREATE MATERIALIZED VIEW `mv_q15`\n" +
                    "DISTRIBUTED BY HASH(`gender`) BUCKETS 2\n" +
                    "REFRESH ASYNC\n" +
                    "AS \n" +
                    "SELECT \n" +
                    "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                    "    count(d_user) AS `cnt`\n" +
                    "FROM `json_tbl`\n" +
                    "GROUP BY `gender`");
            String query =
                    "SELECT \n" +
                            "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                            "    count(d_user) AS `cnt`\n" +
                            "FROM `json_tbl`\n" +
                            "GROUP BY `gender`;";
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
        }

        {
            // Agg with Cast Expr
            String mvName = "mv_q16";
            createAndRefreshMv("test", mvName, "CREATE MATERIALIZED VIEW `mv_q16`\n" +
                    "DISTRIBUTED BY HASH(`gender`) BUCKETS 2\n" +
                    "REFRESH ASYNC\n" +
                    "AS \n" +
                    "SELECT \n" +
                    "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                    "    sum(cast(d_user->'age' as int)) AS `sum`\n" +
                    "FROM `json_tbl`\n" +
                    "GROUP BY `gender`");
            String query =
                    "SELECT \n" +
                            "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                            "    sum(cast(d_user->'age' as int)) AS `sum`\n" +
                            "FROM `json_tbl`\n" +
                            "GROUP BY `gender`;";
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
        }
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
    public void testHiveQueryWithMvs() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        // enforce choose the hive scan operator, not mv plan
        connectContext.getSessionVariable().setUseNthExecPlan(1);
        createAndRefreshMv("test", "hive_union_mv_1",
                "create materialized view hive_union_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
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

        String query1 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 10";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan, "TABLE: supplier", "NON-PARTITION PREDICATES: 19: s_suppkey < 10");

        connectContext.getSessionVariable().setUseNthExecPlan(0);
        dropMv("test", "hive_union_mv_1");
        dropMv("test", "hive_join_mv_1");
    }

    @Test
    public void testUnionRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);

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

        Table emps2 = getTable("test", "emps2");
        PlanTestBase.setTableStatistics((OlapTable) emps2, 1000000);
        Table depts2 = getTable("test", "depts2");
        PlanTestBase.setTableStatistics((OlapTable) depts2, 1000000);

        // single table union
        createAndRefreshMv("test", "union_mv_1", "create materialized view union_mv_1" +
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
                        "     PREAGGREGATION: ON\n",
                "empid < 5,", "empid > 2");

        String query7 = "select deptno, empid from emps2 where empid < 5";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "union_mv_1");
        OptExpression optExpression7 = getOptimizedPlan(query7, connectContext);
        List<PhysicalScanOperator> scanOperators = getScanOperators(optExpression7, "union_mv_1");
        Assert.assertEquals(1, scanOperators.size());
        Assert.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("name"));
        Assert.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("salary"));

        dropMv("test", "union_mv_1");

        {
            // multi tables query
            createAndRefreshMv("test", "join_union_mv_1", "create materialized view join_union_mv_1" +
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

        {
            // multi tables query
            createAndRefreshMv("test", "join_union_mv_1", "create materialized view join_union_mv_1" +
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
                    "     PREDICATES: 15: deptno < 120, 15: deptno > 99\n" +
                    "     partitions=1/1");
            PlanTestBase.assertContains(plan2, "1:OlapScanNode\n" +
                    "     TABLE: depts2\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 18: deptno < 120, 18: deptno > 99");
            dropMv("test", "join_union_mv_1");
        }

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
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `t02` (\n" +
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

        cluster.runSql("test", "insert into t02 values(1, 2, 3)");
        cluster.runSql("test", "insert into test_all_type2 values(" +
                "\"value1\", 1, 2, 3, 4.0, 5.0, 6, \"2022-11-11 10:00:01\", \"2022-11-11\", 10.12)");

        Table t0Table = getTable("test", "t02");
        PlanTestBase.setTableStatistics((OlapTable) t0Table, 1000000);
        Table testAllTypeTable = getTable("test", "test_all_type2");
        PlanTestBase.setTableStatistics((OlapTable) testAllTypeTable, 1000000);
        // aggregate querys

        createAndRefreshMv("test", "join_agg_union_mv_1", "create materialized view join_agg_union_mv_1" +
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

        {
            createAndRefreshMv("test", "join_agg_union_mv_2", "create materialized view join_agg_union_mv_2" +
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
                    "     PREDICATES: 24: t1d > 99, 24: t1d < 120");
            PlanTestBase.assertContains(plan8, "1:OlapScanNode\n" +
                    "     TABLE: t02\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 20: v1 > 99, 20: v1 < 120");
            dropMv("test", "join_agg_union_mv_2");
        }

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
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
        Assert.assertEquals(3, ttlMv1.getPartitions().size());

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
        PlanTestBase.assertContains(plan5, "multi_mv_1", "multi_mv_2", "UNION");
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
        String query6 =
                "SELECT `emps`.`deptno`, `emps`.`name`, sum(salary) as salary FROM `emps` group by deptno, name;";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "mv_agg_1", "emps", "UNION");
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
        PlanTestBase.assertContains(plan10, "partial_mv_6", "UNION", "TABLE: test_base_part\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (10: c3 >= 2000) OR (10: c3 IS NULL)\n" +
                "     partitions=3/6\n" +
                "     rollup: test_base_part");

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
        PlanTestBase.assertContains(plan11, "partial_mv_7", "UNION", "TABLE: test_base_part");
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
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
        Assert.assertEquals(4, ttlMv2.getPartitions().size());

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

    @Test
    public void testHivePartialPartitionWithTTL() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        createAndRefreshMv("test", "hive_parttbl_mv",
                "CREATE MATERIALIZED VIEW `hive_parttbl_mv`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"force_external_table_query_rewrite\" = \"true\",\n" +
                        "\"partition_ttl_number\" = \"3\"\n" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

        MaterializedView ttlMv = getMv("test", "hive_parttbl_mv");
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().runOnceForTest();
        Assert.assertEquals(3, ttlMv.getPartitions().size());

        String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par`";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "0:UNION");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate = '1998-01-01'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "HdfsScanNode");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate >= '1998-01-04'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv");

        dropMv("test", "hive_parttbl_mv");
    }

    @Test
    public void testHivePartialPartition() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        createAndRefreshMv("test", "hive_parttbl_mv",
                "CREATE MATERIALIZED VIEW `hive_parttbl_mv`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`l_shipdate`)\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"force_external_table_query_rewrite\" = \"true\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

        String query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par`";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=" + HiveMetaClient.PARTITION_NULL_VALUE));

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate > '1998-01-04'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_shipdate > '1998-01-04' and l_shipdate < '1998-01-06'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv");

        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` ";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv", "UNION", "PARTITION PREDICATES: ((22: l_shipdate < '1998-01-01')" +
                " OR (22: l_shipdate >= '1998-01-06')) OR (22: l_shipdate IS NULL)");
        dropMv("test", "hive_parttbl_mv");

        createAndRefreshMv("test", "hive_parttbl_mv_2",
                "CREATE MATERIALIZED VIEW `hive_parttbl_mv_2`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"force_external_table_query_rewrite\" = \"true\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                        "where l_orderkey > 100;");
        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "where l_orderkey > 100;";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_2");

        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02"));
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_2", "l_orderkey > 100", "lineitem_par",
                "PARTITION PREDICATES: ((23: l_shipdate >= '1998-01-02') AND ((23: l_shipdate < '1998-01-03')" +
                        " OR (23: l_shipdate >= '1998-01-06'))) OR (23: l_shipdate IS NULL)",
                "NON-PARTITION PREDICATES: 21: l_orderkey > 100");

        dropMv("test", "hive_parttbl_mv_2");

        // test partition prune
        createAndRefreshMv("test", "hive_parttbl_mv_3",
                "CREATE MATERIALIZED VIEW `hive_parttbl_mv_3`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"force_external_table_query_rewrite\" = \"true\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                        "where l_shipdate > '1998-01-02';");
        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` ";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_3", "partitions=3/6", "lineitem_par");
        PlanTestBase.assertNotContains(plan, "partitions=2/6");
        dropMv("test", "hive_parttbl_mv_3");

        createAndRefreshMv("test", "hive_parttbl_mv_4",
                "CREATE MATERIALIZED VIEW `hive_parttbl_mv_4`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"force_external_table_query_rewrite\" = \"true\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                        "where l_shipdate < '1998-01-02' and l_orderkey = 100;");
        query = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` ";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_4", "partitions=1/6", "lineitem_par",
                "NON-PARTITION PREDICATES: (((22: l_shipdate < '1998-01-01') OR (22: l_shipdate >= '1998-01-02')) " +
                        "OR (20: l_orderkey != 100)) OR (22: l_shipdate IS NULL)");
        dropMv("test", "hive_parttbl_mv_4");

        createAndRefreshMv("test", "hive_parttbl_mv_5",
                "CREATE MATERIALIZED VIEW `hive_parttbl_mv_5`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY date_trunc('month', o_orderdate)\n" +
                        "DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"force_external_table_query_rewrite\" = \"true\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders`");
        query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders`";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_5", "360/360");

        query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders` " +
                "where o_orderdate >= '1991-01-01' and o_orderdate < '1991-02-1'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_5", "partitions=1/36");

        mockedHiveMetadata.updatePartitions("partitioned_db", "orders",
                ImmutableList.of("o_orderdate=1991-02-02"));

        query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders` ";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_5", "orders",
                "PARTITION PREDICATES: (((15: o_orderdate < '1991-01-01') OR (15: o_orderdate >= '1991-02-01')) AND" +
                        " ((15: o_orderdate < '1991-03-01') OR (15: o_orderdate >= '1993-12-31')))" +
                        " OR (15: o_orderdate IS NULL)");

        // TODO(Ken Huang): This should support query rewrite
        query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders` " +
                "where o_orderdate >= '1992-05-01' and o_orderdate < '1992-05-31'";
        plan = getFragmentPlan(query);
        System.out.println(plan);

        refreshMaterializedView("test", "hive_parttbl_mv_5");
        mockedHiveMetadata.updatePartitions("partitioned_db", "orders",
                ImmutableList.of("o_orderdate=1991-01-02"));
        query = "SELECT `o_orderkey`, `o_orderstatus`, `o_orderdate`  FROM `hive0`.`partitioned_db`.`orders` " +
                "where o_orderdate >= '1992-05-01' and o_orderdate < '1992-05-31'";
        plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_parttbl_mv_5", "PREDICATES: 12: o_orderdate < '1992-05-31'",
                "partitions=1/36");

        dropMv("test", "hive_parttbl_mv_5");
    }

    @Test
    public void testPkFk() throws SQLException {
        cluster.runSql("test", "CREATE TABLE test.parent_table1(\n" +
                "k1 INT,\n" +
                "k2 VARCHAR(20),\n" +
                "k3 INT,\n" +
                "k4 VARCHAR(20)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(k1)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n," +
                "\"unique_constraints\" = \"k1,k2\"\n" +
                ");");

        cluster.runSql("test", "CREATE TABLE test.parent_table2(\n" +
                "k1 INT,\n" +
                "k2 VARCHAR(20),\n" +
                "k3 INT,\n" +
                "k4 VARCHAR(20)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(k1)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n," +
                "\"unique_constraints\" = \"k1,k2\"\n" +
                ");");

        OlapTable olapTable = (OlapTable) getTable("test", "parent_table1");
        Assert.assertNotNull(olapTable.getUniqueConstraints());
        Assert.assertEquals(1, olapTable.getUniqueConstraints().size());
        UniqueConstraint uniqueConstraint = olapTable.getUniqueConstraints().get(0);
        Assert.assertEquals(2, uniqueConstraint.getUniqueColumns().size());
        Assert.assertEquals("k1", uniqueConstraint.getUniqueColumns().get(0));
        Assert.assertEquals("k2", uniqueConstraint.getUniqueColumns().get(1));

        cluster.runSql("test", "alter table parent_table1 set(\"unique_constraints\"=\"k1, k2; k3; k4\")");
        Assert.assertNotNull(olapTable.getUniqueConstraints());
        Assert.assertEquals(3, olapTable.getUniqueConstraints().size());
        UniqueConstraint uniqueConstraint2 = olapTable.getUniqueConstraints().get(0);
        Assert.assertEquals(2, uniqueConstraint2.getUniqueColumns().size());
        Assert.assertEquals("k1", uniqueConstraint2.getUniqueColumns().get(0));
        Assert.assertEquals("k2", uniqueConstraint2.getUniqueColumns().get(1));

        UniqueConstraint uniqueConstraint3 = olapTable.getUniqueConstraints().get(1);
        Assert.assertEquals(1, uniqueConstraint3.getUniqueColumns().size());
        Assert.assertEquals("k3", uniqueConstraint3.getUniqueColumns().get(0));

        UniqueConstraint uniqueConstraint4 = olapTable.getUniqueConstraints().get(2);
        Assert.assertEquals(1, uniqueConstraint4.getUniqueColumns().size());
        Assert.assertEquals("k4", uniqueConstraint4.getUniqueColumns().get(0));

        cluster.runSql("test", "alter table parent_table1 set(\"unique_constraints\"=\"\")");
        Assert.assertNull(olapTable.getUniqueConstraints());

        cluster.runSql("test", "alter table parent_table1 set(\"unique_constraints\"=\"k1, k2\")");

        cluster.runSql("test", "CREATE TABLE test.base_table1(\n" +
                "k1 INT,\n" +
                "k2 VARCHAR(20),\n" +
                "k3 INT,\n" +
                "k4 VARCHAR(20),\n" +
                "k5 INT,\n" +
                "k6 VARCHAR(20),\n" +
                "k7 INT,\n" +
                "k8 VARCHAR(20),\n" +
                "k9 INT,\n" +
                "k10 VARCHAR(20)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(k1)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"foreign_key_constraints\" = \"(k3,k4) REFERENCES parent_table1(k1, k2)\"\n" +
                ");");
        OlapTable baseTable = (OlapTable) getTable("test", "base_table1");
        Assert.assertNotNull(baseTable.getForeignKeyConstraints());
        List<ForeignKeyConstraint> foreignKeyConstraints = baseTable.getForeignKeyConstraints();
        Assert.assertEquals(1, foreignKeyConstraints.size());
        BaseTableInfo parentTable = foreignKeyConstraints.get(0).getParentTableInfo();
        Assert.assertEquals(olapTable.getId(), parentTable.getTableId());
        Assert.assertEquals(2, foreignKeyConstraints.get(0).getColumnRefPairs().size());
        Assert.assertEquals("k3", foreignKeyConstraints.get(0).getColumnRefPairs().get(0).first);
        Assert.assertEquals("k1", foreignKeyConstraints.get(0).getColumnRefPairs().get(0).second);
        Assert.assertEquals("k4", foreignKeyConstraints.get(0).getColumnRefPairs().get(1).first);
        Assert.assertEquals("k2", foreignKeyConstraints.get(0).getColumnRefPairs().get(1).second);

        cluster.runSql("test", "alter table base_table1 set(" +
                "\"foreign_key_constraints\"=\"(k3,k4) references parent_table1(k1, k2);" +
                "(k5,k6) REFERENCES parent_table2(k1, k2)\")");

        List<ForeignKeyConstraint> foreignKeyConstraints2 = baseTable.getForeignKeyConstraints();
        Assert.assertEquals(2, foreignKeyConstraints2.size());
        BaseTableInfo parentTableInfo2 = foreignKeyConstraints2.get(1).getParentTableInfo();
        OlapTable parentTable2 = (OlapTable) getTable("test", "parent_table2");
        Assert.assertEquals(parentTable2.getId(), parentTableInfo2.getTableId());
        Assert.assertEquals(2, foreignKeyConstraints2.get(1).getColumnRefPairs().size());
        Assert.assertEquals("k5", foreignKeyConstraints2.get(1).getColumnRefPairs().get(0).first);
        Assert.assertEquals("k1", foreignKeyConstraints2.get(1).getColumnRefPairs().get(0).second);
        Assert.assertEquals("k6", foreignKeyConstraints2.get(1).getColumnRefPairs().get(1).first);
        Assert.assertEquals("k2", foreignKeyConstraints2.get(1).getColumnRefPairs().get(1).second);

        cluster.runSql("test", "show create table base_table1");
        cluster.runSql("test", "alter table base_table1 set(" +
                "\"foreign_key_constraints\"=\"\")");
        List<ForeignKeyConstraint> foreignKeyConstraints3 = baseTable.getForeignKeyConstraints();
        Assert.assertNull(foreignKeyConstraints3);
    }

    public String getFragmentPlan(String sql) throws Exception {
        String s = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.NORMAL);
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

    private void refreshMaterializedView(String dbName, String mvName) throws SQLException {
        cluster.runSql(dbName, String.format("refresh materialized view %s with sync mode", mvName));
    }

    private void createAndRefreshMv(String dbName, String mvName, String sql) throws Exception {
        starRocksAssert.withMaterializedView(sql);
        cluster.runSql(dbName, String.format("refresh materialized view %s with sync mode", mvName));
    }

    private void dropMv(String dbName, String mvName) throws Exception {
        starRocksAssert.dropMaterializedView(mvName);
    }

    public static OptExpression getOptimizedPlan(String sql, ConnectContext connectContext) {
        StatementBase mvStmt;
        try {
            List<StatementBase> statementBases =
                    com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable());
            Preconditions.checkState(statementBases.size() == 1);
            mvStmt = statementBases.get(0);
        } catch (ParsingException parsingException) {
            return null;
        }
        Preconditions.checkState(mvStmt instanceof QueryStatement);
        Analyzer.analyze(mvStmt, connectContext);
        QueryRelation query = ((QueryStatement) mvStmt).getQueryRelation();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, connectContext).transformWithSelectLimit(query);
        Optimizer optimizer = new Optimizer();
        return optimizer.optimize(
                connectContext,
                logicalPlan.getRoot(),
                new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()),
                columnRefFactory);
    }

    public List<PhysicalScanOperator> getScanOperators(OptExpression root, String name) {
        List<PhysicalScanOperator> results = Lists.newArrayList();
        getScanOperators(root, name, results);
        return results;
    }

    private void getScanOperators(OptExpression root, String name, List<PhysicalScanOperator> results) {
        if (root.getOp() instanceof PhysicalScanOperator
                && ((PhysicalScanOperator) root.getOp()).getTable().getName().equals(name)) {
            results.add((PhysicalScanOperator) root.getOp());
        }
        for (OptExpression child : root.getInputs()) {
            getScanOperators(child, name, results);
        }
    }

    @Test
    public void testNestedMVs1() throws Exception {
        createAndRefreshMv("test", "nested_mv1", "create materialized view nested_mv1 " +
                " distributed by hash(empid) as" +
                " select empid, deptno, name, salary from emps;");
        createAndRefreshMv("test", "nested_mv2", "create materialized view nested_mv2 " +
                " distributed by hash(empid) as" +
                " select empid, sum(deptno) as sum1 from emps group by empid;");
        createAndRefreshMv("test", "nested_mv3", "create materialized view nested_mv3 " +
                " distributed by hash(empid) as" +
                " select empid, sum1 from nested_mv2 where empid > 1;");
        String plan = getFragmentPlan("select empid, sum(deptno) from emps where empid > 1 group by empid");
        System.out.println(plan);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: nested_mv3\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: nested_mv3"));
        dropMv("test", "nested_mv1");
        dropMv("test", "nested_mv2");
        dropMv("test", "nested_mv3");
    }

    @Test
    public void testExternalNestedMVs1() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        createAndRefreshMv("test", "hive_nested_mv_1",
                "create materialized view hive_nested_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier");
        createAndRefreshMv("test", "hive_nested_mv_2",
                "create materialized view hive_nested_mv_2 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, sum(s_acctbal) as sum1 from hive0.tpch.supplier group by s_suppkey");
        createAndRefreshMv("test", "hive_nested_mv_3",
                "create materialized view hive_nested_mv_3 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, sum1 from hive_nested_mv_2 where s_suppkey > 1");
        String query1 = "select s_suppkey, sum(s_acctbal) from hive0.tpch.supplier where s_suppkey > 1 group by s_suppkey ";
        String plan1 = getFragmentPlan(query1);
        System.out.println(plan1);
        Assert.assertTrue(plan1.contains("0:OlapScanNode\n" +
                "     TABLE: hive_nested_mv_3\n" +
                "     PREAGGREGATION: ON\n"));
        dropMv("test", "hive_nested_mv_1");
        dropMv("test", "hive_nested_mv_2");
        dropMv("test", "hive_nested_mv_3");
    }

    @Test
    public void testTabletHintForbidMvRewrite() throws Exception {
        createAndRefreshMv("test", "forbid_mv_1", "create materialized view forbid_mv_1" +
                " distributed by hash(t1d) as SELECT " +
                " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from test_all_type" +
                " group by test_all_type.t1d");

        String query1 = "SELECT test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from test_all_type" +
                " group by test_all_type.t1d";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "  0:OlapScanNode\n" +
                "     TABLE: forbid_mv_1\n" +
                "     PREAGGREGATION: ON");
        ShowResultSet tablets = starRocksAssert.showTablet("test", "test_all_type");
        List<String> tabletIds = tablets.getResultRows().stream().map(r -> r.get(0)).collect(Collectors.toList());
        String tabletHint = String.format("tablet(%s)", tabletIds.get(0));
        String query2 = "SELECT test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from test_all_type " + tabletHint +
                " group by test_all_type.t1d";

        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "  0:OlapScanNode\n" +
                "     TABLE: test_all_type\n" +
                "     PREAGGREGATION: ON");
        dropMv("test", "forbid_mv_1");
    }

    @Test
<<<<<<< HEAD:fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvRewriteOptimizationTest.java
    public void testNullPartitionRewriteWithLoad() throws Exception {
        {
            cluster.runSql("test", "insert into test_base_part values(1, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(100, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(200, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(1000, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(2000, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(2500, 1, 2, 3)");

            createAndRefreshMv("test", "partial_mv_12", "CREATE MATERIALIZED VIEW `partial_mv_12`\n" +
                    "COMMENT \"MATERIALIZED_VIEW\"\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\",\n" +
                    "\"storage_medium\" = \"HDD\"\n" +
                    ")\n" +
                    "AS SELECT `c1`, `c3`, sum(`c4`) AS `total`\n" +
                    "FROM `test_base_part`\n" +
                    "WHERE `c3` is null\n" +
                    "GROUP BY `c3`, `c1`;");

            String query = "select c1, c3, sum(c4) from test_base_part group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "partial_mv_12", "PREDICATES: 10: c3 IS NOT NULL");
            starRocksAssert.dropMaterializedView("partial_mv_12");
        }

        {
            cluster.runSql("test", "insert into test_base_part values(1, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(100, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(200, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(1000, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(2000, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(2500, 1, 2, 3)");

            createAndRefreshMv("test", "partial_mv_13", "CREATE MATERIALIZED VIEW `partial_mv_13`\n" +
                    "COMMENT \"MATERIALIZED_VIEW\"\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\",\n" +
                    "\"storage_medium\" = \"HDD\"\n" +
                    ")\n" +
                    "AS SELECT `c1`, `c3`, sum(`c4`) AS `total`\n" +
                    "FROM `test_base_part`\n" +
                    "WHERE `c3` is null\n" +
                    "GROUP BY `c3`, `c1`;");

            // test update for null partition
            cluster.runSql("test", "insert into test_base_part partition(p1) values(null, 1, null, 3)");

            String query = "select c1, c3, sum(c4) from test_base_part group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "partial_mv_13");
            starRocksAssert.dropMaterializedView("partial_mv_13");
        }
=======
    public void testJoinPredicatePushdown() throws Exception {
        cluster.runSql("test", "CREATE TABLE pushdown_t1 (\n" +
                "    `c0` string,\n" +
                "    `c1` string,\n" +
                "    `c2` string,\n" +
                "    `c3` string,\n" +
                "    `c4` string,\n" +
                "    `c5` string ,\n" +
                "    `c6` string,\n" +
                "    `c7`  date\n" +
                ") \n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "properties('replication_num' = '1');");
        cluster.runSql("test", "CREATE TABLE `pushdown_t2` (\n" +
                "  `c0` varchar(65533) NULL ,\n" +
                "  `c1` varchar(65533) NULL ,\n" +
                "  `c2` varchar(65533) NULL ,\n" +
                "  `c3` varchar(65533) NULL ,\n" +
                "  `c4` varchar(65533) NULL ,\n" +
                "  `c5` varchar(65533) NULL ,\n" +
                "  `c6` varchar(65533) NULL ,\n" +
                "  `c7` varchar(65533) NULL ,\n" +
                "  `c8` varchar(65533) NULL ,\n" +
                "  `c9` varchar(65533) NULL ,\n" +
                "  `c10` varchar(65533) NULL ,\n" +
                "  `c11` date NULL ,\n" +
                "  `c12` datetime NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "PROPERTIES ( \"replication_num\" = \"1\");");

        // With null-rejecting predicate
        createAndRefreshMv("test", "_pushdown_predicate_join_mv1",
                "CREATE MATERIALIZED VIEW `_pushdown_predicate_join_mv1`  \n" +
                        "DISTRIBUTED BY HASH(c12) BUCKETS 18 \n" +
                        "REFRESH MANUAL \n" +
                        "PROPERTIES ( \"replication_num\" = \"1\", \"storage_medium\" = \"HDD\") \n" +
                        "AS\n" +
                        "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                        "FROM\n" +
                        "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                        "    LEFT OUTER JOIN \n" +
                        "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                        "    ON `t2`.`c0` = `t1`.`c0`\n" +
                        "    AND t2.c0 IS NOT NULL " +
                        "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                        "   ;");

        String query = "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                "FROM\n" +
                "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                "    LEFT OUTER JOIN \n" +
                "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                "    ON `t2`.`c0` = `t1`.`c0`\n" +
                "    AND t2.c0 IS NOT NULL " +
                "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                "   ;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "_pushdown_predicate_join_mv1");
    }

    @Ignore("outer join and pushdown predicate does not work")
    @Test
    public void testJoinPredicatePushdown1() throws Exception {
        cluster.runSql("test", "CREATE TABLE pushdown_t1 (\n" +
                "    `c0` string,\n" +
                "    `c1` string,\n" +
                "    `c2` string,\n" +
                "    `c3` string,\n" +
                "    `c4` string,\n" +
                "    `c5` string ,\n" +
                "    `c6` string,\n" +
                "    `c7`  date\n" +
                ") \n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "properties('replication_num' = '1');");
        cluster.runSql("test", "CREATE TABLE `pushdown_t2` (\n" +
                "  `c0` varchar(65533) NULL ,\n" +
                "  `c1` varchar(65533) NULL ,\n" +
                "  `c2` varchar(65533) NULL ,\n" +
                "  `c3` varchar(65533) NULL ,\n" +
                "  `c4` varchar(65533) NULL ,\n" +
                "  `c5` varchar(65533) NULL ,\n" +
                "  `c6` varchar(65533) NULL ,\n" +
                "  `c7` varchar(65533) NULL ,\n" +
                "  `c8` varchar(65533) NULL ,\n" +
                "  `c9` varchar(65533) NULL ,\n" +
                "  `c10` varchar(65533) NULL ,\n" +
                "  `c11` date NULL ,\n" +
                "  `c12` datetime NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "PROPERTIES ( \"replication_num\" = \"1\");");

        // Without null-rejecting predicate
        createAndRefreshMv("test", "_pushdown_predicate_join_mv2",
                "CREATE MATERIALIZED VIEW `_pushdown_predicate_join_mv2`  \n" +
                        "DISTRIBUTED BY HASH(c12) BUCKETS 18 \n" +
                        "REFRESH MANUAL \n" +
                        "PROPERTIES ( \"replication_num\" = \"1\", \"storage_medium\" = \"HDD\") \n" +
                        "AS\n" +
                        "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                        "FROM\n" +
                        "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                        "    LEFT OUTER JOIN \n" +
                        "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                        "    ON `t2`.`c0` = `t1`.`c0`\n" +
                        "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                        "   ;");

        String query = "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                "FROM\n" +
                "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                "    LEFT OUTER JOIN \n" +
                "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                "    ON `t2`.`c0` = `t1`.`c0`\n" +
                "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                "   ;";

        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "_pushdown_predicate_join_mv2");
>>>>>>> 656e543e84 ([BugFix] fix mv rewrite for join predicate pushdown (#27632)):fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvRewriteTest.java
    }
}
