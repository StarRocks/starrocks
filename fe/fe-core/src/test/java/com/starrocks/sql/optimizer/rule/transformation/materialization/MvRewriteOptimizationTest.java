// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
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
                .withTable("create table dept (\n" +
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
        cluster.runSql("test", "insert into emps values(1, 1, \"emp_name1\", 100);");
        cluster.runSql("test", "insert into emps values(2, 1, \"emp_name1\", 120);");
        cluster.runSql("test", "insert into emps values(3, 1, \"emp_name1\", 150);");
        cluster.runSql("test", "insert into dept values(1, \"dept_name1\")");
        cluster.runSql("test", "insert into dependents values(1, \"dependent_name1\")");
        cluster.runSql("test", "insert into locations values(1, \"location1\")");
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

        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        String query6 = "select empid, deptno, name, salary from emps where empid = 5";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertNotContains(plan6, "mv_1");

        dropMv("test", "mv_1");
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(true);
    }

    public void testSingleTableRangePredicateRewrite() throws Exception {
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
        PlanTestBase.assertContains(plan2, "2:Project\n" +
                "  |  <slot 1> : 5: empid\n" +
                "  |  <slot 2> : 6: deptno\n" +
                "  |  <slot 3> : 7: name\n" +
                "  |  <slot 4> : 8: salary\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 5: empid <= 3\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv_1");

        String query3 = "select empid, deptno, name, salary from emps where empid <= 5";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "mv_1");

        String query4 = "select empid, deptno, name, salary from emps where empid > 5";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "mv_1");

        String query5 = "select empid, length(name), (salary + 1) * 2 from emps where empid = 4";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "2:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 7: empid = 4\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv_1");

        String query6 = "select empid, length(name), (salary + 1) * 2 from emps where empid between 3 and 4";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "2:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 7: empid <= 4, 7: empid >= 3\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv_1");

        String query7 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 and salary > 100";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "2:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 10: salary > 100.0\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv_1");
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

    public String getFragmentPlan(String sql) throws Exception {
        String s = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.NORMAL);
        return s;
    }

    private MaterializedView getMv(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(mvName);
        Assert.assertNotNull(table);
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
        starRocksAssert.withNewMaterializedView(sql);
        refreshMaterializedView(dbName, mvName);
    }

    private void dropMv(String dbName, String mvName) throws Exception {
        starRocksAssert.dropMaterializedView(mvName);
    }
}
