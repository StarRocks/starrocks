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

package com.starrocks.analysis;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.TaskAnalyzer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.common.AuditEncryptionChecker;
import com.starrocks.sql.formatter.AST2StringVisitor;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubmitTaskStmtTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withDatabase("test")
                .useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
    }

    @Test
    public void testBasicSubmitStmtTest() throws Exception {
        starRocksAssert.useDatabase("test");
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
        String submitSQL = "submit task as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL, ctx);

        Assertions.assertEquals(submitTaskStmt.getDbName(), "test");
        Assertions.assertNull(submitTaskStmt.getTaskName());
        Assertions.assertEquals(submitTaskStmt.getProperties().size(), 0);

        String submitSQL2 = "submit /*+ SET_VAR(query_timeout = 1) */ task as " +
                "create table temp as select count(*) as cnt from tbl1";

        SubmitTaskStmt submitTaskStmt2 = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL2, ctx);
        Assertions.assertEquals(submitTaskStmt2.getDbName(), "test");
        Assertions.assertNull(submitTaskStmt2.getTaskName());
        Assertions.assertEquals(submitTaskStmt2.getProperties().size(), 1);
        Map<String, String> properties = submitTaskStmt2.getProperties();
        for (String key : properties.keySet()) {
            Assertions.assertEquals(key, "query_timeout");
            Assertions.assertEquals(properties.get(key), "1");
        }

        String submitSQL3 = "submit task task_name as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt3 = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL3, ctx);

        Assertions.assertEquals(submitTaskStmt3.getDbName(), "test");
        Assertions.assertEquals(submitTaskStmt3.getTaskName(), "task_name");
        Assertions.assertEquals(submitTaskStmt3.getProperties().size(), 0);

        String submitSQL4 = "submit task test.task_name as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt4 = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL4, ctx);

        Assertions.assertEquals(submitTaskStmt4.getDbName(), "test");
        Assertions.assertEquals(submitTaskStmt4.getTaskName(), "task_name");
        Assertions.assertEquals(submitTaskStmt4.getProperties().size(), 0);

        String submitSQL5 = "submit /*+ SET_VAR(query_timeout = 1) */ task as " +
                "insert into files(\"path\"=\"data\", \"format\"=\"csv\", \"aws.s3.access_key\"=\"aaa\", " +
                "\"aws.s3.secret_key\"=\"bbb\") select * from tbl1";
        SubmitTaskStmt submitTaskStmt5 = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL5, ctx);
        AuditEncryptionChecker.getInstance().visitSubmitTaskStatement(submitTaskStmt5, null);
        AST2StringVisitor visitor = new AST2StringVisitor();
        String str = visitor.visitSubmitTaskStatement(submitTaskStmt5, null);
        Assertions.assertFalse(str.contains("aaa"));
        Assertions.assertFalse(str.contains("bbb"));
    }

    @Test
    public void SubmitStmtShouldShow() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String submitSQL = "SUBMIT TASK test1 AS CREATE TABLE t1 AS SELECT SLEEP(5);";
        StatementBase submitStmt = getAnalyzedPlan(submitSQL, ctx);
        Assertions.assertTrue(submitStmt instanceof SubmitTaskStmt);
        SubmitTaskStmt statement = (SubmitTaskStmt) submitStmt;
        ShowResultSet showResult = DDLStmtExecutor.execute(statement, ctx);
        Assertions.assertNotNull(showResult);
    }

    @Test
    public void testSubmitInsert() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.test_insert\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");

        String sql1 = "submit task task1 as insert into test.test_insert select * from test.test_insert";
        UtFrameUtils.parseStmtWithNewParser(sql1, ctx);

        String sql2 = "submit task task1 as insert overwrite test.test_insert select * from test.test_insert";
        UtFrameUtils.parseStmtWithNewParser(sql2, ctx);

        SubmitTaskStmt submitStmt = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        Assertions.assertNotNull(submitStmt.getDbName());
        Assertions.assertNotNull(submitStmt.getSqlText());
    }

    @Test
    public void testSubmitWithWarehouse() throws Exception {
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();

        // not supported
        Exception e = assertThrows(AnalysisException.class, () ->
                starRocksAssert.ddl("submit task t_warehouse properties('warehouse'='w1') as " +
                        "insert into tbl1 select * from tbl1")
        );
        Assertions.assertEquals("Getting analyzing error. Detail message: Invalid parameter warehouse.", e.getMessage());

        // mock the warehouse
        new MockUp<TaskAnalyzer>() {
            @Mock
            public void analyzeTaskProperties(Map<String, String> properties) {
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(String name) {
                return new DefaultWarehouse(123, name);
            }
            @Mock
            public Warehouse getWarehouse(long id) {
                return new DefaultWarehouse(123, "w1");
            }
        };

        starRocksAssert.ddl("submit task t_warehouse properties('warehouse'='w1') as " +
                "insert into tbl1 select * from tbl1");
        Task task = tm.getTask("t_warehouse");
        Assertions.assertTrue(task.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE),
                task.getProperties().toString());
        Assertions.assertEquals("('warehouse'='w1')", task.getPropertiesString());
    }

    @Test
    public void testDropTaskForce() throws Exception {
        String name = "mv_force";
        starRocksAssert.withMaterializedView("create materialized view " + name +
                " refresh async every(interval 1 minute)" +
                "as select * from tbl1");
        MaterializedView mv = starRocksAssert.getMv("test", name);
        String taskName = TaskBuilder.getMvTaskName(mv.getId());

        // regular drop
        Exception e = assertThrows(RuntimeException.class,
                () -> starRocksAssert.ddl(String.format("drop task `%s`", taskName)));
        assertEquals("Can not drop task generated by materialized view. " +
                "You can use DROP MATERIALIZED VIEW to drop task, when the materialized view is deleted, " +
                "the related task will be deleted automatically.", e.getMessage());

        // force drop
        starRocksAssert.ddl(String.format("drop task `%s` force", taskName));
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
        Assertions.assertNull(tm.getTask(taskName));
        starRocksAssert.dropMaterializedView(name);
    }

    @Test
    public void testDropTaskIfExists() throws Exception {
        String taskName = "test_task";

        // regular drop
        Exception e = assertThrows(RuntimeException.class,
                () -> starRocksAssert.ddl(String.format("drop task `%s`", taskName)));
        assertEquals("Getting analyzing error. Detail message: Task " + taskName + " is not exist.", e.getMessage());

        // drop if exists
        ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(String.format("drop task if exists `%s`", taskName)));
    }

    @Test
    public void createTaskWithUser() throws Exception {
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
        connectContext.executeSql("CREATE USER 'test2' IDENTIFIED BY ''");
        connectContext.executeSql("GRANT all on DATABASE test to test2");
        connectContext.executeSql("GRANT all on test.* to test2");
        connectContext.executeSql("EXECUTE AS test2 WITH NO REVERT");
        connectContext.executeSql(("submit task task_with_user as create table t_tmp as select * from test.tbl1"));
        Assertions.assertEquals("test2", tm.getTask("task_with_user").getCreateUser());

        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setCurrentRoleIds(starRocksAssert.getCtx().getGlobalStateMgr()
                .getAuthorizationMgr().getRoleIdsByUser(UserIdentity.ROOT));
    }

    @Test
    public void testPeriodicalTask() throws Exception {
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();

        {
            connectContext.executeSql("submit task t1 " +
                    "schedule every(interval 1 minute) " +
                    "as insert overwrite tbl1 select * from tbl1");
            Task task = tm.getTask("t1");
            Assertions.assertNotNull(task);
            Assertions.assertEquals(TimeUnit.MINUTES, task.getSchedule().getTimeUnit());
            Assertions.assertEquals(1, task.getSchedule().getPeriod());
            Assertions.assertEquals(Constants.TaskSource.INSERT, task.getSource());
            Assertions.assertEquals(Constants.TaskType.PERIODICAL, task.getType());
            connectContext.executeSql("drop task t1");
        }

        {
            connectContext.executeSql("submit task t2 " +
                    "schedule start('1997-01-01 00:00:00') every(interval 1 minute) " +
                    "as insert overwrite tbl1 select * from tbl1");
            Task task = tm.getTask("t2");
            Assertions.assertNotNull(task);
            Assertions.assertEquals("START(1997-01-01T00:00) EVERY(1 MINUTES)", task.getSchedule().toString());
            Assertions.assertEquals(Constants.TaskSource.INSERT, task.getSource());
            Assertions.assertEquals(Constants.TaskType.PERIODICAL, task.getType());
            connectContext.executeSql("drop task t2");
        }
        {
            // timezone not supported
            Exception e =
                    Assertions.assertThrows(ParsingException.class, () -> connectContext.executeSql("submit task t3 " +
                            "schedule start('1997-01-01 00:00:00 PST') every(interval 1 minute) " +
                            "as insert overwrite tbl1 select * from tbl1"));
            Assertions.assertEquals("Getting syntax error from line 1, column 15 to line 1, column 80. " +
                            "Detail message: Invalid date literal 1997-01-01 00:00:00 PST.",
                    e.getMessage());
        }
        {
            // multiple clauses
            connectContext.executeSql("submit task t4 " +
                    "properties('session.query_timeout'='1') " +
                    "schedule start('1997-01-01 00:00:00') every(interval 1 minute) " +
                    "as insert overwrite tbl1 select * from tbl1");
            Task task = tm.getTask("t4");
            Assertions.assertNotNull(task);
            Assertions.assertEquals("START(1997-01-01T00:00) EVERY(1 MINUTES)", task.getSchedule().toString());
            Assertions.assertEquals(Constants.TaskSource.INSERT, task.getSource());
            Assertions.assertEquals(Constants.TaskType.PERIODICAL, task.getType());
            connectContext.executeSql("drop task t4");
        }
        {
            // multiple clauses
            connectContext.executeSql("submit task t4 " +
                    "schedule start('1997-01-01 00:00:00') every(interval 1 minute) " +
                    "properties('session.query_timeout'='1') " +
                    "as insert overwrite tbl1 select * from tbl1");
            Task task = tm.getTask("t4");
            Assertions.assertNotNull(task);
            Assertions.assertEquals("START(1997-01-01T00:00) EVERY(1 MINUTES)", task.getSchedule().toString());
            Assertions.assertEquals(Constants.TaskSource.INSERT, task.getSource());
            Assertions.assertEquals(Constants.TaskType.PERIODICAL, task.getType());
            connectContext.executeSql("drop task t4");
        }

        {
            // illegal
            connectContext.executeSql("submit task t_illegal " +
                    "schedule every(interval 1 second) " +
                    "as insert overwrite tbl1 select * from tbl1");
            Assertions.assertEquals("Getting analyzing error. Detail message: schedule interval is too small, " +
                            "the minimum value is 10 SECONDS.",
                    connectContext.getState().getErrorMessage());

            // year
            Exception e = Assertions.assertThrows(ParsingException.class, () ->
                    connectContext.executeSql("submit task t_illegal " +
                            "schedule every(interval 1 year) " +
                            "as insert overwrite tbl1 select * from tbl1"));
            Assertions.assertEquals("Getting syntax error at line 1, column 48. " +
                            "Detail message: Unexpected input 'year', " +
                            "the most similar input is {'DAY', 'HOUR', 'SECOND', 'MINUTE'}.",
                    e.getMessage());

            // week
            e = Assertions.assertThrows(ParsingException.class, () ->
                    connectContext.executeSql("submit task t_illegal " +
                            "schedule every(interval 1 week) " +
                            "as insert overwrite tbl1 select * from tbl1"));
            Assertions.assertEquals("Getting syntax error at line 1, column 48. " +
                            "Detail message: Unexpected input 'week', " +
                            "the most similar input is {'SECOND', 'DAY', 'HOUR', 'MINUTE'}.",
                    e.getMessage());

            // syntax error
            e = Assertions.assertThrows(ParsingException.class, () ->
                    connectContext.executeSql("submit task t_illegal " +
                            "schedule every(interval 1) " +
                            "as insert overwrite tbl1 select * from tbl1"));
            Assertions.assertEquals("Getting syntax error at line 1, column 47. " +
                            "Detail message: Unexpected input ')', " +
                            "the most similar input is {'DAY', 'HOUR', 'MINUTE', 'SECOND'}.",
                    e.getMessage());
        }
    }

    @Test
    public void testSubmitStmtWithSessionProperties() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String submitSQL = "SUBMIT TASK task_test1 " +
                "schedule every(interval 1 minute) " +
                "PROPERTIES ('session.new_planner_optimize_timeout' = '10000', 'session.enable_profile' = 'true') " +
                "AS CREATE TABLE t1 AS SELECT SLEEP(10);";
        StatementBase submitStmt = getAnalyzedPlan(submitSQL, ctx);
        Assertions.assertTrue(submitStmt instanceof SubmitTaskStmt);
        SubmitTaskStmt statement = (SubmitTaskStmt) submitStmt;
        ShowResultSet showResult = DDLStmtExecutor.execute(statement, ctx);
        Assertions.assertNotNull(showResult);
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = tm.getTask("task_test1");
        Assertions.assertNotNull(task);

        TaskRun taskRun = null;
        int i = 0;
        while (taskRun == null && i++ < 100) {
            taskRun = tm.getTaskRunScheduler().getRunnableTaskRun(task.getId());
            Thread.sleep(100);
        }
        if (taskRun == null) {
            return;
        }
        Assertions.assertNotNull(taskRun);
        Assertions.assertEquals("10000", taskRun.getProperties().get("session.new_planner_optimize_timeout"));
        connectContext.executeSql("drop task task_test1");
    }

    @Test
    public void testSubmitStmtWithSessionPropertiesWithNPE(@Mocked Task task) {
        new Expectations() {
            {
                task.getDefinition();
                result = null;
            }
        };
        TaskRun taskRun = new TaskRun();
        taskRun.setTask(task);
        Exception exception = Assertions.assertThrows(NullPointerException.class, taskRun::executeTaskRun);
        Assertions.assertEquals("The definition of task run should not null", exception.getMessage());
    }
}