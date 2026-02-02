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

package com.starrocks.qe;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.scheduler.PartitionBasedMvRefreshProcessor;
import com.starrocks.scheduler.SqlTaskRunProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class QueryDetailTest {
    @Test
    public void testQueryDetail() {
        QueryDetail queryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e3", true, 1, "127.0.0.1",
                System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.RUNNING,
                "testDb", "select * from table1 limit 1",
                "root", "", "default_catalog", "MySQL.Query", null);
        queryDetail.setProfile("bbbbb");
        queryDetail.setErrorMessage("cancelled");

        QueryDetail copyOfQueryDetail = queryDetail.copy();
        Gson gson = new Gson();
        Assertions.assertEquals(gson.toJson(queryDetail), gson.toJson(copyOfQueryDetail));

        queryDetail.setLatency(10);
        Assertions.assertEquals(-1, copyOfQueryDetail.getLatency());

        queryDetail.calculateCacheMissRatio(100, 100);
        Assertions.assertEquals((float) 50, queryDetail.getCacheMissRatio());
    }

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_collect_query_detail_info = true;
    }

    @Test
    public void testExternalQuerySource() throws Exception {
        // Test external query (user-initiated)
        String sql = "SELECT 1";
        List<StatementBase> statements = SqlParser.parse(sql, connectContext.getSessionVariable());
        StatementBase statement = statements.get(0);

        // Create a new context for this test
        ConnectContext testContext = new ConnectContext();
        testContext.setGlobalStateMgr(connectContext.getGlobalStateMgr());
        testContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        testContext.setQualifiedUser(connectContext.getQualifiedUser());
        testContext.setDatabase("test_db");
        testContext.setCurrentCatalog("default_catalog");
        testContext.setQueryId(UUIDUtil.genUUID());
        testContext.setStartTime();

        // Execute using normal StmtExecutor (external query)
        StmtExecutor executor = new StmtExecutor(testContext, statement);
        testContext.setExecutor(executor);
        testContext.setThreadLocalInfo();

        // Add running query detail
        executor.addRunningQueryDetail(statement);

        // Simulate auditAfterExec to set querySource in AuditEvent
        ConnectProcessor processor = new ConnectProcessor(testContext);
        processor.auditAfterExec(sql, statement, null);

        // Verify QueryDetail query source is EXTERNAL
        QueryDetail queryDetail = testContext.getQueryDetail();
        Assertions.assertNotNull(queryDetail);
        Assertions.assertEquals(QueryDetail.QuerySource.EXTERNAL, queryDetail.getQuerySource());

        // Verify AuditEvent query source is EXTERNAL
        AuditEvent event = testContext.getAuditEventBuilder().build();
        Assertions.assertEquals("EXTERNAL", event.querySource);
    }

    @Test
    public void testInternalQuerySource() throws Exception {
        // Test internal query (system query)
        String sql = "SELECT 1";
        List<StatementBase> statements = SqlParser.parse(sql, connectContext.getSessionVariable());
        StatementBase statement = statements.get(0);

        // Create a new context for this test
        ConnectContext testContext = new ConnectContext();
        testContext.setGlobalStateMgr(connectContext.getGlobalStateMgr());
        testContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        testContext.setQualifiedUser(connectContext.getQualifiedUser());
        testContext.setDatabase("test_db");
        testContext.setCurrentCatalog("default_catalog");
        testContext.setQueryId(UUIDUtil.genUUID());
        testContext.setStartTime();

        // Execute using internal StmtExecutor
        StmtExecutor executor = StmtExecutor.newInternalExecutor(testContext, statement);
        testContext.setExecutor(executor);
        testContext.setThreadLocalInfo();

        // Add running query detail
        executor.addRunningQueryDetail(statement);

        // Simulate auditAfterExec to set querySource in AuditEvent
        ConnectProcessor processor = new ConnectProcessor(testContext);
        processor.auditAfterExec(sql, statement, null);

        // Verify QueryDetail query source is INTERNAL
        QueryDetail queryDetail = testContext.getQueryDetail();
        Assertions.assertNotNull(queryDetail);
        Assertions.assertEquals(QueryDetail.QuerySource.INTERNAL, queryDetail.getQuerySource());

        // Verify AuditEvent query source is INTERNAL
        AuditEvent event = testContext.getAuditEventBuilder().build();
        Assertions.assertEquals("INTERNAL", event.querySource);
    }

    @Test
    public void testImpersonatedUserInQueryDetail() throws Exception {
        String sql = "SELECT 1";
        List<StatementBase> statements = SqlParser.parse(sql, connectContext.getSessionVariable());
        StatementBase statement = statements.get(0);

        ConnectContext testContext = new ConnectContext();
        testContext.setGlobalStateMgr(connectContext.getGlobalStateMgr());
        testContext.setQualifiedUser("user_a");
        testContext.setCurrentUserIdentity(new UserIdentity("user_b", "%"));
        testContext.setDatabase("test_db");
        testContext.setCurrentCatalog("default_catalog");
        testContext.setQueryId(UUIDUtil.genUUID());
        testContext.setStartTime();

        StmtExecutor executor = new StmtExecutor(testContext, statement);
        testContext.setExecutor(executor);
        testContext.setThreadLocalInfo();

        executor.addRunningQueryDetail(statement);

        QueryDetail queryDetail = testContext.getQueryDetail();
        Assertions.assertNotNull(queryDetail);
        Assertions.assertEquals("user_a", queryDetail.getUser());
        Assertions.assertEquals("user_b", queryDetail.getImpersonatedUser());
    }

    @Test
    public void testTaskQuerySource(@Mocked StatementBase mockStmt) throws Exception {
        // Test task query using SqlTaskRunProcessor
        String sql = "SELECT 1";

        // Create a new context for this test
        ConnectContext testContext = new ConnectContext();
        testContext.setGlobalStateMgr(connectContext.getGlobalStateMgr());
        testContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        testContext.setQualifiedUser(connectContext.getQualifiedUser());
        testContext.setDatabase("test_db");
        testContext.setCurrentCatalog("default_catalog");
        testContext.setQueryId(UUIDUtil.genUUID());
        testContext.setStartTime();

        Task task = new Task("test_task");
        task.setDefinition(sql);
        task.setCatalogName("default_catalog");
        task.setDbName("test_db");

        TaskRun taskRun = new TaskRun();
        taskRun.setTask(task);
        taskRun.setConnectContext(testContext);
        taskRun.setProperties(new java.util.HashMap<>());
        taskRun.initStatus(UUID.randomUUID().toString(), System.currentTimeMillis());

        TaskRunContext taskRunContext = new TaskRunContext();
        taskRunContext.setCtx(testContext);
        taskRunContext.setDefinition(sql);
        taskRunContext.setTaskRun(taskRun);

        SqlTaskRunProcessor processor = new SqlTaskRunProcessor();
        processor.processTaskRun(taskRunContext);

        Assertions.assertEquals(QueryDetail.QuerySource.TASK, testContext.getQuerySource());
        AuditEvent event = testContext.getAuditEventBuilder().build();
        Assertions.assertEquals(QueryDetail.QuerySource.TASK.name(), event.querySource);
    }

    @Test
    public void testMVQuerySource() throws Exception {
        String dbName = "test_mv_query_source_db";
        String tableName = "test_mv_query_source_table";
        String mvName = "test_mv_query_source";

        starRocksAssert
                .withDatabase(dbName)
                .useDatabase(dbName)
                .withTable("CREATE TABLE " + tableName + " (id INT, name VARCHAR(100)) " +
                        "PROPERTIES('replication_num'='1')");
        String mvSql = "CREATE MATERIALIZED VIEW " + mvName +
                " REFRESH DEFERRED MANUAL AS SELECT id, name FROM " + tableName;
        starRocksAssert.ddl(mvSql);

        MaterializedView mv = starRocksAssert.getMv(dbName, mvName);

        Task task = TaskBuilder.buildMvTask(mv, dbName);
        java.util.Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUID.randomUUID().toString(), System.currentTimeMillis());

        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();

        TaskRunContext taskRunContext = new TaskRunContext();
        ConnectContext mvCtx = taskRun.buildTaskRunConnectContext();
        // Set query source to MV for materialized view refresh
        mvCtx.setQuerySource(QueryDetail.QuerySource.MV);
        taskRunContext.setCtx(mvCtx);
        taskRunContext.setDefinition(task.getDefinition());
        taskRunContext.setTaskRun(taskRun);
        taskRunContext.setProperties(Maps.newHashMap());
        taskRunContext.getProperties().put(TaskRun.MV_ID, String.valueOf(mv.getMvId().getId()));
        processor.prepare(taskRunContext);

        processor.processTaskRun(taskRunContext);

        // Manually call auditAfterExec to set querySource in AuditEvent
        ConnectProcessor connectProcessor = new ConnectProcessor(mvCtx);
        connectProcessor.auditAfterExec("REFRESH MATERIALIZED VIEW " + mvName, null, null);

        Assertions.assertEquals(QueryDetail.QuerySource.MV, mvCtx.getQuerySource());
        AuditEvent event = mvCtx.getAuditEventBuilder().build();
        Assertions.assertEquals("MV", event.querySource);

        starRocksAssert.dropDatabase(dbName);

    }
}
