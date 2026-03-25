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

import com.google.gson.Gson;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricRepo;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.SqlTaskRunProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.apache.iceberg.SnapshotRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    private ConnectContext createQueryContext() {
        ConnectContext testContext = new ConnectContext();
        testContext.setGlobalStateMgr(connectContext.getGlobalStateMgr());
        testContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        testContext.setQualifiedUser(connectContext.getQualifiedUser());
        testContext.setDatabase("test_db");
        testContext.setCurrentCatalog("default_catalog");
        testContext.setQueryId(UUIDUtil.genUUID());
        testContext.setStartTime();
        testContext.getState().setIsQuery(true);
        return testContext;
    }

    private long getTimeTravelCounterValue(String timeTravelType) {
        return MetricRepo.getMetricsByName("iceberg_time_travel_query_total").stream()
                .filter(metric -> hasTimeTravelTypeLabel(metric, timeTravelType))
                .map(Metric::getValue)
                .map(Long.class::cast)
                .findFirst()
                .orElse(0L);
    }

    private boolean hasTimeTravelTypeLabel(Metric metric, String timeTravelType) {
        for (Object labelObject : metric.getLabels()) {
            MetricLabel label = (MetricLabel) labelObject;
            if ("time_travel_type".equals(label.getKey()) && timeTravelType.equals(label.getValue())) {
                return true;
            }
        }
        return false;
    }

    private IcebergTable createIcebergTableWithRef(String refName, SnapshotRef snapshotRef) {
        org.apache.iceberg.Table nativeTable = Mockito.mock(org.apache.iceberg.Table.class);
        Mockito.when(nativeTable.refs()).thenReturn(Map.of(refName, snapshotRef));
        return new IcebergTable(1L, "t0", "iceberg_catalog", null,
                "db0", "t0", "", Collections.emptyList(), nativeTable, Collections.emptyMap());
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
    public void testIcebergTimeTravelQueryMetric() throws Exception {
        String sql = "SELECT * FROM db0.t0 FOR VERSION AS OF 1 JOIN db0.t1 FOR VERSION AS OF 2 ON 1 = 1";
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        AnalyzerUtils.collectTableRelations(statement).forEach(tableRelation -> tableRelation.setTable(new IcebergTable()));
        ConnectContext testContext = createQueryContext();

        long before = MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue();
        long snapshotBefore = getTimeTravelCounterValue("snapshot");
        new ConnectProcessor(testContext).auditAfterExec(sql, statement, null);
        long after = MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue();
        long snapshotAfter = getTimeTravelCounterValue("snapshot");

        Assertions.assertEquals(before + 1, after);
        Assertions.assertEquals(snapshotBefore + 1, snapshotAfter);
    }

    @Test
    public void testIcebergTimeTravelQueryMetricByBranch() throws Exception {
        String sql = "SELECT * FROM db0.t0 FOR VERSION AS OF 'test_branch'";
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        AnalyzerUtils.collectTableRelations(statement).forEach(tableRelation ->
                tableRelation.setTable(createIcebergTableWithRef("test_branch", SnapshotRef.branchBuilder(1L).build())));

        ConnectContext testContext = createQueryContext();
        long before = MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue();
        long branchBefore = getTimeTravelCounterValue("branch");

        new ConnectProcessor(testContext).auditAfterExec(sql, statement, null);

        Assertions.assertEquals(before + 1, MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue());
        Assertions.assertEquals(branchBefore + 1, getTimeTravelCounterValue("branch"));
    }

    @Test
    public void testIcebergTimeTravelQueryMetricByTag() throws Exception {
        String sql = "SELECT * FROM db0.t0 FOR VERSION AS OF 'test_tag'";
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        AnalyzerUtils.collectTableRelations(statement).forEach(tableRelation ->
                tableRelation.setTable(createIcebergTableWithRef("test_tag", SnapshotRef.tagBuilder(1L).build())));

        ConnectContext testContext = createQueryContext();
        long before = MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue();
        long tagBefore = getTimeTravelCounterValue("tag");

        new ConnectProcessor(testContext).auditAfterExec(sql, statement, null);

        Assertions.assertEquals(before + 1, MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue());
        Assertions.assertEquals(tagBefore + 1, getTimeTravelCounterValue("tag"));
    }

    @Test
    public void testIcebergTimeTravelQueryMetricByTimestamp() throws Exception {
        String sql = "SELECT * FROM db0.t0 FOR TIMESTAMP AS OF '2024-01-01 00:00:00'";
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        AnalyzerUtils.collectTableRelations(statement).forEach(tableRelation -> tableRelation.setTable(new IcebergTable()));

        ConnectContext testContext = createQueryContext();
        long before = MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue();
        long timestampBefore = getTimeTravelCounterValue("timestamp");

        new ConnectProcessor(testContext).auditAfterExec(sql, statement, null);

        Assertions.assertEquals(before + 1, MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue());
        Assertions.assertEquals(timestampBefore + 1, getTimeTravelCounterValue("timestamp"));
    }

    @Test
    public void testIcebergTimeTravelQueryMetricWithMultipleTypes() throws Exception {
        String sql = "SELECT * FROM db0.t0 FOR VERSION AS OF 'test_branch' " +
                "JOIN db0.t1 FOR TIMESTAMP AS OF '2024-01-01 00:00:00' ON 1 = 1";
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        List<com.starrocks.sql.ast.TableRelation> tableRelations = AnalyzerUtils.collectTableRelations(statement);
        tableRelations.get(0).setTable(createIcebergTableWithRef("test_branch", SnapshotRef.branchBuilder(1L).build()));
        tableRelations.get(1).setTable(new IcebergTable());

        ConnectContext testContext = createQueryContext();
        long before = MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue();
        long branchBefore = getTimeTravelCounterValue("branch");
        long timestampBefore = getTimeTravelCounterValue("timestamp");

        new ConnectProcessor(testContext).auditAfterExec(sql, statement, null);

        Assertions.assertEquals(before + 1, MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue());
        Assertions.assertEquals(branchBefore + 1, getTimeTravelCounterValue("branch"));
        Assertions.assertEquals(timestampBefore + 1, getTimeTravelCounterValue("timestamp"));
    }

    @Test
    public void testRegularIcebergQueryDoesNotIncreaseTimeTravelMetric() throws Exception {
        String sql = "SELECT * FROM db0.t0";
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        AnalyzerUtils.collectTableRelations(statement).forEach(tableRelation -> tableRelation.setTable(new IcebergTable()));
        ConnectContext testContext = createQueryContext();

        long before = MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue();
        new ConnectProcessor(testContext).auditAfterExec(sql, statement, null);
        long after = MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.getValue();

        Assertions.assertEquals(before, after);
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

        TaskRunContext taskRunContext = taskRun.buildTaskRunContext();
        taskRunContext.setCtx(testContext);

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

        MVTaskRunProcessor processor = (MVTaskRunProcessor) taskRun.getProcessor();

        TaskRunContext taskRunContext = taskRun.buildTaskRunContext();
        processor.prepare(taskRunContext);

        processor.processTaskRun(taskRunContext);

        ConnectContext mvCtx = taskRunContext.getCtx();

        // Manually call auditAfterExec to set querySource in AuditEvent
        ConnectProcessor connectProcessor = new ConnectProcessor(mvCtx);
        connectProcessor.auditAfterExec("REFRESH MATERIALIZED VIEW " + mvName, null, null);

        Assertions.assertEquals(QueryDetail.QuerySource.MV, mvCtx.getQuerySource());
        AuditEvent event = mvCtx.getAuditEventBuilder().build();
        Assertions.assertEquals("MV", event.querySource);

        starRocksAssert.dropDatabase(dbName);

    }
}
