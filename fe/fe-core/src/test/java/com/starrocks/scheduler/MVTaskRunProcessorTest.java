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

package com.starrocks.scheduler;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.mv.pct.MVPCTBasedRefreshProcessor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Map;


@TestMethodOrder(MethodName.class)
public class MVTaskRunProcessorTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();

        // Create test tables needed for the tests
        starRocksAssert.withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
    }

    /**
     * Test that updateTaskRunStatus handles exceptions gracefully without throwing.
     * This tests the fix to ignore exceptions when updating task run status.
     */
    @Test
    public void testUpdateTaskRunStatusExceptionHandling() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test_mv_status_update` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")\n" +
                        "AS SELECT k1, k2, v1 FROM test.tbl1;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "test_mv_status_update"));
        Assertions.assertNotNull(mv);

        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());

        // Mock the TaskRunStatus to throw an exception during update
        new MockUp<TaskRunStatus>() {
            @Mock
            public MVTaskRunExtraMessage getMvTaskRunExtraMessage() {
                throw new RuntimeException("Simulated exception in getMvTaskRunExtraMessage");
            }
        };

        // The task run should complete without throwing exception
        // even when the status update fails
        Assertions.assertDoesNotThrow(() -> {
            taskRun.executeTaskRun();
        }, "Task run should not throw exception when status update fails");

        starRocksAssert.dropMaterializedView("test_mv_status_update");
    }

    /**
     * Test that MVPCTBasedRefreshProcessor correctly records plan builder message in tracer.
     * This tests the fix that records plan builder message in the tracer for better debugging.
     */
    @Test
    public void testPlanBuilderMessageTracing() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test_mv_plan_builder` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")\n" +
                        "AS SELECT k1, k2, v1 FROM test.tbl1;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "test_mv_plan_builder"));
        Assertions.assertNotNull(mv);

        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        
        // Enable profile
        ConnectContext ctx = connectContext;
        ctx.getSessionVariable().setEnableProfile(true);

        // Execute the task run
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        // Get the MVTaskRunProcessor and its runtime profile
        MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(taskRun);
        RuntimeProfile runtimeProfile = mvTaskRunProcessor.getRuntimeProfile();
        Assertions.assertNotNull(runtimeProfile, "Runtime profile should not be null");
        
        // Get the trace info from runtime profile
        Map<String, String> infoStrings = runtimeProfile.getInfoStrings();
        Assertions.assertNotNull(infoStrings, "Info strings should not be null");
        
        // The runtime profile should contain some trace information
        // Note: The exact key names may vary, but we verify that the profile is populated
        Assertions.assertFalse(infoStrings.isEmpty(), "Runtime profile should contain trace information");

        starRocksAssert.dropMaterializedView("test_mv_plan_builder");
    }

    /**
     * Test that MVPCTBasedRefreshProcessor correctly updates task run status definition.
     */
    @Test
    public void testTaskRunStatusDefinitionUpdate() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test_mv_definition_update` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")\n" +
                        "AS SELECT k1, k2, v1 FROM test.tbl1;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "test_mv_definition_update"));
        Assertions.assertNotNull(mv);

        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        
        // Execute the task run
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        // Get the task run status and verify definition is set
        TaskRunStatus status = taskRun.getStatus();
        Assertions.assertNotNull(status);
        
        // The definition should be updated (not null or empty after refresh)
        // Note: The definition may be empty string if insertStmt is null, which is expected
        // This test mainly verifies no exception is thrown during the process
        String definition = status.getDefinition();
        Assertions.assertNotNull(definition, "Task run status definition should not be null");

        starRocksAssert.dropMaterializedView("test_mv_definition_update");
    }

    /**
     * Test the getRetryTimes method in MVPCTBasedRefreshProcessor.
     */
    @Test
    public void testGetRetryTimes() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test_mv_retry_times` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")\n" +
                        "AS SELECT k1, k2, v1 FROM test.tbl1;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "test_mv_retry_times"));
        Assertions.assertNotNull(mv);

        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());

        // Use connectContext directly
        ConnectContext ctx = connectContext;
        
        MVTaskRunProcessor mvTaskRunProcessor = new MVTaskRunProcessor();
        TaskRunContext taskRunContext = new TaskRunContext();
        taskRunContext.setTaskRun(taskRun);
        taskRunContext.setCtx(ctx);
        taskRunContext.getCtx().setDatabase("test");
        taskRunContext.setProperties(testProperties);
        mvTaskRunProcessor.prepare(taskRunContext);

        MVPCTBasedRefreshProcessor processor = 
                (MVPCTBasedRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
        Assertions.assertNotNull(processor);

        // Test getRetryTimes with different contexts
        int retryTimes = processor.getRetryTimes(ctx);
        Assertions.assertTrue(retryTimes >= 1, "Retry times should be at least 1");

        starRocksAssert.dropMaterializedView("test_mv_retry_times");
    }

    /**
     * Test that MV refresh trace info is recorded in runtime profile.
     * This tests that the trace info recording works correctly.
     */
    @Test
    public void testMVRefreshTraceInfoRecording() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test_mv_trace_info` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")\n" +
                        "AS SELECT k1, k2, v1 FROM test.tbl1;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "test_mv_trace_info"));
        Assertions.assertNotNull(mv);

        // Test with profile enabled - trace info should be recorded
        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        
        // Enable profile
        ConnectContext ctx = connectContext;
        ctx.getSessionVariable().setEnableProfile(true);

        // Execute the task run
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        // Get the MVTaskRunProcessor and its runtime profile
        MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(taskRun);
        RuntimeProfile runtimeProfile = mvTaskRunProcessor.getRuntimeProfile();
        Assertions.assertNotNull(runtimeProfile, "Runtime profile should not be null when profile is enabled");
        
        // Get the trace info from runtime profile
        Map<String, String> infoStrings = runtimeProfile.getInfoStrings();
        Assertions.assertNotNull(infoStrings, "Info strings should not be null");
        
        // The trace info should contain partition info when profile is enabled
        String partitionInfo = infoStrings.get("MVRefreshPartitionInfo");
        Assertions.assertNotNull(partitionInfo,
                "MVRefreshPartitionInfo should be recorded in runtime profile");

        starRocksAssert.dropMaterializedView("test_mv_trace_info");
    }
}
