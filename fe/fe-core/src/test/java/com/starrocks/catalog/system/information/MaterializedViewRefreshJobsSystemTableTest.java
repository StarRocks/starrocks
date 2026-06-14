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

package com.starrocks.catalog.system.information;

import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TListMaterializedViewRefreshJobsResult;
import com.starrocks.thrift.TMaterializedViewRefreshJobInfo;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MaterializedViewRefreshJobsSystemTableTest {

    private static final List<String> EXPECTED_COLUMNS = Lists.newArrayList(
            "JOB_ID",
            "MATERIALIZED_VIEW_ID",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "TASK_ID",
            "WAREHOUSE",
            "RESOURCE_GROUP",
            "CREATOR",
            "SUBMIT_USER",
            "RUN_AS_USER",
            "SUBMIT_TIME",
            "REFRESH_STATE",
            "FINISH_TIME",
            "DURATION_TIME",
            "REFRESH_TRIGGER",
            "REFRESH_MODE",
            "IMV_SOURCE_VERSION_RANGE",
            "IMV_SOURCE_TIMESTAMP_RANGE",
            "IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP",
            "FAILED_TASK_RUN_ID",
            "FAILED_QUERY_ID",
            "ERROR_CODE",
            "ERROR_MESSAGE");

    @Test
    public void testColumnContractMatchesThriftOrder() {
        SystemTable table = MaterializedViewRefreshJobsSystemTable.create();
        List<String> actual = table.getBaseSchema().stream()
                .map(Column::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals(23, actual.size());
        Assertions.assertEquals(EXPECTED_COLUMNS, actual);
        Assertions.assertEquals(Table.TableType.SCHEMA, table.getType());
    }

    @Test
    public void testRegisteredInInternalCatalogInfoSchema() {
        InfoSchemaDb db = new InfoSchemaDb();
        Table table = db.getTable(MaterializedViewRefreshJobsSystemTable.NAME);
        Assertions.assertNotNull(table);
        Assertions.assertEquals(Table.TableType.SCHEMA, table.getType());
        Assertions.assertEquals(23, table.getBaseSchema().size());
    }

    @Test
    public void testQueryRollsUpBatchByStartTaskRunId(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) throws TException {
        TaskRunStatus first = newRun("job1", "run1", "SUCCESS", Constants.TaskRunState.SUCCESS, 1000L);
        TaskRunStatus second = newRun("job1", "run2", "SUCCESS", Constants.TaskRunState.SUCCESS, 2000L);

        mockTaskRunStatus(globalStateMgr, taskManager, Lists.newArrayList(first, second));
        mockAuthorizerPass();

        TListMaterializedViewRefreshJobsResult result =
                MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

        Assertions.assertEquals(1, result.getJobs().size());
        TMaterializedViewRefreshJobInfo job = result.getJobs().get(0);
        Assertions.assertEquals("job1", job.getJob_id());
        Assertions.assertEquals("100", job.getMaterialized_view_id());
        Assertions.assertEquals("db1", job.getTable_schema());
        // Empty extra-message maps serialize to "{}" rather than null.
        Assertions.assertEquals("{}", job.getImv_source_version_range());
        Assertions.assertEquals("{}", job.getImv_source_timestamp_range());
        Assertions.assertEquals("{}", job.getImv_source_pinned_snapshot_id_map());
    }

    @Test
    public void testGroupingFallsBackToTaskRunIdAndDropsIdlessRun(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) throws TException {
        TaskRunStatus fallback = newRun(null, "query-fb", "SUCCESS", Constants.TaskRunState.SUCCESS, 1000L);
        fallback.setTaskRunId("taskrun-fb");
        TaskRunStatus idless = newRun(null, "query-idless", "SUCCESS", Constants.TaskRunState.SUCCESS, 2000L);

        mockTaskRunStatus(globalStateMgr, taskManager, Lists.newArrayList(fallback, idless));
        mockAuthorizerPass();

        TListMaterializedViewRefreshJobsResult result =
                MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

        Assertions.assertEquals(1, result.getJobs().size());
        Assertions.assertEquals("taskrun-fb", result.getJobs().get(0).getJob_id());
    }

    @Test
    public void testMvDeletedYieldsNullEnrichmentAndUnknownTrigger(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) throws TException {
        TaskRunStatus run = newRun("job-deleted", "run-deleted", "SUCCESS", Constants.TaskRunState.SUCCESS, 1000L);

        mockTaskRunStatus(globalStateMgr, taskManager, Lists.newArrayList(run));
        mockAuthorizerPass();

        TListMaterializedViewRefreshJobsResult result =
                MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

        Assertions.assertEquals(1, result.getJobs().size());
        TMaterializedViewRefreshJobInfo job = result.getJobs().get(0);
        Assertions.assertNull(job.getTable_name());
        Assertions.assertNull(job.getResource_group());
        Assertions.assertNull(job.getRefresh_mode());
        Assertions.assertEquals("UNKNOWN", job.getRefresh_trigger());
    }

    @Test
    public void testFailedRunColumnsComeFromLatestFailure(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) throws TException {
        TaskRunStatus earlierFailure = newRun("job-failed", "query-early", "FAILED", Constants.TaskRunState.FAILED, 1000L);
        earlierFailure.setTaskRunId("taskrun-early");
        earlierFailure.setErrorCode(1);
        earlierFailure.setErrorMessage("early failure");

        TaskRunStatus laterFailure = newRun("job-failed", "query-late", "FAILED", Constants.TaskRunState.FAILED, 2000L);
        laterFailure.setTaskRunId("taskrun-late");
        laterFailure.setErrorCode(42);
        laterFailure.setErrorMessage("late failure");

        // Reverse-chronological input: the latest failure only wins after query() sorts the batch by processStartTime.
        mockTaskRunStatus(globalStateMgr, taskManager, Lists.newArrayList(laterFailure, earlierFailure));
        mockAuthorizerPass();

        TListMaterializedViewRefreshJobsResult result =
                MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

        Assertions.assertEquals(1, result.getJobs().size());
        TMaterializedViewRefreshJobInfo job = result.getJobs().get(0);
        Assertions.assertEquals("taskrun-late", job.getFailed_task_run_id());
        Assertions.assertEquals("query-late", job.getFailed_query_id());
        Assertions.assertEquals("42", job.getError_code());
        Assertions.assertEquals("late failure", job.getError_message());
    }

    @Test
    public void testWallClockDurationTime(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) throws TException {
        TaskRunStatus run = newRun("job-duration", "run-duration", "SUCCESS", Constants.TaskRunState.SUCCESS, 10000L);
        run.setFinishTime(13500L);

        mockTaskRunStatus(globalStateMgr, taskManager, Lists.newArrayList(run));
        mockAuthorizerPass();

        TListMaterializedViewRefreshJobsResult result =
                MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

        // Wall-clock span = (finishTime - processStartTime) / 1000 = (13500 - 10000) / 1000.
        Assertions.assertEquals(1, result.getJobs().size());
        Assertions.assertEquals("3.500", result.getJobs().get(0).getDuration_time());
    }

    private void mockTaskRunStatus(GlobalStateMgr globalStateMgr, TaskManager taskManager, List<TaskRunStatus> runs) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getTaskManager();
                minTimes = 0;
                result = taskManager;

                taskManager.getMatchedTaskRunStatus((TGetTasksParams) any);
                result = runs;
            }
        };
    }

    private void mockAuthorizerPass() {
        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnOrInDb(ConnectContext context, String catalogName, String db)
                    throws AccessDeniedException {
            }
        };
    }

    private static TaskRunStatus newRun(String startTaskRunId, String queryId, String taskName,
                                        Constants.TaskRunState state, long processStartTime) {
        TaskRunStatus run = new TaskRunStatus();
        run.setStartTaskRunId(startTaskRunId);
        run.setQueryId(queryId);
        run.setTaskName(taskName);
        run.setDbName("db1");
        run.setSource(Constants.TaskSource.MV);
        run.setState(state);
        run.setProcessStartTime(processStartTime);
        Map<String, String> props = new HashMap<>();
        props.put(TaskRun.MV_ID, "100");
        run.setProperties(props);
        return run;
    }
}
