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
import com.starrocks.catalog.BasicTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
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
    public void testRoutedToLeader() {
        // Reads leader-owned TaskManager run state + MV metadata (like task_runs / materialized_views), so a
        // follower FE must redirect the scan to the leader; otherwise pending/running refreshes or MV
        // enrichment could be missing or stale.
        Assertions.assertTrue(SystemTable.needQueryFromLeader(MaterializedViewRefreshJobsSystemTable.NAME));
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
        // A non-IMV job consumed no source ranges and pinned no snapshots: these columns are NULL, not "{}".
        Assertions.assertNull(job.getImv_source_version_range());
        Assertions.assertNull(job.getImv_source_timestamp_range());
        Assertions.assertNull(job.getImv_source_pinned_snapshot_id_map());
    }

    @Test
    public void testGroupingKeyedByStartTaskRunIdDropsRunsWithoutIt(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) throws TException {
        // JOB_ID is keyed strictly on startTaskRunId so it matches task_runs.JOB_ID; a run without one is
        // dropped rather than falling back to taskRunId (which would not drill down to task_runs).
        TaskRunStatus withStart = newRun("job-x", "query-x", "SUCCESS", Constants.TaskRunState.SUCCESS, 1000L);
        withStart.setTaskRunId("taskrun-x");
        TaskRunStatus noStart = newRun(null, "query-nostart", "SUCCESS", Constants.TaskRunState.SUCCESS, 2000L);
        noStart.setTaskRunId("taskrun-nostart");

        mockTaskRunStatus(globalStateMgr, taskManager, Lists.newArrayList(withStart, noStart));
        mockAuthorizerPass();

        TListMaterializedViewRefreshJobsResult result =
                MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

        Assertions.assertEquals(1, result.getJobs().size());
        Assertions.assertEquals("job-x", result.getJobs().get(0).getJob_id());
    }

    @Test
    public void testImvSourceVersionRangeUnionsDeltaRunsByCreateTime(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) throws TException {
        // Two incremental runs of one job record adjacent delta ranges for the same base table; the job-level
        // range must keep the earliest run's start and the latest run's end, not just the last run's slice.
        TaskRunStatus run1 = newRun("job-r", "query-r1", "SUCCESS", Constants.TaskRunState.SUCCESS, 1000L);
        run1.setCreateTime(1000L);
        run1.getMvTaskRunExtraMessage().setImvSourceVersionRange(Map.of("baseT", Map.of("start", "1", "end", "3")));
        TaskRunStatus run2 = newRun("job-r", "query-r2", "SUCCESS", Constants.TaskRunState.SUCCESS, 2000L);
        run2.setCreateTime(2000L);
        run2.getMvTaskRunExtraMessage().setImvSourceVersionRange(Map.of("baseT", Map.of("start", "3", "end", "7")));

        // Reverse-chronological input to prove the merge orders by createTime, not encounter order.
        mockTaskRunStatus(globalStateMgr, taskManager, Lists.newArrayList(run2, run1));
        mockAuthorizerPass();

        TListMaterializedViewRefreshJobsResult result =
                MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

        Assertions.assertEquals(1, result.getJobs().size());
        String versionRange = result.getJobs().get(0).getImv_source_version_range();
        Assertions.assertTrue(versionRange.contains("\"start\":\"1\""), versionRange);
        Assertions.assertTrue(versionRange.contains("\"end\":\"7\""), versionRange);
        Assertions.assertFalse(versionRange.contains("\"start\":\"3\""), versionRange);
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
    public void testManualRunReportsManualTrigger(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) throws TException {
        // REFRESH_TRIGGER is per-job: a manually-submitted run reports MANUAL from its ExecuteOption.isManual,
        // independent of the MV's configured scheme (here the MV is absent, which would otherwise be UNKNOWN).
        TaskRunStatus run = newRun("job-manual", "run-manual", "SUCCESS", Constants.TaskRunState.SUCCESS, 1000L);
        ExecuteOption manualOption = new ExecuteOption(0, false, new HashMap<>());
        manualOption.setManual(true);
        run.getMvTaskRunExtraMessage().setExecuteOption(manualOption);

        mockTaskRunStatus(globalStateMgr, taskManager, Lists.newArrayList(run));
        mockAuthorizerPass();

        TListMaterializedViewRefreshJobsResult result =
                MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

        Assertions.assertEquals(1, result.getJobs().size());
        Assertions.assertEquals("MANUAL", result.getJobs().get(0).getRefresh_trigger());
    }

    @Test
    public void testUnauthorizedMvIsFiltered(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager,
            @Mocked LocalMetastore localMetastore,
            @Mocked Database database,
            @Mocked MaterializedView mv) throws TException {
        // A live MV is authorized per object (like information_schema.materialized_views): a caller lacking
        // access to the MV must not see its job, else error/query-id/IMV details would leak.
        TaskRunStatus run = newRun("job-unauth", "run-unauth", "SUCCESS", Constants.TaskRunState.SUCCESS, 1000L);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getTaskManager();
                minTimes = 0;
                result = taskManager;

                taskManager.getMatchedTaskRunStatus((TGetTasksParams) any);
                result = Lists.newArrayList(run);

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;

                localMetastore.getDb("db1");
                minTimes = 0;
                result = database;

                localMetastore.getTable(anyLong, anyLong);
                minTimes = 0;
                result = mv;
            }
        };
        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnTableLikeObject(ConnectContext context, String dbName, BasicTable table)
                    throws AccessDeniedException {
                throw new AccessDeniedException("denied");
            }
        };

        TListMaterializedViewRefreshJobsResult result =
                MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

        Assertions.assertTrue(result.getJobs().isEmpty());
    }

    @Test
    public void testRunAsUserIsRootUnderRootBasedAuthorization(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) throws TException {
        // Root-based authorization runs every MV refresh as root (TaskRun.switchUser), so RUN_AS_USER reports
        // root even though the run still stores the creator identity.
        boolean original = Config.mv_use_creator_based_authorization;
        Config.mv_use_creator_based_authorization = false;
        try {
            TaskRunStatus run = newRun("job-rootauth", "run-rootauth", "SUCCESS", Constants.TaskRunState.SUCCESS, 1000L);
            run.setUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));

            mockTaskRunStatus(globalStateMgr, taskManager, Lists.newArrayList(run));
            mockAuthorizerPass();

            TListMaterializedViewRefreshJobsResult result =
                    MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

            Assertions.assertEquals(1, result.getJobs().size());
            Assertions.assertEquals(UserIdentity.ROOT.toString(), result.getJobs().get(0).getRun_as_user());
        } finally {
            Config.mv_use_creator_based_authorization = original;
        }
    }

    @Test
    public void testFailedRunColumnsComeFromLatestFailure(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) throws TException {
        TaskRunStatus olderFailure = newRun("job-failed", "query-old", "FAILED", Constants.TaskRunState.FAILED, 1000L);
        olderFailure.setCreateTime(1000L);
        olderFailure.setTaskRunId("taskrun-old");
        olderFailure.setErrorCode(1);
        olderFailure.setErrorMessage("old failure");

        // The latest failure has processStartTime 0 (a pending/legacy row): selecting by createTime picks it,
        // whereas selecting by processStartTime would wrongly rank it before olderFailure.
        TaskRunStatus latestFailure = newRun("job-failed", "query-latest", "FAILED", Constants.TaskRunState.FAILED, 0L);
        latestFailure.setCreateTime(2000L);
        latestFailure.setTaskRunId("taskrun-latest");
        latestFailure.setErrorCode(42);
        latestFailure.setErrorMessage("latest failure");

        mockTaskRunStatus(globalStateMgr, taskManager, Lists.newArrayList(latestFailure, olderFailure));
        mockAuthorizerPass();

        TListMaterializedViewRefreshJobsResult result =
                MaterializedViewRefreshJobsSystemTable.query(new TGetTasksParams(), new ConnectContext());

        Assertions.assertEquals(1, result.getJobs().size());
        TMaterializedViewRefreshJobInfo job = result.getJobs().get(0);
        Assertions.assertEquals("taskrun-latest", job.getFailed_task_run_id());
        Assertions.assertEquals("query-latest", job.getFailed_query_id());
        Assertions.assertEquals("42", job.getError_code());
        Assertions.assertEquals("latest failure", job.getError_message());
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
