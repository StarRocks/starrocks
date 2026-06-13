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

import com.starrocks.qe.ShowMaterializedViewStatus;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TMaterializedViewStatus;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ShowMaterializedViewStatusTest {

    @BeforeEach
    public void setUp() {
        // fromTaskRuns looks up the task owner via the global task manager; keep it offline in this unit test.
        TaskManager taskManager = new MockUp<TaskManager>() {
            @Mock
            public Task getTask(String taskName) {
                return null;
            }
        }.getMockInstance();
        MockUp<GlobalStateMgr> globalStateMgrMock = new MockUp<GlobalStateMgr>() {
            @Mock
            public TaskManager getTaskManager() {
                return taskManager;
            }
        };
        GlobalStateMgr mockGlobalStateMgr = globalStateMgrMock.getMockInstance();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return mockGlobalStateMgr;
            }
        };
    }

    private static TaskRunStatus mvTaskRun(String startTaskRunId, String taskRunId, long createTime,
                                           long processStartTime, long finishTime,
                                           Constants.TaskRunState state) {
        TaskRunStatus taskRun = new TaskRunStatus();
        taskRun.setTaskName("mvTask");
        taskRun.setSource(Constants.TaskSource.MV);
        taskRun.setStartTaskRunId(startTaskRunId);
        taskRun.setTaskRunId(taskRunId);
        taskRun.setCreateTime(createTime);
        taskRun.setProcessStartTime(processStartTime);
        taskRun.setFinishTime(finishTime);
        taskRun.setState(state);
        taskRun.setErrorCode(0);
        taskRun.setErrorMessage("");
        taskRun.setMvTaskRunExtraMessage(new MVTaskRunExtraMessage());
        return taskRun;
    }

    @Test
    public void fromTaskRunsRollsUpEarliestAndLatestRun() {
        // Out-of-order batch: the rollup must sort by processStartTime, so run "b" (earliest) is first
        // and run "c" (latest) is last regardless of list order.
        TaskRunStatus runB = mvTaskRun("job-1", "run-b", 1000L, 1000L, 1500L, Constants.TaskRunState.FAILED);
        TaskRunStatus runA = mvTaskRun("job-1", "run-a", 3000L, 3000L, 3700L, Constants.TaskRunState.FAILED);
        TaskRunStatus runC = mvTaskRun("job-1", "run-c", 5000L, 5000L, 5900L, Constants.TaskRunState.FAILED);

        List<TaskRunStatus> batch = Arrays.asList(runA, runC, runB);
        ShowMaterializedViewStatus.RefreshJobStatus status = ShowMaterializedViewStatus.fromTaskRuns(batch);

        Assertions.assertEquals(1000L, status.getMvRefreshStartTime());
        Assertions.assertEquals(1000L, status.getMvRefreshProcessTime());
        Assertions.assertEquals(runC.getStartTaskRunId(), status.getJobId());
        Assertions.assertEquals(runC.getLastRefreshState(), status.getRefreshState());

        Assertions.assertTrue(status.isRefreshFinished());
        Assertions.assertEquals(5900L, status.getMvRefreshEndTime());
        long expectedDuration = (1500L - 1000L) + (3700L - 3000L) + (5900L - 5000L);
        Assertions.assertEquals(expectedDuration, status.getTotalProcessDuration());
    }

    @Test
    public void fromTaskRunsDoesNotMutateInputOrder() {
        TaskRunStatus runLate = mvTaskRun("job-2", "run-late", 9000L, 9000L, 9500L, Constants.TaskRunState.FAILED);
        TaskRunStatus runEarly = mvTaskRun("job-2", "run-early", 1000L, 1000L, 1500L, Constants.TaskRunState.FAILED);

        List<TaskRunStatus> batch = Arrays.asList(runLate, runEarly);
        ShowMaterializedViewStatus.fromTaskRuns(batch);

        Assertions.assertSame(runLate, batch.get(0));
        Assertions.assertSame(runEarly, batch.get(1));
    }

    @Test
    public void fromTaskRunsReturnsEmptyStatusForEmptyOrNull() {
        ShowMaterializedViewStatus.RefreshJobStatus empty =
                ShowMaterializedViewStatus.fromTaskRuns(Collections.emptyList());
        Assertions.assertNotNull(empty);
        Assertions.assertFalse(empty.isRefreshFinished());
        Assertions.assertNull(empty.getJobId());

        ShowMaterializedViewStatus.RefreshJobStatus fromNull = ShowMaterializedViewStatus.fromTaskRuns(null);
        Assertions.assertNotNull(fromNull);
        Assertions.assertFalse(fromNull.isRefreshFinished());
        Assertions.assertNull(fromNull.getJobId());
    }

    @Test
    public void getRefreshJobStatusDelegatesToFromTaskRuns() {
        TaskRunStatus run = mvTaskRun("job-3", "run-1", 2000L, 2000L, 2600L, Constants.TaskRunState.FAILED);
        ShowMaterializedViewStatus viewStatus = new ShowMaterializedViewStatus(1L, "testDb", "testView");
        viewStatus.setLastJobTaskRunStatus(Collections.singletonList(run));

        ShowMaterializedViewStatus.RefreshJobStatus status = viewStatus.getRefreshJobStatus();

        Assertions.assertEquals(2000L, status.getMvRefreshStartTime());
        Assertions.assertEquals("job-3", status.getJobId());
        Assertions.assertTrue(status.isRefreshFinished());
        Assertions.assertEquals(2600L, status.getMvRefreshEndTime());
        Assertions.assertEquals(600L, status.getTotalProcessDuration());
    }

    @Test
    public void toThriftReturnsDefaultValuesWhenNoRefreshJobStatus() {
        ShowMaterializedViewStatus viewStatus = new ShowMaterializedViewStatus(1L, "testDb", "testView");

        TMaterializedViewStatus thriftStatus = viewStatus.toThrift();

        Assertions.assertEquals("1", thriftStatus.getId());
        Assertions.assertEquals("testDb", thriftStatus.getDatabase_name());
        Assertions.assertEquals("testView", thriftStatus.getName());
        Assertions.assertNull(thriftStatus.getRefresh_type());
        Assertions.assertEquals("false", thriftStatus.getIs_active());
        Assertions.assertNull(thriftStatus.getInactive_reason());
        Assertions.assertNull(thriftStatus.getPartition_type());
        Assertions.assertEquals("null", thriftStatus.getLast_refresh_state());
        Assertions.assertEquals("\\N", thriftStatus.getLast_refresh_start_time());
        Assertions.assertEquals("\\N", thriftStatus.getLast_refresh_process_time());
        Assertions.assertNull(thriftStatus.getLast_refresh_finished_time());
        Assertions.assertNull(thriftStatus.getLast_refresh_duration());
        Assertions.assertNull(thriftStatus.getLast_refresh_error_code());
        Assertions.assertNull(thriftStatus.getLast_refresh_error_message());
        Assertions.assertEquals("", thriftStatus.getExtra_message());
        Assertions.assertEquals("0", thriftStatus.getRows());
        Assertions.assertNull(thriftStatus.getQuery_rewrite_status());
        Assertions.assertNull(thriftStatus.getCreator());
    }

    @Test
    public void toResultSetReturnsEmptyFieldsWhenNoRefreshJobStatus() {
        ShowMaterializedViewStatus viewStatus = new ShowMaterializedViewStatus(1L, "testDb", "testView");

        List<String> resultSet = viewStatus.toResultSet();

        Assertions.assertEquals(35, resultSet.size());
        Assertions.assertEquals("", resultSet.get(3)); // refresh type
        Assertions.assertEquals("false", resultSet.get(4)); // is active
        Assertions.assertEquals("", resultSet.get(5)); // inactive reason
        Assertions.assertEquals("", resultSet.get(6)); // partition type
        Assertions.assertEquals("0", resultSet.get(7)); // task id
        Assertions.assertEquals("", resultSet.get(8)); // task name
        Assertions.assertEquals("\\N", resultSet.get(9)); // start time
        Assertions.assertEquals("\\N", resultSet.get(10)); // process finish time
        Assertions.assertEquals("0.000", resultSet.get(11)); // process duration
        Assertions.assertEquals("", resultSet.get(12)); // last refresh state
        Assertions.assertEquals("false", resultSet.get(13)); // force refresh
        Assertions.assertEquals("", resultSet.get(14)); // partition start
        Assertions.assertEquals("", resultSet.get(15)); // partition end
        Assertions.assertEquals("", resultSet.get(16)); // base partitions
        Assertions.assertEquals("", resultSet.get(17)); // mv partitions
        Assertions.assertEquals("", resultSet.get(18)); // error code
        Assertions.assertEquals("", resultSet.get(19)); // error message
        Assertions.assertEquals("0", resultSet.get(20)); // extra message
        Assertions.assertEquals("", resultSet.get(21)); // query rewrite status
        Assertions.assertEquals("", resultSet.get(22)); // owner
        Assertions.assertEquals("", resultSet.get(23)); // process start time
        Assertions.assertEquals("", resultSet.get(24)); // last refresh job id
        Assertions.assertEquals("", resultSet.get(27)); // last refresh time
        Assertions.assertEquals("", resultSet.get(28)); // warehouse
        Assertions.assertEquals("", resultSet.get(29)); // refresh mode
        Assertions.assertEquals("", resultSet.get(30)); // refresh trigger
        Assertions.assertEquals("", resultSet.get(31)); // refresh policy
        Assertions.assertEquals("", resultSet.get(32)); // resource group
        Assertions.assertEquals("", resultSet.get(33)); // query rewrite status reason
    }

    @Test
    public void toThriftIncludesLastRefreshTimeWhenPresent() {
        ShowMaterializedViewStatus viewStatus = new ShowMaterializedViewStatus(1L, "testDb", "testView");
        viewStatus.setLastRefreshTime(1735697100000L);

        TMaterializedViewStatus thriftStatus = viewStatus.toThrift();

        Assertions.assertEquals("2025-01-01 10:05:00", thriftStatus.getLast_refresh_time());
    }
}
