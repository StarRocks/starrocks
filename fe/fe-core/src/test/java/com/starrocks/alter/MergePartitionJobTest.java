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

package com.starrocks.alter;

import com.google.common.collect.Multimap;
import com.starrocks.alter.AlterCancelException;
import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Config;
import com.starrocks.persist.OperationType;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MergePartitionJobTest extends DDLTestBase {

    @BeforeAll
    public static void beforeAll() {
        DDLTestBase.beforeAll();
        UtFrameUtils.setUpForPersistTest();
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        // Only the tests that read the journal drain the follower queue, so start every test from an empty one.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
    }

    @Test
    public void testTempPartitionNotVisibleRaisesFailure() throws Exception {
        String tempPartitionName = "tmp_p1";
        MergePartitionJob job = new MergePartitionJob(1L, 2L, 3L, "tbl", 1000L) {
            @Override
            protected boolean hasCommittedNotVisible(long partitionId) {
                return partitionId == 100L;
            }
        };

        Field nameMappingField = MergePartitionJob.class.getDeclaredField("tempPartitionNameToSourcePartitionNames");
        nameMappingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Multimap<String, String> nameMapping = (Multimap<String, String>) nameMappingField.get(job);
        nameMapping.put(tempPartitionName, "p1");

        OptimizeTask task = new OptimizeTask("task-1");
        task.setTempPartitionName(tempPartitionName);
        task.setPartitionName("p1");
        task.setLastVersion(0L);
        task.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        job.getOptimizeTasks().add(task);

        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getId()).thenReturn(2L);

        OlapTable table = Mockito.mock(OlapTable.class);
        Partition tempPartition = Mockito.mock(Partition.class);
        Mockito.when(tempPartition.getId()).thenReturn(100L);
        Mockito.when(tempPartition.getName()).thenReturn(tempPartitionName);
        Mockito.when(table.getPartition(tempPartitionName, true)).thenReturn(tempPartition);
        Mockito.doNothing().when(table).dropTempPartition(tempPartitionName, true);
        Mockito.when(table.getPartition("p1")).thenReturn(null);

        Method onFinished = MergePartitionJob.class.getDeclaredMethod("onFinished", Database.class, OlapTable.class);
        onFinished.setAccessible(true);

        AlterCancelException ex = Assertions.assertThrows(AlterCancelException.class, () -> {
            try {
                onFinished.invoke(job, db, table);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof AlterCancelException) {
                    throw (AlterCancelException) cause;
                }
                throw new RuntimeException(cause);
            }
        });
        Assertions.assertTrue(ex.getMessage().contains("rewrite failed"));

        Mockito.verify(table).dropTempPartition(tempPartitionName, true);
        Assertions.assertEquals(Constants.TaskRunState.FAILED, task.getOptimizeTaskState());
        Assertions.assertTrue(nameMapping.isEmpty());
    }

    // ---- expiry of the rewrite tasks registered in TaskManager ------------------------------------

    @Test
    public void testRewriteTasksOutliveJobDeadline() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_task_expiry");
        MergePartitionJob job = submitMerge(table);
        runToRunning(job);
        List<String> taskNames = rewriteTaskNames(job);
        Assertions.assertEquals(1, taskNames.size());

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        taskManager.removeExpiredTasks();
        long deadline = job.createTimeMs + job.getTimeoutMs();
        Map<String, Long> expireTimes = new HashMap<>();
        for (String taskName : taskNames) {
            Task task = taskManager.getTask(taskName);
            Assertions.assertNotNull(task, taskName);
            Assertions.assertTrue(task.getExpireTime() >= deadline + Config.task_ttl_second * 1000L,
                    taskName + " expires at " + task.getExpireTime() + ", less than a task TTL after the job deadline "
                            + deadline);
            expireTimes.put(taskName, task.getExpireTime());
        }
        assertExpireTimesPersisted(expireTimes);
    }

    @Test
    public void testExpiredRewriteTasksAreRemoved() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_task_expired");
        MergePartitionJob job = submitMerge(table);
        try (MockedStatic<TaskBuilder> ignored = backdateRewriteTasks(job.getTimeoutMs())) {
            runToRunning(job);
        }
        List<String> taskNames = rewriteTaskNames(job);
        Assertions.assertEquals(1, taskNames.size());
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        for (String taskName : taskNames) {
            Assertions.assertNotNull(taskManager.getTask(taskName), taskName);
        }

        taskManager.removeExpiredTasks();

        for (String taskName : taskNames) {
            Assertions.assertNull(taskManager.getTask(taskName), taskName);
        }
    }

    private static List<String> rewriteTaskNames(MergePartitionJob job) {
        return job.getOptimizeTasks().stream().map(Task::getName).collect(Collectors.toList());
    }

    /**
     * Makes the rewrite tasks built in the calling thread look as if they were created so long ago that the job
     * timeout and the task TTL have both elapsed since.
     */
    private static MockedStatic<TaskBuilder> backdateRewriteTasks(long timeoutMs) {
        long ageMs = timeoutMs + Config.task_ttl_second * 1000L + 1000L;
        return Mockito.mockStatic(TaskBuilder.class, invocation -> {
            Object result = invocation.callRealMethod();
            if (result instanceof OptimizeTask) {
                OptimizeTask task = (OptimizeTask) result;
                task.setCreateTime(task.getCreateTime() - ageMs);
            }
            return result;
        });
    }

    /**
     * A follower gets the expiry of each task from the task creation journal entry, and keeps it across an image
     * save and load.
     */
    private static void assertExpireTimesPersisted(Map<String, Long> expireTimes) throws Exception {
        TaskManager follower = new TaskManager();
        for (int i = 0; i < expireTimes.size(); i++) {
            follower.replayCreateTask(
                    (Task) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_TASK));
        }
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        follower.saveTasksV2(image.getImageWriter());
        TaskManager restored = new TaskManager();
        restored.loadTasksV2(image.getMetaBlockReader());

        for (Map.Entry<String, Long> entry : expireTimes.entrySet()) {
            for (TaskManager taskManager : List.of(follower, restored)) {
                Task task = taskManager.getTask(entry.getKey());
                Assertions.assertNotNull(task, entry.getKey());
                Assertions.assertEquals(entry.getValue(), task.getExpireTime(), entry.getKey());
            }
        }
    }

    /**
     * Creates a table with three daily partitions, which a merge into monthly partitions turns into one.
     */
    private static OlapTable createDailyPartitionedTable(String tableName) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + tableName + " (k DATE, v INT) DUPLICATE KEY(k) " +
                "PARTITION BY date_trunc('day', k) (START ('2020-02-01') END ('2020-02-04') EVERY (INTERVAL 1 DAY)) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        return (OlapTable) starRocksAssert.getTable(GlobalStateMgrTestUtil.testDb1, tableName);
    }

    /**
     * Submits the merge through a handler whose daemon is never started, so only the test runs the job. The returned
     * job sees every earlier load as finished and every load into its temp partition as visible.
     */
    private static MergePartitionJob submitMerge(OlapTable table) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE " + table.getName() + " PARTITION BY date_trunc('month', k)", starRocksAssert.getCtx());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        SchemaChangeHandler handler = new SchemaChangeHandler();
        handler.process(stmt.getAlterClauseList(), db, table);
        Assertions.assertEquals(1, handler.getAlterJobsV2().size());
        MergePartitionJob job = Mockito.spy((MergePartitionJob) handler.getAlterJobsV2().values().iterator().next());
        Mockito.doReturn(true).when(job).isPreviousLoadFinished();
        Mockito.doReturn(false).when(job).hasCommittedNotVisible(Mockito.anyLong());
        return job;
    }

    private static void runToRunning(MergePartitionJob job) throws Exception {
        job.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState());
        job.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());
    }
}
