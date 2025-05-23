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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TaskRunManagerTest {

    private static final int N = 100;
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.createDefaultCtx();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        globalStateMgr.getWarehouseMgr().addWarehouse(new DefaultWarehouse(1, "w1"));
        globalStateMgr.getWarehouseMgr().addWarehouse(new DefaultWarehouse(2, "w2"));
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync, int priority) {
        return makeExecuteOption(isMergeRedundant, isSync, priority, Maps.newHashMap());
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync, int priority,
                                                   Map<String, String> properties) {
        ExecuteOption executeOption = new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(), isMergeRedundant,
                properties);
        executeOption.setSync(isSync);
        executeOption.setPriority(priority);
        return executeOption;
    }

    private TaskRun makeTaskRun(long taskId, Task task, ExecuteOption executeOption) {
        return makeTaskRun(taskId, task, executeOption, -1);
    }

    private TaskRun makeTaskRun(long taskId, Task task, ExecuteOption executeOption, long createTime) {
        TaskRun taskRun = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(executeOption)
                .build();
        taskRun.setTaskId(taskId);
        // submitTaskRun needs task run status is empty
        if (createTime >= 0) {
            taskRun.initStatus("1", createTime);
            taskRun.getStatus().setPriority(executeOption.getPriority());
        }
        return taskRun;
    }

    @Test
    public void testKillTaskRun() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        List<TaskRun> taskRuns = Lists.newArrayList();
        long taskId = 100;

        TaskRunScheduler scheduler = new TaskRunScheduler();
        TaskRunManager taskRunManager = new TaskRunManager(scheduler);

        boolean[] forces = {false, true};
        for (boolean force : forces) {
            for (int i = 0; i < N; i++) {
                TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, 1));
                taskRuns.add(taskRun);
                scheduler.addPendingTaskRun(taskRun);
            }

            scheduler.scheduledPendingTaskRun(taskRun -> {
                Assert.assertTrue(taskRun.getTaskId() == taskId);
            });

            Assert.assertTrue(scheduler.getRunningTaskRun(taskId) != null);
            Assert.assertTrue(scheduler.getRunnableTaskRun(taskId) != null);
            Assert.assertTrue(scheduler.getPendingTaskRunsByTaskId(taskId).size() == N - 1);

            // no matter whether force is true or not, we always clear running and pending task run
            taskRunManager.killTaskRun(taskId, force);

            System.out.println("force:" + force);
            Assert.assertTrue(CollectionUtils.isEmpty(scheduler.getPendingTaskRunsByTaskId(taskId)));
            if (force) {
                Assert.assertTrue(scheduler.getRunningTaskRun(taskId) == null);
            } else {
                Assert.assertTrue(scheduler.getRunningTaskRun(taskId) != null);
                scheduler.removeRunningTask(taskId);
            }
        }
    }

    private Map<String, String> makeTaskRunProperties(String partitionStart,
                                                      String partitionEnd,
                                                      boolean isForce) {
        Map<String, String> result = Maps.newHashMap();
        result.put(TaskRun.PARTITION_START, partitionStart);
        result.put(TaskRun.PARTITION_END, partitionEnd);
        result.put(TaskRun.FORCE, String.valueOf(isForce));
        return result;
    }

    private Map<String, String> makeMVTaskRunProperties(String partitionStart,
                                                      String partitionEnd,
                                                      boolean isForce) {
        Map<String, String> result = Maps.newHashMap();
        result.put(TaskRun.PARTITION_START, partitionStart);
        result.put(TaskRun.PARTITION_END, partitionEnd);
        result.put(TaskRun.MV_ID, "1");
        result.put(TaskRun.FORCE, String.valueOf(isForce));
        return result;
    }

    @Test
    public void testExecutionOption() {
        {
            ExecuteOption option1 = makeExecuteOption(true, false, 1);
            ExecuteOption option2 = makeExecuteOption(true, false, 10);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
        {
            ExecuteOption option1 = makeExecuteOption(true, false, 1);
            ExecuteOption option2 = makeExecuteOption(false, false, 10);
            Assert.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeTaskRunProperties("2023-01-01", "2023-01-02", true);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", true);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            prop1.put("a", "a");
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop11 = option1.getTaskRunComparableProperties();
            Assert.assertTrue(prop11.size() == 4);

            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            prop2.put("a", "b");
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
    }
}
