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


package com.starrocks.leader;

import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.scheduler.persist.TaskRunPeriodStatusChange;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TaskRunStateSynchronizer extends FrontendDaemon {
    public static final Logger LOG = LogManager.getLogger(TaskRunStateSynchronizer.class);
    // taskId -> progress
    private Map<Long, Integer> runningTaskRunProgressMap;
    private TaskRunManager taskRunManager;
    private TaskRunScheduler taskRunScheduler;

    public TaskRunStateSynchronizer() {
        super("TaskRunStateSynchronizer", FeConstants.SYNC_TASK_RUNS_STATE_INTERVAL);
        taskRunManager = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager();
        runningTaskRunProgressMap = new HashMap<>();
        taskRunScheduler = taskRunManager.getTaskRunScheduler();

        Set<TaskRun> runningTaskRuns = taskRunScheduler.getCopiedRunningTaskRuns();
        for (TaskRun taskRun : runningTaskRuns) {
            runningTaskRunProgressMap.put(taskRun.getTaskId(), taskRun.getStatus().getProgress());
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        Set<TaskRun> runningTaskRuns = taskRunScheduler.getCopiedRunningTaskRuns();
        Map<Long, Integer> jobProgressMap = new HashMap<>();
        for (TaskRun taskRun : runningTaskRuns) {
            Long taskId = taskRun.getTaskId();
            int nowProgress = taskRun.getStatus().getProgress();
            // nowProgress == 100 indicates that the job is finished
            // the progress will be updated by TaskRunStatusChange
            if (nowProgress == 100) {
                if (runningTaskRunProgressMap.containsKey(taskId)) {
                    runningTaskRunProgressMap.remove(taskId);
                }
                continue;
            }
            if (nowProgress == 0) {
                continue;
            }
            if (runningTaskRunProgressMap.containsKey(taskId)) {
                int preProgress = runningTaskRunProgressMap.get(taskId);
                if (preProgress != nowProgress) {
                    jobProgressMap.put(taskId, nowProgress);
                }
            } else {
                jobProgressMap.put(taskId, nowProgress);
            }
            runningTaskRunProgressMap.put(taskId, nowProgress);

        }
        if (!jobProgressMap.isEmpty()) {
            GlobalStateMgr.getCurrentState().getEditLog().
                    logAlterRunningTaskRunProgress(new TaskRunPeriodStatusChange(jobProgressMap));
        }
    }
}
