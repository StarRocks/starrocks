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
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.scheduler.persist.TaskRunPeriodStatusChange;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class TaskRunStateSynchronizer extends LeaderDaemon {
    public static final Logger LOG = LogManager.getLogger(TaskRunStateSynchronizer.class);
    // taskId -> progress
    private Map<Long, Integer> runningTaskRunProgressMap;
    private TaskRunManager taskRunManager;

    public TaskRunStateSynchronizer() {
        super("TaskRunStateSynchronizer", FeConstants.SYNC_TASK_RUNS_STATE_INTERVAL);
        taskRunManager = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager();
        runningTaskRunProgressMap = new HashMap<>();
        for (Map.Entry<Long, TaskRun> entry : taskRunManager.getRunningTaskRunMap().entrySet()) {
            runningTaskRunProgressMap.put(entry.getKey(), entry.getValue().getStatus().getProgress());
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        Map<Long, TaskRun> runningTaskRunMapLatest = taskRunManager.getRunningTaskRunMap();
        Map<Long, Integer> jobProgressMap = new HashMap<>();
        for (Map.Entry<Long, TaskRun> item : runningTaskRunMapLatest.entrySet()) {

            int nowProgress = item.getValue().getStatus().getProgress();
            // nowProgress == 100 indicates that the job is finished
            // the progress will be updated by TaskRunStatusChange
            if (nowProgress == 100) {
                if (runningTaskRunProgressMap.containsKey(item.getKey())) {
                    runningTaskRunProgressMap.remove(item.getKey());
                }
                continue;
            }
            if (nowProgress == 0) {
                continue;
            }
            if (runningTaskRunProgressMap.containsKey(item.getKey())) {
                int preProgress = runningTaskRunProgressMap.get(item.getKey());
                if (preProgress != nowProgress) {
                    jobProgressMap.put(item.getKey(), nowProgress);
                }

            } else {
                jobProgressMap.put(item.getKey(), nowProgress);
            }
            runningTaskRunProgressMap.put(item.getKey(), nowProgress);

        }
        if (!jobProgressMap.isEmpty()) {
            GlobalStateMgr.getCurrentState().getEditLog().
                    logAlterRunningTaskRunProgress(new TaskRunPeriodStatusChange(jobProgressMap));
        }
    }
}
