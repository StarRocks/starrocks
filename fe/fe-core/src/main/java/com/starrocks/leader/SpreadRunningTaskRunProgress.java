// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.leader;

import com.starrocks.common.FeConstants;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.scheduler.persist.RunningTaskRunProgressInfo;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class SpreadRunningTaskRunProgress extends LeaderDaemon {
    public static final Logger LOG = LogManager.getLogger(SpreadRunningTaskRunProgress.class);
    // taskId -> progress
    private Map<Long, Integer> runningTaskRunProgressMap;
    private TaskRunManager taskRunManager;

    public SpreadRunningTaskRunProgress() {
        super("SpreadRunningTaskRunProgress", FeConstants.default_spread_running_task_run_progress_ms);
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
                    logUpdateRunningTaskRunProgress(new RunningTaskRunProgressInfo(jobProgressMap));
        }
    }
}
