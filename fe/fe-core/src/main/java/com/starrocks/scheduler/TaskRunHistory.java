// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.google.common.collect.Queues;
import com.starrocks.common.Config;
import com.starrocks.scheduler.persist.TaskRunStatus;

import java.util.Deque;

public class TaskRunHistory {

    private final Deque<TaskRunStatus> historyDeque = Queues.newLinkedBlockingDeque();

    public void addHistory(TaskRunStatus status) {
        historyDeque.addFirst(status);
    }

    public Deque<TaskRunStatus> getAllHistory() {
        return historyDeque;
    }

    public void forceGC() {
        if (historyDeque.size() >= Config.task_runs_max_history_number) {
            int startIndex = historyDeque.size() - Config.task_runs_max_history_number;
            int currentIndex = 0;
            for (TaskRunStatus taskRunStatus : historyDeque) {
                if (currentIndex >= startIndex) {
                    break;
                }
                historyDeque.remove(taskRunStatus);
                currentIndex++;
            }
        }
    }

    public long getTaskRunCount() {
        return historyTaskRunMap.size();
    }
}
