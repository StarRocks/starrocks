// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

<<<<<<< HEAD
import com.google.common.collect.Queues;
import com.starrocks.scheduler.persist.TaskRunStatus;

import java.util.Deque;
=======
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.scheduler.persist.TaskRunStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
>>>>>>> 777734bfc ([Enhancement] Control the writing of metadata, for task_runs (#21710))

public class TaskRunHistory {

    private final Deque<TaskRunStatus> historyDeque = Queues.newLinkedBlockingDeque();

    public void addHistory(TaskRunStatus status) {
        historyDeque.addFirst(status);
    }

<<<<<<< HEAD
    public Deque<TaskRunStatus> getAllHistory() {
        return historyDeque;
=======
    public TaskRunStatus getTask(String queryId) {
        if (queryId == null) {
            return null;
        }
        return historyTaskRunMap.get(queryId);
    }

    public void removeTask(String queryId) {
        if (queryId == null) {
            return;
        }
        historyTaskRunMap.remove(queryId);
    }

    // Reserve historyTaskRunMap values to keep the last insert at the first.
    public List<TaskRunStatus> getAllHistory() {
        List<TaskRunStatus> historyRunStatus =
                new ArrayList<>(historyTaskRunMap.values());
        Collections.reverse(historyRunStatus);
        return historyRunStatus;
>>>>>>> 777734bfc ([Enhancement] Control the writing of metadata, for task_runs (#21710))
    }

    public void forceGC() {
        List<TaskRunStatus> allHistory = getAllHistory();
        int startIndex = Math.max(0, allHistory.size() - Config.task_runs_max_history_number);
        allHistory.subList(startIndex, allHistory.size())
                .forEach(taskRunStatus -> historyTaskRunMap.remove(taskRunStatus.getQueryId()));
    }
}
