// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.google.common.collect.Queues;

import java.util.Deque;

public class TaskRunHistory {

    private final Deque<TaskRun> historyDeque = Queues.newLinkedBlockingDeque();

    public void addHistory(TaskRun pendingTaskRun) {
        historyDeque.addFirst(pendingTaskRun);
    }

    public Deque<TaskRun> getAllHistory() {
        return historyDeque;
    }
}
