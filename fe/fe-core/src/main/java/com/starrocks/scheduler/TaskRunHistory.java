// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.google.common.collect.Queues;
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
}
