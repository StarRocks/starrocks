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

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

/**
 * Schedule pending task runs to running task runs, it uses priority queue to schedule task runs.
 * The task run with higher priority or older created time will be scheduled first.
 */
public class TaskRunScheduler {
    private static final Logger LOG = LogManager.getLogger(TaskRunScheduler.class);

    // TODO: Refactor this to find a better way to store the task runs.
    // taskId -> pending TaskRun Queue, for each Task only support 1 running taskRun currently,
    // so the map value is priority queue need to be sorted by priority from large to small
    private final Map<Long, Queue<TaskRun>> pendingTaskRunMap = Maps.newConcurrentMap();
    // pending TaskRun Queue, compared by priority and created time
    private final PriorityBlockingQueue<TaskRun> pendingTaskRunQueue = new PriorityBlockingQueue<>();

    // taskId -> running TaskRun, for each Task only support 1 running taskRun currently,
    // so the map value is not queue
    private final Map<Long, TaskRun> runningTaskRunMap = Maps.newConcurrentMap();

    /**
     * Get the count of pending task run
     */
    public long getPendingQueueCount() {
        return pendingTaskRunQueue.size();
    }

    /**
     * Get the count of running task run
     * @param taskId: task id
     */
    public TaskRun getRunningTaskRun(long taskId) {
        return runningTaskRunMap.get(taskId);
    }

    /**
     * @param taskId: task id
     * @return: pending task run queue
     */
    public Queue<TaskRun> getPendingTaskRunsByTaskId(long taskId) {
        return pendingTaskRunMap.get(taskId);
    }

    /**
     * Get the pending task run queue
     */
    public Queue<TaskRun> getPendingTaskRuns() {
        return pendingTaskRunQueue;
    }

    /**
     * Add a task run to pending queue
     * @param taskRun: task run
     * @return: true if add success, false if add failed
     */
    public boolean addPendingTaskRun(TaskRun taskRun) {
        if (taskRun == null) {
            return false;
        }
        if (!pendingTaskRunQueue.offer(taskRun)) {
            return false;
        }
        if (!pendingTaskRunMap.computeIfAbsent(taskRun.getTaskId(), ignored -> new PriorityBlockingQueue<>()).add(taskRun)) {
            pendingTaskRunQueue.remove(taskRun);
            return false;
        }
        return true;
    }

    /**
     * Remove a task run from pending queue
     * @param taskRun: task run
     * @return true if remove success, false if remove failed
     */
    public boolean removePendingTaskRunFromQueue(TaskRun taskRun) {
        if (taskRun == null) {
            return false;
        }
        return pendingTaskRunQueue.remove(taskRun);
    }

    /**
     * Return the current running task run map.
     */
    public Map<Long, TaskRun> getRunningTaskRunMap() {
        return runningTaskRunMap;
    }

    /**
     * Return the current pendign task run map.
     */
    public Map<Long, Queue<TaskRun>> getPendingTaskRunMap() {
        return pendingTaskRunMap;
    }

    /**
     * schedule the pending TaskRun that can be run into running TaskRun map
     * @param action: the action to run the task run before task runs queue is full
     */
    public void scheduledPendingTaskRun(Consumer<TaskRun> action) {
        int currentRunning = runningTaskRunMap.size();

        List<TaskRun> runningTaskRuns = new ArrayList<>();
        while (!pendingTaskRunQueue.isEmpty()) {
            if (currentRunning >= Config.task_runs_concurrency) {
                break;
            }

            TaskRun taskRun = pendingTaskRunQueue.poll();
            if (taskRun == null) {
                continue;
            }

            Long taskId = taskRun.getTaskId();
            if (runningTaskRunMap.containsKey(taskId)) {
                // add into pending queue after polling, no needs to change pendingTaskRunMap since it's not really removed
                runningTaskRuns.add(taskRun);
                continue;
            }

            // remove task run from pending task run map
            Queue<TaskRun> taskRunQueue = pendingTaskRunMap.get(taskId);
            if (taskRunQueue == null) {
                pendingTaskRunMap.remove(taskId);
            } else {
                TaskRun taskRunInMap = taskRunQueue.poll();
                if (!taskRun.equals(taskRunInMap)) {
                    LOG.warn("task run is not equal, taskRun: {}, taskRun in map: {}", taskRun, taskRunInMap);
                }
                // remove task run from pending queue map
                if (taskRunQueue.isEmpty()) {
                    pendingTaskRunMap.remove(taskId);
                }
            }

            action.accept(taskRun);

            // put it into running task run map
            runningTaskRunMap.put(taskId, taskRun);
            currentRunning++;
        }

        for (TaskRun taskRun : runningTaskRuns) {
            pendingTaskRunQueue.offer(taskRun);
        }
    }

    /**
     * Get the runnable task run by task id
     * @param taskId: task id
     * @return: the runnable task run, null if not found
     */
    public TaskRun getRunnableTaskRun(long taskId) {
        TaskRun res = runningTaskRunMap.get(taskId);
        if (res != null) {
            return res;
        }
        Queue<TaskRun> queue = pendingTaskRunMap.get(taskId);
        if (queue != null && !queue.isEmpty()) {
            return queue.peek();
        }
        return null;
    }

    @Override
    public String toString() {
        JsonObject res = new JsonObject();
        res.addProperty("running", GsonUtils.GSON.toJson(runningTaskRunMap));
        res.addProperty("pending_map", GsonUtils.GSON.toJson(pendingTaskRunMap));
        res.addProperty("pending_queue", GsonUtils.GSON.toJson(pendingTaskRunQueue));
        return res.toString();
    }
}
