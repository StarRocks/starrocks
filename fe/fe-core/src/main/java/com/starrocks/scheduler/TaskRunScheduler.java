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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Schedule pending task runs to running task runs, it uses priority queue to schedule task runs.
 * The task run with higher priority or older created time will be scheduled first.
 */
public class TaskRunScheduler {
    private static final Logger LOG = LogManager.getLogger(TaskRunScheduler.class);

    // pending task run queue, it will schedule in fifo mode to ensure the task run scheduled in priority order.
    @SerializedName("pendingTaskRunQueue")
    private final TaskRunFIFOQueue pendingTaskRunQueue = new TaskRunFIFOQueue();

    // taskId -> running TaskRun, for each Task only support 1 running taskRun currently,
    // so the map value is not queue
    @SerializedName("runningTaskRunMap")
    private final Map<Long, TaskRun> runningTaskRunMap = Maps.newConcurrentMap();

    @SerializedName("runningSyncTaskRunMap")
    private final Map<Long, TaskRun> runningSyncTaskRunMap = Maps.newConcurrentMap();

    ////////// pending task run map //////////
    /**
     * Get the count of pending task run
     */
    public long getPendingQueueCount() {
        return pendingTaskRunQueue.size();
    }

    /**
     * Get the pending task run queue
     */
    public List<TaskRun> getCopiedPendingTaskRuns() {
        return pendingTaskRunQueue.getCopiedPendingTaskRuns();
    }

    /**
     * @param taskId: task id
     * @return: pending task run queue
     */
    public Set<TaskRun> getPendingTaskRunsByTaskId(long taskId) {
        return pendingTaskRunQueue.getByTaskId(taskId);
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
        return pendingTaskRunQueue.add(taskRun);
    }

    public void removePendingTaskRun(TaskRun taskRun, Constants.TaskRunState state) {
        if (taskRun == null) {
            return;
        }
        LOG.debug("remove pending task run: {}", taskRun);
        pendingTaskRunQueue.remove(taskRun, state);
    }

    public void removePendingTask(Task task) {
        if (task == null) {
            return;
        }
        LOG.debug("remove pending task: {}", task);
        pendingTaskRunQueue.remove(task.getId());
    }

    public TaskRun getTaskRunByQueryId(Long taskId, String queryId) {
        if (taskId == null || queryId == null) {
            return null;
        }
        Collection<TaskRun> taskRunQueue = pendingTaskRunQueue.getByTaskId(taskId);
        if (taskRunQueue == null) {
            return null;
        }
        return taskRunQueue.stream()
                .filter(taskRun -> queryId.equals(taskRun.getStatus().getQueryId()))
                .findFirst()
                .orElse(null);
    }

    boolean canTaskRunBeScheduled(TaskRun taskRun) {
        // if the task is running, it can't be scheduled
        return !runningTaskRunMap.containsKey(taskRun.getTaskId());
    }

    /**
     * schedule the pending TaskRun that can be run into running TaskRun map
     * @param action: the action to run the task run before task runs queue is full
     */
    public void scheduledPendingTaskRun(Consumer<TaskRun> action) {
        int currentRunning = runningTaskRunMap.size();
        if (currentRunning >= Config.task_runs_concurrency) {
            return;
        }
        while (!pendingTaskRunQueue.isEmpty()) {
            if (currentRunning >= Config.task_runs_concurrency) {
                break;
            }
            TaskRun taskRun = pendingTaskRunQueue.poll(this::canTaskRunBeScheduled);
            if (taskRun == null) {
                break;
            }
            // do schedule action
            action.accept(taskRun);
            // put it into running task run map
            runningTaskRunMap.put(taskRun.getTaskId(), taskRun);
            currentRunning += 1;
        }
    }

    public long getTaskIdPendingTaskRunCount(long taskId) {
        Set<TaskRun> pendingTaskRuns = getPendingTaskRunsByTaskId(taskId);
        return  pendingTaskRuns == null ? 0L : pendingTaskRuns.size();
    }

    //////////// running task run map ////////////

    public void addRunningTaskRun(TaskRun taskRun) {
        if (taskRun == null) {
            return;
        }
        runningTaskRunMap.put(taskRun.getTaskId(), taskRun);
    }

    public Set<Long> getCopiedRunningTaskIds() {
        return ImmutableSet.copyOf(runningTaskRunMap.keySet());
    }

    public TaskRun removeRunningTask(long taskId) {
        return runningTaskRunMap.remove(taskId);
    }

    public Set<TaskRun> getCopiedRunningTaskRuns() {
        return ImmutableSet.copyOf(runningTaskRunMap.values());
    }

    public boolean isTaskRunning(long taskId) {
        return runningTaskRunMap.containsKey(taskId);
    }

    public long getRunningTaskCount() {
        return runningTaskRunMap.size();
    }

    /**
     * Get the count of running task run
     * @param taskId: task id
     */
    public TaskRun getRunningTaskRun(long taskId) {
        return runningTaskRunMap.get(taskId);
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
        Set<TaskRun> queue = pendingTaskRunQueue.getByTaskId(taskId);
        if (queue != null) {
            return queue.stream().findFirst().orElse(null);
        }
        return null;
    }

    //////////// sync running task run map ////////////
    public void addSyncRunningTaskRun(TaskRun taskRun) {
        if (taskRun == null) {
            return;
        }
        runningSyncTaskRunMap.put(taskRun.getTaskId(), taskRun);
    }

    public TaskRun removeSyncRunningTaskRun(TaskRun taskRun) {
        if (taskRun == null) {
            return null;
        }
        return runningSyncTaskRunMap.remove(taskRun.getTaskId());
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public Map<Long, Long> getAllRunnableTaskCount() {
        Map<Long, Long> result = new HashMap<>();
        for (TaskRun taskRun : runningTaskRunMap.values()) {
            result.compute(taskRun.getRunCtx().getCurrentWarehouseId(),
                    (key, value) -> value == null ? 1L : value + 1);
        }
        for (TaskRun taskRun : runningSyncTaskRunMap.values()) {
            result.compute(taskRun.getRunCtx().getCurrentWarehouseId(),
                    (key, value) -> value == null ? 1L : value + 1);
        }

        Map<Long, Long> pending = pendingTaskRunQueue.getTaskCount();

        pending.forEach((key, value) -> result.merge(key, value, Long::sum));

        return result;
    }
}
