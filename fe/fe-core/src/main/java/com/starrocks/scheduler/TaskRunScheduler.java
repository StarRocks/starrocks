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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
    @SerializedName("pendingTaskRunMap")
    private final Map<Long, Queue<TaskRun>> pendingTaskRunMap = Maps.newConcurrentMap();

    // pending TaskRun Queue, compared by priority and created time
    @SerializedName("pendingTaskRunQueue")
    private final Queue<TaskRun> pendingTaskRunQueue = new PriorityBlockingQueue<>();

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
        return ImmutableList.copyOf(pendingTaskRunQueue);
    }

    /**
     * @param taskId: task id
     * @return: pending task run queue
     */
    public Collection<TaskRun> getPendingTaskRunsByTaskId(long taskId) {
        Queue<TaskRun> pendingTaskRuns = pendingTaskRunMap.get(taskId);
        if (pendingTaskRuns == null) {
            return null;
        }
        return pendingTaskRuns;
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

    public void removePendingTaskRun(TaskRun taskRun) {
        if (taskRun == null) {
            return;
        }
        LOG.info("remove pending task run: {}", taskRun);

        if (taskRun.getStatus().getState() != Constants.TaskRunState.PENDING) {
            LOG.warn("task run is not in pending state: {}", taskRun);
        }

        synchronized (this) {
            if (!pendingTaskRunQueue.remove(taskRun)) {
                LOG.warn("remove pending task run from queue failed: {}", taskRun);
            }

            Queue<TaskRun> taskRunQueue = pendingTaskRunMap.get(taskRun.getTaskId());
            if (!taskRunQueue.remove(taskRun)) {
                LOG.warn("remove pending task run from pending map failed: {}", taskRun);
            }
            if (taskRunQueue.isEmpty()) {
                LOG.info("remove pending task run from pending map: {}", taskRun);
                pendingTaskRunMap.remove(taskRun.getTaskId());
            }
        }
        // make sure future is canceled.
        CompletableFuture<?> future = taskRun.getFuture();
        boolean isCancel = future.cancel(true);
        if (!isCancel) {
            LOG.warn("fail to cancel scheduler for task [{}]", taskRun);
        }
    }

    public void removePendingTask(Task task) {
        if (task == null) {
            return;
        }
        LOG.info("remove pending task: {}", task);
        Queue<TaskRun> taskRunQueue = pendingTaskRunMap.get(task.getId());
        if (taskRunQueue == null || taskRunQueue.isEmpty()) {
            return;
        }

        synchronized (this) {
            while (!taskRunQueue.isEmpty()) {
                TaskRun taskRun = taskRunQueue.poll();
                // make sure future is canceled.
                CompletableFuture<?> future = taskRun.getFuture();
                boolean isCancel = future.cancel(true);
                if (!isCancel) {
                    LOG.warn("fail to cancel scheduler for task [{}]", taskRun);
                }
                if (!pendingTaskRunQueue.remove(taskRun)) {
                    LOG.warn("remove pending task run from queue failed: {}", taskRun);
                }
            }
            pendingTaskRunMap.remove(task.getId());
        }
    }

    public TaskRun getTaskRunByQueryId(Long taskId, String queryId) {
        if (taskId == null || queryId == null) {
            return null;
        }
        Queue<TaskRun> taskRunQueue = pendingTaskRunMap.get(taskId);
        if (taskRunQueue == null) {
            return null;
        }
        return taskRunQueue.stream()
                .filter(taskRun -> queryId.equals(taskRun.getStatus().getQueryId()))
                .findFirst()
                .orElse(null);
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
            if (taskRunQueue == null || pendingTaskRunMap.isEmpty()) {
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

    public long getTaskIdPendingTaskRunCount(long taskId) {
        Collection<TaskRun> pendingTaskRuns = getPendingTaskRunsByTaskId(taskId);
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
        Queue<TaskRun> queue = pendingTaskRunMap.get(taskId);
        if (queue != null && !queue.isEmpty()) {
            return queue.peek();
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
}
