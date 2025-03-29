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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

/**
 * A thread safe FIFO queue for pending task runs, it uses an ordered set and hash map to schedule task runs.
 * - ordered set is used to sort task runs by priority and created time.
 * - hash map is used to store task runs by task id.
 */
public class TaskRunFIFOQueue {
    private static final Logger LOG = LogManager.getLogger(TaskRunFIFOQueue.class);

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock wLock = rwLock.writeLock();
    private final ReentrantReadWriteLock.ReadLock rLock = rwLock.readLock();

    // <id, task runs> map which partitioned by task id.
    @SerializedName("pendingTaskRunMap")
    private final Map<Long, Set<TaskRun>> gIdToTaskRunsMap = Maps.newHashMap();

    // task runs' global FIFO queue, sort by priority and created time
    @SerializedName("pendingTaskRunQueue")
    private final TreeSet<TaskRun> gTaskRunQueue = new TreeSet<>();

    /**
     * Get the count of pending task run
     */
    public long size() {
        rLock.lock();
        try {
            return gTaskRunQueue.size();
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Whether the pending queue is empty or not.
     */
    public boolean isEmpty() {
        rLock.lock();
        try {
            return gTaskRunQueue.isEmpty();
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Get the pending task run queue
     */
    public List<TaskRun> getCopiedPendingTaskRuns() {
        rLock.lock();
        try {
            return ImmutableList.copyOf(gTaskRunQueue);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * @param taskId: task id
     * @return: pending task run queue
     */
    public Set<TaskRun> getByTaskId(long taskId) {
        rLock.lock();
        try {
            return gIdToTaskRunsMap.get(taskId);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Add a task run to pending queue
     * @param taskRun: task run
     * @return: true if add success, false if add failed
     */
    public boolean add(TaskRun taskRun) {
        if (taskRun == null) {
            return false;
        }
        wLock.lock();
        try {
            if (!gTaskRunQueue.add(taskRun)) {
                return false;
            }
            Set<TaskRun> taskRuns = gIdToTaskRunsMap.computeIfAbsent(taskRun.getTaskId(),
                    ignored -> Sets.newConcurrentHashSet());
            if (!taskRuns.add(taskRun)) {
                gTaskRunQueue.remove(taskRun);
                return false;
            }
        } finally {
            wLock.unlock();
        }
        return true;
    }

    /**
     * Remove a specific task run from the queue.
     * @param taskRun: task run to remove
     * @param state: complete or cancel task run and set it with the state if the task run's future is not null
     */
    public boolean remove(TaskRun taskRun, Constants.TaskRunState state) {
        if (taskRun == null) {
            return false;
        }

        wLock.lock();
        try {
            CompletableFuture<Constants.TaskRunState> future = taskRun.getFuture();
            // make sure the future is canceled or completed
            if (future != null) {
                if (state != null && state.isSuccessState()) {
                    boolean isComplete = future.complete(state);
                    if (!isComplete) {
                        LOG.warn("fail to complete scheduler for task [{}]", taskRun);
                    }
                } else {
                    boolean isCancel = future.cancel(true);
                    if (!isCancel) {
                        LOG.warn("fail to cancel scheduler for task [{}]", taskRun);
                    }
                }
            }
            // remove it from pending map.
            removeFromMapUnlock(taskRun);
            // remove it from pending queue.
            if (!gTaskRunQueue.remove(taskRun)) {
                LOG.warn("remove pending task run from queue failed: {}", taskRun);
            }
        } finally {
            wLock.unlock();
        }

        return true;
    }

    /**
     * Remove a specific task run from the <id, task run> map.
     */
    private boolean removeFromMapUnlock(TaskRun taskRun) {
        Set<TaskRun> taskRuns = gIdToTaskRunsMap.get(taskRun.getTaskId());
        if (taskRuns == null || taskRuns.isEmpty()) {
            LOG.warn("poll task run from pending queue failed: task run queue is null or empty!!!");
            return false;
        }
        taskRuns.remove(taskRun);
        if (taskRuns.isEmpty()) {
            gIdToTaskRunsMap.remove(taskRun.getTaskId());
        }
        return true;
    }

    /**
     * Remove all pending task runs for a specific task id.
     * @param taskId: task id to remove
     */
    public void remove(long taskId) {
        wLock.lock();
        try {
            Set<TaskRun> taskRunSet = getByTaskId(taskId);
            if (taskRunSet == null || taskRunSet.isEmpty()) {
                return;
            }

            // remove all task run from pending queue
            Iterator<TaskRun> iter = taskRunSet.iterator();
            while (iter.hasNext()) {
                TaskRun taskRun = iter.next();

                // make sure future is canceled.
                CompletableFuture<?> future = taskRun.getFuture();
                if (!future.cancel(true)) {
                    LOG.warn("fail to cancel scheduler for task [{}]", taskRun);
                }
                // remove it from pending map
                iter.remove();
                // remove it from pending queue
                if (!gTaskRunQueue.remove(taskRun)) {
                    LOG.warn("remove pending task run from queue failed: {}", taskRun);
                }
            }
            // remove from pending map
            gIdToTaskRunsMap.remove(taskId);
        } finally {
            wLock.unlock();
        }
    }

    /**
     * Poll a task run from the queue by a predicate iterating by the default order.
     * @param predicate: predicate to filter task run
     * @return: task run if found, null if not found
     */
    public TaskRun poll(Predicate<TaskRun> predicate) {
        if (isEmpty()) {
            return null;
        }
        wLock.lock();
        try {
            Iterator<TaskRun> iter = gTaskRunQueue.iterator();
            while (iter.hasNext()) {
                TaskRun taskRun = iter.next();
                if (!predicate.test(taskRun)) {
                    continue;
                }
                // remove it from queue
                iter.remove();
                // remove it from map
                removeFromMapUnlock(taskRun);
                return taskRun;
            }
        } finally {
            wLock.unlock();
        }
        return null;
    }

    public Map<Long, Long> getTaskCount() {
        Map<Long, Long> result = new HashMap<>();
        rLock.lock();
        try {
            for (Set<TaskRun> taskRuns : gIdToTaskRunsMap.values()) {
                for (TaskRun taskRun : taskRuns) {
                    result.compute(taskRun.getRunCtx().getCurrentWarehouseId(),
                            (key, value) -> value == null ? 1 : value + 1);
                }
            }
            return result;
        } finally {
            rLock.unlock();
        }
    }
}
