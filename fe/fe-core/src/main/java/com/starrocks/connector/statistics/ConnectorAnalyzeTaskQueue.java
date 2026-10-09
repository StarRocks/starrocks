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

package com.starrocks.connector.statistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConnectorAnalyzeTaskQueue {
    private static final Logger LOG = LogManager.getLogger(ConnectorAnalyzeTaskQueue.class);
    // rwLock is used to protect pendingTasks
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock wLock = rwLock.writeLock();
    private final ReentrantReadWriteLock.ReadLock rLock = rwLock.readLock();

    // use LinkedHashMap to keep the order of pending tasks
    private final Map<String, ConnectorAnalyzeTask> pendingTasks = Maps.newLinkedHashMap();
    private final Map<String, ConnectorAnalyzeTask> runningTasks = Maps.newConcurrentMap();

    // Use abort policy instead of the blocked policy, submitting is done with wLock held and must not block.
    // A rejected task is kept in pendingTasks and retried in the next schedule round.
    private final Executor taskRunPool;

    public ConnectorAnalyzeTaskQueue() {
        this(ThreadPoolManager.newDaemonFixedThreadPoolWithAbortPolicy(
                Math.max(Config.connector_table_query_trigger_analyze_max_running_task_num, 1),
                Math.max(Config.connector_table_query_trigger_analyze_max_running_task_num, 1),
                "connector-trigger-analyze-pool", true));
    }

    @VisibleForTesting
    ConnectorAnalyzeTaskQueue(Executor taskRunPool) {
        this.taskRunPool = taskRunPool;
    }

    public boolean addPendingTask(String tableUUID, ConnectorAnalyzeTask task) {
        if (task == null) {
            return false;
        }

        wLock.lock();
        try {
            if (pendingTasks.size() >= Config.connector_table_query_trigger_analyze_max_pending_task_num) {
                LOG.warn("[ExternalStats] trigger drop | table_uuid={} reason=pending_queue_full queue_size={}",
                        tableUUID, pendingTasks.size());
                return false;
            }

            ConnectorAnalyzeTask runningTask = runningTasks.get(tableUUID);
            if (runningTask != null) {
                // there is a running task for this table, remove columns which are already in running task
                task.removeColumns(runningTask.getColumns());
                if (task.getColumns().isEmpty()) {
                    LOG.info("[ExternalStats] trigger skip | table_uuid={} reason=running_task", tableUUID);
                    return true;
                }
            }

            if (!pendingTasks.containsKey(tableUUID)) {
                pendingTasks.put(tableUUID, task);
            } else {
                pendingTasks.get(tableUUID).mergeTask(task);
            }
        } finally {
            wLock.unlock();
        }

        return true;
    }

    public int getPendingTaskSize() {
        rLock.lock();
        try {
            return pendingTasks.size();
        } finally {
            rLock.unlock();
        }
    }

    @VisibleForTesting
    int getRunningTaskSize() {
        return runningTasks.size();
    }

    public boolean isMaxRunningConcurrencyReached() {
        return runningTasks.size() >= Config.connector_table_query_trigger_analyze_max_running_task_num;
    }

    public void schedulePendingTask() {
        // do not dispatch task if max running concurrency reached or no pending task
        if (isMaxRunningConcurrencyReached()) {
            LOG.info("[ExternalStats] running limit | running={} limit={}",
                    runningTasks.size(), Config.connector_table_query_trigger_analyze_max_running_task_num);
            return;
        }

        wLock.lock();
        try {
            if (pendingTasks.isEmpty()) {
                return;
            }
            Iterator<Map.Entry<String, ConnectorAnalyzeTask>> iterator = pendingTasks.entrySet().iterator();
            while (iterator.hasNext() && !isMaxRunningConcurrencyReached()) {
                Map.Entry<String, ConnectorAnalyzeTask> entry = iterator.next();
                String tableUUID = entry.getKey();
                ConnectorAnalyzeTask task = entry.getValue();
                if (runningTasks.containsKey(tableUUID)) {
                    // keep the task pending until the running task of the same table finishes,
                    // otherwise the running entry would be overwritten and removed by the running task
                    continue;
                }
                // register before submitting, so that a fast finished task can not remove the entry before it is added
                runningTasks.put(tableUUID, task);
                try {
                    CompletableFuture.supplyAsync(task::run, taskRunPool).whenComplete((result, e) -> {
                        if (e != null) {
                            LOG.warn("[ExternalStats] trigger fail | table_uuid={} error={}", tableUUID, e.getMessage(), e);
                        }
                        runningTasks.remove(tableUUID, task);
                    });
                } catch (RejectedExecutionException e) {
                    // keep the task pending and retry in the next round
                    runningTasks.remove(tableUUID, task);
                    LOG.warn("[ExternalStats] trigger delay | table_uuid={} reason=pool_rejected running={}",
                            tableUUID, runningTasks.size());
                    break;
                }
                iterator.remove();
            }
        } finally {
            wLock.unlock();
        }

    }
}