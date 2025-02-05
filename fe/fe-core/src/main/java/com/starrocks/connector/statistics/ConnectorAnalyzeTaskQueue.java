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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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

    private final ExecutorService taskRunPool = ThreadPoolManager.newDaemonFixedThreadPool(
            Config.connector_table_query_trigger_analyze_max_running_task_num,
            Config.connector_table_query_trigger_analyze_max_running_task_num,
            "connector-trigger-analyze-pool", true);

    public boolean addPendingTask(String tableUUID, ConnectorAnalyzeTask task) {
        if (task == null) {
            return false;
        }

        wLock.lock();
        try {
            if (pendingTasks.size() >= Config.connector_table_query_trigger_analyze_max_pending_task_num) {
                LOG.warn("Connector Analyze TaskQueue pending task num reach limit: {}, current: {}",
                        Config.connector_table_query_trigger_analyze_max_pending_task_num, pendingTasks.size());
                return false;
            }

            ConnectorAnalyzeTask runningTask = runningTasks.get(tableUUID);
            if (runningTask != null) {
                // there is a running task for this table, remove columns which are already in running task
                task.removeColumns(runningTask.getColumns());
                if (task.getColumns().isEmpty()) {
                    LOG.info("Table {} has a running task, skip this task", tableUUID);
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

    public boolean isMaxRunningConcurrencyReached() {
        return runningTasks.size() >= Config.connector_table_query_trigger_analyze_max_running_task_num;
    }

    public void schedulePendingTask() {
        // do not dispatch task if max running concurrency reached or no pending task
        if (isMaxRunningConcurrencyReached()) {
            LOG.info("Connector Analyze TaskQueue running task num reach limit: {}, current: {}",
                    Config.connector_table_query_trigger_analyze_max_running_task_num, runningTasks.size());
            return;
        }

        wLock.lock();
        try {
            if (pendingTasks.isEmpty()) {
                return;
            }
            List<String> removePendingTasks = Lists.newArrayList();
            for (Map.Entry<String, ConnectorAnalyzeTask> entry : pendingTasks.entrySet()) {
                ConnectorAnalyzeTask task = entry.getValue();
                removePendingTasks.add(entry.getKey());
                runningTasks.put(entry.getKey(), task);
                CompletableFuture.supplyAsync(task::run, taskRunPool).whenComplete((result, e) -> {
                    if (e != null) {
                        LOG.warn("Table {} analyze task failed", entry.getKey(), e);
                    }
                    runningTasks.remove(entry.getKey());
                });
                if (isMaxRunningConcurrencyReached()) {
                    break;
                }
            }
            removePendingTasks.forEach(pendingTasks::remove);
        } finally {
            wLock.unlock();
        }

    }
}