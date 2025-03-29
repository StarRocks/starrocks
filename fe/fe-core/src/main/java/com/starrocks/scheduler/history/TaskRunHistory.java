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

package com.starrocks.scheduler.history;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.scheduler.persist.ArchiveTaskRunsLog;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetTasksParams;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TaskRunHistory {
    private static final Logger LOG = LogManager.getLogger(TaskRunHistory.class);
    private static final int MEMORY_TASK_RUN_SAMPLES = 10;

    // Thread-Safe history map:
    // QueryId -> TaskRunStatus
    // The same task-id may contain multi history task run status, so use query_id instead.
    private final Map<String, TaskRunStatus> historyTaskRunMap = Maps.newLinkedHashMap();
    // TaskName -> TaskRunStatus
    private final Map<String, TaskRunStatus> taskName2Status = Maps.newLinkedHashMap();

    private final TaskRunHistoryTable historyTable = new TaskRunHistoryTable();

    public synchronized void addHistory(TaskRunStatus status) {
        historyTaskRunMap.put(status.getQueryId(), status);
        taskName2Status.put(status.getTaskName(), status);
    }

    public synchronized TaskRunStatus getTaskByName(String taskName) {
        return taskName2Status.get(taskName);
    }

    public synchronized TaskRunStatus getTask(String queryId) {
        if (queryId == null) {
            return null;
        }
        return historyTaskRunMap.get(queryId);
    }

    public synchronized void removeTaskByQueryId(String queryId) {
        if (queryId == null) {
            return;
        }
        TaskRunStatus task = historyTaskRunMap.remove(queryId);
        if (task != null) {
            taskName2Status.remove(task.getTaskName());
        }
    }

    // Reserve historyTaskRunMap values to keep the last insert at the first.
    public synchronized List<TaskRunStatus> getInMemoryHistory() {
        List<TaskRunStatus> historyRunStatus = new ArrayList<>(historyTaskRunMap.values());
        Collections.reverse(historyRunStatus);
        return historyRunStatus;
    }

    public synchronized long getTaskRunCount() {
        return historyTaskRunMap.size();
    }

    public synchronized List<Object> getSamplesForMemoryTracker() {
        return historyTaskRunMap.values()
                .stream()
                .limit(MEMORY_TASK_RUN_SAMPLES)
                .collect(Collectors.toList());
    }

    public List<TaskRunStatus> lookupHistoryByTaskNames(String dbName, Set<String> taskNames) {
        List<TaskRunStatus> result = getInMemoryHistory().stream()
                .filter(x -> x.matchByTaskName(dbName, taskNames))
                .collect(Collectors.toList());
        if (isEnableArchiveHistory()) {
            result.addAll(historyTable.lookupByTaskNames(dbName, taskNames));
        }
        return result;
    }

    /**
     * Return the list of task runs belong to the LAST JOB:
     * Each task run has a `startTaskRunId` as JobId, a job may have multiple task runs.
     */
    public List<TaskRunStatus> lookupLastJobOfTasks(String dbName, Set<String> taskNames) {
        List<TaskRunStatus> result = getInMemoryHistory().stream()
                .filter(x -> x.matchByTaskName(dbName, taskNames))
                .collect(Collectors.toList());
        if (isEnableArchiveHistory()) {
            result.addAll(historyTable.lookupLastJobOfTasks(dbName, taskNames));
        }
        return result;
    }

    public List<TaskRunStatus> lookupHistory(TGetTasksParams params) {
        List<TaskRunStatus> result = getInMemoryHistory().stream()
                .filter(x -> x.match(params))
                .collect(Collectors.toList());
        if (isEnableArchiveHistory()) {
            result.addAll(historyTable.lookup(params));
        }
        return result;
    }

    // TODO: make it thread safe
    /**
     * Remove expired task runs, would be called in regular daemon thread, and also in checkpoint
     */
    public void vacuum() {
        removeExpiredRuns();

        // trigger to force gc to avoid too many history task runs.
        forceGC();

        // archive histories
        if (!GlobalStateMgr.isCheckpointThread()) {
            archiveHistory();
        }
    }

    public void replay(ArchiveTaskRunsLog log) {
        for (String queryId : ListUtils.emptyIfNull(log.getTaskRuns())) {
            removeTaskByQueryId(queryId);
        }
    }

    private boolean isEnableArchiveHistory() {
        return Config.enable_task_history_archive && !FeConstants.runningUnitTest;
    }

    /**
     * Remove expired history records, which is controlled by {@link Config.task_runs_ttl_second}
     */
    private void removeExpiredRuns() {
        if (isEnableArchiveHistory()) {
            return;
        }

        long currentTimeMs = System.currentTimeMillis();
        List<String> historyToDelete = Lists.newArrayList();
        List<TaskRunStatus> taskRunHistory = getInMemoryHistory();
        // only SUCCESS and FAILED in taskRunHistory
        Iterator<TaskRunStatus> iterator = taskRunHistory.iterator();
        while (iterator.hasNext()) {
            TaskRunStatus taskRunStatus = iterator.next();
            long expireTime = taskRunStatus.getExpireTime();
            if (currentTimeMs > expireTime) {
                historyToDelete.add(taskRunStatus.getQueryId());
                removeTaskByQueryId(taskRunStatus.getQueryId());
                iterator.remove();
            }
        }
        LOG.info("remove expired run history:{}", historyToDelete);
    }

    protected void archiveHistory() {
        if (!isEnableArchiveHistory()) {
            return;
        }

        List<TaskRunStatus> runs = getInMemoryHistory();
        runs.removeIf(x -> !x.getState().isFinishState());
        if (CollectionUtils.isEmpty(runs)) {
            return;
        }

        try {
            // 1. Persist into table
            Stopwatch watch = Stopwatch.createStarted();
            historyTable.addHistories(runs);

            // 2. Remove from memory
            for (var run : runs) {
                removeTaskByQueryId(run.getQueryId());
            }

            // 3. Write EditLog
            List<String> queryIdList = runs.stream().map(TaskRunStatus::getQueryId).collect(Collectors.toList());
            ArchiveTaskRunsLog log = new ArchiveTaskRunsLog(queryIdList);
            GlobalStateMgr.getCurrentState().getEditLog().logArchiveTaskRuns(log);
            LOG.info("archive task-run history, {} records took {}ms",
                    runs.size(), watch.elapsed(TimeUnit.MILLISECONDS));
        } catch (Throwable e) {
            LOG.warn("archive task-run history failed: ", e);
        }
    }

    /**
     * Keep only limited task runs to save memory usage
     */
    public void forceGC() {
        if (isEnableArchiveHistory()) {
            return;
        }

        List<TaskRunStatus> allHistory = getInMemoryHistory();
        int beforeSize = allHistory.size();
        LOG.info("try to trigger force gc, size before GC:{}, task_runs_max_history_number:{}.", beforeSize,
                Config.task_runs_max_history_number);
        if (beforeSize <= Config.task_runs_max_history_number) {
            return;
        }
        int startIndex = Math.min(allHistory.size(), Config.task_runs_max_history_number);
        // Creating a new list for elements to be removed to avoid ConcurrentModificationException
        List<String> idsToRemove = allHistory.subList(startIndex, beforeSize)
                .stream()
                .map(TaskRunStatus::getQueryId)
                .collect(Collectors.toList());

        // Now remove outside of iteration
        idsToRemove.forEach(this::removeTaskByQueryId);
        LOG.warn("Too much task metadata triggers forced task_run GC, " +
                "size before GC:{}, size after GC:{}.", beforeSize, getTaskRunCount());
    }
}
