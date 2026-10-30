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
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.StatsConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
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

    // Tables whose last triggered analyze failed, and when they may be tried again.
    //
    // Nothing else holds a failing table back. A query enqueues an analyze when a column has no statistics
    // (ConnectorTableTriggerAnalyzeMgr#checkAndUpdateTableStats); a failed job commits no metadata, so the
    // statistics stay missing, so the next query enqueues it again, and the scheduler picks it up on the
    // next tick - every 30 seconds, indefinitely. The interval checks cannot intervene: they compare
    // against metadata that a failed job never wrote, and ConnectorAnalyzeTask#run explicitly ignores
    // FAILED statuses when deciding whether an analyze is already under way. When the failure is the
    // cluster being short of memory, that loop keeps piling work on at exactly the wrong moment.
    //
    // So back off, and lengthen the wait while the failures continue. A transient failure costs one extra
    // minute; a table that cannot be collected at all stops being retried every half minute. Cleared as
    // soon as one attempt succeeds.
    private final Map<String, FailureBackoff> failureBackoffs = Maps.newConcurrentMap();

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

        long cooldownRemainingMs = cooldownRemainingMs(tableUUID);
        if (cooldownRemainingMs > 0) {
            LOG.info("[ExternalStats] trigger skip | table_uuid={} reason=failure_backoff remaining_ms={} " +
                    "consecutive_failures={}", tableUUID, cooldownRemainingMs,
                    failureBackoffs.get(tableUUID).consecutiveFailures);
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
                        recordOutcome(tableUUID, result, e);
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

    // How long this table is still barred from being re-triggered, <= 0 when it is free to run.
    @VisibleForTesting
    long cooldownRemainingMs(String tableUUID) {
        if (Config.connector_table_query_trigger_analyze_failure_backoff_max_second <= 0) {
            return 0;
        }
        FailureBackoff backoff = failureBackoffs.get(tableUUID);
        return backoff == null ? 0 : backoff.retryAfterMs - System.currentTimeMillis();
    }

    // Lengthens this table's cooldown after a failed attempt, or clears it after one that worked. An
    // attempt that decided there was nothing to do (empty result, e.g. another analyze already running)
    // is neither: it says nothing about whether collection works.
    @VisibleForTesting
    void recordOutcome(String tableUUID, Optional<AnalyzeStatus> result, Throwable thrown) {
        boolean failed = thrown != null
                || (result != null && result.isPresent()
                    && result.get().getStatus() == StatsConstants.ScheduleStatus.FAILED);
        if (failed) {
            FailureBackoff backoff = failureBackoffs.computeIfAbsent(tableUUID, k -> new FailureBackoff());
            backoff.recordFailure();
            LOG.warn("[ExternalStats] trigger backoff | table_uuid={} consecutive_failures={} next_attempt_in_ms={}",
                    tableUUID, backoff.consecutiveFailures, backoff.retryAfterMs - System.currentTimeMillis());
        } else if (result != null && result.isPresent()) {
            failureBackoffs.remove(tableUUID);
        }
    }

    /** One table's consecutive-failure count and the instant it may be tried again. */
    private static final class FailureBackoff {
        private static final long BASE_DELAY_MS = 60_000L;

        private int consecutiveFailures;
        private long retryAfterMs;

        private void recordFailure() {
            consecutiveFailures++;
            // Doubling from a minute, capped: long enough that a cluster-wide problem is not hammered,
            // short enough that a table which failed once is collected again within the hour. Shifting is
            // bounded at 20 so the doubling cannot overflow however long the failures go on.
            long capMs = Config.connector_table_query_trigger_analyze_failure_backoff_max_second * 1000L;
            long delayMs = Math.min(capMs, BASE_DELAY_MS << Math.min(consecutiveFailures - 1, 20));
            retryAfterMs = System.currentTimeMillis() + delayMs;
        }
    }
}