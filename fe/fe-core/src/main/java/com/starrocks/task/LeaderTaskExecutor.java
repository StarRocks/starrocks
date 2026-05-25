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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/MasterTaskExecutor.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.task;

import com.google.common.collect.Maps;
import com.starrocks.common.ThreadPoolManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LeaderTaskExecutor {
    private static final Logger LOG = LogManager.getLogger(LeaderTaskExecutor.class);

    // Not final: close() shuts down both pools and start() rebuilds them so a singleton
    // executor instance can survive a leader demote / re-elect cycle.
    // Package-private so same-package tests can swap in a stuck pool to exercise the
    // restart guard without reflection.
    ThreadPoolExecutor executor;
    private Map<Long, Future<?>> runningTasks;
    public ScheduledThreadPoolExecutor scheduledThreadPool;

    // Saved for rebuild on restart - see start().
    private final String name;
    private final int threadNum;
    private final int queueSize;
    private final boolean needRegisterMetric;

    public LeaderTaskExecutor(String name, int threadNum, boolean needRegisterMetric) {
        this(name, threadNum, threadNum * 2, needRegisterMetric);
    }

    public LeaderTaskExecutor(String name, int threadNum, int queueSize, boolean needRegisterMetric) {
        this.name = name;
        this.threadNum = threadNum;
        this.queueSize = queueSize;
        this.needRegisterMetric = needRegisterMetric;
        runningTasks = Maps.newHashMap();
        buildPools();
    }

    private void buildPools() {
        executor = ThreadPoolManager.newDaemonFixedThreadPool(threadNum, queueSize, name + "_pool", needRegisterMetric);
        scheduledThreadPool =
                ThreadPoolManager.newDaemonScheduledThreadPool(1, name + "_scheduler_thread_pool", needRegisterMetric);
    }

    public synchronized void start() {
        // Refuse to overlap with a previous worker that did not drain in close(): if the pool
        // is shutdown but not yet terminated, in-flight submissions from the previous leader
        // session are still running. Mirrors the BatchWriteMgr / AlterHandler restart guard.
        if (executor.isShutdown() && !executor.isTerminated()) {
            throw new IllegalStateException(
                    "LeaderTaskExecutor " + name + " executor has not terminated; refuse to restart");
        }
        // Rebuild the pools if a previous demotion shut them down so subsequent submissions
        // do not raise RejectedExecutionException on the new leader session.
        if (executor.isShutdown()) {
            buildPools();
        }
        scheduledThreadPool.scheduleAtFixedRate(new TaskChecker(), 0L, 1000L, TimeUnit.MILLISECONDS);
    }

    /**
     * submit task to task executor
     * Notice:
     * If the queue is full, this function will be blocked until the pool enqueue task succeed or timeout (60s default),
     * so this function should be used outside any lock to prevent holding lock for long time.
     *
     * @param task
     * @return true if submit success
     * false if task exists
     */
    public boolean submit(PriorityLeaderTask task) {
        long signature = task.getSignature();
        synchronized (runningTasks) {
            if (runningTasks.containsKey(signature)) {
                return false;
            }

            try {
                Future<?> future = executor.submit(task);
                runningTasks.put(signature, future);
                return true;
            } catch (RejectedExecutionException e) {
                LOG.warn("submit task {} failed.", task.getSignature(), e);
                return false;
            }
        }
    }

    public void close() {
        close(0L);
    }

    /**
     * Coordinated stop for leader demotion. Uses shutdown() (not shutdownNow()) so in-flight
     * tasks complete their bounded {@code subTasksDoneSignal.await(timeoutSec)} naturally —
     * interrupting via shutdownNow() would cause the caller (e.g. ExportExportingTask) to
     * translate the InterruptedException into a TIMEOUT cancel of a healthy job and skip
     * the job.setDoExportingThread(null) cleanup.
     *
     * If {@code awaitMillis > 0}, also blocks up to that budget waiting for both internal
     * pools to actually terminate. This is required on the leader demotion drain path so a
     * re-elected leader does not race the old leader's still-running tasks.
     *
     * @param awaitMillis maximum time to wait for both pools to drain, in milliseconds. Zero
     *                    or negative means do not wait (legacy behaviour). The budget is
     *                    split across the two internal pools.
     */
    public void close(long awaitMillis) {
        scheduledThreadPool.shutdown();
        executor.shutdown();
        if (awaitMillis > 0L) {
            long deadline = System.currentTimeMillis() + awaitMillis;
            try {
                long remaining = Math.max(1L, deadline - System.currentTimeMillis());
                if (!scheduledThreadPool.awaitTermination(remaining, TimeUnit.MILLISECONDS)) {
                    LOG.warn("LeaderTaskExecutor scheduledThreadPool did not terminate within drain timeout");
                }
                remaining = Math.max(1L, deadline - System.currentTimeMillis());
                if (!executor.awaitTermination(remaining, TimeUnit.MILLISECONDS)) {
                    LOG.warn("LeaderTaskExecutor executor did not terminate within drain timeout");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for LeaderTaskExecutor to terminate");
            }
        }
        synchronized (runningTasks) {
            runningTasks.clear();
        }
    }

    public int getTaskNum() {
        synchronized (runningTasks) {
            return runningTasks.size();
        }
    }

    public int getCorePoolSize() {
        return executor.getCorePoolSize();
    }

    public void setPoolSize(int poolSize) {
        ThreadPoolManager.setFixedThreadPoolSize(executor, poolSize);
    }

    private class TaskChecker implements Runnable {
        @Override
        public void run() {
            try {
                synchronized (runningTasks) {
                    Iterator<Entry<Long, Future<?>>> iterator = runningTasks.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Entry<Long, Future<?>> entry = iterator.next();
                        Future<?> future = entry.getValue();
                        if (future.isDone()) {
                            iterator.remove();
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("check task error", e);
            }
        }
    }
}
