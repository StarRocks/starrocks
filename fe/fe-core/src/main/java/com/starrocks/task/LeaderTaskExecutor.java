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

    private ThreadPoolExecutor executor;
    private Map<Long, Future<?>> runningTasks;
    public ScheduledThreadPoolExecutor scheduledThreadPool;

    public LeaderTaskExecutor(String name, int threadNum, boolean needRegisterMetric) {
        executor = ThreadPoolManager
                .newDaemonFixedThreadPool(threadNum, threadNum * 2, name + "_pool", needRegisterMetric);
        runningTasks = Maps.newHashMap();
        scheduledThreadPool =
                ThreadPoolManager.newDaemonScheduledThreadPool(1, name + "_scheduler_thread_pool", needRegisterMetric);
    }

    public LeaderTaskExecutor(String name, int threadNum, int queueSize, boolean needRegisterMetric) {
        executor = ThreadPoolManager.newDaemonFixedThreadPool(threadNum, queueSize, name + "_pool", needRegisterMetric);
        runningTasks = Maps.newHashMap();
        scheduledThreadPool =
                ThreadPoolManager.newDaemonScheduledThreadPool(1, name + "_scheduler_thread_pool", needRegisterMetric);
    }

    public void start() {
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
        scheduledThreadPool.shutdown();
        executor.shutdown();
        runningTasks.clear();
    }

    public int getTaskNum() {
        synchronized (runningTasks) {
            return runningTasks.size();
        }
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
