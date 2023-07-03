// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.task;

import com.google.common.collect.Maps;
import com.starrocks.common.PriorityFutureTask;
import com.starrocks.common.PriorityThreadPoolExecutor;
import com.starrocks.common.ThreadPoolManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PriorityLeaderTaskExecutor {
    private static final Logger LOG = LogManager.getLogger(PriorityLeaderTaskExecutor.class);

    private PriorityThreadPoolExecutor executor;
    private Map<Long, PriorityFutureTask<?>> runningTasks;
    public ScheduledThreadPoolExecutor scheduledThreadPool;

    public PriorityLeaderTaskExecutor(String name, int threadNum, int queueSize, boolean needRegisterMetric) {
        executor = ThreadPoolManager.newDaemonFixedPriorityThreadPool(threadNum, queueSize, name + "_priority_pool",
                needRegisterMetric);
        runningTasks = Maps.newHashMap();
        scheduledThreadPool = ThreadPoolManager.newDaemonScheduledThreadPool(1,
                name + "_scheduler_priority_thread_pool", needRegisterMetric);
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
                PriorityFutureTask<?> future = executor.submit(task);
                runningTasks.put(signature, future);
                return true;
            } catch (RejectedExecutionException e) {
                LOG.warn("submit task {} failed.", task.getSignature(), e);
                return false;
            }
        }
    }

    /**
     * update task prirority still in queue, no effect if task already executed
     * @param signature
     * @param priority
     * @return trun if update task priority success, false if update fail which
     *         means task not exists or already executed
     */
    public boolean updatePriority(long signature, int priority) {
        synchronized (runningTasks) {
            PriorityFutureTask<?> task = runningTasks.get(signature);
            if (task == null) {
                return false;
            }

            return executor.updatePriority(task, priority);
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

    public int getCorePoolSize() {
        return executor.getCorePoolSize();
    }

    public void setCorePoolSize(int corePoolSize) {
        executor.setCorePoolSize(corePoolSize);
    }

    private class TaskChecker implements Runnable {
        @Override
        public void run() {
            try {
                synchronized (runningTasks) {
                    Iterator<Entry<Long, PriorityFutureTask<?>>> iterator = runningTasks.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Entry<Long, PriorityFutureTask<?>> entry = iterator.next();
                        PriorityFutureTask<?> future = entry.getValue();
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
