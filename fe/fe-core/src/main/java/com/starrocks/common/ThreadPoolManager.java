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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/ThreadPoolManager.java

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

package com.starrocks.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.metric.GaugeMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ThreadPoolManager is a helper class for construct daemon thread pool with limit thread and memory resource.
 * thread names in thread pool are formatted as poolName-ID, where ID is a unique, sequentially assigned integer.
 * <p>
 * Choosing a pool. For every task, ThreadPoolExecutor (1) starts a new thread if it has fewer than corePoolSize
 * threads, even when other threads are idle; otherwise (2) offers the task to its work queue; and if the queue
 * refuses the task, (3) starts a new thread up to maximumPoolSize, past which it calls the rejection handler. The
 * factories set up these steps differently, so pick one by what must happen once every thread is busy:
 * <ul>
 * <li>newDaemonFixedThreadPool, newDaemonFixedThreadPoolWithAbortPolicy, newDaemonFixedThreadPoolWithUnboundedQueue:
 * core = max = numThread over a LinkedBlockingQueue. The pool starts a thread for each of the first numThread tasks
 * whatever the load and keeps them; after that every task goes through the queue. When a bounded queue is full,
 * the submitter blocks for up to 60 s and then gets a RejectedExecutionException, or gets one at once with
 * WithAbortPolicy; WithUnboundedQueue never rejects. Each hand-off takes the queue's lock, which is cheap for a
 * few dozen threads but costly for hundreds of threads running short tasks. Use it for bounded, CPU-sized
 * parallelism.
 * <li>newDaemonCacheThreadPool(maxNumThread): core 0 over a SynchronousQueue. A task goes to an idle thread, else
 * a new thread starts, up to maxNumThread; idle threads exit after KEEP_ALIVE_TIME. With every thread busy the task
 * is logged and dropped, and a Future returned by submit() never completes. Use it only for work that may be
 * skipped, such as best-effort cache loads.
 * <li>newDaemonCacheThreadPool(maxNumThread, queueSize): core = max = maxNumThread over a bounded queue, with core
 * threads timing out. It grows like a fixed pool, then queues up to queueSize, then blocks the submitter for up to
 * 5 s and rejects. An idle pool shrinks to zero threads.
 * <li>newDaemonScalingThreadPool: coreNumThread threads, at least one, stay. A task goes to an idle thread if
 * there is one, else a new thread starts, up to maxNumThread; past that tasks wait in an unbounded queue. It never
 * blocks the submitter and never rejects a task before shutdown, and it hands tasks to idle threads without a lock.
 * Use it for a server pool that mostly runs short tasks but must also absorb many blocking ones, where a task must
 * not be dropped or fail.
 * <li>newDaemonFixedPriorityThreadPool: a fixed pool whose queue runs tasks in priority order.
 * <li>newDaemonThreadPool: the ThreadPoolExecutor constructor, for any other combination.
 * <li>newDaemonScheduledThreadPool: a ScheduledThreadPoolExecutor, without a limit on delayed tasks or threads
 * (NOTICE).
 * </ul>
 * <p>
 * All thread pool constructed by ThreadPoolManager will be added to the nameToThreadPoolMap,
 * so the thread pool name in fe must be unique.
 * when all thread pools are constructed, ThreadPoolManager will register some metrics of all thread pool to MetricRepo,
 * so we can know the runtime state for all thread pool by prometheus metrics
 */

public class ThreadPoolManager {

    private static Map<String, ThreadPoolExecutor> nameToThreadPoolMap = Maps.newConcurrentMap();

    private static final String[] POOL_METRIC_TYPES = {"pool_size", "active_thread_num", "task_in_queue",
            "completed_task_count"};

    private static final long KEEP_ALIVE_TIME = 60L;

    private static boolean shouldRegisterMetric(boolean needRegisterMetric) {
        return needRegisterMetric && !GlobalStateMgr.isCheckpointThread();
    }

    private static final ThreadPoolExecutor STATS_CACHE_THREAD_POOL =
            ThreadPoolManager.newCollectThreadPool(Config.dict_collect_thread_pool_size, "cache-stats"
            );

    private static final ThreadPoolExecutor STATS_CACHE_THREAD_POOL_FOR_LAKE =
            ThreadPoolManager.newCollectThreadPool(Config.dict_collect_thread_pool_for_lake_size,
                    "cache-stats-lake");

    private static ThreadPoolExecutor newCollectThreadPool(int maxNumThread, String poolName) {
        if (Config.dict_collect_reject_policy.equals("ignore")) {
            return newDaemonCacheThreadPool(maxNumThread, poolName, false);
        } else if (Config.dict_collect_reject_policy.equals("queue")) {
            return newDaemonCacheThreadPool(maxNumThread, Config.dict_collect_queue_size, poolName, false);
        } else {
            throw new RuntimeException("unknown config" + Config.dict_collect_reject_policy);
        }
    }

    public static ThreadPoolExecutor getStatsCacheThread() {
        return STATS_CACHE_THREAD_POOL;
    }

    public static ThreadPoolExecutor getStatsCacheThreadPoolForLake() {
        return STATS_CACHE_THREAD_POOL_FOR_LAKE;
    }

    public static void registerAllThreadPoolMetric() {
        for (Map.Entry<String, ThreadPoolExecutor> entry : nameToThreadPoolMap.entrySet()) {
            registerThreadPoolMetric(entry.getKey(), entry.getValue());
        }
        nameToThreadPoolMap.clear();
    }

    public static void registerThreadPoolMetric(String poolName, ThreadPoolExecutor threadPool) {
        for (String poolMetricType : POOL_METRIC_TYPES) {
            GaugeMetric<Integer> gauge =
                    new GaugeMetric<Integer>("thread_pool", MetricUnit.NOUNIT, "thread_pool statistics") {
                        @Override
                        public Integer getValue() {
                            String metricType = this.getLabels().get(1).getValue();
                            switch (metricType) {
                                case "pool_size":
                                    return threadPool.getPoolSize();
                                case "active_thread_num":
                                    return threadPool.getActiveCount();
                                case "task_in_queue":
                                    return threadPool.getQueue().size();
                                case "completed_task_count":
                                    return (int) threadPool.getCompletedTaskCount();
                                default:
                                    return 0;
                            }
                        }
                    };
            gauge.addLabel(new MetricLabel("name", poolName))
                    .addLabel(new MetricLabel("type", poolMetricType));
            MetricRepo.addMetric(gauge);
        }
    }

    public static ThreadPoolExecutor newDaemonCacheThreadPool(int maxNumThread, String poolName,
                                                              boolean needRegisterMetric) {
        return newDaemonThreadPool(0, maxNumThread, KEEP_ALIVE_TIME, TimeUnit.SECONDS, new SynchronousQueue(),
                new LogDiscardPolicy(poolName), poolName, needRegisterMetric);
    }

    public static ThreadPoolExecutor newDaemonCacheThreadPool(int maxNumThread, int queueSize, String poolName,
                                                              boolean needRegisterMetric) {
        // ThreadPoolExecutor adds threads beyond the core size only when the queue is full, so set
        // core == max to run up to maxNumThread tasks before queueing. Core threads time out too,
        // so an idle pool still shrinks to zero threads.
        ThreadPoolExecutor threadPool = newDaemonThreadPool(maxNumThread, maxNumThread, KEEP_ALIVE_TIME,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(queueSize),
                new BlockedPolicy(poolName, 5), poolName, needRegisterMetric);
        threadPool.allowCoreThreadTimeOut(true);
        return threadPool;
    }

    public static ThreadPoolExecutor newDaemonFixedThreadPool(int numThread, int queueSize, String poolName,
                                                              boolean needRegisterMetric) {
        return newDaemonThreadPool(numThread, numThread, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueSize),
                new BlockedPolicy(poolName, 60), poolName, needRegisterMetric);
    }

    public static ThreadPoolExecutor newDaemonFixedThreadPoolWithAbortPolicy(
            int numThread, int queueSize, String poolName, boolean needRegisterMetric) {
        return newDaemonThreadPool(numThread, numThread, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueSize),
                new FastAbortPolicy(), poolName, needRegisterMetric);
    }

    public static ThreadPoolExecutor newDaemonFixedThreadPoolWithUnboundedQueue(int numThread, String poolName,
                                                                                boolean needRegisterMetric) {
        return newDaemonThreadPool(numThread, numThread, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new LogDiscardPolicy(poolName), poolName, needRegisterMetric);
    }

    /**
     * A pool sized by demand: a task goes to an idle thread if there is one, a new thread starts only when
     * every thread is busy, and once maxNumThread threads are busy tasks wait in an unbounded queue. The
     * pool never blocks the submitter and never rejects a task before shutdown. coreNumThread threads stay
     * for good; the others exit after KEEP_ALIVE_TIME idle. coreNumThread must be at least 1: a task queued once
     * maxNumThread threads are busy relies on a thread that never exits to run it.
     * <p>
     * Use it for a pool that must be able to grow large for blocking tasks but mostly runs short ones. A
     * fixed pool over a LinkedBlockingQueue does not fit that: it starts a thread for each task until it is
     * full, whatever the load, and then wakes one of its parked threads through the queue's take lock for
     * every task, which costs far more than a short task once hundreds of threads wait on that lock. Nor does
     * newDaemonCacheThreadPool(maxNumThread), which grows the same way but drops a task once maxNumThread
     * threads are busy, where this pool queues it.
     */
    public static ThreadPoolExecutor newDaemonScalingThreadPool(int coreNumThread, int maxNumThread, String poolName,
                                                                boolean needRegisterMetric) {
        return newDaemonScalingThreadPool(coreNumThread, maxNumThread, KEEP_ALIVE_TIME, TimeUnit.SECONDS, poolName,
                needRegisterMetric);
    }

    @VisibleForTesting
    static ThreadPoolExecutor newDaemonScalingThreadPool(int coreNumThread, int maxNumThread, long keepAliveTime,
                                                         TimeUnit unit, String poolName, boolean needRegisterMetric) {
        // With no core thread, the last thread can time out and exit between the executor refusing a task and
        // ForceQueuePolicy queueing it, which leaves the task queued with no thread to run it.
        Preconditions.checkArgument(coreNumThread >= 1, "a scaling pool needs at least one core thread, got %s",
                coreNumThread);
        return newDaemonThreadPool(coreNumThread, maxNumThread, keepAliveTime, unit, new ScalingQueue(),
                new ForceQueuePolicy(), poolName, needRegisterMetric);
    }

    public static PriorityThreadPoolExecutor newDaemonFixedPriorityThreadPool(int numThread, int queueSize,
                                                                              String poolName,
                                                                              boolean needRegisterMetric) {
        ThreadFactory threadFactory = namedThreadFactory(poolName);
        PriorityThreadPoolExecutor threadPool = new PriorityThreadPoolExecutor(numThread, numThread, 0,
                TimeUnit.SECONDS, new PriorityBlockingQueue<>(queueSize), threadFactory,
                new BlockedPolicy(poolName, 60));
        if (shouldRegisterMetric(needRegisterMetric)) {
            nameToThreadPoolMap.put(poolName, threadPool);
        }
        return threadPool;
    }

    public static ThreadPoolExecutor newDaemonThreadPool(int corePoolSize,
                                                         int maximumPoolSize,
                                                         long keepAliveTime,
                                                         TimeUnit unit,
                                                         BlockingQueue<Runnable> workQueue,
                                                         RejectedExecutionHandler handler,
                                                         String poolName,
                                                         boolean needRegisterMetric) {
        ThreadFactory threadFactory = namedThreadFactory(poolName);
        ThreadPoolExecutor threadPool =
                new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory,
                        handler);
        if (shouldRegisterMetric(needRegisterMetric)) {
            nameToThreadPoolMap.put(poolName, threadPool);
        }
        return threadPool;
    }

    // Now, we have no delay task num limit and thread num limit in ScheduledThreadPoolExecutor,
    // so it may cause oom when there are too many delay tasks or threads in ScheduledThreadPoolExecutor
    // Please use this api only for scheduling short task at fix rate.
    public static ScheduledThreadPoolExecutor newDaemonScheduledThreadPool(int corePoolSize, String poolName,
                                                                           boolean needRegisterMetric) {
        ThreadFactory threadFactory = namedThreadFactory(poolName);
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
                new ScheduledThreadPoolExecutor(corePoolSize, threadFactory);
        if (shouldRegisterMetric(needRegisterMetric)) {
            nameToThreadPoolMap.put(poolName, scheduledThreadPoolExecutor);
        }
        return scheduledThreadPoolExecutor;
    }

    /**
     * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
     */
    private static ThreadFactory namedThreadFactory(String poolName) {
        return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(poolName + "-%d").build();
    }

    /**
     * A handler for rejected task that fails the submission immediately and says nothing, used for a pool
     * whose caller reports the rejection itself.
     * <p>
     * {@link ThreadPoolExecutor.AbortPolicy} composes its message from {@link ThreadPoolExecutor#toString()},
     * which takes the pool's mainLock and walks every worker to tally completed tasks. On a saturated pool
     * that is thousands of iterations under the same lock {@code addWorker} and {@code processWorkerExit}
     * need, paid per rejected task, to build a string a caller that logs its own message never reads.
     */
    static class FastAbortPolicy implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            throw new StacklessRejectedExecutionException();
        }

        /**
         * Skips the stack-trace capture, which dominates the cost of throwing and is the last thing
         * left to pay for once the message is gone. Nothing prints this trace: the caller logs its
         * own message and discards the exception.
         */
        private static class StacklessRejectedExecutionException extends RejectedExecutionException {
            @Override
            public synchronized Throwable fillInStackTrace() {
                return this;
            }
        }
    }

    /**
     * A handler for rejected task that discards and log it, used for cached thread pool
     */
    static class LogDiscardPolicy implements RejectedExecutionHandler {

        private static final Logger LOG = LogManager.getLogger(LogDiscardPolicy.class);

        private String threadPoolName;

        public LogDiscardPolicy(String threadPoolName) {
            this.threadPoolName = threadPoolName;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            LOG.warn("Task " + r.toString() + " rejected from " + threadPoolName + " " + executor.toString());
        }
    }

    /**
     * A handler for rejected task that try to be blocked until the pool enqueue task succeed or timeout,
     * used for fixed thread pool throw RejectedExecutionException if timeout or catch InterruptedException
     */
    static class BlockedPolicy implements RejectedExecutionHandler {

        private static final Logger LOG = LogManager.getLogger(BlockedPolicy.class);

        private String threadPoolName;

        private int timeoutSeconds;

        public BlockedPolicy(String threadPoolName, int timeoutSeconds) {
            this.threadPoolName = threadPoolName;
            this.timeoutSeconds = timeoutSeconds;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            String msg = "Task " + r.toString() + " wait to enqueue in " + threadPoolName + " "
                    + executor.toString() + " failed";
            try {
                if (!executor.getQueue().offer(r, timeoutSeconds, TimeUnit.SECONDS)) {
                    throw new RejectedExecutionException(msg);
                }
            } catch (InterruptedException e) {
                throw new RejectedExecutionException(msg);
            }
        }
    }

    /**
     * The work queue of a scaling pool. ThreadPoolExecutor calls offer() to ask whether to queue a task
     * rather than start a thread: this hands the task to a thread already waiting for one, and otherwise
     * refuses, so the executor starts a new thread. When the pool is at its maximum the executor rejects
     * the task instead, and {@link ForceQueuePolicy} queues it for the next thread that frees up.
     */
    static class ScalingQueue extends LinkedTransferQueue<Runnable> {
        @Override
        public boolean offer(Runnable task) {
            return tryTransfer(task);
        }

        void forceOffer(Runnable task) {
            super.offer(task);
        }
    }

    /**
     * A handler for rejected task that queues it, used for scaling thread pool, which rejects a task only
     * when all of its threads are busy. Rejects for real once the pool is shut down, and takes back a task
     * queued while the pool was shutting down, which no thread may be left to run.
     */
    static class ForceQueuePolicy implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (executor.isShutdown()) {
                throw new RejectedExecutionException("Task " + r + " rejected from a shut down pool");
            }
            ((ScalingQueue) executor.getQueue()).forceOffer(r);
            if (executor.isShutdown() && executor.remove(r)) {
                throw new RejectedExecutionException("Task " + r + " rejected from a shut down pool");
            }
        }
    }

    public static void setCacheThreadPoolSize(ThreadPoolExecutor executor, int newMaxPoolSize) {
        if (executor.getCorePoolSize() > 0) {
            // skip for non-cache thread pool
            return;
        }
        int maxPoolSize = executor.getMaximumPoolSize();
        if (newMaxPoolSize <= 0 || maxPoolSize == newMaxPoolSize) { // no change
            return;
        }
        executor.setMaximumPoolSize(newMaxPoolSize);
    }

    public static void setFixedThreadPoolSize(ThreadPoolExecutor executor, int poolSize) {
        int coreSize = executor.getCorePoolSize();
        if (coreSize == poolSize) { // no change
            return;
        }
        if (coreSize < poolSize) {
            // increase the pool size, set the `MaximumPoolSize` first and then the `CoreSize`
            executor.setMaximumPoolSize(poolSize);
            executor.setCorePoolSize(poolSize);
        } else {
            // decrease the pool size, set `CoreSize` first and then the `MaximumPoolSize`
            executor.setCorePoolSize(poolSize);
            executor.setMaximumPoolSize(poolSize);
        }
    }

    /**
     * Resize a pool from {@link #newDaemonScalingThreadPool}. coreNumThread is clamped to [1, maxNumThread], and
     * a non-positive maxNumThread is ignored so that a bad config value leaves the pool as it is. Raising the
     * maximum starts threads for tasks already queued, up to the new maximum. An unchanged size is a no-op.
     */
    public static void setScalingThreadPoolSize(ThreadPoolExecutor executor, int coreNumThread, int maxNumThread) {
        if (maxNumThread <= 0) {
            return;
        }
        int core = Math.max(1, Math.min(coreNumThread, maxNumThread));
        int oldMax = executor.getMaximumPoolSize();
        // setCorePoolSize interrupts the idle threads whenever the pool is above the core size, even with the size
        // unchanged, and an interrupted thread starts its keep-alive over. The config refresh calls this every
        // 10 s, so touching the pool on every call would keep the threads above the core from ever exiting.
        if (core == executor.getCorePoolSize() && maxNumThread == oldMax) {
            return;
        }
        // ThreadPoolExecutor throws if either setter leaves core above max, so set max first unless the
        // current core is above the new max
        if (maxNumThread >= executor.getCorePoolSize()) {
            executor.setMaximumPoolSize(maxNumThread);
            if (maxNumThread > oldMax) {
                // Raising the maximum starts no thread, so tasks queued at the old maximum would wait for the
                // threads already running. Raising the core size starts threads for queued tasks, so raise it to
                // cover the backlog first, then set the core size the pool keeps.
                int backlogTarget = Math.min(maxNumThread, executor.getPoolSize() + executor.getQueue().size());
                if (backlogTarget > executor.getCorePoolSize()) {
                    executor.setCorePoolSize(backlogTarget);
                }
            }
            if (core != executor.getCorePoolSize()) {
                executor.setCorePoolSize(core);
            }
        } else {
            executor.setCorePoolSize(core);
            executor.setMaximumPoolSize(maxNumThread);
        }
    }

    /**
     * Calculate the number of CPU cores available on the machine.
     *
     * @return The number of CPU cores available.
     */
    public static int cpuCores() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Use at most 3/4 to execute cpu-intensive background tasks
     */
    public static int cpuIntensiveThreadPoolSize() {
        return Integer.max(2, cpuCores() * 3 / 4);
    }
}
