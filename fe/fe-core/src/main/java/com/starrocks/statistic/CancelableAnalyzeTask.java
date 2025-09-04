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

package com.starrocks.statistic;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CancelableAnalyzeTask implements RunnableFuture<Void> {
    private static final Logger LOG = LogManager.getLogger(CancelableAnalyzeTask.class);

    private final Runnable originalTask;
    private final AnalyzeStatus analyzeStatus;
    private volatile boolean cancelled = false;
    private volatile boolean done = false;
    private volatile Throwable exception = null;
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile Thread runningThread = null;

    public CancelableAnalyzeTask(Runnable originalTask, AnalyzeStatus analyzeStatus) {
        this.originalTask = Preconditions.checkNotNull(originalTask, "originalTask cannot be null");
        this.analyzeStatus = Preconditions.checkNotNull(analyzeStatus, "analyzeStatus cannot be null");
    }

    @Override
    public void run() {
        if (cancelled) {
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            done = true;
            latch.countDown();
            return;
        }

        runningThread = Thread.currentThread();

        try {
            originalTask.run();
        } catch (Throwable t) {
            exception = t;
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            LOG.error("Analyze task failed", t);
        } finally {
            done = true;
            runningThread = null;
            latch.countDown();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (done) {
            return false;
        }

        cancelled = true;
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);

        if (mayInterruptIfRunning && runningThread != null) {
            runningThread.interrupt();
        }

        if (!done) {
            done = true;
            latch.countDown();
        }

        return true;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isDone() {
        return done;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        latch.await();
        if (cancelled) {
            throw new CancellationException("Task was cancelled");
        }
        if (exception != null) {
            throw new ExecutionException(exception);
        }
        return null;
    }

    @Override
    public Void get(long timeout, @NotNull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!latch.await(timeout, unit)) {
            throw new TimeoutException("Task did not complete within timeout");
        }
        return get();
    }

    public void cancel() {
        cancel(true);
    }

}


