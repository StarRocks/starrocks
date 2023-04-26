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


package com.starrocks.connector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Stolen from io.airlift.concurrent.
 * Guarantees that no more than maxThreads will be used to execute tasks submitted
 * through {@link #execute(Runnable) execute()}.
 */
public class BoundedExecutor implements Executor {
    private static final Logger LOG = LogManager.getLogger(BoundedExecutor.class);

    private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicBoolean failed = new AtomicBoolean();

    private final Executor coreExecutor;
    private final int maxThreads;

    public BoundedExecutor(Executor coreExecutor, int maxThreads) {
        requireNonNull(coreExecutor, "coreExecutor is null");
        checkArgument(maxThreads > 0, "maxThreads must be greater than zero");
        this.coreExecutor = coreExecutor;
        this.maxThreads = maxThreads;
    }

    @Override
    public void execute(@NotNull Runnable task) {
        if (failed.get()) {
            throw new RejectedExecutionException("BoundedExecutor is in a failed state");
        }

        queue.add(task);

        int size = queueSize.incrementAndGet();
        if (size <= maxThreads) {
            try {
                coreExecutor.execute(this::releaseQueue);
            } catch (Throwable e) {
                failed.set(true);
                LOG.error("BoundedExecutor state corrupted due to underlying executor failure");
                throw e;
            }
        }
    }

    private void releaseQueue() {
        do {
            try {
                requireNonNull(queue.poll()).run();
            } catch (Throwable e) {
                LOG.error("Task failed", e);
            }
        } while (queueSize.getAndDecrement() > maxThreads);
    }
}