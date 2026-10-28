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
package org.apache.iceberg.util;

import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Copy of Iceberg 1.11.0 {@code ParallelIterable}, shadowing the one in iceberg-core.
 *
 * <p>The only change: the queue length is tracked in an {@link AtomicInteger}. Upstream calls
 * {@link ConcurrentLinkedQueue#size()}, which walks the whole queue, once per task slot on every
 * {@code checkTasks()} pass (2 x worker threads slots) and once per task start. When the workers dump
 * whole manifests into the queue while the consumer waits for the first item, that walk is O(slots x
 * queued files) and cost seconds of planning on tables with ~100k files.
 */
public class ParallelIterable<T> extends CloseableGroup implements CloseableIterable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelIterable.class);

    // Logic behind default value: ParallelIterable is often used for file planning.
    // Assuming that a DataFile or DeleteFile is about 500 bytes, a 30k limit uses 14.3 MB of memory.
    private static final int DEFAULT_MAX_QUEUE_SIZE = 30_000;

    private final Iterable<? extends Iterable<T>> iterables;
    private final ExecutorService workerPool;

    // Bound for number of items in the queue to limit memory consumption
    // even in the case when input iterables are large.
    private final int approximateMaxQueueSize;

    public ParallelIterable(Iterable<? extends Iterable<T>> iterables, ExecutorService workerPool) {
        this(iterables, workerPool, DEFAULT_MAX_QUEUE_SIZE);
    }

    public ParallelIterable(
            Iterable<? extends Iterable<T>> iterables,
            ExecutorService workerPool,
            int approximateMaxQueueSize) {
        this.iterables = Preconditions.checkNotNull(iterables, "Input iterables cannot be null");
        this.workerPool = Preconditions.checkNotNull(workerPool, "Worker pool cannot be null");
        this.approximateMaxQueueSize = approximateMaxQueueSize;
    }

    @Override
    public CloseableIterator<T> iterator() {
        ParallelIterator<T> iter =
                new ParallelIterator<>(iterables, workerPool, approximateMaxQueueSize);
        addCloseable(iter);
        return iter;
    }

    @VisibleForTesting
    static class ParallelIterator<T> implements CloseableIterator<T> {
        private final Iterator<Task<T>> tasks;
        private final Deque<Task<T>> yieldedTasks = new ArrayDeque<>();
        private final ExecutorService workerPool;
        private final CompletableFuture<Optional<Task<T>>>[] taskFutures;
        private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
        // approximate length of queue: incremented after an add, decremented after a poll
        private final AtomicInteger queueSize = new AtomicInteger(0);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final int maxQueueSize;

        private ParallelIterator(
                Iterable<? extends Iterable<T>> iterables, ExecutorService workerPool, int maxQueueSize) {
            Preconditions.checkArgument(maxQueueSize > 0, "Max queue size must be greater than 0");
            this.tasks =
                    Iterables.transform(
                                    iterables, iterable -> new Task<>(iterable, queue, queueSize, closed, maxQueueSize))
                            .iterator();
            this.workerPool = workerPool;
            // submit 2 tasks per worker at a time
            this.taskFutures = new CompletableFuture[2 * ThreadPools.WORKER_THREAD_POOL_SIZE];
            this.maxQueueSize = maxQueueSize;
        }

        @Override
        @SuppressWarnings("FutureReturnValueIgnored")
        public void close() {
            // close first, avoid new task submit
            this.closed.set(true);

            try (Closer closer = Closer.create()) {
                synchronized (this) {
                    yieldedTasks.forEach(closer::register);
                    yieldedTasks.clear();
                }

                // cancel background tasks and close continuations if any
                for (CompletableFuture<Optional<Task<T>>> taskFuture : taskFutures) {
                    if (taskFuture != null) {
                        taskFuture.cancel(true);
                        taskFuture.thenAccept(
                                continuation -> {
                                    if (continuation.isPresent()) {
                                        try {
                                            continuation.get().close();
                                        } catch (IOException e) {
                                            LOG.error("Task close failed", e);
                                        }
                                    }
                                });
                    }
                }

                // clean queue
                this.queue.clear();
            } catch (IOException e) {
                throw new UncheckedIOException("Close failed", e);
            }
        }

        /**
         * Checks on running tasks and submits new tasks if needed.
         *
         * <p>This should not be called after {@link #close()}.
         *
         * @return true if there are pending tasks, false otherwise
         */
        private synchronized boolean checkTasks() {
            Preconditions.checkState(!closed.get(), "Already closed");
            boolean hasRunningTask = false;

            for (int i = 0; i < taskFutures.length; i += 1) {
                if (taskFutures[i] == null || taskFutures[i].isDone()) {
                    if (taskFutures[i] != null) {
                        // check for task failure and re-throw any exception. Enqueue continuation if any.
                        try {
                            Optional<Task<T>> continuation = taskFutures[i].get();
                            continuation.ifPresent(yieldedTasks::addLast);
                            taskFutures[i] = null;
                        } catch (ExecutionException e) {
                            if (e.getCause() instanceof RuntimeException) {
                                // rethrow a runtime exception
                                throw (RuntimeException) e.getCause();
                            } else {
                                throw new RuntimeException("Failed while running parallel task", e.getCause());
                            }
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Interrupted while running parallel task", e);
                        }
                    }

                    // submit a new task if there is space in the queue
                    if (queueSize.get() < maxQueueSize) {
                        taskFutures[i] = submitNextTask();
                    }
                }

                if (taskFutures[i] != null) {
                    hasRunningTask = true;
                }
            }

            return !closed.get() && (tasks.hasNext() || hasRunningTask);
        }

        private CompletableFuture<Optional<Task<T>>> submitNextTask() {
            if (!closed.get()) {
                if (!yieldedTasks.isEmpty()) {
                    return CompletableFuture.supplyAsync(yieldedTasks.removeFirst(), workerPool);
                } else if (tasks.hasNext()) {
                    return CompletableFuture.supplyAsync(tasks.next(), workerPool);
                }
            }
            return null;
        }

        @Override
        public synchronized boolean hasNext() {
            Preconditions.checkState(!closed.get(), "Already closed");

            // If the consumer is processing records more slowly than the producers, the producers will
            // eventually fill the queue and yield, returning continuations. Continuations and new tasks
            // are started by checkTasks(). The check here prevents us from restarting continuations or
            // starting new tasks before the queue is emptied. Restarting too early would lead to tasks
            // yielding very quickly (CPU waste on scheduling).
            if (!queue.isEmpty()) {
                return true;
            }

            // this cannot conclude that there are no more records until tasks have finished. while some
            // are running, return true when there is at least one item to return.
            while (checkTasks()) {
                if (!queue.isEmpty()) {
                    return true;
                }

                try {
                    Thread.sleep(10);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

            // when tasks are no longer running, return whether the queue has items
            return !queue.isEmpty();
        }

        @Override
        public synchronized T next() {
            // use hasNext to block until there is an available record
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T item = queue.poll();
            if (item != null) {
                queueSize.decrementAndGet();
            }
            return item;
        }

        @VisibleForTesting
        int queueSize() {
            return queueSize.get();
        }
    }

    private static class Task<T> implements Supplier<Optional<Task<T>>>, Closeable {
        private final Iterable<T> input;
        private final ConcurrentLinkedQueue<T> queue;
        private final AtomicInteger queueSize;
        private final AtomicBoolean closed;
        private final int approximateMaxQueueSize;

        private Iterator<T> iterator = null;

        Task(
                Iterable<T> input,
                ConcurrentLinkedQueue<T> queue,
                AtomicInteger queueSize,
                AtomicBoolean closed,
                int approximateMaxQueueSize) {
            this.input = Preconditions.checkNotNull(input, "input cannot be null");
            this.queue = Preconditions.checkNotNull(queue, "queue cannot be null");
            this.queueSize = Preconditions.checkNotNull(queueSize, "queueSize cannot be null");
            this.closed = Preconditions.checkNotNull(closed, "closed cannot be null");
            this.approximateMaxQueueSize = approximateMaxQueueSize;
        }

        @Override
        public Optional<Task<T>> get() {
            try {
                if (queueSize.get() >= approximateMaxQueueSize) {
                    // Yield when queue is over the size limit. Task will be resubmitted later and continue
                    // the work.
                    //
                    // Tasks might hold references (via iterator) to constrained resources
                    // (e.g. pooled connections). Hence, tasks should yield only when
                    // iterator is not instantiated. Otherwise, there could be
                    // a deadlock when yielded tasks are waiting to be executed while
                    // currently executed tasks are waiting for the resources that are held
                    // by the yielded tasks.
                    return Optional.of(this);
                }

                if (iterator == null) {
                    iterator = input.iterator();
                }

                while (iterator.hasNext()) {
                    T next = iterator.next();
                    if (closed.get()) {
                        break;
                    }

                    queue.add(next);
                    queueSize.incrementAndGet();
                }
            } catch (Throwable e) {
                try {
                    close();
                } catch (IOException closeException) {
                    // self-suppression is not permitted
                    // (e and closeException to be the same is unlikely, but possible)
                    if (closeException != e) {
                        e.addSuppressed(closeException);
                    }
                }

                throw e;
            }

            try {
                close();
            } catch (IOException e) {
                throw new UncheckedIOException("Close failed", e);
            }

            // The task is complete. Returning empty means there is no continuation that should be
            // executed.
            return Optional.empty();
        }

        @Override
        public void close() throws IOException {
            iterator = null;
            if (input instanceof Closeable) {
                ((Closeable) input).close();
            }
        }
    }
}
