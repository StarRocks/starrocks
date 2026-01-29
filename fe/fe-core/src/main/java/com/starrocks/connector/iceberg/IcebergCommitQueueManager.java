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

package com.starrocks.connector.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Manages serialized commit operations for Iceberg tables to avoid concurrent commit conflicts.
 *
 * <p>Iceberg uses optimistic concurrency control (OCC) for metadata commits. When multiple threads
 * concurrently commit to the same table, conflicts can occur:
 *
 * <pre>
 * Thread A reads metadata V1 -&gt; writes data -&gt; attempts to commit
 * Thread B reads metadata V1 -&gt; writes data -&gt; commits successfully (metadata now V2)
 * Thread A attempts to commit -&gt; FAILS because current metadata is V2, not V1
 * </pre>
 *
 * <p>This manager creates a single-threaded executor per table to serialize commits, ensuring
 * that only one commit operation per table proceeds at a time. Different tables can commit
 * concurrently, maintaining overall throughput.
 *
 * <p>Table executors are automatically evicted after a period of inactivity (default: 1 hour)
 * to prevent unbounded memory growth.
 *
 * <p>Thread safety: This class is thread-safe and can be used concurrently from multiple threads.
 */
public class IcebergCommitQueueManager {

    private static final Logger LOG = LogManager.getLogger(IcebergCommitQueueManager.class);

    /**
     * Configuration for the commit queue manager.
     */
    public static class Config {
        private final boolean enabled;
        private final long timeoutSeconds;
        private final int maxQueueSize;

        // Minimum valid values for configuration
        private static final long MIN_TIMEOUT_SECONDS = 120L;
        private static final int MIN_QUEUE_SIZE = 10;

        public Config(boolean enabled, long timeoutSeconds, int maxQueueSize) {
            this.enabled = enabled;

            // Use minimum value with warning if timeout is less than minimum
            if (timeoutSeconds < MIN_TIMEOUT_SECONDS) {
                LOG.warn("Invalid iceberg_commit_queue_timeout_seconds: {}, using minimum value: {}",
                         timeoutSeconds, MIN_TIMEOUT_SECONDS);
                this.timeoutSeconds = MIN_TIMEOUT_SECONDS;
            } else {
                this.timeoutSeconds = timeoutSeconds;
            }

            // Use minimum value with warning if queue size is less than minimum
            if (maxQueueSize < MIN_QUEUE_SIZE) {
                LOG.warn("Invalid iceberg_commit_queue_max_size: {}, using minimum value: {}",
                         maxQueueSize, MIN_QUEUE_SIZE);
                this.maxQueueSize = MIN_QUEUE_SIZE;
            } else {
                this.maxQueueSize = maxQueueSize;
            }
        }

        /**
         * Get the minimum valid timeout value in seconds.
         * Used when invalid timeout is configured.
         */
        public static long getMinTimeoutSeconds() {
            return MIN_TIMEOUT_SECONDS;
        }

        /**
         * Get the minimum valid queue size.
         * Used when invalid queue size is configured.
         */
        public static int getMinQueueSize() {
            return MIN_QUEUE_SIZE;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public long getTimeoutSeconds() {
            return timeoutSeconds;
        }

        public int getMaxQueueSize() {
            return maxQueueSize;
        }
    }

    /**
     * Represents a commit task to be executed.
     */
    @FunctionalInterface
    public interface CommitTask {
        /**
         * Execute the commit operation. May throw any exception.
         */
        void execute() throws Exception;
    }

    /**
     * Result of a commit operation.
     */
    public static class CommitResult {
        private final boolean success;
        private final Throwable error;

        private CommitResult(boolean success, Throwable error) {
            this.success = success;
            this.error = error;
        }

        public static CommitResult success() {
            return new CommitResult(true, null);
        }

        public static CommitResult failure(Throwable error) {
            return new CommitResult(false, error);
        }

        public boolean isSuccess() {
            return success;
        }

        public Throwable getError() {
            return error;
        }

        /**
         * Rethrows the exception if the commit failed, preserving the original exception type.
         *
         * <p>This method preserves exception types to maintain backwards compatibility:
         * <ul>
         *   <li>{@link Error} types are rethrown as-is (e.g., {@link InternalError})</li>
         *   <li>{@link RuntimeException} types are rethrown as-is</li>
         *   <li>Checked exceptions are wrapped in {@link RuntimeException}</li>
         * </ul>
         *
         * @throws Error if the original error was an {@link Error}
         * @throws RuntimeException if the original exception was a {@link RuntimeException} or checked exception
         */
        public void rethrowIfFailed() {
            if (!success && error != null) {
                // Preserve Error types - they should never be wrapped
                if (error instanceof Error) {
                    throw (Error) error;
                }
                // Preserve RuntimeException types
                if (error instanceof RuntimeException) {
                    throw (RuntimeException) error;
                }
                // Wrap checked exceptions
                throw new RuntimeException(error);
            }
        }
    }

    /**
     * Holds the executor service for a specific table.
     */
    private static class TableCommitExecutor {
        private final ExecutorService executor;

        TableCommitExecutor(ExecutorService executor) {
            this.executor = executor;
        }

        ExecutorService getExecutor() {
            return executor;
        }
    }

    /**
     * Rejected execution handler that blocks until the queue has capacity.
     * This preserves serialized execution without running tasks in the caller thread.
     * The wait time is tied to the commit timeout to avoid premature rejections.
     */
    private static class BlockWhenQueueFullPolicy implements RejectedExecutionHandler {
        private final long maxWaitSeconds;

        BlockWhenQueueFullPolicy(long maxWaitSeconds) {
            this.maxWaitSeconds = maxWaitSeconds;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (executor.isShutdown()) {
                throw new RejectedExecutionException("Executor is shutdown");
            }
            try {
                // Block until queue has capacity, but with a maximum wait time
                // The wait time is tied to the commit timeout to ensure we don't reject
                // commits that would eventually succeed if given enough time.
                boolean enqueued = executor.getQueue().offer(r, maxWaitSeconds, TimeUnit.SECONDS);
                if (!enqueued) {
                    throw new RejectedExecutionException("Commit queue full - waited " + maxWaitSeconds +
                            " seconds but queue still at capacity. Consider increasing iceberg_commit_queue_max_size " +
                            "or iceberg_commit_queue_timeout_seconds.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException("Interrupted while waiting for commit queue capacity", e);
            }
        }
    }

    private final Supplier<Config> configSupplier;
    private final Cache<TableCommitKey, TableCommitExecutor> tableExecutors;

    /**
     * Creates a new commit queue manager with a config supplier.
     * The supplier is called on each submit to get the latest configuration,
     * allowing runtime config changes to take effect immediately.
     *
     * @param configSupplier the supplier that provides the current configuration
     */
    public IcebergCommitQueueManager(Supplier<Config> configSupplier) {
        this.configSupplier = Preconditions.checkNotNull(configSupplier, "configSupplier cannot be null");
        // Use Guava Cache with expireAfterAccess to automatically evict idle executors
        // This prevents unbounded memory growth when many tables are written to over time
        this.tableExecutors = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.HOURS)  // Evict after 1 hour of inactivity
                .removalListener(new RemovalListener<TableCommitKey, TableCommitExecutor>() {
                    @Override
                    public void onRemoval(RemovalNotification<TableCommitKey, TableCommitExecutor> notification) {
                        if (notification.getValue() != null) {
                            shutdownExecutor(notification.getValue().getExecutor(), notification.getKey());
                        }
                    }
                })
                .build();
        LOG.info("IcebergCommitQueueManager initialized with 1 hour idle timeout for table executors");
    }

    /**
     * Creates a new commit queue manager with the given configuration.
     *
     * @param config the configuration for this manager
     */
    public IcebergCommitQueueManager(Config config) {
        this(() -> config);
    }

    /**
     * Creates a new commit queue manager with default configuration.
     */
    public IcebergCommitQueueManager() {
        this(new Config(true, 120, 1000));
    }

    /**
     * Submits a commit task for the specified table and waits for completion.
     *
     * <p>If the queue manager is disabled, the task is executed immediately in the calling thread.
     * If enabled, the task is submitted to a single-threaded executor for the table, ensuring
     * serialized execution.
     *
     * <p>Configuration is read fresh on each call, so runtime config changes take effect immediately.
     *
     * @param catalogName the catalog name
     * @param dbName      the database name
     * @param tableName   the table name
     * @param task        the commit task to execute
     * @return the result of the commit operation
     * @throws RuntimeException if the task fails or times out
     */
    public CommitResult submitCommit(String catalogName, String dbName, String tableName, CommitTask task) {
        if (Thread.currentThread().isInterrupted()) {
            return CommitResult.failure(new InterruptedException("Commit interrupted"));
        }
        Config config = configSupplier.get();

        if (!config.isEnabled()) {
            // Queue is disabled, execute directly
            try {
                task.execute();
                return CommitResult.success();
            } catch (Exception e) {
                return CommitResult.failure(e);
            }
        }

        TableCommitKey key = new TableCommitKey(catalogName, dbName, tableName);
        TableCommitExecutor tableExecutor = getOrCreateTableExecutor(key);

        long timeoutSeconds = config.getTimeoutSeconds();
        long startNanos = System.nanoTime();
        Future<CommitResult> future;
        try {
            future = tableExecutor.getExecutor().submit(() -> {
                try {
                    task.execute();
                    return CommitResult.success();
                } catch (Exception e) {
                    LOG.warn("Failed to execute commit task for {}.{}.{}: {}",
                            catalogName, dbName, tableName, e.getMessage(), e);
                    return CommitResult.failure(e);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.error("Failed to submit commit task for {}.{}.{}",
                    catalogName, dbName, tableName, e);
            return CommitResult.failure(e);
        }

        long remainingNanos = TimeUnit.SECONDS.toNanos(timeoutSeconds) - (System.nanoTime() - startNanos);
        if (remainingNanos <= 0) {
            future.cancel(true);
            LOG.warn("Commit timeout for {}.{}.{}, but the actual commit may still be in progress. " +
                    "State unknown - do NOT retry without verification",
                    catalogName, dbName, tableName);
            // Use the Throwable-only constructor
            return CommitResult.failure(new CommitStateUnknownException(
                    new TimeoutException("Commit timeout after " + timeoutSeconds + " seconds")));
        }

        try {
            return future.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for commit to complete for {}.{}.{}, " +
                    "but the actual commit may still be in progress. State unknown - do NOT retry without verification",
                    catalogName, dbName, tableName, e);
            future.cancel(true);
            // Use (String, Throwable) constructor to provide clear message
            return CommitResult.failure(new CommitStateUnknownException(
                    "Commit interrupted", e));
        } catch (ExecutionException e) {
            LOG.error("Exception during commit execution for {}.{}.{}",
                    catalogName, dbName, tableName, e.getCause());
            return CommitResult.failure(e.getCause() != null ? e.getCause() : e);
        } catch (TimeoutException e) {
            LOG.warn("Timeout waiting for commit to complete for {}.{}.{} after {} seconds. " +
                    "The actual commit may still be in progress. State unknown - do NOT retry without verification",
                    catalogName, dbName, tableName, timeoutSeconds, e);
            future.cancel(true);
            // Use the Throwable-only constructor
            return CommitResult.failure(new CommitStateUnknownException(
                    new TimeoutException("Commit timeout after " + timeoutSeconds + " seconds")));
        }
    }

    /**
     * Submits a commit task and rethrows any exception as a RuntimeException.
     *
     * @param catalogName the catalog name
     * @param dbName      the database name
     * @param tableName   the table name
     * @param task        the commit task to execute
     * @throws RuntimeException if the task fails or times out
     */
    public void submitCommitAndRethrow(String catalogName, String dbName, String tableName, CommitTask task) {
        CommitResult result = submitCommit(catalogName, dbName, tableName, task);
        result.rethrowIfFailed();
    }

    /**
     * Escapes percent signs in a string for safe use in String.format() patterns.
     * Guava's ThreadFactoryBuilder.setNameFormat() uses String.format() internally,
     * so any % characters in the name must be escaped to %% to avoid being interpreted
     * as format specifiers.
     *
     * @param name the name to escape
     * @return the name with % escaped to %%
     */
    private static String escapeForFormat(String name) {
        return name.replace("%", "%%");
    }

    private TableCommitExecutor getOrCreateTableExecutor(TableCommitKey key) {
        try {
            return tableExecutors.get(key, () -> {
                Config config = configSupplier.get();
                ThreadFactory threadFactory = new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("iceberg-commit-" + escapeForFormat(key.catalogName) + "-" +
                                    escapeForFormat(key.dbName) + "-" + escapeForFormat(key.tableName) + "-%d")
                        .build();
                // Use bounded ThreadPoolExecutor with BlockWhenQueueFullPolicy to enforce maxQueueSize
                // When queue is full, block until capacity is available.
                // The wait time is set to half of the commit timeout to leave room for actual execution.
                long maxQueueWaitSeconds = config.getTimeoutSeconds() / 2;
                ExecutorService executor = new ThreadPoolExecutor(
                        1,  // corePoolSize
                        1,  // maximumPoolSize
                        0L, TimeUnit.MILLISECONDS,  // idle thread timeout
                        new LinkedBlockingQueue<>(config.getMaxQueueSize()),  // bounded queue
                        threadFactory,
                        new BlockWhenQueueFullPolicy(maxQueueWaitSeconds));
                LOG.info("Created commit queue executor for {}.{}.{}, max queue size: {}, " +
                                "max queue wait: {} seconds (commit timeout: {} seconds)",
                        key.catalogName, key.dbName, key.tableName, config.getMaxQueueSize(),
                        maxQueueWaitSeconds, config.getTimeoutSeconds());
                return new TableCommitExecutor(executor);
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to create table executor", e);
        }
    }

    /**
     * Shuts down the executor for a specific table.
     *
     * <p>This method is idempotent - calling it multiple times for the same table is safe.
     *
     * @param catalogName the catalog name
     * @param dbName      the database name
     * @param tableName   the table name
     */
    public void shutdownTableExecutor(String catalogName, String dbName, String tableName) {
        TableCommitKey key = new TableCommitKey(catalogName, dbName, tableName);
        tableExecutors.invalidate(key);
    }

    /**
     * Shuts down all executors for tables matching the given database name.
     *
     * @param catalogName the catalog name
     * @param dbName      the database name
     */
    public void shutdownDatabaseExecutors(String catalogName, String dbName) {
        // Invalidate all matching entries - the removal listener will handle shutdown
        tableExecutors.asMap().entrySet().removeIf(entry -> {
            TableCommitKey key = entry.getKey();
            return key.catalogName.equals(catalogName) && key.dbName.equals(dbName);
        });
    }

    /**
     * Shuts down all executors in this manager.
     */
    public void shutdownAll() {
        // Invalidate all entries - the removal listener will handle shutdown
        tableExecutors.invalidateAll();
    }

    private void shutdownExecutor(ExecutorService executor, TableCommitKey key) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Executor for {}.{}.{} did not terminate in time, forcing shutdown",
                        key.catalogName, key.dbName, key.tableName);
                executor.shutdownNow();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.error("Executor for {}.{}.{} did not terminate after forced shutdown",
                            key.catalogName, key.dbName, key.tableName);
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the number of tables with active executors.
     */
    @VisibleForTesting
    public long getActiveTableCount() {
        return tableExecutors.size();
    }

    /**
     * Returns whether the manager is enabled.
     */
    public boolean isEnabled() {
        return configSupplier.get().isEnabled();
    }

    /**
     * Key for identifying a specific table.
     */
    @VisibleForTesting
    static class TableCommitKey {
        private final String catalogName;
        private final String dbName;
        private final String tableName;

        TableCommitKey(String catalogName, String dbName, String tableName) {
            this.catalogName = Preconditions.checkNotNull(catalogName, "catalogName cannot be null");
            this.dbName = Preconditions.checkNotNull(dbName, "dbName cannot be null");
            this.tableName = Preconditions.checkNotNull(tableName, "tableName cannot be null");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableCommitKey)) {
                return false;
            }
            TableCommitKey that = (TableCommitKey) o;
            return catalogName.equals(that.catalogName)
                    && dbName.equals(that.dbName)
                    && tableName.equals(that.tableName);
        }

        @Override
        public int hashCode() {
            int result = catalogName.hashCode();
            result = 31 * result + dbName.hashCode();
            result = 31 * result + tableName.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return catalogName + "." + dbName + "." + tableName;
        }
    }
}
