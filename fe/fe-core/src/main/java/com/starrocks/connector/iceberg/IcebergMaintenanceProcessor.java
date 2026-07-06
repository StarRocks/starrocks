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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.procedure.ExpireSnapshotsProcedure;
import com.starrocks.connector.iceberg.procedure.IcebergMaintenanceTaskStats;
import com.starrocks.connector.iceberg.procedure.IcebergTableProcedureContext;
import com.starrocks.connector.iceberg.procedure.RemoveOrphanFilesProcedure;
import com.starrocks.connector.iceberg.procedure.RewriteManifestsProcedure;
import com.starrocks.metric.IcebergMaintenanceMetricsMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Transaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Standalone processor for Iceberg catalog metadata auto maintenance (expire_snapshots,
 * remove_orphan_files, rewrite_manifests, rewrite_data_files).
 */
public class IcebergMaintenanceProcessor extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(IcebergMaintenanceProcessor.class);

    private static final long RECENT_WRITE_WINDOW_MILLIS = 24L * 3600L * 1000L;
    private static final String SNAPSHOT_SUMMARY_OPERATION = "operation";

    // Thread pool for executing per-table metadata maintenance tasks. This allows concurrent
    // processing of tables across catalogs while keeping the daemon loop simple.
    private static final int MAINTENANCE_THREAD_NUM =
            Math.max(1, Math.min(Config.iceberg_background_maintenance_pool_size, Runtime.getRuntime().availableProcessors()));
    private static final int MAINTENANCE_QUEUE_SIZE = 1024;

    // Per-table timeout in seconds for maintenance tasks. Default 10 minutes.
    private static final long PER_TABLE_TIMEOUT_SECONDS = 600;

    private static final int ICEBERG_PLAN_WORKER_THREAD_NUM = Math.max(2, Runtime.getRuntime().availableProcessors() / 8);

    private final ConcurrentHashMap<String, IcebergMaintenanceInfo> maintenanceInfoMap = new ConcurrentHashMap<>();
    private final ExecutorService maintenanceExecutor;
    private final ExecutorService icebergPlanWorkerExecutor;

    public IcebergMaintenanceProcessor() {
        super(IcebergMaintenanceProcessor.class.getName(),
                Config.iceberg_background_check_maintenance_interval_seconds * 1000L);

        this.maintenanceExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                MAINTENANCE_THREAD_NUM,
                MAINTENANCE_QUEUE_SIZE,
                "iceberg-maintenance-pool",
                true);
        this.icebergPlanWorkerExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                ICEBERG_PLAN_WORKER_THREAD_NUM,
                ICEBERG_PLAN_WORKER_THREAD_NUM,
                "iceberg-plan-worker-pool",
                true);
    }

    public void shutdown() {
        setStop();
        shutdownExecutor(maintenanceExecutor, "iceberg-maintenance-pool");
        shutdownExecutor(icebergPlanWorkerExecutor, "iceberg-plan-worker-pool");
    }

    private void shutdownExecutor(ExecutorService executor, String name) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                LOG.warn("Executor {} did not terminate in time, forcing shutdown", name);
                executor.shutdownNow();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOG.warn("Executor {} did not terminate after forced shutdown", name);
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static class IcebergMaintenanceInfo {
        final String catalogName;
        final IcebergCatalog catalog;
        final HdfsEnvironment hdfsEnvironment;
        final int cleanupIntervalHours;
        final int rewriteIntervalHours;
        volatile long lastCleanupTimeMillis = 0L;
        volatile long lastRewriteTimeMillis = 0L;

        IcebergMaintenanceInfo(String catalogName, IcebergCatalog catalog, HdfsEnvironment hdfsEnvironment,
                               int cleanupIntervalHours, int rewriteIntervalHours) {
            this.catalogName = catalogName;
            this.catalog = catalog;
            this.hdfsEnvironment = hdfsEnvironment;
            this.cleanupIntervalHours = cleanupIntervalHours;
            this.rewriteIntervalHours = rewriteIntervalHours;
        }
    }

    public void registerIcebergCatalogForMaintenance(String catalogName, IcebergCatalog catalog,
                                                     HdfsEnvironment hdfsEnvironment,
                                                     int cleanupIntervalHours, int rewriteIntervalHours) {
        maintenanceInfoMap.put(catalogName,
                new IcebergMaintenanceInfo(catalogName, catalog, hdfsEnvironment, cleanupIntervalHours, rewriteIntervalHours));
        LOG.info("Register iceberg catalog {} for auto maintenance: cleanup_interval_hours={}, optimize_interval_hours={}",
                catalogName, cleanupIntervalHours, rewriteIntervalHours);
    }

    public void unRegisterIcebergCatalogForMaintenance(String catalogName) {
        maintenanceInfoMap.remove(catalogName);
        LOG.info("Unregister iceberg catalog {} from auto maintenance", catalogName);
    }

    @Override
    protected void runAfterCatalogReady() {
        // Only run Iceberg metadata maintenance on leader FE.
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }
        // update interval
        if (getInterval() != Config.iceberg_background_check_maintenance_interval_seconds * 1000L) {
            setInterval(Config.iceberg_background_check_maintenance_interval_seconds * 1000L);
        }
        if (maintenanceInfoMap.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        ConnectContext ctx = new ConnectContext();
        for (IcebergMaintenanceInfo info : maintenanceInfoMap.values()) {
            long checkStartMillis = System.currentTimeMillis();
            List<Pair<String, String>> tableNames = listTablesForMaintenance(info.catalog, ctx);
            boolean cleanupDue = info.cleanupIntervalHours > 0
                    && (now - info.lastCleanupTimeMillis) >= info.cleanupIntervalHours * 3600L * 1000L;
            boolean rewriteDue = info.rewriteIntervalHours > 0
                    && (now - info.lastRewriteTimeMillis) >= info.rewriteIntervalHours * 3600L * 1000L;
            IcebergMaintenanceMetricsMgr.recordCheck(info.catalogName, System.currentTimeMillis() - checkStartMillis);
            if (tableNames.isEmpty()) {
                continue;
            }
            if (!cleanupDue && !rewriteDue) {
                continue;
            }
            if (cleanupDue) {
                LOG.info("Start auto maintenance cleanup (expire_snapshots + remove_orphan_files) on iceberg catalog {}",
                        info.catalogName);
                runCleanupForCatalog(info, tableNames);
                info.lastCleanupTimeMillis = now;
                LOG.info("Finish auto maintenance cleanup on iceberg catalog {}", info.catalogName);
            }
            if (rewriteDue) {
                LOG.info("Start auto maintenance rewrite_manifests on iceberg catalog {}", info.catalogName);
                runRewriteForCatalog(info, tableNames);
                info.lastRewriteTimeMillis = now;
                LOG.info("Finish auto maintenance rewrite_manifests on iceberg catalog {}", info.catalogName);
            }
        }
    }

    private List<Pair<String, String>> listTablesForMaintenance(IcebergCatalog catalog, ConnectContext ctx) {
        List<Pair<String, String>> result = Lists.newArrayList();
        try {
            List<String> dbs = catalog.listAllDatabases(ctx);
            for (String db : dbs) {
                try {
                    List<String> tables = catalog.listTables(ctx, db);
                    for (String tbl : tables) {
                        if (isRecentlyWrittenTable(catalog, ctx, db, tbl, System.currentTimeMillis())) {
                            result.add(Pair.create(db, tbl));
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("List tables failed for catalog {} db {}: {}", catalog.toString(), db, e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("List databases failed for catalog {}: {}", catalog.toString(), e.getMessage());
        }
        return result;
    }

    private boolean isRecentlyWrittenTable(IcebergCatalog catalog, ConnectContext ctx, String db, String tbl, long nowMillis) {
        try {
            org.apache.iceberg.Table table = catalog.getTable(ctx, db, tbl);
            if (table == null) {
                return false;
            }

            final long cutoffMillis = nowMillis - RECENT_WRITE_WINDOW_MILLIS;
            List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
            snapshots.sort(Comparator.comparingLong(Snapshot::sequenceNumber).reversed());
            for (Snapshot snapshot : snapshots) {
                Map<String, String> summary = snapshot.summary();
                if (summary == null) {
                    continue;
                }
                String op = summary.get(SNAPSHOT_SUMMARY_OPERATION);
                if (!isWriteOperation(op)) {
                    continue;
                }

                long ts = snapshot.timestampMillis();
                if (ts >= cutoffMillis) {
                    // nowMillis - RECENT_WRITE_WINDOW_MILLIS <= latestWriteMillis
                    return true;
                }

                // We only care about the latest write snapshot. Since we iterate from newest to oldest
                // (by sequence number), once we hit a write snapshot older than cutoff, we can stop.
                break;
            }

            return false;
        } catch (Exception e) {
            LOG.warn("Check iceberg table recent write failed on {}.{}: {}", db, tbl, e.getMessage());
            return false;
        }
    }

    private boolean isWriteOperation(String op) {
        if (op == null) {
            return false;
        }
        return op.equalsIgnoreCase("append")
                || op.equalsIgnoreCase("delete")
                || op.equalsIgnoreCase("overwrite");
    }

    private void runCleanupForCatalog(IcebergMaintenanceInfo info, List<Pair<String, String>> tableNames) {
        List<Future<?>> futures = Lists.newArrayListWithCapacity(tableNames.size());
        List<Pair<String, String>> submittedTables = Lists.newArrayListWithCapacity(tableNames.size());
        for (Pair<String, String> name : tableNames) {
            try {
                futures.add(maintenanceExecutor.submit(() -> {
                    org.apache.iceberg.Table table = getTableForMaintenance(info, name.first, name.second);
                    if (table == null) {
                        return;
                    }
                    runTableCleanup(info, table, name.first, name.second);
                }));
                submittedTables.add(name);
            } catch (RejectedExecutionException e) {
                LOG.warn("Maintenance queue full, skipping cleanup for remaining tables on catalog {}, "
                        + "current table: {}.{}.{}", info.catalogName, info.catalogName, name.first, name.second);
                break;
            }
        }
        waitForFutures(futures, submittedTables, info.catalogName, "cleanup");
    }

    private void runRewriteForCatalog(IcebergMaintenanceInfo info, List<Pair<String, String>> tableNames) {
        List<Future<?>> futures = Lists.newArrayListWithCapacity(tableNames.size());
        List<Pair<String, String>> submittedTables = Lists.newArrayListWithCapacity(tableNames.size());
        for (Pair<String, String> name : tableNames) {
            try {
                futures.add(maintenanceExecutor.submit(() -> {
                    org.apache.iceberg.Table table = getTableForMaintenance(info, name.first, name.second);
                    if (table == null) {
                        return;
                    }
                    runMaintenanceTask(info.catalogName, name.first, name.second,
                            IcebergMaintenanceMetricsMgr.ACTION_REWRITE_MANIFESTS,
                            stats -> runRewriteManifests(info.catalog, table, info.hdfsEnvironment, stats));
                }));
                submittedTables.add(name);
            } catch (RejectedExecutionException e) {
                LOG.warn("Maintenance queue full, skipping rewrite_manifests for remaining tables on catalog {}, "
                        + "current table: {}.{}.{}", info.catalogName, info.catalogName, name.first, name.second);
                break;
            }
        }
        waitForFutures(futures, submittedTables, info.catalogName, "rewrite_manifests");
    }

    private org.apache.iceberg.Table getTableForMaintenance(IcebergMaintenanceInfo info, String db, String tbl) {
        try {
            org.apache.iceberg.Table table = info.catalog.getTable(new ConnectContext(), db, tbl);
            if (table == null || table.currentSnapshot() == null) {
                return null;
            }
            return table;
        } catch (Exception e) {
            LOG.warn("Get iceberg table for maintenance failed on {}.{}.{}: {}",
                    info.catalogName, db, tbl, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Run expire_snapshots then remove_orphan_files on one table. The two actions are independent:
     * a normal expire failure does not block orphan-file removal on the same table. But if the
     * maintenance thread was cancelled (Future.cancel(true) on a per-table or total deadline), the
     * interrupt flag is re-asserted by {@link #runMaintenanceTask}; we honor it and skip orphan
     * removal instead of scanning/deleting files past the deadline.
     */
    private void runTableCleanup(IcebergMaintenanceInfo info, org.apache.iceberg.Table table,
                                 String db, String tbl) {
        runMaintenanceTask(info.catalogName, db, tbl, IcebergMaintenanceMetricsMgr.ACTION_EXPIRE_SNAPSHOTS,
                stats -> runExpireSnapshots(info.catalog, table, info.hdfsEnvironment, stats));
        if (Thread.currentThread().isInterrupted()) {
            LOG.warn("Cleanup on {}.{}.{} was cancelled after expire_snapshots, skipping remove_orphan_files",
                    info.catalogName, db, tbl);
            return;
        }
        runMaintenanceTask(info.catalogName, db, tbl, IcebergMaintenanceMetricsMgr.ACTION_REMOVE_ORPHAN_FILES,
                stats -> runRemoveOrphanFiles(info.catalog, table, info.hdfsEnvironment, stats));
    }

    /**
     * Run one maintenance action on one table, threading a fresh {@link IcebergMaintenanceTaskStats}
     * into the procedure and recording the outcome into the task history. The run gets one of four
     * statuses: success (changed table state), skipped (ran fine but had nothing to do), failed, or
     * partial. Exceptions are swallowed (logged + recorded) so the remaining actions of the same
     * table can proceed. If the failure was caused by cancellation (Future.cancel(true) interrupts
     * the worker thread), catching the interrupt-derived exception clears the interrupt flag, so we
     * re-assert it and let the caller decide whether to skip the remaining actions.
     */
    private void runMaintenanceTask(String catalog, String db, String tbl, String action,
                                    Consumer<IcebergMaintenanceTaskStats> body) {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        IcebergMaintenanceTaskRecord record = IcebergMaintenanceTaskRecord.start(
                catalog, db, tbl, IcebergMaintenanceTaskRecord.TRIGGER_REASON_SCHEDULE, null);
        String status;
        try {
            body.accept(stats);
            status = stats.hasMaterialChange()
                    ? IcebergMaintenanceTaskRecord.STATUS_SUCCESS : IcebergMaintenanceTaskRecord.STATUS_SKIPPED;
        } catch (Exception e) {
            if (isInterruption(e)) {
                Thread.currentThread().interrupt();
            }
            status = stats.isPartiallyApplied()
                    ? IcebergMaintenanceTaskRecord.STATUS_PARTIAL : IcebergMaintenanceTaskRecord.STATUS_FAILED;
            record.setFailureReason(e.getMessage() != null ? e.getMessage() : e.getClass().getName());
            LOG.warn("Auto maintenance {} failed on {}.{}.{}: {}",
                    action, catalog, db, tbl, e.getMessage(), e);
        }
        record.setStatus(status);
        try {
            IcebergMaintenanceMetricsMgr.recordExecute(catalog, action, status,
                    System.currentTimeMillis() - record.getStartTimeMs());
            IcebergMaintenanceMetricsMgr.reportEffectMetrics(catalog, stats);
            record.finish(stats);
            GlobalStateMgr.getCurrentState().getIcebergMaintenanceTaskHistory().addRecord(record);
        } catch (Exception e) {
            LOG.warn("Record iceberg maintenance task failed on {}.{}.{}", catalog, db, tbl, e);
        }
    }

    /**
     * Whether a swallowed exception was caused by a thread interrupt (cancellation), in which case
     * the JDK typically clears the interrupt flag when the interrupt-derived exception is thrown.
     * The interrupt may surface through the concurrency, classic-IO, or NIO blocking-call surface,
     * and iceberg/Hadoop often wrap it, so we walk the cause chain.
     */
    private static boolean isInterruption(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof InterruptedException || t instanceof ClosedByInterruptException) {
                return true;
            }
            // a genuine interrupt during a Hadoop/classic-IO call can surface as
            // InterruptedIOException (e.g. Hadoop IPC converts InterruptedException to it and
            // clears the interrupt flag); but its SocketTimeoutException subclass is an ordinary
            // I/O timeout, not a cancellation, and must not be treated as one
            if (t instanceof InterruptedIOException && !(t instanceof SocketTimeoutException)) {
                return true;
            }
        }
        return false;
    }

    private void waitForFutures(List<Future<?>> futures, List<Pair<String, String>> tableNames,
                               String catalogName, String taskType) {
        final long perTableTimeoutMillis = PER_TABLE_TIMEOUT_SECONDS * 1000L;
        final long totalTimeoutMillis = Math.max(1L, Config.iceberg_background_check_maintenance_interval_seconds) * 1000L;
        final long deadlineMillis = System.currentTimeMillis() + totalTimeoutMillis;
        for (int i = 0; i < futures.size(); i++) {
            String tableName = catalogName + "." + tableNames.get(i).first + "." + tableNames.get(i).second;
            try {
                long remainingMillis = deadlineMillis - System.currentTimeMillis();
                if (remainingMillis <= 0) {
                    for (int j = i; j < futures.size(); j++) {
                        futures.get(j).cancel(true);
                    }
                    LOG.warn("Total deadline exceeded, cancelled remaining {} tasks on catalog {}, current table: {}",
                            taskType, catalogName, tableName);
                    break;
                }
                long timeoutMillis = Math.min(perTableTimeoutMillis, remainingMillis);
                futures.get(i).get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                futures.get(i).cancel(true);
                if (System.currentTimeMillis() >= deadlineMillis) {
                    for (int j = i + 1; j < futures.size(); j++) {
                        futures.get(j).cancel(true);
                    }
                    LOG.warn("Total deadline exceeded during wait on table {}, cancelled remaining {} tasks on catalog {}",
                            tableName, taskType, catalogName);
                    break;
                }
                LOG.warn("Timeout (per-table {}s) for {} task on table {}, cancelled",
                        PER_TABLE_TIMEOUT_SECONDS, taskType, tableName, te);
            } catch (Exception e) {
                LOG.warn("Unexpected exception while waiting for {} task on table {}", taskType, tableName, e);
            }
        }
    }

    private void runExpireSnapshots(IcebergCatalog catalog, org.apache.iceberg.Table table,
                                    HdfsEnvironment hdfsEnvironment, IcebergMaintenanceTaskStats stats) {
        Transaction txn = table.newTransaction();
        IcebergTableProcedureContext procedureContext = new IcebergTableProcedureContext(
                catalog, table, null, txn, hdfsEnvironment, null, null, icebergPlanWorkerExecutor, stats);
        ExpireSnapshotsProcedure.getInstance().execute(procedureContext, Collections.emptyMap());
        commitAndCollectOutputs(txn, stats);
    }

    private void runRemoveOrphanFiles(IcebergCatalog catalog, org.apache.iceberg.Table table,
                                      HdfsEnvironment hdfsEnvironment, IcebergMaintenanceTaskStats stats) {
        // remove_orphan_files deletes files directly and never touches a transaction
        IcebergTableProcedureContext procedureContext = new IcebergTableProcedureContext(
                catalog, table, null, null, hdfsEnvironment, null, null, null, stats);
        RemoveOrphanFilesProcedure.getInstance().execute(procedureContext, Collections.emptyMap());
    }

    private void runRewriteManifests(IcebergCatalog catalog, org.apache.iceberg.Table table,
                                    HdfsEnvironment hdfsEnvironment, IcebergMaintenanceTaskStats stats) {
        Transaction txn = table.newTransaction();
        IcebergTableProcedureContext procedureContext = new IcebergTableProcedureContext(
                catalog, table, null, txn, hdfsEnvironment, null, null, icebergPlanWorkerExecutor, stats);
        RewriteManifestsProcedure.getInstance().execute(procedureContext, Collections.emptyMap());
        commitAndCollectOutputs(txn, stats);
    }

    /**
     * Publish the staged changes and collect output-side stats.
     * Gated on stats.isExecuted() so that no-op early returns do not commit an empty transaction.
     * The committed flag is set only after a successful publish: it gates the expire/rewrite effect
     * metrics, which must not be reported when the publication fails.
     */
    private void commitAndCollectOutputs(Transaction txn, IcebergMaintenanceTaskStats stats) {
        if (!stats.isExecuted()) {
            return;
        }
        txn.commitTransaction();
        stats.setCommitted(true);
        stats.collectOutputs(txn.table());
    }
}
