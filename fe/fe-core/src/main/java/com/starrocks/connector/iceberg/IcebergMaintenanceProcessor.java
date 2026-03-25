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
import com.starrocks.connector.iceberg.procedure.IcebergTableProcedureContext;
import com.starrocks.connector.iceberg.procedure.RemoveOrphanFilesProcedure;
import com.starrocks.connector.iceberg.procedure.RewriteManifestsProcedure;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Transaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private final ConcurrentHashMap<String, IcebergMaintenanceInfo> maintenanceInfoMap = new ConcurrentHashMap<>();
    private final ExecutorService maintenanceExecutor;

    public IcebergMaintenanceProcessor() {
        super(IcebergMaintenanceProcessor.class.getName(),
                Config.iceberg_background_check_maintenance_interval_seconds * 1000L);

        this.maintenanceExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                MAINTENANCE_THREAD_NUM,
                MAINTENANCE_QUEUE_SIZE,
                "iceberg-maintenance-pool",
                true);
    }

    public void shutdown() {
        setStop();
        maintenanceExecutor.shutdown();
        try {
            if (!maintenanceExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                LOG.warn("Executor for auto maintenance did not terminate in time, forcing shutdown");
                maintenanceExecutor.shutdownNow();
                if (!maintenanceExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOG.warn("Executor for auto maintenance did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            maintenanceExecutor.shutdownNow();
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
            List<Pair<String, String>> tableNames = listTablesForMaintenance(info.catalog, ctx);
            if (tableNames.isEmpty()) {
                continue;
            }
            boolean cleanupDue = info.cleanupIntervalHours > 0
                    && (now - info.lastCleanupTimeMillis) >= info.cleanupIntervalHours * 3600L * 1000L;
            boolean rewriteDue = info.rewriteIntervalHours > 0
                    && (now - info.lastRewriteTimeMillis) >= info.rewriteIntervalHours * 3600L * 1000L;
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
                    ConnectContext taskCtx = new ConnectContext();
                    try {
                        org.apache.iceberg.Table table = info.catalog.getTable(taskCtx, name.first, name.second);
                        if (table == null || table.currentSnapshot() == null) {
                            return;
                        }
                        runExpireSnapshots(info.catalog, table, info.hdfsEnvironment);
                        runRemoveOrphanFiles(info.catalog, table, info.hdfsEnvironment);
                    } catch (Exception e) {
                        LOG.warn("Auto maintenance cleanup failed on {}.{}.{}: {}",
                                info.catalogName, name.first, name.second, e.getMessage(), e);
                    }
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
                    ConnectContext taskCtx = new ConnectContext();
                    try {
                        org.apache.iceberg.Table table = info.catalog.getTable(taskCtx, name.first, name.second);
                        if (table == null || table.currentSnapshot() == null) {
                            return;
                        }
                        runRewriteManifests(info.catalog, table, info.hdfsEnvironment);
                    } catch (Exception e) {
                        LOG.warn("Auto maintenance rewrite_manifests failed on {}.{}.{}: {}",
                                info.catalogName, name.first, name.second, e.getMessage(), e);
                    }
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
                                    HdfsEnvironment hdfsEnvironment) {
        Transaction txn = table.newTransaction();
        IcebergTableProcedureContext procedureContext = new IcebergTableProcedureContext(
                catalog, table, null, txn, hdfsEnvironment, null, null);
        ExpireSnapshotsProcedure.getInstance().execute(procedureContext, Collections.emptyMap());
    }

    private void runRemoveOrphanFiles(IcebergCatalog catalog, org.apache.iceberg.Table table,
                                      HdfsEnvironment hdfsEnvironment) {
        Transaction txn = table.newTransaction();
        IcebergTableProcedureContext procedureContext = new IcebergTableProcedureContext(
                catalog, table, null, txn, hdfsEnvironment, null, null);
        RemoveOrphanFilesProcedure.getInstance().execute(procedureContext, Collections.emptyMap());
    }

    private void runRewriteManifests(IcebergCatalog catalog, org.apache.iceberg.Table table,
                                    HdfsEnvironment hdfsEnvironment) {
        Transaction txn = table.newTransaction();
        IcebergTableProcedureContext procedureContext = new IcebergTableProcedureContext(
                catalog, table, null, txn, hdfsEnvironment, null, null);
        RewriteManifestsProcedure.getInstance().execute(procedureContext, Collections.emptyMap());
    }
}
