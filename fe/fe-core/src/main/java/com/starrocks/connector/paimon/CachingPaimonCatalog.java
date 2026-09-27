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

package com.starrocks.connector.paimon;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * Paimon's caching catalog plus the bookkeeping refresh needs to prune its work: per-table last
 * access time, and the snapshot / schema revision each table was last refreshed at. An access also
 * queues a refresh of that table once the interval has elapsed, the way Caffeine's
 * refreshAfterWrite does for iceberg. Mirrors CachingIcebergCatalog.
 */
public class CachingPaimonCatalog extends CachingCatalog {
    private static final Logger LOG = LogManager.getLogger(CachingPaimonCatalog.class);

    private final String catalogName;
    private final ExecutorService refreshExecutor;
    private final long refreshIntervalSec;
    private final Map<Identifier, Long> tableLatestAccessTime = new ConcurrentHashMap<>();
    private final Map<Identifier, TableRevision> lastRefreshedRevision = new ConcurrentHashMap<>();
    private final Map<Identifier, Long> lastRefreshTime = new ConcurrentHashMap<>();
    private final Set<Identifier> refreshInFlight = ConcurrentHashMap.newKeySet();

    /** What the lake looked like when a table was last refreshed. */
    private record TableRevision(long snapshotId, long schemaId) {
    }

    public CachingPaimonCatalog(String catalogName, Catalog wrapped, Options options, ExecutorService refreshExecutor,
                                long refreshIntervalSec) {
        super(wrapped, options);
        this.catalogName = catalogName;
        this.refreshExecutor = refreshExecutor;
        this.refreshIntervalSec = refreshIntervalSec;
    }

    @Override
    public Table getTable(Identifier id) throws Catalog.TableNotExistException {
        Table table = super.getTable(id);
        // a system table has no snapshot of its own, a branch/tag pins a fixed version: neither goes stale
        if (!id.isSystemTable() && id.getBranchName() == null) {
            tableLatestAccessTime.put(id, System.currentTimeMillis());
            maybeRefreshAsync(id);
        }
        return table;
    }

    @Override
    public void invalidateTable(Identifier id) {
        super.invalidateTable(id);
        // the revision described the evicted entry, not the next one
        lastRefreshedRevision.remove(id);
        lastRefreshTime.remove(id);
    }

    /**
     * Queue a refresh for a table that has not been looked at for a while. Only the interval check
     * runs on the caller's thread; reading the lake and comparing revisions happens on the pool, so
     * an access never waits and never adds an RPC of its own.
     */
    @VisibleForTesting
    void maybeRefreshAsync(Identifier id) {
        // zero or less turns this off, as a non-positive interval turns off Caffeine's refreshAfterWrite for iceberg
        if (refreshIntervalSec <= 0) {
            return;
        }
        Long refreshedAt = lastRefreshTime.get(id);
        if (refreshedAt != null && System.currentTimeMillis() - refreshedAt < refreshIntervalSec * 1000) {
            return;
        }
        if (!refreshInFlight.add(id)) {
            return;
        }
        try {
            refreshExecutor.submit(() -> {
                try {
                    refreshTable(id);
                } finally {
                    refreshInFlight.remove(id);
                }
            });
        } catch (RejectedExecutionException e) {
            refreshInFlight.remove(id);
            LOG.debug("Dropped queued refresh of paimon table {} of catalog {}", id.getFullName(), catalogName);
        }
    }

    /** Refresh one table if the lake moved. Background daemon and query-triggered refresh only. */
    public void refreshTable(Identifier id) {
        try {
            // via super: probing must not count as an access
            Table table = super.getTable(id);
            if (!(table instanceof DataTable)) {
                return;
            }
            DataTable dataTable = (DataTable) table;
            Long latestSnapshotId = dataTable.snapshotManager().latestSnapshotId();
            if (latestSnapshotId == null) {
                return;
            }
            // a schema-only ALTER TABLE bumps the schema id without creating a snapshot
            long latestSchemaId = dataTable.schemaManager().latest().map(TableSchema::id).orElse(-1L);
            TableRevision latest = new TableRevision(latestSnapshotId, latestSchemaId);
            // stamped even when nothing moved, and by the daemon too: it means "the lake was just probed",
            // which is what the query-triggered interval should be measured from
            lastRefreshTime.put(id, System.currentTimeMillis());
            if (latest.equals(lastRefreshedRevision.get(id))) {
                return;
            }

            super.invalidateTable(id);
            super.getTable(id); // repopulate, so queries don't pay for the reload
            super.refreshPartitions(id);
            lastRefreshedRevision.put(id, latest);
            LOG.debug("Refreshed paimon table {} of catalog {} to snapshot {} schema {}",
                    id.getFullName(), catalogName, latestSnapshotId, latestSchemaId);
        } catch (Exception e) {
            LOG.warn("Failed to refresh paimon table {} of catalog {}, evict it",
                    id.getFullName(), catalogName, e);
            invalidateTable(id);
            tableLatestAccessTime.remove(id);
        }
    }

    public void refreshCatalog() {
        // negative means never expire, as in CachingHiveMetastore
        long idleWindowSec = Config.background_refresh_metadata_time_secs_since_last_access_secs;
        for (Map.Entry<Identifier, Long> entry : tableLatestAccessTime.entrySet()) {
            long idleSec = (System.currentTimeMillis() - entry.getValue()) / 1000;
            if (idleWindowSec >= 0 && idleSec > idleWindowSec) {
                invalidateTable(entry.getKey());
                tableLatestAccessTime.remove(entry.getKey());
                continue;
            }
            if (!refreshInFlight.contains(entry.getKey())) {
                refreshTable(entry.getKey());
            }
        }
    }

    @VisibleForTesting
    Map<Identifier, Long> getTableLatestAccessTime() {
        return tableLatestAccessTime;
    }

    @VisibleForTesting
    Map<Identifier, TableRevision> getLastRefreshedRevision() {
        return lastRefreshedRevision;
    }

    @VisibleForTesting
    static TableRevision revision(long snapshotId, long schemaId) {
        return new TableRevision(snapshotId, schemaId);
    }
}
