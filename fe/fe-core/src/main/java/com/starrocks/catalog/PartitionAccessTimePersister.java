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

package com.starrocks.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.statistic.StatsConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Daemon that runs on <b>every</b> FE and gives per-partition {@code LAST_ACCESS_TIME} durability across
 * restart/failover.
 * <ul>
 *   <li><b>Flush</b> (every FE, every cycle) -- persists this FE's own in-memory increment (entries at or newer
 *       than its last persisted watermark) into {@code _statistics_.partition_access_time}. The table is an
 *       aggregate table keyed on (db, table, partition) with a {@code MAX} timestamp, so blind per-FE INSERTs
 *       merge into the newest value with no cross-FE coordination.</li>
 *   <li><b>Baseline load</b> (leader only) -- while this FE is the leader it loads the whole table into its
 *       in-memory map once ({@link #loadBaseline()}); that copy is the historical baseline the read path serves
 *       (reads merge every FE's map over the {@code getPartitionAccessTimes} RPC). Followers do not load it, and
 *       the load is re-armed whenever this FE is not leader so a promotion reloads a fresh baseline.</li>
 *   <li><b>Memory cleanup</b> (every FE, rate-limited) -- evicts from this FE's own map the partitions that no
 *       longer resolve anywhere ({@link #cleanupMemory()}); it never touches the table.</li>
 *   <li><b>Table cleanup</b> (leader only, rate-limited) -- full-scans the table and deletes rows whose partition
 *       is gone ({@link #cleanupTable()}).</li>
 * </ul>
 * Everything is gated by {@link Config#enable_collect_partition_access_time}; the cycle is skipped until
 * {@code StatisticsMetaManager} has created the table.
 */
public class PartitionAccessTimePersister extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(PartitionAccessTimePersister.class);

    // Run the (full-scan) cleanup only once per this many cycles -- ~12h at the default 600s flush interval.
    @VisibleForTesting
    static final long CLEANUP_EVERY_N_CYCLES = 72;

    private final PartitionAccessTimeStore store;

    // Leader-only: whether this FE (as leader) has loaded the persisted baseline into memory. Forced false while
    // not leader so a promotion reloads. Touched only from the single daemon worker thread.
    private boolean baselineLoaded = false;

    // Max access time this FE has already persisted; the flush persists only entries at or newer than this.
    // Advanced only after a successful upsert. Touched only from the single daemon worker thread.
    private long lastPersistedMaxTs = 0;

    // Cycle counter that rate-limits cleanup; only touched from the single daemon worker thread.
    private long flushCycles = 0;

    public PartitionAccessTimePersister() {
        this(new PartitionAccessTimeStore());
    }

    @VisibleForTesting
    PartitionAccessTimePersister(PartitionAccessTimeStore store) {
        super("partition-access-time-persister", intervalMs());
        this.store = store;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.enable_collect_partition_access_time) {
            return;
        }
        // StatisticsMetaManager creates the table on its own loop; resume next cycle once it exists.
        if (!tableExists()) {
            return;
        }
        // Pick up the mutable flush interval each cycle (mirrors LoadsHistorySyncer).
        setInterval(intervalMs());

        boolean isLeader = GlobalStateMgr.getCurrentState().isLeader();
        // Only the leader seeds its map from the table (the read path's baseline). Re-arm the load whenever this
        // FE is not leader, so a later promotion rebuilds a fresh baseline instead of trusting a stale one.
        if (!isLeader) {
            baselineLoaded = false;
        } else if (!baselineLoaded) {
            loadBaseline();
        }

        // Every FE persists its own recorded increment; the table's MAX aggregate merges all FEs.
        flushOnce();

        if (flushCycles++ % CLEANUP_EVERY_N_CYCLES == 0) {
            // Every FE trims its own memory; only the leader GCs the shared table.
            cleanupMemory();
            if (isLeader) {
                cleanupTable();
            }
        }
    }

    // Flush interval in ms, clamped to >= 1s so a misconfigured 0/negative value can't turn the daemon into a
    // busy loop (Daemon.run sleeps for getInterval() between cycles).
    private static long intervalMs() {
        return Math.max(1, Config.partition_access_time_flush_interval_sec) * 1000L;
    }

    /**
     * Seed this (leader) FE's in-memory map from the persisted table exactly once per leadership term, and set the
     * watermark to the max timestamp loaded so the first flush does not re-insert the whole baseline. On failure
     * the map is left as-is and the load is retried next cycle (do not start from an empty baseline).
     */
    @VisibleForTesting
    void loadBaseline() {
        try {
            List<PartitionAccessTimeEntry> baseline = store.loadAll();
            GlobalStateMgr.getCurrentState().getPartitionAccessTimeMgr().mergeEntries(baseline);
            long max = 0;
            for (PartitionAccessTimeEntry e : baseline) {
                max = Math.max(max, e.getAccessTimeMs());
            }
            lastPersistedMaxTs = max;
            baselineLoaded = true;
            LOG.info("loaded {} persisted partition access time rows into memory (watermark={})",
                    baseline.size(), max);
        } catch (Exception e) {
            LOG.warn("failed to load partition access time baseline, will retry next cycle: {}", e.getMessage());
        }
    }

    /**
     * Persist this FE's own increment -- the map entries at or newer than its last persisted watermark. The
     * boundary is inclusive so an entry whose ts equals the watermark but was not in the batch that advanced it is
     * still persisted (a harmless re-write under the table's MAX aggregate, which also merges every FE's writes).
     * Nothing is drained from memory here; the read path keeps serving from it.
     */
    @VisibleForTesting
    void flushOnce() {
        PartitionAccessTimeMgr mgr = GlobalStateMgr.getCurrentState().getPartitionAccessTimeMgr();
        List<PartitionAccessTimeEntry> increment = mgr.snapshotSince(lastPersistedMaxTs);
        if (increment.isEmpty()) {
            return;
        }
        try {
            store.upsert(increment);
            // Advance the watermark only after the persist succeeds, so a failure retries the same increment.
            long max = lastPersistedMaxTs;
            for (PartitionAccessTimeEntry e : increment) {
                max = Math.max(max, e.getAccessTimeMs());
            }
            lastPersistedMaxTs = max;
        } catch (Exception e) {
            LOG.warn("failed to persist partition access times, will retry next cycle: {}", e.getMessage());
        }
    }

    /**
     * Evict from THIS FE's in-memory map the partitions that no longer resolve anywhere (live catalog or recycle
     * bin). Runs on every FE against its own map and never touches the table (that is the leader's job in
     * {@link #cleanupTable()}); it just keeps memory from growing without bound as partitions are dropped.
     */
    @VisibleForTesting
    void cleanupMemory() {
        PartitionAccessTimeMgr mgr = GlobalStateMgr.getCurrentState().getPartitionAccessTimeMgr();
        List<long[]> droppedKeys = new ArrayList<>();
        for (long[] key : mgr.collectAllKeys()) {
            if (key.length < 3) {
                continue;
            }
            if (!partitionExists(key[0], key[1], key[2])) {
                droppedKeys.add(key);
            }
        }
        if (!droppedKeys.isEmpty()) {
            mgr.removePartitions(droppedKeys);
        }
    }

    /**
     * Leader-only: delete persisted rows whose partition no longer resolves anywhere. A full {@code SELECT} of the
     * table ({@link PartitionAccessTimeStore#loadAll()}) is the authoritative row set -- the in-memory map only
     * holds this FE's own writes plus the baseline, not every FE's, so it is not a superset of the table. Re-checks
     * leadership immediately before the {@code DELETE} so a demotion landing mid-scan cannot delete under a
     * leadership this FE has already lost. A failed load/delete just retries next cleanup (best-effort GC).
     */
    @VisibleForTesting
    void cleanupTable() {
        List<PartitionAccessTimeEntry> persisted;
        try {
            persisted = store.loadAll();
        } catch (Exception e) {
            LOG.warn("failed to load persisted partition access times for cleanup, will retry next cycle: {}",
                    e.getMessage());
            return;
        }
        List<Long> droppedPartitionIds = new ArrayList<>();
        for (PartitionAccessTimeEntry e : persisted) {
            if (!partitionExists(e.getDbId(), e.getTableId(), e.getPartitionId())) {
                droppedPartitionIds.add(e.getPartitionId());
            }
        }
        if (droppedPartitionIds.isEmpty()) {
            return;
        }
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }
        try {
            store.deleteByPartitionIds(droppedPartitionIds);
        } catch (Exception e) {
            LOG.warn("failed to delete dropped partition access time rows, will retry next cycle: {}",
                    e.getMessage());
        }
    }

    /**
     * Whether the {@code (db, table, partition)} still resolves to an existing partition -- either live in the
     * catalog or recoverable from the {@link CatalogRecycleBin}. Only a clear absence of the exact partition from
     * every layer prunes the row, so a {@code RECOVER PARTITION}/{@code TABLE}/{@code DATABASE} keeps its persisted
     * access times, while a partition that is truly gone (e.g. individually dropped and GC'd before its table was
     * dropped) is not kept forever as an orphan.
     *
     * <p>Follows the statistics-cleanup convention of not taking a DB/table metadata lock ("statistics job doesn't
     * lock DB, partition may be dropped, skip it" -- e.g. {@code FullStatisticsCollectJob}, {@code MetaQueryJob}): a
     * point lookup racing a concurrent drop/create only yields a stale answer, and a spurious prune of a still-live
     * partition self-heals (its next scan re-records and re-persists the access time).
     */
    @VisibleForTesting
    boolean partitionExists(long dbId, long tableId, long partitionId) {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = metastore.getDbIncludeRecycleBin(dbId);
        if (db == null) {
            return false;
        }
        Table table = metastore.getTableIncludeRecycleBin(db, tableId);
        if (table == null) {
            return false;
        }
        if (!(table instanceof OlapTable)) {
            // An unexpected non-OlapTable type keeps the row defensively (prune only on clear absence).
            return true;
        }
        return metastore.getPartitionIncludeRecycleBin((OlapTable) table, partitionId) != null;
    }

    /** Whether {@code _statistics_.partition_access_time} exists yet (mirrors {@code StatisticsMetaManager}). */
    @VisibleForTesting
    boolean tableExists() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(StatsConstants.STATISTICS_DB_NAME);
        if (db == null) {
            return false;
        }
        return GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), StatsConstants.PARTITION_ACCESS_TIME_TABLE_NAME) != null;
    }
}
