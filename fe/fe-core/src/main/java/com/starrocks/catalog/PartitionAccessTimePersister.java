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
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TDumpPartitionAccessTimesRequest;
import com.starrocks.thrift.TDumpPartitionAccessTimesResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPartitionAccessTimeEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Leader-only daemon that gives per-partition {@code LAST_ACCESS_TIME} durability across restart/failover.
 * <p>
 * The leader's in-memory map (in {@link PartitionAccessTimeMgr}) is the authoritative full baseline:
 * <ul>
 *   <li><b>Startup / failover</b> -- the first cycle after becoming leader loads the whole
 *       {@code _statistics_.partition_access_time} table into memory ({@link #loadBaseline()}) and sets the
 *       persisted-watermark to the max timestamp loaded, so the map begins life as a superset of the table.</li>
 *   <li><b>Flush</b> (every cycle) -- drains each alive peer's transient increment (RPC, clears the peer) and
 *       folds it into the authoritative map, then persists only the <i>increment</i>: entries newer than the
 *       watermark (the leader's own recent records) unioned with the just-drained peer entries.</li>
 *   <li><b>Cleanup</b> (rate-limited) -- walks the in-memory map for partitions no longer in the live catalog
 *       and deletes their rows, then removes them from memory.</li>
 * </ul>
 * Everything is gated by {@link Config#enable_collect_partition_access_time}; the cycle is skipped until
 * {@code StatisticsMetaManager} has created the table.
 */
public class PartitionAccessTimePersister extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(PartitionAccessTimePersister.class);

    // Run the (full-scan) cleanup only once per this many flush cycles -- ~12h at the default 60s flush interval.
    @VisibleForTesting
    static final long CLEANUP_EVERY_N_CYCLES = 720;

    private final PartitionAccessTimeStore store;

    private boolean baselineLoaded = false;

    // Max access time already persisted; the flush persists only entries strictly newer than this. Advanced
    // only after a successful upsert. Touched only from the single daemon worker thread.
    private long lastPersistedMaxTs = 0;

    // Flush-cycle counter that rate-limits cleanup; only touched from the single daemon worker thread.
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
    protected void runAfterLeaseValid() {
        if (!Config.enable_collect_partition_access_time) {
            return;
        }
        // StatisticsMetaManager creates the table on its own loop; resume next cycle once it exists.
        if (!tableExists()) {
            return;
        }
        // Pick up the mutable flush interval each cycle (mirrors LoadsHistorySyncer).
        setInterval(intervalMs());
        // One-time full load into the authoritative map for this leadership term; retried next cycle on failure.
        if (!baselineLoaded) {
            loadBaseline();
        }
        flushOnce();
        if (flushCycles++ % CLEANUP_EVERY_N_CYCLES == 0) {
            cleanupOnce();
        }
    }

    /**
     * Reset the leader-session bookkeeping when this leadership term ends (see {@link LeaderDaemon#onStopped()}).
     */
    @Override
    protected void onStopped() {
        baselineLoaded = false;
        lastPersistedMaxTs = 0;
        flushCycles = 0;
    }

    // Flush interval in ms, clamped to >= 1s so a misconfigured 0/negative value can't turn the daemon into a
    // busy loop (LeaderDaemon treats intervalMs <= 0 as no delay).
    private static long intervalMs() {
        return Math.max(1, Config.partition_access_time_flush_interval_sec) * 1000L;
    }

    /**
     * Seed the authoritative in-memory map from the persisted table exactly once per leadership term, and set
     * the watermark to the max timestamp loaded so the first flush does not re-insert the whole baseline. On
     * failure the map is left as-is and the load is retried next cycle (do not start from an empty baseline).
     */
    @VisibleForTesting
    void loadBaseline() {
        try {
            List<TPartitionAccessTimeEntry> baseline = store.loadAll();
            GlobalStateMgr.getCurrentState().getPartitionAccessTimeMgr().mergeEntries(baseline);
            long max = 0;
            for (TPartitionAccessTimeEntry e : baseline) {
                max = Math.max(max, e.getAccess_time_ms());
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
     * Drain every alive peer into the authoritative map, then persist just the increment. See the class doc.
     */
    @VisibleForTesting
    void flushOnce() {
        PartitionAccessTimeMgr mgr = GlobalStateMgr.getCurrentState().getPartitionAccessTimeMgr();
        // Fold each alive peer's transient increment straight into the authoritative map as it arrives (each
        // dump clears the peer), so we never hold every peer's list at once. getOtherFrontends() excludes self
        // by node name; the leader's own records are already in the map.
        List<Frontend> peers = GlobalStateMgr.getCurrentState().getNodeMgr().getOtherFrontends().stream()
                .filter(Frontend::isAlive)
                .collect(Collectors.toList());
        for (Frontend fe : peers) {
            try {
                mgr.mergeEntries(dumpPeer(fe));
            } catch (Exception e) {
                LOG.debug("skip FE {} while flushing partition access times: {}", fe.getHost(), e.getMessage());
            }
        }

        // Increment to persist = every map entry newer than the last persisted watermark: the leader's own
        // recent records plus the peer entries just folded in above (their fresh ts sit above the watermark).
        // A peer entry whose ts already predates the watermark (only possible after a peer stayed alive but
        // unreachable for several cycles) is left in memory for reads and re-persisted when it is next accessed.
        List<TPartitionAccessTimeEntry> increment = mgr.snapshotNewerThan(lastPersistedMaxTs);
        if (increment.isEmpty()) {
            return;
        }

        try {
            store.upsert(increment);
            // Advance the watermark only after the persist succeeds, so a failure retries the same increment.
            long max = lastPersistedMaxTs;
            for (TPartitionAccessTimeEntry e : increment) {
                max = Math.max(max, e.getAccess_time_ms());
            }
            lastPersistedMaxTs = max;
        } catch (Exception e) {
            LOG.warn("failed to persist partition access times, will retry next cycle: {}", e.getMessage());
        }
    }

    /**
     * Delete persisted rows whose {@code (db, table, partition)} no longer resolves in the live catalog, using
     * the in-memory map as the row set (the map is a superset of the table for this leadership term). Deletes
     * the table rows first and only then drops them from memory, so a failed DELETE leaves the keys in memory
     * to be retried next cycle rather than leaking orphan rows.
     */
    @VisibleForTesting
    void cleanupOnce() {
        PartitionAccessTimeMgr mgr = GlobalStateMgr.getCurrentState().getPartitionAccessTimeMgr();
        List<long[]> droppedKeys = new ArrayList<>();
        List<Long> droppedPartitionIds = new ArrayList<>();
        for (long[] key : mgr.collectAllKeys()) {
            if (key.length < 3) {
                continue;
            }
            if (!partitionExists(key[0], key[1], key[2])) {
                droppedKeys.add(key);
                droppedPartitionIds.add(key[2]);
            }
        }
        if (droppedPartitionIds.isEmpty()) {
            return;
        }
        try {
            store.deleteByPartitionIds(droppedPartitionIds);
        } catch (Exception e) {
            LOG.warn("failed to delete dropped partition access time rows, will retry next cycle: {}",
                    e.getMessage());
            return;
        }
        mgr.removePartitions(droppedKeys);
    }

    /** RPC a single peer's dump (which clears the peer's map). Mirrors the {@code ThriftRPCRequestExecutor} idiom. */
    @VisibleForTesting
    List<TPartitionAccessTimeEntry> dumpPeer(Frontend fe) throws TException {
        TDumpPartitionAccessTimesRequest req = new TDumpPartitionAccessTimesRequest();
        TNetworkAddress addr = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
        TDumpPartitionAccessTimesResponse resp = ThriftRPCRequestExecutor.call(
                ThriftConnectionPool.frontendPool, addr, Config.thrift_rpc_timeout_ms,
                client -> client.dumpPartitionAccessTimes(req));
        if (resp == null || resp.getEntries() == null) {
            return Collections.emptyList();
        }
        return resp.getEntries();
    }

    /**
     * Whether the {@code (db, table, partition)} still resolves in the live catalog; a missing table alone is
     * enough to prune. Deliberately lock-free, following the statistics-cleanup convention ("statistics job
     * doesn't lock DB, partition may be dropped, skip it" -- e.g. {@code FullStatisticsCollectJob},
     * {@code MetaQueryJob}): the metastore and {@code OlapTable} partition maps are concurrent, so a
     * {@code null} from {@code getPartition} unambiguously means the partition was dropped and its row can be
     * pruned. Taking a DB/table lock here would be both unnecessary and a needless contention point on the
     * leader's cleanup path.
     */
    @VisibleForTesting
    boolean partitionExists(long dbId, long tableId, long partitionId) {
        // Lock-free metastore lookup (see method doc); a dropped table resolves to null.
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            return false;
        }
        if (!(table instanceof OlapTable)) {
            // Unexpected type for a persisted logical partition: keep the row (only prune on a clear absence).
            return true;
        }
        // Lock-free partition lookup; null == dropped, exactly as the statistics jobs treat it.
        return ((OlapTable) table).getPartition(partitionId) != null;
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
