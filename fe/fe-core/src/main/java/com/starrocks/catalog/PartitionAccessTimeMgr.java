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
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.Pair;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TGetPartitionAccessTimesRequest;
import com.starrocks.thrift.TGetPartitionAccessTimesResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPartitionAccessTimeTableRef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the last time each partition was scanned by a user query (lastAccessTime).
 * <p>
 * In-memory, keyed by {@code dbId -> tableId -> (logicalPartitionId -> ms)} (all three ids are globally
 * unique). The query hot path records with a single lock-free, catalog-free {@code merge} (monotonic max)
 * via {@link #recordAccess}, and reads are lock-free too: the read path just snapshots the relevant tables'
 * inner maps without touching the catalog.
 * <p>
 * Durability across restart/failover is provided out-of-band by {@link PartitionAccessTimePersister}, which
 * runs only on the leader.
 * <p>
 * The read paths (SHOW PARTITIONS and information_schema.partitions_meta) just call {@link #getAccessTimes}
 * and map each logical timestamp onto its physical sub-partition rows using the logical parent already in
 * scope during their {@code for (logical) for (physical)} row-building loop.
 */
public class PartitionAccessTimeMgr {
    private static final Logger LOG = LogManager.getLogger(PartitionAccessTimeMgr.class);

    // dbId -> (tableId -> (logicalPartitionId -> last query access time ms epoch)).
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Long>>> partitionAccessTimes =
            new ConcurrentHashMap<>();

    /**
     * Record that the given logical partitions of {@code (dbId, tableId)} were accessed now.
     */
    public void recordAccess(long dbId, long tableId, Collection<Long> logicalPartitionIds) {
        if (logicalPartitionIds == null || logicalPartitionIds.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        ConcurrentHashMap<Long, Long> perTable = partitionAccessTimes
                .computeIfAbsent(dbId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());
        for (Long partitionId : logicalPartitionIds) {
            if (partitionId != null) {
                perTable.merge(partitionId, now, Math::max);
            }
        }
    }

    /**
     * Cluster-aggregated access times for a single table (SHOW PARTITIONS).
     */
    public Map<Long, Long> getAccessTimes(long dbId, long tableId) {
        TPartitionAccessTimeTableRef ref = new TPartitionAccessTimeTableRef();
        ref.setDb_id(dbId);
        ref.setTable_id(tableId);
        return getAccessTimes(Collections.singletonList(ref));
    }

    /**
     * Cluster-aggregated access times for a batch of tables (logicalPartitionId -&gt; ms), keyed by logical
     * partition id across all tables (ids are globally unique).
     * <p>
     * Assembled purely from memory -- this FE's own records plus a best-effort RPC to every alive peer's
     * {@link #getLocalAccessTimes}; the leader's reply carries the full persisted baseline, so no FE ever
     * queries the internal table on this path.
     * <p>
     * When the merged result is empty AND every peer we actually contacted failed, an empty result would be
     * indistinguishable from "genuinely never accessed", so this raises {@link ErrorCode#ERR_GET_PARTITION_ACCESS_TIME}
     * instead of returning a misleading NULL. A single-FE cluster (or one where every peer is down and thus
     * skipped) contacts no peer, so nothing "failed" and the empty result is honored as "never accessed".
     */
    public Map<Long, Long> getAccessTimes(List<TPartitionAccessTimeTableRef> tables) {
        // Seed with this FE's own records, then fold in the other FEs' (best-effort).
        Map<Long, Long> merged = getLocalAccessTimes(tables);
        if (tables == null || tables.isEmpty()) {
            return merged;
        }
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        Pair<String, Integer> self = state.getNodeMgr().getSelfNode();
        if (self == null) {
            // Node topology not initialized yet (self is unknown): treat as "no peers to contact" rather
            // than a total failure, so a transient startup window does not fail metadata reads.
            return merged;
        }
        String selfHost = self.first;
        List<Frontend> frontends = state.getNodeMgr().getFrontends(null);
        boolean anyRemoteSucceeded = false;
        List<String> failures = new ArrayList<>();
        for (Frontend fe : frontends) {
            if (fe.getHost().equals(selfHost) || !fe.isAlive()) {
                continue;
            }
            try {
                TNetworkAddress addr = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
                TGetPartitionAccessTimesRequest req = new TGetPartitionAccessTimesRequest();
                req.setTables(tables);
                TGetPartitionAccessTimesResponse resp = ThriftRPCRequestExecutor.call(
                        ThriftConnectionPool.frontendPool, addr, Config.thrift_rpc_timeout_ms,
                        client -> client.getPartitionAccessTimes(req));
                anyRemoteSucceeded = true;
                if (resp != null && resp.getPartition_id_to_access_time_ms() != null) {
                    resp.getPartition_id_to_access_time_ms().forEach((pid, ts) -> merged.merge(pid, ts, Math::max));
                }
            } catch (Exception e) {
                LOG.debug("skip FE {} while collecting partition access times: {}", fe.getHost(), e.getMessage());
                failures.add(fe.getHost() + " (" + e.getMessage() + ")");
            }
        }
        // No record anywhere AND every peer we contacted failed (a single-FE cluster / all-peers-down contacts
        // nobody, so `failures` is empty and this does not fire): surface it rather than a misleading NULL.
        if (merged.isEmpty() && !anyRemoteSucceeded && !failures.isEmpty()) {
            throw ErrorReportException.report(ErrorCode.ERR_GET_PARTITION_ACCESS_TIME, String.join(", ", failures));
        }
        return merged;
    }

    /**
     * This FE's local access times for a batch of tables (logicalPartitionId -&gt; ms).
     */
    public Map<Long, Long> getLocalAccessTimes(List<TPartitionAccessTimeTableRef> tables) {
        Map<Long, Long> merged = new HashMap<>();
        if (tables == null) {
            return merged;
        }
        for (TPartitionAccessTimeTableRef ref : tables) {
            getLocalTableAccessTimes(ref.getDb_id(), ref.getTable_id())
                    .forEach((pid, ts) -> merged.merge(pid, ts, Math::max));
        }
        return merged;
    }

    // This FE's local access times for (dbId, tableId) (logicalPartitionId -> ms).
    private Map<Long, Long> getLocalTableAccessTimes(long dbId, long tableId) {
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, Long>> perDb = partitionAccessTimes.get(dbId);
        if (perDb == null) {
            return new HashMap<>();
        }
        ConcurrentHashMap<Long, Long> perTable = perDb.get(tableId);
        return perTable == null ? new HashMap<>() : new HashMap<>(perTable);
    }

    // The recorded access time (ms) for a (dbId, tableId, logicalPartitionId), 0 if none.
    @VisibleForTesting
    public long getLastAccessTime(long dbId, long tableId, long logicalPartitionId) {
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, Long>> perDb = partitionAccessTimes.get(dbId);
        if (perDb == null) {
            return 0L;
        }
        ConcurrentHashMap<Long, Long> perTable = perDb.get(tableId);
        return perTable == null ? 0L : perTable.getOrDefault(logicalPartitionId, 0L);
    }

    /**
     * Max-merge flat entries into the map without clearing anything. The leader uses this both to fold each
     * drained follower's increment into its authoritative map and to seed that map from the persisted table
     * when it becomes leader. Uses the same lock-free {@code merge(max)} as {@link #recordAccess}.
     */
    public void mergeEntries(List<PartitionAccessTimeEntry> entries) {
        if (entries == null) {
            return;
        }
        for (PartitionAccessTimeEntry e : entries) {
            partitionAccessTimes
                    .computeIfAbsent(e.getDbId(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(e.getTableId(), k -> new ConcurrentHashMap<>())
                    .merge(e.getPartitionId(), e.getAccessTimeMs(), Math::max);
        }
    }

    /**
     * Snapshot the map entries whose access time is greater than or equal to {@code sinceInclusiveMs} (no clear).
     * The leader flush uses this with the last persisted max time to persist the increment. The boundary is
     * inclusive so a record whose ts equals the watermark but was not in the batch that advanced it is still
     * persisted; re-persisting an already-stored boundary entry is harmless under the table's MAX aggregate.
     * Weakly consistent iteration over the concurrent map -- safe against a concurrent {@link #recordAccess}.
     */
    public List<PartitionAccessTimeEntry> snapshotSince(long sinceInclusiveMs) {
        List<PartitionAccessTimeEntry> out = new ArrayList<>();
        for (Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Long>>> dbE : partitionAccessTimes.entrySet()) {
            for (Map.Entry<Long, ConcurrentHashMap<Long, Long>> tblE : dbE.getValue().entrySet()) {
                for (Map.Entry<Long, Long> pE : tblE.getValue().entrySet()) {
                    if (pE.getValue() >= sinceInclusiveMs) {
                        out.add(new PartitionAccessTimeEntry(dbE.getKey(), tblE.getKey(), pE.getKey(), pE.getValue()));
                    }
                }
            }
        }
        return out;
    }

    /**
     * Snapshot every {@code (dbId, tableId, logicalPartitionId)} key currently in the map (no clear). The
     * leader cleanup walks these to find rows whose partition was dropped from the live catalog.
     */
    public List<long[]> collectAllKeys() {
        List<long[]> out = new ArrayList<>();
        for (Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Long>>> dbE : partitionAccessTimes.entrySet()) {
            for (Map.Entry<Long, ConcurrentHashMap<Long, Long>> tblE : dbE.getValue().entrySet()) {
                for (Long pid : tblE.getValue().keySet()) {
                    out.add(new long[] {dbE.getKey(), tblE.getKey(), pid});
                }
            }
        }
        return out;
    }

    /**
     * Remove the given {@code (dbId, tableId, logicalPartitionId)} keys from the map (leader cleanup, in step
     * with the corresponding table DELETE). Empty inner maps are trimmed. A concurrent {@link #recordAccess}
     * that re-adds a just-dropped partition is harmless -- the next cleanup cycle prunes it again.
     */
    public void removePartitions(Collection<long[]> keys) {
        if (keys == null) {
            return;
        }
        for (long[] key : keys) {
            if (key.length < 3) {
                continue;
            }
            long dbId = key[0];
            long tableId = key[1];
            ConcurrentHashMap<Long, ConcurrentHashMap<Long, Long>> perDb = partitionAccessTimes.get(dbId);
            if (perDb == null) {
                continue;
            }
            ConcurrentHashMap<Long, Long> perTable = perDb.get(tableId);
            if (perTable == null) {
                continue;
            }
            perTable.remove(key[2]);
            perDb.computeIfPresent(tableId, (k, v) -> v.isEmpty() ? null : v);
            partitionAccessTimes.computeIfPresent(dbId, (k, v) -> v.isEmpty() ? null : v);
        }
    }
}
