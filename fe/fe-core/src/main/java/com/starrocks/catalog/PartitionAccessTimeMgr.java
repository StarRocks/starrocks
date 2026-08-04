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
 * Pure in-memory: the timestamps live in {@link #partitionAccessTimes} on this FE and are NOT persisted
 * (no {@code @SerializedName}, no edit log), so they reset on FE restart/failover. Keyed by
 * {@code dbId -> tableId -> (logicalPartitionId -> ms)}; all three ids are globally unique. The query hot path
 * records with a single lock-free, catalog-free {@code merge} (monotonic max) via {@link #recordAccess},
 * and reads are lock-free too: the read path just snapshots the relevant tables' inner maps without
 * touching the catalog or walking the partition structure.
 * <p>
 * A cluster-consistent value is assembled entirely inside {@link #getAccessTimes}: each FE only records the
 * queries it coordinates, so it folds this FE's own records together with the other FEs' (a best-effort
 * cross-FE RPC that lands in each peer's {@link #getLocalAccessTimes}), all keyed by logical partition id,
 * and raises {@link ErrorCode#ERR_GET_PARTITION_ACCESS_TIME} when there is nothing to show and every peer
 * failed. The read paths (SHOW PARTITIONS and information_schema.partitions_meta) just call it and map each
 * logical timestamp onto its physical sub-partition rows using the logical parent already in scope during
 * their {@code for (logical) for (physical)} row-building loop -- no reverse lookup, and no lock is needed
 * for the access-time column.
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
}
