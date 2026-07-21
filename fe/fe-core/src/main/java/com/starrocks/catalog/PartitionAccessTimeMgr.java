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

import com.starrocks.common.Config;
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tracks the last time each partition was scanned by a query (lastAccessTime).
 * <p>
 * Pure in-memory: the timestamp lives on {@link PhysicalPartition#getLastAccessTime()} and is NOT
 * persisted (no {@code @SerializedName}, no edit log), so it resets to 0 on FE restart/failover.
 * Updates are monotonic (max-merge).
 * <p>
 * A cluster-consistent value is assembled by the read paths (SHOW PARTITIONS and
 * information_schema.partitions_meta): each FE only records the queries it coordinates, so a reader
 * first calls {@link #getRemoteAccessTimes} to collect the other FEs' values <b>before</b> taking the
 * table lock (the cross-FE RPC must never run while holding a metadata lock), then, <b>while holding
 * the table READ lock</b>, merges in {@link #getLocalTableAccessTimes} (which walks this FE's
 * partitions and therefore relies on the caller already holding that lock).
 */
public class PartitionAccessTimeMgr {
    private static final Logger LOG = LogManager.getLogger(PartitionAccessTimeMgr.class);

    /**
     * Record that the given logical partitions of {@code table} were accessed now. Only updates the
     * live in-memory field on this FE; no dbId is needed here.
     */
    public void recordAccess(OlapTable table, Collection<Long> logicalPartitionIds) {
        if (table == null || logicalPartitionIds == null) {
            return;
        }
        long now = System.currentTimeMillis();
        // Best-effort, lock-free update on the query hot path: recording the access time must never
        // fail the query. getSubPartitions() exposes a live HashMap view, so a concurrent ALTER / ADD /
        // DROP PARTITION (a structural modification made under the table WRITE lock) can make this
        // iteration throw ConcurrentModificationException. Swallow it: a missed update is harmless.
        try {
            for (Long partitionId : logicalPartitionIds) {
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                for (PhysicalPartition pp : partition.getSubPartitions()) {
                    pp.updateLastAccessTime(now);
                }
            }
        } catch (Exception e) {
            LOG.debug("skip recording partition access time for table {}: {}", table.getName(), e.getMessage());
        }
    }

    /**
     * Cross-FE aggregated (best-effort) access times for a single table (SHOW PARTITIONS): delegates
     * to the batch form with a one-element list.
     */
    public Map<Long, Long> getRemoteAccessTimes(long dbId, long tableId) {
        TPartitionAccessTimeTableRef ref = new TPartitionAccessTimeTableRef();
        ref.setDb_id(dbId);
        ref.setTable_id(tableId);
        return getRemoteAccessTimes(Collections.singletonList(ref));
    }

    /**
     * Cross-FE aggregated (best-effort) access times for a batch of tables (partitions_meta). Sends
     * ONE RPC per FE covering all tables instead of one RPC per table, so a meta scan over N tables
     * costs O(FEs) round-trips rather than O(N * FEs). Partition ids are globally unique, so the merged
     * result is keyed by physicalPartitionId across all tables.
     */
    public Map<Long, Long> getRemoteAccessTimes(List<TPartitionAccessTimeTableRef> tables) {
        Map<Long, Long> merged = new HashMap<>();
        if (tables == null || tables.isEmpty()) {
            return merged;
        }
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        Pair<String, Integer> self = state.getNodeMgr().getSelfNode();
        if (self == null) {
            // Node topology not initialized yet (self is unknown): skip cross-FE collection so we do
            // not RPC ourselves. Local access times are merged in separately by the caller.
            return merged;
        }
        String selfHost = self.first;
        List<Frontend> frontends = state.getNodeMgr().getFrontends(null);
        for (Frontend fe : frontends) {
            // Skip self and dead-but-registered FEs: a not-alive FE would otherwise block on the RPC
            // until timeout, amplifying the stall across the whole batch.
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
                if (resp != null && resp.getPartition_id_to_access_time_ms() != null) {
                    resp.getPartition_id_to_access_time_ms().forEach((pid, ts) -> merged.merge(pid, ts, Math::max));
                }
            } catch (Exception e) {
                LOG.debug("skip FE {} while collecting partition access times: {}", fe.getHost(), e.getMessage());
            }
        }
        return merged;
    }

    /**
     * This FE's local access times for the given table (physicalPartitionId -&gt; ms, non-zero only).
     */
    public Map<Long, Long> getLocalTableAccessTimes(long dbId, long tableId) {
        Map<Long, Long> result = new HashMap<>();
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (!(table instanceof OlapTable)) {
            return result;
        }
        for (Partition partition : ((OlapTable) table).getAllPartitions()) {
            for (PhysicalPartition pp : partition.getSubPartitions()) {
                long accessTime = pp.getLastAccessTime();
                if (accessTime > 0) {
                    result.put(pp.getId(), accessTime);
                }
            }
        }
        return result;
    }
}
