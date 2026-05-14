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

package com.starrocks.lake.bookmark;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.server.GlobalStateMgr;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * An immutable copy of one OlapTable's base-index physical partitions at a
 * single moment in time.
 *
 * <p>Identity: {@code bookmarkId} identifies the bookmark within its table —
 * monotonic, with gaps allowed; ordering across tables has no meaning.
 * {@code bookmarkTimeMs} is the bookmark's creation time, and its partition
 * meta reflects the table state at that moment. Timestamp-based lookups
 * compare against it; it is not the time of the DML or DDL that produced
 * that state.
 *
 * <p>Per-partition fields, in the bookmark: {@code baseMaterializedIndexId},
 * {@code baseMaterializedIndexMetaId}, {@code visibleVersion}.
 * {@code visibleVersionTimeMs} is kept for diagnostics and is not used by
 * dedup or timestamp lookup.
 *
 * <p>Fields not present (callers that need these read them off the live
 * table): schema, rollup / non-base indexes, bucket layout, tablet placement,
 * statistics, temporary partitions.
 *
 * <p>Retention while the bookmark holds a reference:
 * <ul>
 *   <li>On each partition that stays in the live table with the same base
 *       index identity, vacuum keeps the bookmark's {@code visibleVersion}
 *       and every newer version reachable.</li>
 *   <li>If the partition is dropped, is rewritten (data-rewrite schema
 *       change), or is resharded (tablet split / merge), the floor stops
 *       applying — the data is reclaimed by that operation itself
 *       (recycle-bin sweep on drop, schema-change cleanup, reshard cleanup),
 *       not by vacuum.</li>
 * </ul>
 */
public final class Bookmark {
    @SerializedName("db")
    private final long dbId;
    @SerializedName("t")
    private final long tableId;
    @SerializedName("b")
    private final long bookmarkId;
    @SerializedName("bt")
    private final long bookmarkTimeMs;
    /**
     * Per-partition meta indexed by (logical partition id, physical partition
     * id); a logical partition can hold multiple physical sub-partitions.
     */
    @SerializedName("p")
    private final Map<Long, Map<Long, PhysicalPartitionMeta>> partitionsMeta;

    public Bookmark(long dbId, long tableId, long bookmarkId, long bookmarkTimeMs,
                    Map<Long, Map<Long, PhysicalPartitionMeta>> partitionsMeta) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.bookmarkId = bookmarkId;
        this.bookmarkTimeMs = bookmarkTimeMs;
        this.partitionsMeta = partitionsMeta;
    }

    /**
     * Build a bookmark from the table's current state. Caller must hold the
     * table read-lock so the partition meta read here stays stable.
     */
    public static Bookmark fromTable(long dbId, OlapTable table) {
        long bookmarkId = GlobalStateMgr.getCurrentState().getNextId();
        long bookmarkTimeMs = System.currentTimeMillis();
        Map<Long, Map<Long, PhysicalPartitionMeta>> parts = new HashMap<>();
        for (Partition p : table.getPartitions()) {
            Map<Long, PhysicalPartitionMeta> inner = new HashMap<>();
            for (PhysicalPartition pp : p.getSubPartitions()) {
                MaterializedIndex base = pp.getLatestBaseIndex();
                inner.put(pp.getId(), new PhysicalPartitionMeta(
                        base.getId(), base.getMetaId(),
                        pp.getVisibleVersion(), pp.getVisibleVersionTime()));
            }
            parts.put(p.getId(), inner);
        }
        return new Bookmark(dbId, table.getId(), bookmarkId, bookmarkTimeMs, parts);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getBookmarkId() {
        return bookmarkId;
    }

    public long getBookmarkTimeMs() {
        return bookmarkTimeMs;
    }

    /**
     * Returns the bookmark's partition meta. The returned map and its inner
     * maps are the bookmark's internal state — callers must treat them as
     * read-only. Mutating them corrupts dedup and vacuum-fence decisions for
     * any live bookmark sharing the partition meta.
     */
    public Map<Long, Map<Long, PhysicalPartitionMeta>> getPartitionsMeta() {
        return partitionsMeta;
    }

    public Optional<PhysicalPartitionMeta> getPhysicalPartitionMeta(long logicalPartitionId, long physicalPartitionId) {
        Map<Long, PhysicalPartitionMeta> inner = partitionsMeta.get(logicalPartitionId);
        if (inner == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(inner.get(physicalPartitionId));
    }

    public Optional<Long> getPhysicalPartitionVersion(long logicalPartitionId, long physicalPartitionId) {
        return getPhysicalPartitionMeta(logicalPartitionId, physicalPartitionId)
                .map(PhysicalPartitionMeta::getVisibleVersion);
    }

    public int getLogicalPartitionCount() {
        return partitionsMeta.size();
    }

    public int getPhysicalPartitionCount() {
        return partitionsMeta.values().stream().mapToInt(Map::size).sum();
    }
}
