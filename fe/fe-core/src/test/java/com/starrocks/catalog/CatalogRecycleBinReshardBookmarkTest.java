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
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.LongUnaryOperator;

/**
 * Verifies the bookmark gate on {@link CatalogRecycleBin#eraseMaterializedIndex}: a live bookmark
 * anchoring a pre-reshard version holds the erase of a reshard-parked index generation. The hold is
 * bounded by {@link Config#bookmark_reference_max_ttl_ms} -- once the sweep reclaims the reference
 * no bookmark anchors the old version and the gate stops holding -- which is exercised through the
 * bookmark lifecycle rather than here.
 */
public class CatalogRecycleBinReshardBookmarkTest extends BookmarkTestBase {

    private long partitionVisibleVersion(long tableId, long logicalPartitionId, long physicalPartitionId) {
        return GlobalStateMgr.getCurrentState().getBookmarkManager()
                .getPhysicalPartitionFenceVersion(dbId, tableId, logicalPartitionId, physicalPartitionId)
                .orElseThrow();
    }

    /**
     * Creates a default table, takes a bookmark on it, and recycles a materialized index anchored
     * to it with {@code supersededAtVersionFn} applied to the bookmark's anchored version.
     */
    private CatalogRecycleBin recycleIndexWithBookmark(
            String bookmarkName, long indexId, LongUnaryOperator supersededAtVersionFn) throws Exception {
        long tableId = createDefaultTable();
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId).getTable(tableId);
        Partition partition = table.getPartition("p1");
        long logicalPartitionId = partition.getId();
        long physicalPartitionId = partition.getSubPartitions().iterator().next().getId();
        GlobalStateMgr.getCurrentState().getBookmarkManager()
                .create(dbId, tableId, BookmarkHolder.forEmptyInfo(bookmarkName));
        long v = partitionVisibleVersion(tableId, logicalPartitionId, physicalPartitionId);
        CatalogRecycleBin bin = new CatalogRecycleBin();
        bin.recycleMaterializedIndex(new RecycleMaterializedIndexInfo(
                dbId, tableId, logicalPartitionId, physicalPartitionId, indexId,
                supersededAtVersionFn.applyAsLong(v)));
        return bin;
    }

    @Test
    public void testEraseGatedByLiveBookmark() throws Exception {
        long tableId = createDefaultTable();
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId).getTable(tableId);
        Partition p1 = table.getPartition("p1");
        long logicalPartitionId = p1.getId();
        long physicalPartitionId = p1.getSubPartitions().iterator().next().getId();

        // Bookmark anchors the partition's current version v.
        GlobalStateMgr.getCurrentState().getBookmarkManager()
                .create(dbId, tableId, BookmarkHolder.forEmptyInfo("gate_h1"));
        long v = partitionVisibleVersion(tableId, logicalPartitionId, physicalPartitionId);

        // The parked generation was superseded right after the bookmark was taken.
        RecycleMaterializedIndexInfo info = new RecycleMaterializedIndexInfo(
                dbId, tableId, logicalPartitionId, physicalPartitionId, /*indexId*/ 987654321L, v + 1);
        CatalogRecycleBin bin = new CatalogRecycleBin();
        bin.recycleMaterializedIndex(info);

        // The generic retention timer elapsed long ago, so only the bookmark is holding this.
        long now = System.currentTimeMillis() + Config.partition_recycle_retention_period_secs * 1000L * 10;
        bin.eraseMaterializedIndex(now);
        Assertions.assertTrue(bin.isMaterializedIndexRecycled(987654321L), "live bookmark must hold the erase");

        // No time-based escape from this gate: the hold ends when the bookmark reference is
        // reclaimed, not when some reshard-local deadline passes. Erasing must still be blocked
        // however far the clock is advanced.
        bin.eraseMaterializedIndex(now + Config.bookmark_reference_max_ttl_ms * 10);
        Assertions.assertTrue(bin.isMaterializedIndexRecycled(987654321L),
                "the gate releases on reference reclamation, not on elapsed time");
    }

    /**
     * A non-positive {@link Config#bookmark_reference_max_ttl_ms} disables the only bound this hold
     * has, and a reference taken without its own ttl then never expires. Holding would pin every
     * parked generation forever, so the gate must not hold at all in that configuration.
     */
    @Test
    public void testEraseProceedsWhenTheReferenceTtlCeilingIsDisabled() throws Exception {
        CatalogRecycleBin bin = recycleIndexWithBookmark("unbounded_h1", 333L, v -> v + 1);
        long now = System.currentTimeMillis() + Config.partition_recycle_retention_period_secs * 1000L * 10;

        // Sanity: with a positive ceiling the hold is in effect.
        bin.eraseMaterializedIndex(now);
        Assertions.assertTrue(bin.isMaterializedIndexRecycled(333L),
                "a bounded hold must still gate the erase");

        long savedCeiling = Config.bookmark_reference_max_ttl_ms;
        try {
            Config.bookmark_reference_max_ttl_ms = -1L;
            bin.eraseMaterializedIndex(now);
            Assertions.assertFalse(bin.isMaterializedIndexRecycled(333L),
                    "a disabled ceiling leaves nothing to bound the hold, so the erase must proceed");
        } finally {
            Config.bookmark_reference_max_ttl_ms = savedCeiling;
        }
    }

    @Test
    public void testEraseProceedsWithoutBookmarkOrWithNewerBookmark() throws Exception {
        long now = System.currentTimeMillis() + Config.partition_recycle_retention_period_secs * 1000L * 10;

        // (a) supersededAtVersion == 0 (legacy entry) -> never gated, even with a live older bookmark.
        CatalogRecycleBin legacyBin = recycleIndexWithBookmark("legacy_h1", 111L, v -> 0);
        legacyBin.eraseMaterializedIndex(now);
        Assertions.assertFalse(legacyBin.isMaterializedIndexRecycled(111L),
                "supersededAtVersion == 0 must never be gated");

        // (b) live bookmark's anchored version >= supersededAtVersion -> not gated.
        CatalogRecycleBin newerBin = recycleIndexWithBookmark("newer_h1", 222L, v -> v);
        newerBin.eraseMaterializedIndex(now);
        Assertions.assertFalse(newerBin.isMaterializedIndexRecycled(222L),
                "a bookmark anchored at or after the supersede point must not gate the erase");
    }
}
