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

package com.starrocks.lake.ivm;

import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.PhysicalPartitionMeta;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookmarkChangeTvrAdapterTest {

    private static final long DB_ID = 1L;
    private static final long TABLE_ID = 10L;
    private static final long LOGICAL_PARTITION_1 = 100L;
    private static final long LOGICAL_PARTITION_2 = 101L;
    private static final long PHYSICAL_PARTITION_1 = 1000L;
    private static final long PHYSICAL_PARTITION_2 = 1001L;
    private static final long BASE_INDEX_ID = 50L;
    private static final long BASE_INDEX_META_ID = 500L;

    private static PhysicalPartitionMeta meta(long visibleVersion) {
        return new PhysicalPartitionMeta(BASE_INDEX_ID, BASE_INDEX_META_ID, visibleVersion, 0L);
    }

    private static PhysicalPartitionMeta metaWithIndex(long indexId, long indexMetaId, long visibleVersion) {
        return new PhysicalPartitionMeta(indexId, indexMetaId, visibleVersion, 0L);
    }

    private static Bookmark bookmark(long bookmarkId, Map<Long, Map<Long, PhysicalPartitionMeta>> parts) {
        return new Bookmark(DB_ID, TABLE_ID, bookmarkId, 1000L, parts);
    }

    private static Map<Long, Map<Long, PhysicalPartitionMeta>> partitions(long logicalId, long physicalId,
                                                                         PhysicalPartitionMeta m) {
        Map<Long, Map<Long, PhysicalPartitionMeta>> outer = new HashMap<>();
        Map<Long, PhysicalPartitionMeta> inner = new HashMap<>();
        inner.put(physicalId, m);
        outer.put(logicalId, inner);
        return outer;
    }

    @Test
    public void testFirstRefresh_baseNull_allAdded_returnsMonotonic() {
        // First refresh: no prior bookmark. Every partition in head is ADDED → trackable → MONOTONIC.
        Bookmark head = bookmark(10L, partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(5L)));

        List<TvrTableDeltaTrait> traits = BookmarkChangeTvrAdapter.toTvrTraits(null, head);

        assertEquals(1, traits.size());
        assertTrue(traits.get(0).isAppendOnly());
        // First refresh's delta: from MIN (no prior id) to head's bookmarkId.
        assertEquals(10L, traits.get(0).getTvrDelta().end().orElseThrow());
    }

    @Test
    public void testEquivalentEndpoints_returnsMonotonic() {
        // Different bookmark ids but equivalent partition meta — reachable when an
        // ADD then DROP of the same partition lands between two bookmarks (or any
        // round-trip the dedup misses, since findLatestEquivalent only consults
        // the latest active bookmark). The trait must still be emitted so IVM
        // treats it as a no-op refresh that advances the version pointer rather
        // than as the "missing traits" error in MVIVMRefreshProcessor.
        Map<Long, Map<Long, PhysicalPartitionMeta>> parts = partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(5L));
        Bookmark base = bookmark(10L, parts);
        Bookmark head = bookmark(11L, parts);

        List<TvrTableDeltaTrait> traits = BookmarkChangeTvrAdapter.toTvrTraits(base, head);

        assertEquals(1, traits.size());
        assertTrue(traits.get(0).isAppendOnly());
        assertEquals(10L, traits.get(0).getTvrDelta().start().orElseThrow());
        assertEquals(11L, traits.get(0).getTvrDelta().end().orElseThrow());
    }

    @Test
    public void testOnlyDataChanged_returnsMonotonic() {
        // Same partition meta except visibleVersion advances → DATA_CHANGED → trackable.
        Bookmark base = bookmark(10L, partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(5L)));
        Bookmark head = bookmark(11L, partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(7L)));

        List<TvrTableDeltaTrait> traits = BookmarkChangeTvrAdapter.toTvrTraits(base, head);

        assertEquals(1, traits.size());
        assertTrue(traits.get(0).isAppendOnly());
        assertEquals(10L, traits.get(0).getTvrDelta().start().orElseThrow());
        assertEquals(11L, traits.get(0).getTvrDelta().end().orElseThrow());
    }

    @Test
    public void testOnlyAdded_returnsMonotonic() {
        // head has a new physical partition that base lacked → ADDED → trackable.
        Bookmark base = bookmark(10L, partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(5L)));
        Map<Long, Map<Long, PhysicalPartitionMeta>> headParts = partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(5L));
        headParts.computeIfAbsent(LOGICAL_PARTITION_2, k -> new HashMap<>()).put(PHYSICAL_PARTITION_2, meta(1L));
        Bookmark head = bookmark(11L, headParts);

        List<TvrTableDeltaTrait> traits = BookmarkChangeTvrAdapter.toTvrTraits(base, head);

        assertEquals(1, traits.size());
        assertTrue(traits.get(0).isAppendOnly());
    }

    @Test
    public void testAddedPlusDataChanged_returnsMonotonic() {
        // Mix of safe changes: existing partition's version bumps AND a new partition appears.
        Bookmark base = bookmark(10L, partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(5L)));
        Map<Long, Map<Long, PhysicalPartitionMeta>> headParts = partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(7L));
        headParts.computeIfAbsent(LOGICAL_PARTITION_2, k -> new HashMap<>()).put(PHYSICAL_PARTITION_2, meta(1L));
        Bookmark head = bookmark(11L, headParts);

        List<TvrTableDeltaTrait> traits = BookmarkChangeTvrAdapter.toTvrTraits(base, head);

        assertEquals(1, traits.size());
        assertTrue(traits.get(0).isAppendOnly());
    }

    @Test
    public void testHasDropped_returnsRetractable() {
        // Partition present in base but missing in head → DROPPED → not trackable.
        Map<Long, Map<Long, PhysicalPartitionMeta>> baseParts = partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(5L));
        baseParts.computeIfAbsent(LOGICAL_PARTITION_2, k -> new HashMap<>()).put(PHYSICAL_PARTITION_2, meta(3L));
        Bookmark base = bookmark(10L, baseParts);
        Bookmark head = bookmark(11L, partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(5L)));

        List<TvrTableDeltaTrait> traits = BookmarkChangeTvrAdapter.toTvrTraits(base, head);

        assertEquals(1, traits.size());
        assertFalse(traits.get(0).isAppendOnly());
    }

    @Test
    public void testHasIndexReplaced_returnsRetractable() {
        // metaId changed → data-rewrite schema change → INDEX_REPLACED → not trackable.
        Bookmark base = bookmark(10L,
                partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, metaWithIndex(BASE_INDEX_ID, 500L, 5L)));
        Bookmark head = bookmark(11L,
                partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, metaWithIndex(BASE_INDEX_ID, 501L, 5L)));

        List<TvrTableDeltaTrait> traits = BookmarkChangeTvrAdapter.toTvrTraits(base, head);

        assertEquals(1, traits.size());
        assertFalse(traits.get(0).isAppendOnly());
    }

    @Test
    public void testHasTabletReshard_returnsRetractable() {
        // indexId changed but metaId same → tablet split/merge → TABLET_RESHARD → not trackable.
        Bookmark base = bookmark(10L,
                partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, metaWithIndex(50L, BASE_INDEX_META_ID, 5L)));
        Bookmark head = bookmark(11L,
                partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, metaWithIndex(51L, BASE_INDEX_META_ID, 5L)));

        List<TvrTableDeltaTrait> traits = BookmarkChangeTvrAdapter.toTvrTraits(base, head);

        assertEquals(1, traits.size());
        assertFalse(traits.get(0).isAppendOnly());
    }

    @Test
    public void testMixedSafeAndUnsafe_returnsRetractable() {
        // ADDED + DROPPED: any unsafe change makes the whole delta non-trackable.
        Map<Long, Map<Long, PhysicalPartitionMeta>> baseParts = partitions(LOGICAL_PARTITION_1, PHYSICAL_PARTITION_1, meta(5L));
        Bookmark base = bookmark(10L, baseParts);
        Bookmark head = bookmark(11L, partitions(LOGICAL_PARTITION_2, PHYSICAL_PARTITION_2, meta(1L)));

        List<TvrTableDeltaTrait> traits = BookmarkChangeTvrAdapter.toTvrTraits(base, head);

        assertEquals(1, traits.size());
        assertFalse(traits.get(0).isAppendOnly());
    }
}
