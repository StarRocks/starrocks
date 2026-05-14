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

import com.starrocks.lake.bookmark.BookmarkChange.ChangeType;
import com.starrocks.lake.bookmark.BookmarkChange.DataChanged;
import com.starrocks.lake.bookmark.BookmarkChange.IndexReplaced;
import com.starrocks.lake.bookmark.BookmarkChange.PartitionAdded;
import com.starrocks.lake.bookmark.BookmarkChange.PartitionDropped;
import com.starrocks.lake.bookmark.BookmarkChange.PhysicalPartitionChange;
import com.starrocks.lake.bookmark.BookmarkChange.TabletReshard;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookmarkChangeTest {

    private static final long DB_ID = 1L;
    private static final long TABLE_ID = 2L;

    /** Build a Bookmark from 5-tuples (logical, physical, indexId, metaId, version). */
    private static Bookmark meta(long bookmarkId, long... parts) {
        Map<Long, Map<Long, PhysicalPartitionMeta>> m = new HashMap<>();
        for (int i = 0; i < parts.length; i += 5) {
            long log = parts[i];
            long phys = parts[i + 1];
            long idxId = parts[i + 2];
            long metaId = parts[i + 3];
            long ver = parts[i + 4];
            m.computeIfAbsent(log, k -> new HashMap<>())
                    .put(phys, new PhysicalPartitionMeta(idxId, metaId, ver, 0L));
        }
        return new Bookmark(DB_ID, TABLE_ID, bookmarkId, 1000L, m);
    }

    private static List<PhysicalPartitionChange> flatten(BookmarkChange change) {
        List<PhysicalPartitionChange> out = new ArrayList<>();
        change.getChanges().values().forEach(out::addAll);
        return out;
    }

    @Test
    public void testChangeClassification() {
        // Identical → no change
        Bookmark b1 = meta(1L, 10L, 100L, 1L, 1L, 5L);
        Bookmark b2 = meta(2L, 10L, 100L, 1L, 1L, 5L);
        BookmarkChange noChange = BookmarkChange.computeChanges(b1, b2);
        assertTrue(noChange.isNoChange());
        assertTrue(flatten(noChange).isEmpty());

        // Only visible version differs → DataChanged
        Bookmark b3 = meta(2L, 10L, 100L, 1L, 1L, 7L);
        DataChanged dc = assertInstanceOf(DataChanged.class,
                flatten(BookmarkChange.computeChanges(b1, b3)).get(0));
        assertEquals(ChangeType.DATA_CHANGED, dc.getChangeType());
        assertEquals(5L, dc.getBasePartition().getVisibleVersion());
        assertEquals(7L, dc.getHeadPartition().getVisibleVersion());

        // metaId differs (id same) → IndexReplaced
        Bookmark b4 = meta(2L, 10L, 100L, 1L, 9L, 5L);
        IndexReplaced ir1 = assertInstanceOf(IndexReplaced.class,
                flatten(BookmarkChange.computeChanges(b1, b4)).get(0));
        assertEquals(ChangeType.INDEX_REPLACED, ir1.getChangeType());
        assertEquals(1L, ir1.getBasePartition().getBaseMaterializedIndexMetaId());
        assertEquals(9L, ir1.getHeadPartition().getBaseMaterializedIndexMetaId());

        // metaId AND id differ → IndexReplaced (metaId wins)
        Bookmark b5 = meta(2L, 10L, 100L, 5L, 9L, 5L);
        IndexReplaced ir2 = assertInstanceOf(IndexReplaced.class,
                flatten(BookmarkChange.computeChanges(b1, b5)).get(0));
        assertEquals(9L, ir2.getHeadPartition().getBaseMaterializedIndexMetaId());
        assertEquals(5L, ir2.getHeadPartition().getBaseMaterializedIndexId());

        // Only id differs (metaId stable) → TabletReshard
        Bookmark b6 = meta(2L, 10L, 100L, 5L, 1L, 5L);
        TabletReshard tr1 = assertInstanceOf(TabletReshard.class,
                flatten(BookmarkChange.computeChanges(b1, b6)).get(0));
        assertEquals(ChangeType.TABLET_RESHARD, tr1.getChangeType());
        assertEquals(1L, tr1.getBasePartition().getBaseMaterializedIndexId());
        assertEquals(5L, tr1.getHeadPartition().getBaseMaterializedIndexId());
        assertEquals(1L, tr1.getBasePartition().getBaseMaterializedIndexMetaId());
        assertEquals(1L, tr1.getHeadPartition().getBaseMaterializedIndexMetaId());

        // Precedence — id and version differ but metaId same → TabletReshard wins (id beats version)
        Bookmark b7 = meta(2L, 10L, 100L, 5L, 1L, 9L);
        assertInstanceOf(TabletReshard.class,
                flatten(BookmarkChange.computeChanges(b1, b7)).get(0));
    }

    @Test
    public void testPartitionLifecycle() {
        Bookmark base = meta(1L,
                10L, 100L, 1L, 1L, 5L,
                10L, 101L, 2L, 2L, 5L,
                20L, 200L, 3L, 3L, 5L);
        // Head: physical 100 unchanged, physical 101 dropped, new physical 102 within logical 10,
        //       logical 20 entirely dropped, new logical 30 with one physical 300.
        Bookmark head = meta(2L,
                10L, 100L, 1L, 1L, 5L,
                10L, 102L, 4L, 4L, 1L,
                30L, 300L, 5L, 5L, 1L);

        Map<Long, List<PhysicalPartitionChange>> byLogical =
                BookmarkChange.computeChanges(base, head).getChanges();

        // Logical 10: one ADDED (102) + one DROPPED (101).
        List<PhysicalPartitionChange> log10 = byLogical.get(10L);
        assertEquals(2, log10.size());
        boolean has102Added = log10.stream().anyMatch(c ->
                c instanceof PartitionAdded && c.getPhysicalPartitionId() == 102L);
        boolean has101Dropped = log10.stream().anyMatch(c ->
                c instanceof PartitionDropped && c.getPhysicalPartitionId() == 101L);
        assertTrue(has102Added);
        assertTrue(has101Dropped);

        // Logical 20: one DROPPED (200).
        List<PhysicalPartitionChange> log20 = byLogical.get(20L);
        assertEquals(1, log20.size());
        PartitionDropped dropped200 = assertInstanceOf(PartitionDropped.class, log20.get(0));
        assertEquals(200L, dropped200.getPhysicalPartitionId());

        // Logical 30: one ADDED (300).
        List<PhysicalPartitionChange> log30 = byLogical.get(30L);
        assertEquals(1, log30.size());
        PartitionAdded added300 = assertInstanceOf(PartitionAdded.class, log30.get(0));
        assertEquals(300L, added300.getPhysicalPartitionId());
    }

    @Test
    public void testIsTrackable() {
        Bookmark b1 = meta(1L, 10L, 100L, 1L, 1L, 5L);

        // No change → trackable.
        assertTrue(BookmarkChange.computeChanges(b1, b1).isTrackable());

        // ADDED only.
        Bookmark added = meta(2L, 10L, 100L, 1L, 1L, 5L, 10L, 101L, 2L, 2L, 5L);
        assertTrue(BookmarkChange.computeChanges(b1, added).isTrackable());

        // DATA_CHANGED only.
        Bookmark dataChanged = meta(2L, 10L, 100L, 1L, 1L, 9L);
        assertTrue(BookmarkChange.computeChanges(b1, dataChanged).isTrackable());

        // INDEX_REPLACED → not trackable.
        Bookmark replaced = meta(2L, 10L, 100L, 1L, 5L, 5L);
        assertFalse(BookmarkChange.computeChanges(b1, replaced).isTrackable());

        // TABLET_RESHARD → not trackable.
        Bookmark resharded = meta(2L, 10L, 100L, 9L, 1L, 5L);
        assertFalse(BookmarkChange.computeChanges(b1, resharded).isTrackable());

        // PartitionDropped → not trackable.
        Bookmark dropped = meta(2L);
        assertFalse(BookmarkChange.computeChanges(b1, dropped).isTrackable());
    }

    @Test
    public void testInputs() {
        Bookmark head = meta(2L, 10L, 100L, 1L, 1L, 5L);

        // Null base → every partition in head is ADDED.
        BookmarkChange nullBase = BookmarkChange.computeChanges(null, head);
        List<PhysicalPartitionChange> entries = flatten(nullBase);
        assertEquals(1, entries.size());
        assertInstanceOf(PartitionAdded.class, entries.get(0));

        // Non-null base + null head → NPE (isolates the null-head check, not "both null").
        Bookmark validBase = meta(1L, 10L, 100L, 1L, 1L, 5L);
        assertThrows(NullPointerException.class,
                () -> BookmarkChange.computeChanges(validBase, null));

        // Table mismatch → IAE.
        Map<Long, Map<Long, PhysicalPartitionMeta>> empty = new HashMap<>();
        Bookmark base = new Bookmark(DB_ID, TABLE_ID, 1L, 1000L, empty);
        Bookmark headOtherTable = new Bookmark(DB_ID, 99L, 2L, 1000L, empty);
        assertThrows(IllegalArgumentException.class,
                () -> BookmarkChange.computeChanges(base, headOtherTable));

        // base.bookmarkId > head.bookmarkId → IAE.
        Bookmark older = new Bookmark(DB_ID, TABLE_ID, 50L, 1000L, empty);
        Bookmark newer = new Bookmark(DB_ID, TABLE_ID, 30L, 1000L, empty);
        assertThrows(IllegalArgumentException.class,
                () -> BookmarkChange.computeChanges(older, newer));
    }
}
