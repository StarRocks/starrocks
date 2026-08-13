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

import com.google.common.collect.Lists;
import com.starrocks.thrift.TPartitionAccessTimeTableRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PartitionAccessTimeMgrTest {

    private static final long DB_ID = 10L;
    private static final long TABLE_ID = 1000L;

    private static List<TPartitionAccessTimeTableRef> refs(long dbId, long tableId) {
        TPartitionAccessTimeTableRef ref = new TPartitionAccessTimeTableRef();
        ref.setDb_id(dbId);
        ref.setTable_id(tableId);
        return Collections.singletonList(ref);
    }

    @Test
    public void testRecordAccessStampsGivenLogicalIds() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();

        long before = System.currentTimeMillis();
        mgr.recordAccess(DB_ID, TABLE_ID, Lists.newArrayList(100L, 300L));

        Assertions.assertTrue(mgr.getLastAccessTime(DB_ID, TABLE_ID, 100L) >= before, "accessed id must be stamped");
        Assertions.assertTrue(mgr.getLastAccessTime(DB_ID, TABLE_ID, 300L) >= before, "accessed id must be stamped");
        Assertions.assertEquals(0L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 200L), "un-accessed id must stay 0");
    }

    @Test
    public void testRecordAccessIsMonotonic() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.recordAccess(DB_ID, TABLE_ID, Lists.newArrayList(100L));
        long first = mgr.getLastAccessTime(DB_ID, TABLE_ID, 100L);
        Assertions.assertTrue(first > 0);

        // A subsequent record never moves the timestamp backwards (max-merge).
        mgr.recordAccess(DB_ID, TABLE_ID, Lists.newArrayList(100L));
        Assertions.assertTrue(mgr.getLastAccessTime(DB_ID, TABLE_ID, 100L) >= first);
    }

    @Test
    public void testTablesAreIsolated() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.recordAccess(DB_ID, TABLE_ID, Lists.newArrayList(100L));

        // A different table sharing the same logical id is tracked separately.
        Assertions.assertTrue(mgr.getLastAccessTime(DB_ID, TABLE_ID, 100L) > 0);
        Assertions.assertEquals(0L, mgr.getLastAccessTime(DB_ID, 2000L, 100L));
        // A different db is also isolated, even with the same table/logical id.
        Assertions.assertEquals(0L, mgr.getLastAccessTime(20L, TABLE_ID, 100L));

        // getLocalAccessTimes returns exactly the requested table's logicalPartitionId -> ts snapshot.
        Map<Long, Long> local = mgr.getLocalAccessTimes(refs(DB_ID, TABLE_ID));
        Assertions.assertEquals(1, local.size());
        Assertions.assertTrue(local.containsKey(100L));
        Assertions.assertTrue(mgr.getLocalAccessTimes(refs(DB_ID, 2000L)).isEmpty(), "unknown table -> empty");
        Assertions.assertTrue(mgr.getLocalAccessTimes(refs(20L, TABLE_ID)).isEmpty(), "unknown db -> empty");
    }

    @Test
    public void testNullOrEmptyArgumentsAreNoOps() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        // Must not throw on a null / empty id collection, and must not create an entry.
        mgr.recordAccess(DB_ID, TABLE_ID, null);
        mgr.recordAccess(DB_ID, TABLE_ID, Lists.newArrayList());
        mgr.mergeEntries(null);
        mgr.removePartitions(null);
        Assertions.assertTrue(mgr.getLocalAccessTimes(refs(DB_ID, TABLE_ID)).isEmpty());
        Assertions.assertTrue(mgr.collectAllKeys().isEmpty());
    }

    @Test
    public void testMergeEntriesMaxMergesWithoutClearing() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(
                new PartitionAccessTimeEntry(DB_ID, TABLE_ID, 100L, 500L),
                new PartitionAccessTimeEntry(DB_ID, TABLE_ID, 200L, 20L)));
        // A lower ts for an existing key does not move it backwards; a higher one wins.
        mgr.mergeEntries(Lists.newArrayList(
                new PartitionAccessTimeEntry(DB_ID, TABLE_ID, 100L, 400L),
                new PartitionAccessTimeEntry(DB_ID, TABLE_ID, 200L, 999L)));
        Assertions.assertEquals(500L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 100L));
        Assertions.assertEquals(999L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 200L));
        // mergeEntries leaves the map populated (it is a max-merge, never a drain).
        Assertions.assertEquals(2, mgr.getLocalAccessTimes(refs(DB_ID, TABLE_ID)).size());
    }

    @Test
    public void testSnapshotSinceIsInclusiveAndDoesNotClear() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(
                new PartitionAccessTimeEntry(DB_ID, TABLE_ID, 100L, 40L),
                new PartitionAccessTimeEntry(DB_ID, TABLE_ID, 200L, 50L),
                new PartitionAccessTimeEntry(DB_ID, TABLE_ID, 300L, 60L)));
        // Inclusive boundary: >= 50 returns the 50 and 60 entries; the 40 entry is excluded.
        List<PartitionAccessTimeEntry> newer = mgr.snapshotSince(50L);
        List<Long> ids = Lists.newArrayList();
        for (PartitionAccessTimeEntry e : newer) {
            ids.add(e.getPartitionId());
        }
        Assertions.assertEquals(2, ids.size());
        Assertions.assertTrue(ids.contains(200L) && ids.contains(300L));
        // A watermark below every entry returns them all; the snapshot never clears the map.
        Assertions.assertEquals(3, mgr.snapshotSince(0L).size());
        Assertions.assertEquals(60L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 300L));
    }

    @Test
    public void testCollectAllKeysAndRemovePartitions() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(
                new PartitionAccessTimeEntry(DB_ID, TABLE_ID, 100L, 10L),
                new PartitionAccessTimeEntry(DB_ID, TABLE_ID, 200L, 20L)));
        Assertions.assertEquals(2, mgr.collectAllKeys().size());

        // Removing one key drops it from the map (and trims the now-smaller table map) but keeps the other.
        mgr.removePartitions(Lists.newArrayList(new long[] {DB_ID, TABLE_ID, 200L}));
        Assertions.assertEquals(0L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 200L));
        Assertions.assertEquals(10L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 100L));
        List<long[]> remaining = mgr.collectAllKeys();
        Assertions.assertEquals(1, remaining.size());
        Assertions.assertArrayEquals(new long[] {DB_ID, TABLE_ID, 100L}, remaining.get(0));

        // Removing the last key trims the whole table/db entry.
        mgr.removePartitions(Lists.newArrayList(new long[] {DB_ID, TABLE_ID, 100L}));
        Assertions.assertTrue(mgr.collectAllKeys().isEmpty());
        Assertions.assertTrue(mgr.getLocalAccessTimes(refs(DB_ID, TABLE_ID)).isEmpty());
    }

    @Test
    public void testGetAccessTimesReturnsMemoryWithoutQueryingTable() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        // Single-FE test env: getSelfNode() is null (no cluster bootstrap), so remote collection is skipped and
        // the read is served purely from memory -- the redesign removed the internal-table SELECT entirely.
        mgr.mergeEntries(Lists.newArrayList(new PartitionAccessTimeEntry(DB_ID, TABLE_ID, 100L, 777L)));
        Map<Long, Long> res = mgr.getAccessTimes(DB_ID, TABLE_ID);
        Assertions.assertEquals(777L, res.get(100L));
    }
}
