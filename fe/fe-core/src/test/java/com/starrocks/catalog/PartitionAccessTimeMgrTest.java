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
import com.starrocks.thrift.TPartitionAccessTimeEntry;
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

    private static TPartitionAccessTimeEntry entry(long db, long tbl, long part, long ts) {
        TPartitionAccessTimeEntry e = new TPartitionAccessTimeEntry();
        e.setDb_id(db);
        e.setTable_id(tbl);
        e.setPartition_id(part);
        e.setAccess_time_ms(ts);
        return e;
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
    public void testDumpReturnsAllAndClearsImmediately() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.recordAccess(DB_ID, TABLE_ID, Lists.newArrayList(100L, 200L));
        List<TPartitionAccessTimeEntry> dump = mgr.dumpAccessTimes();
        Assertions.assertEquals(2, dump.size());
        // The dump drains the map in the same pass: nothing left behind (a follower's transient increment).
        Assertions.assertEquals(0L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 100L));
        Assertions.assertEquals(0L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 200L));
        Assertions.assertTrue(mgr.getLocalAccessTimes(refs(DB_ID, TABLE_ID)).isEmpty());
    }

    @Test
    public void testDumpAfterClearReportsOnlyNewAccesses() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.recordAccess(DB_ID, TABLE_ID, Lists.newArrayList(100L, 200L));
        Assertions.assertEquals(2, mgr.dumpAccessTimes().size()); // cycle N drains {100,200}
        // An empty map dumps nothing.
        Assertions.assertTrue(mgr.dumpAccessTimes().isEmpty());
        // Only a subsequently-accessed partition is reported next cycle.
        mgr.recordAccess(DB_ID, TABLE_ID, Lists.newArrayList(200L));
        List<TPartitionAccessTimeEntry> dump = mgr.dumpAccessTimes();
        Assertions.assertEquals(1, dump.size());
        Assertions.assertEquals(200L, dump.get(0).getPartition_id());
    }

    @Test
    public void testMergeEntriesMaxMergesWithoutClearing() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(entry(DB_ID, TABLE_ID, 100L, 500L), entry(DB_ID, TABLE_ID, 200L, 20L)));
        // A lower ts for an existing key does not move it backwards; a higher one wins.
        mgr.mergeEntries(Lists.newArrayList(entry(DB_ID, TABLE_ID, 100L, 400L), entry(DB_ID, TABLE_ID, 200L, 999L)));
        Assertions.assertEquals(500L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 100L));
        Assertions.assertEquals(999L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 200L));
        // Unlike dumpAccessTimes, mergeEntries leaves the map populated (it is the authoritative baseline).
        Assertions.assertEquals(2, mgr.getLocalAccessTimes(refs(DB_ID, TABLE_ID)).size());
    }

    @Test
    public void testSnapshotNewerThanIsStrictlyGreaterAndDoesNotClear() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(
                entry(DB_ID, TABLE_ID, 100L, 40L),
                entry(DB_ID, TABLE_ID, 200L, 50L),
                entry(DB_ID, TABLE_ID, 300L, 60L)));
        // Strictly greater than 50 => only the 60 entry; the boundary value 50 is excluded.
        List<TPartitionAccessTimeEntry> newer = mgr.snapshotNewerThan(50L);
        Assertions.assertEquals(1, newer.size());
        Assertions.assertEquals(300L, newer.get(0).getPartition_id());
        Assertions.assertEquals(60L, newer.get(0).getAccess_time_ms());
        // A watermark below every entry returns them all; the snapshot never clears the map.
        Assertions.assertEquals(3, mgr.snapshotNewerThan(0L).size());
        Assertions.assertEquals(60L, mgr.getLastAccessTime(DB_ID, TABLE_ID, 300L));
    }

    @Test
    public void testCollectAllKeysAndRemovePartitions() {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(
                entry(DB_ID, TABLE_ID, 100L, 10L),
                entry(DB_ID, TABLE_ID, 200L, 20L)));
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
        mgr.mergeEntries(Lists.newArrayList(entry(DB_ID, TABLE_ID, 100L, 777L)));
        Map<Long, Long> res = mgr.getAccessTimes(DB_ID, TABLE_ID);
        Assertions.assertEquals(777L, res.get(100L));
    }
}
