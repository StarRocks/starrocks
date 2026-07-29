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
        Assertions.assertTrue(mgr.getLocalAccessTimes(refs(DB_ID, TABLE_ID)).isEmpty());
    }
}
