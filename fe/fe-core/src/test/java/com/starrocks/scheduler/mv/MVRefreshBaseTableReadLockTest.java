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

package com.starrocks.scheduler.mv;

import com.google.common.base.Throwables;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.optimizer.function.MetaFunctions;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An internal base table's partition maps are mutated in place under its WRITE lock: automatic partition creation by
 * a load, a TTL drop, INSERT OVERWRITE. A MV refresh that syncs its partitions from them, or detects which of them
 * changed, must read them under the base table's READ lock. Without it
 * those reads race the writer and fail with a ConcurrentModificationException, or, for a partition dropped
 * between listing its name and looking it up, a NullPointerException.
 *
 * <p>The race itself needs a writer at the exact moment, so this asks what the race needs instead: whether the
 * reader held the lock. That holds on a single thread, every run.
 */
public class MVRefreshBaseTableReadLockTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE base_range (k1 date, v1 int)\n"
                + "PARTITION BY RANGE(k1) (\n"
                + "  PARTITION p20240101 VALUES [('2024-01-01'), ('2024-01-02')),\n"
                + "  PARTITION p20240102 VALUES [('2024-01-02'), ('2024-01-03')))\n"
                + "DISTRIBUTED BY HASH(v1) BUCKETS 1 PROPERTIES('replication_num' = '1');");
        starRocksAssert.withTable("CREATE TABLE base_list (k1 varchar(10), v1 int)\n"
                + "PARTITION BY LIST(k1) (\n"
                + "  PARTITION pa VALUES IN ('a'),\n"
                + "  PARTITION pb VALUES IN ('b'))\n"
                + "DISTRIBUTED BY HASH(v1) BUCKETS 1 PROPERTIES('replication_num' = '1');");
        executeInsertSql("INSERT INTO base_range VALUES ('2024-01-01', 1), ('2024-01-02', 2);");
        executeInsertSql("INSERT INTO base_list VALUES ('a', 1), ('b', 2);");
    }

    /**
     * Counts reads of the published table's partition maps on the test thread, and remembers the first one taken
     * without a READ or WRITE lock on the table or on its database. An intention lock on the database alone does
     * not exclude a writer of the table, so it does not count.
     */
    private static class PartitionMapReadProbe {
        private final Thread owner = Thread.currentThread();
        private final long dbId;
        private final OlapTable published;
        private final AtomicInteger reads = new AtomicInteger();
        private volatile Throwable firstUnlocked;
        private volatile Throwable firstLocked;

        private PartitionMapReadProbe(OlapTable table) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
            this.dbId = db.getId();
            this.published = table;
            PartitionMapReadProbe probe = this;
            new MockUp<OlapTable>() {
                @Mock
                public PCellSortedSet getRangePartitionMap(Invocation inv) {
                    probe.sample(inv.getInvokedInstance(), "getRangePartitionMap");
                    return inv.proceed();
                }

                @Mock
                public PCellSortedSet getPartitionCells(Invocation inv, Optional<List<Column>> partitionColumnsOpt) {
                    probe.sample(inv.getInvokedInstance(), "getPartitionCells");
                    return inv.proceed();
                }

                @Mock
                public Set<String> getVisiblePartitionNames(Invocation inv) {
                    probe.sample(inv.getInvokedInstance(), "getVisiblePartitionNames");
                    return inv.proceed();
                }
            };
        }

        private void sample(Object table, String call) {
            // The refresh's snapshot copies share the id but are private to it; only the published object races.
            if (Thread.currentThread() != owner || table != published) {
                return;
            }
            reads.incrementAndGet();
            boolean locked = holdsReadOrWrite(published.getId()) || holdsReadOrWrite(dbId);
            if (firstUnlocked == null && !locked) {
                firstUnlocked = new Throwable(call + " without the base table's lock");
            }
            if (firstLocked == null && locked) {
                firstLocked = new Throwable(call + " under the base table's lock");
            }
        }

        private static boolean holdsReadOrWrite(long rid) {
            LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
            return lockManager.isOwnedByCurrentThread(rid, LockType.READ)
                    || lockManager.isOwnedByCurrentThread(rid, LockType.WRITE);
        }

        private void assertAllLocked(String what) {
            Assertions.assertTrue(reads.get() > 0, what + " never read the base table's partitions, "
                    + "so this test proves nothing");
            Assertions.assertNull(firstUnlocked, () -> what + " read the live base table's partitions unlocked:\n"
                    + Throwables.getStackTraceAsString(firstUnlocked));
        }

        private void assertNoneLocked(String what) {
            Assertions.assertTrue(reads.get() > 0, what + " never read the base table's partitions, "
                    + "so this test proves nothing");
            Assertions.assertNull(firstLocked, () -> what + " took the base table's lock:\n"
                    + Throwables.getStackTraceAsString(firstLocked));
        }
    }

    private static OlapTable baseTable(String name) {
        return (OlapTable) getTable(DB_NAME, name);
    }

    /** N-1 (range): partition sync and change detection of a range-partitioned MV over a range base table. */
    @Test
    public void testRangeMVRefreshReadsBaseTableUnderLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_lock_range\n"
                + "PARTITION BY k1\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT k1, v1 FROM base_range;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_lock_range");
            PartitionMapReadProbe probe = new PartitionMapReadProbe(baseTable("base_range"));
            withMVRefreshTaskRun(DB_NAME, mv);
            probe.assertAllLocked("the refresh of a range-partitioned mv");
        } finally {
            starRocksAssert.dropMaterializedView("mv_lock_range");
        }
    }

    /** N-1 (list): partition sync and change detection of a list-partitioned MV over a list base table. */
    @Test
    public void testListMVRefreshReadsBaseTableUnderLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_lock_list\n"
                + "PARTITION BY k1\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT k1, v1 FROM base_list;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_lock_list");
            PartitionMapReadProbe probe = new PartitionMapReadProbe(baseTable("base_list"));
            withMVRefreshTaskRun(DB_NAME, mv);
            probe.assertAllLocked("the refresh of a list-partitioned mv");
        } finally {
            starRocksAssert.dropMaterializedView("mv_lock_list");
        }
    }

    /** N-2: a second refresh only detects what changed, through getMvBaseTableUpdateInfo. */
    @Test
    public void testChangeDetectionReadsBaseTableUnderLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_lock_detect\n"
                + "PARTITION BY k1\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT k1, v1 FROM base_range;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_lock_detect");
            withMVRefreshTaskRun(DB_NAME, mv);
            executeInsertSql("INSERT INTO base_range VALUES ('2024-01-02', 3);");
            PartitionMapReadProbe probe = new PartitionMapReadProbe(baseTable("base_range"));
            withMVRefreshTaskRun(DB_NAME, mv);
            probe.assertAllLocked("change detection of a mv refresh");
        } finally {
            starRocksAssert.dropMaterializedView("mv_lock_detect");
        }
    }

    /** N-2: the refresh of a non-partitioned MV detects a base table change through needsToRefreshTable. */
    @Test
    public void testNonPartitionedMVRefreshReadsBaseTableUnderLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_lock_unpartitioned\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT k1, v1 FROM base_range;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_lock_unpartitioned");
            withMVRefreshTaskRun(DB_NAME, mv);
            executeInsertSql("INSERT INTO base_range VALUES ('2024-01-01', 4);");
            PartitionMapReadProbe probe = new PartitionMapReadProbe(baseTable("base_range"));
            withMVRefreshTaskRun(DB_NAME, mv);
            probe.assertAllLocked("the refresh of a non-partitioned mv");
        } finally {
            starRocksAssert.dropMaterializedView("mv_lock_unpartitioned");
        }
    }

    /**
     * The freshness check of query rewrite reads unlocked, as before: the planner may already hold other tables'
     * locks there, so taking a base table's lock could wait out of order.
     */
    @Test
    public void testRewriteFreshnessCheckTakesNoBaseTableLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_lock_fresh\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT k1, v1 FROM base_range;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_lock_fresh");
            withMVRefreshTaskRun(DB_NAME, mv);
            PartitionMapReadProbe probe = new PartitionMapReadProbe(baseTable("base_range"));
            getMvUpdateInfo(mv);
            probe.assertNoneLocked("the rewrite freshness check of a non-partitioned mv");
        } finally {
            starRocksAssert.dropMaterializedView("mv_lock_fresh");
        }
    }

    /**
     * A1: a lock timeout on a non-ref base table fails the refresh, as one on the ref base table does. Read as
     * "changed", it would turn the refresh into a full one. The failure counts as lock contention, so the refresh
     * gets the lock retries (max_mv_refresh_try_lock_failure_retry_times), not the ordinary refresh retries.
     */
    @Test
    public void testNonRefBaseTableLockTimeoutFailsRefresh() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_lock_nonref_timeout\n"
                + "PARTITION BY k1\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT r.k1, r.v1, l.k1 AS lk FROM base_range r JOIN base_list l ON r.v1 = l.v1;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_lock_nonref_timeout");
            withMVRefreshTaskRun(DB_NAME, mv);
            executeInsertSql("INSERT INTO base_range VALUES ('2024-01-01', 5);");
            long nonRefTableId = baseTable("base_list").getId();
            new MockUp<MvUtils>() {
                @Mock
                public <T, E extends Exception> T readBaseTableUnderLock(Invocation inv, MaterializedView view,
                                                                         Table baseTable, boolean unlocked,
                                                                         MvUtils.BaseTableRead<T, E> read)
                        throws Exception {
                    if (!unlocked && baseTable.getId() == nonRefTableId) {
                        throw new LockTimeoutException("injected lock timeout on " + baseTable.getName());
                    }
                    return inv.proceed();
                }
            };
            Exception e = Assertions.assertThrows(Exception.class, () -> withMVRefreshTaskRun(DB_NAME, mv));
            String stack = Throwables.getStackTraceAsString(e);
            Assertions.assertTrue(stack.contains("injected lock timeout on base_list"), stack);
            Assertions.assertTrue(stack.contains("failed after 0 times, try lock failed: "
                    + Config.max_mv_refresh_try_lock_failure_retry_times), stack);
        } finally {
            starRocksAssert.dropMaterializedView("mv_lock_nonref_timeout");
        }
    }

    /**
     * A2: inspect_mv_refresh_info is constant-folded while a statement is planned, where the planner may hold other
     * tables' locks. A thread that holds one reads the base tables unlocked in its freshness check, as query rewrite
     * does, instead of waiting on them out of order.
     */
    @Test
    public void testInspectMVRefreshInfoReadsBaseTablesUnlocked() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_lock_inspect\n"
                + "PARTITION BY k1\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT r.k1, r.v1, l.k1 AS lk FROM base_range r JOIN base_list l ON r.v1 = l.v1;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_lock_inspect");
            withMVRefreshTaskRun(DB_NAME, mv);
            executeInsertSql("INSERT INTO base_range VALUES ('2024-01-02', 6);");
            AtomicInteger reads = new AtomicInteger();
            AtomicInteger lockedReads = new AtomicInteger();
            new MockUp<MvUtils>() {
                @Mock
                public <T, E extends Exception> T readBaseTableUnderLock(Invocation inv, MaterializedView view,
                                                                         Table baseTable, boolean unlocked,
                                                                         MvUtils.BaseTableRead<T, E> read)
                        throws Exception {
                    reads.incrementAndGet();
                    if (!unlocked) {
                        lockedReads.incrementAndGet();
                    }
                    return inv.proceed();
                }
            };
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
            // the planner's lock on a table the statement references
            OlapTable planned = baseTable("base_list");
            String info;
            try (AutoCloseableLock ignored = new AutoCloseableLock(db.getId(), planned.getId(), LockType.READ)) {
                info = MetaFunctions.inspectMVRefreshInfo(db, mv);
            }
            Assertions.assertTrue(info.contains("p20240102"), info);
            Assertions.assertTrue(reads.get() > 0, "inspect_mv_refresh_info never read a base table");
            Assertions.assertEquals(0, lockedReads.get(), "inspect_mv_refresh_info read a base table under its lock");
        } finally {
            starRocksAssert.dropMaterializedView("mv_lock_inspect");
        }
    }
}
