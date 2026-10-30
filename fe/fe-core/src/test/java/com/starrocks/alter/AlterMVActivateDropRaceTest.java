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

package com.starrocks.alter;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * ALTER MATERIALIZED VIEW ... ACTIVE relinks the MV into its base tables holding the MV's lock only. A DROP TABLE of
 * a base table in another database is not excluded by that lock: run right before or right after the link, the drop
 * either finds nothing linked or finds the MV still inactive from the rebuild, and journals no inactivation; the
 * activation then went on to journal ACTIVE for an MV whose base table is gone.
 */
public class AlterMVActivateDropRaceTest extends MVTestBase {
    private static final String BASE_DB = "activate_drop_race_db";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withDatabase(BASE_DB);
    }

    @Test
    public void testBaseTableDroppedBeforeTheActivationLinksIntoIt() throws Exception {
        assertDropDuringActivationInactivates("activate_drop_before", true);
    }

    @Test
    public void testBaseTableDroppedAfterTheActivationLinksIntoIt() throws Exception {
        assertDropDuringActivationInactivates("activate_drop_after", false);
    }

    private void assertDropDuringActivationInactivates(String name, boolean dropBeforeLink) throws Exception {
        String baseName = name + "_base";
        String mvName = name + "_mv";
        starRocksAssert.withTable("CREATE TABLE " + BASE_DB + "." + baseName + " (k1 int, v1 int)\n" +
                "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                "PROPERTIES ('replication_num' = '1');");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + DB_NAME + "." + mvName + "\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES ('replication_num' = '1')\n" +
                "AS SELECT k1, v1 FROM " + BASE_DB + "." + baseName + ";");
        MaterializedView mv = getMv(DB_NAME, mvName);
        mv.setInactiveAndReason("test: forced inactive before the activation");
        Database baseDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(BASE_DB);

        Thread testThread = Thread.currentThread();
        AtomicInteger drops = new AtomicInteger();
        // Confined to the test thread: the async mv plan cache may touch the same table on another one.
        new MockUp<Table>() {
            @Mock
            public void addRelatedMaterializedView(Invocation invocation, MvId mvId) {
                Table table = invocation.getInvokedInstance();
                boolean race = Thread.currentThread() == testThread && baseName.equals(table.getName())
                        && drops.get() == 0;
                if (race && dropBeforeLink) {
                    dropUnderTheLock(baseDb, table, drops);
                }
                invocation.proceed(mvId);
                if (race && !dropBeforeLink) {
                    dropUnderTheLock(baseDb, table, drops);
                }
            }
        };
        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        try {
            starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + DB_NAME + "." + mvName + " ACTIVE");
        } catch (Exception e) {
            // An activation that saw the drop itself fails; what matters is that nothing stays active.
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
        try {
            Assertions.assertEquals(1, drops.get(), "the drop never ran inside the activation, nothing was tested");
            Assertions.assertNull(baseDb.getTable(baseName), "the base table should be gone");
            Assertions.assertFalse(mv.isActive(), "an mv whose base table was dropped must not stay active");
            // Followers replay the drop without inactivating anything: the last status journaled must be INACTIVE.
            ArgumentCaptor<AlterMaterializedViewStatusLog> captor =
                    ArgumentCaptor.forClass(AlterMaterializedViewStatusLog.class);
            verify(spyEditLog, atLeastOnce()).logAlterMvStatus(captor.capture(), any());
            // Only this mv's: the task-run pool may journal for the views of earlier cases meanwhile.
            List<AlterMaterializedViewStatusLog> logs = captor.getAllValues().stream()
                    .filter(log -> log.getTableId() == mv.getId()).toList();
            Assertions.assertFalse(logs.isEmpty(), "nothing was journaled for the mv");
            Assertions.assertEquals(AlterMaterializedViewStatusClause.INACTIVE, logs.get(logs.size() - 1).getStatus());
        } finally {
            starRocksAssert.dropMaterializedView(DB_NAME + "." + mvName);
        }
    }

    /** What DROP TABLE does under the database write lock, see Database#unprotectDropTable. */
    private static void dropUnderTheLock(Database db, Table table, AtomicInteger drops) {
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            db.unprotectDropTable(table.getId(), false, false);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
        drops.incrementAndGet();
    }

    /**
     * The recheck runs once the activation's lock is released. By then the drop may have inactivated the MV and a new
     * ALTER ... ACTIVE relinked it to a table recreated since; a recheck for the earlier activation must stand down
     * rather than journal an inactivation for the old, dropped table.
     */
    @Test
    public void testARecheckForAnEarlierActivationStandsDown() throws Exception {
        String baseName = "activate_recheck_stale_base";
        String mvName = "activate_recheck_stale_mv";
        starRocksAssert.withTable("CREATE TABLE " + BASE_DB + "." + baseName + " (k1 int, v1 int)\n" +
                "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                "PROPERTIES ('replication_num' = '1');");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + DB_NAME + "." + mvName + "\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES ('replication_num' = '1')\n" +
                "AS SELECT k1, v1 FROM " + BASE_DB + "." + baseName + ";");
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            Assertions.assertTrue(mv.isActive());
            long activatedAtEpoch = mv.getStatusSnapshot().epoch();
            Database baseDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(BASE_DB);
            dropUnderTheLock(baseDb, baseDb.getTable(baseName), new AtomicInteger());
            // What a later, successful ALTER ... ACTIVE over a recreated table leaves behind.
            mv.setActive();

            mv.inactivateIfBaseTableDroppedMeanwhile(activatedAtEpoch);
            Assertions.assertTrue(mv.isActive(), "a recheck for an earlier activation undid a later one");

            mv.inactivateIfBaseTableDroppedMeanwhile(mv.getStatusSnapshot().epoch());
            Assertions.assertFalse(mv.isActive(), "the recheck for the current activation must still act");
        } finally {
            starRocksAssert.dropMaterializedView(DB_NAME + "." + mvName);
        }
    }
}
