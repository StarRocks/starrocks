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

package com.starrocks.qe;

import com.starrocks.catalog.Database;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SHOW TABLES / SHOW MATERIALIZED VIEWS must not lock an external-catalog database. Its id is minted by the
 * connector (MockedHiveMetadata mirrors the real HiveMetastoreApiConverter.toDatabase by handing out a fresh id
 * per call), so the lock protects nothing and can collide with an unrelated real id.
 * <p>
 * Detection works by making the mocked hive database report a *chosen* id and parking a WRITE lock on that id:
 * if the executor still locks the external database, the statement blocks forever. testDetectorSeesARealLock is
 * the control that proves the harness can actually observe a lock being taken.
 */
public class ShowExecutorExternalCatalogLockTest extends ConnectorPlanTestBase {

    private static final long COLLIDING_DB_ID = 987654321L;

    /** Mirrors MockedHiveMetadata except that the database id is fixed, so the test can contend on it. */
    private static class FixedIdHiveMetadata extends MockedHiveMetadata {
        @Override
        public Database getDb(ConnectContext context, String dbName) {
            return new Database(COLLIDING_DB_ID, dbName);
        }
    }

    @AfterEach
    public void restoreHiveMetadata() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, new MockedHiveMetadata());
    }

    private static void useFixedIdHiveMetadata() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, new FixedIdHiveMetadata());
    }

    /**
     * Runs {@code sql} on another thread while this thread holds a WRITE lock on {@code contendedDbId}.
     *
     * @return true if the statement completed while the lock was held, i.e. it never tried to lock that id.
     */
    private static boolean completesWhileDbIsWriteLocked(String sql, long contendedDbId) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        // Parse and analyze before taking the lock, so the measurement covers ShowExecutor.execute only.
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ShowStmt stmt = (ShowStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        Locker holder = new Locker();
        holder.lockDatabase(contendedDbId, LockType.WRITE);
        Thread worker = new Thread(() -> {
            try {
                ctx.setThreadLocalInfo();
                ShowExecutor.execute(stmt, ctx);
            } catch (Throwable t) {
                error.set(t);
            } finally {
                ConnectContext.remove();
                done.countDown();
            }
        });
        try {
            worker.start();
            boolean completed = done.await(3, TimeUnit.SECONDS);
            if (completed && error.get() != null) {
                throw new RuntimeException(error.get());
            }
            return completed;
        } finally {
            holder.unLockDatabase(contendedDbId, LockType.WRITE);
            // If the statement was blocked, releasing lets it finish; never leave the thread parked.
            worker.join(TimeUnit.SECONDS.toMillis(20));
            if (error.get() != null) {
                throw new RuntimeException(error.get());
            }
        }
    }

    /**
     * Control: the same harness pointed at a genuine internal database must observe the lock. Without this,
     * the two tests below would pass even if the detector were blind.
     */
    @Test
    public void testDetectorSeesARealLock() throws Exception {
        long internalDbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getId();
        Assertions.assertFalse(completesWhileDbIsWriteLocked("show tables from test", internalDbId),
                "SHOW TABLES on an internal database must still block behind a DB WRITE lock");
    }

    @Test
    public void testShowTablesFromExternalCatalogTakesNoLock() throws Exception {
        useFixedIdHiveMetadata();
        Assertions.assertTrue(completesWhileDbIsWriteLocked("show tables from hive0.tpch", COLLIDING_DB_ID),
                "SHOW TABLES on an external catalog must not lock the connector-minted database id");
    }

    @Test
    public void testShowMaterializedViewsFromExternalCatalogTakesNoLock() throws Exception {
        useFixedIdHiveMetadata();
        Assertions.assertTrue(
                completesWhileDbIsWriteLocked("show materialized views from hive0.tpch", COLLIDING_DB_ID),
                "SHOW MATERIALIZED VIEWS on an external catalog must not lock the connector-minted database id");
    }

    /**
     * The local-metastore walk is skipped entirely for an external catalog, so a connector-minted id that
     * happens to equal a real internal database id cannot make the statement enumerate that database's tables
     * without holding its lock. COLLIDING_DB_ID is pointed at the internal "test" database to force exactly
     * that collision.
     */
    @Test
    public void testShowMaterializedViewsFromExternalCatalogDoesNotWalkACollidingLocalDb() throws Exception {
        // The internal database must actually contain an MV, otherwise the walk returns nothing either way
        // and the assertion below would hold vacuously.
        starRocksAssert.useDatabase("test").withMaterializedView(
                "CREATE MATERIALIZED VIEW test.collision_probe_mv " +
                        "DISTRIBUTED BY HASH(`v1`) BUCKETS 3 " +
                        "REFRESH DEFERRED MANUAL " +
                        "PROPERTIES ('replication_num' = '1') " +
                        "AS SELECT v1, v2 FROM test.t0;");
        try {
            long internalDbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getId();
            // Sanity: the same statement against the internal database does list it.
            ConnectContext internalCtx = UtFrameUtils.createDefaultCtx();
            ShowStmt internalStmt = (ShowStmt) UtFrameUtils.parseStmtWithNewParser(
                    "show materialized views from test", internalCtx);
            Assertions.assertFalse(ShowExecutor.execute(internalStmt, internalCtx).getResultRows().isEmpty());

            MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
            metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME,
                    new MockedHiveMetadata() {
                        @Override
                        public Database getDb(ConnectContext context, String dbName) {
                            return new Database(internalDbId, dbName);
                        }
                    });

            ConnectContext ctx = UtFrameUtils.createDefaultCtx();
            ShowStmt stmt = (ShowStmt) UtFrameUtils.parseStmtWithNewParser(
                    "show materialized views from hive0.tpch", ctx);
            Assertions.assertTrue(ShowExecutor.execute(stmt, ctx).getResultRows().isEmpty(),
                    "an external catalog must not enumerate the internal database its id collides with");
        } finally {
            starRocksAssert.dropMaterializedView("collision_probe_mv");
        }
    }

    @Test
    public void testShowTablesStillWorksForBothCatalogKinds() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        ShowStmt internal = (ShowStmt) UtFrameUtils.parseStmtWithNewParser("show tables from test", ctx);
        Assertions.assertFalse(ShowExecutor.execute(internal, ctx).getResultRows().isEmpty());

        ShowStmt external = (ShowStmt) UtFrameUtils.parseStmtWithNewParser("show tables from hive0.tpch", ctx);
        Assertions.assertFalse(ShowExecutor.execute(external, ctx).getResultRows().isEmpty());
    }
}
