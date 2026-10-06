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

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.mv.PreResolvedBaseTables;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ALTER MATERIALIZED VIEW ... ACTIVE -- run by a user, by MVActiveChecker, and by an MV task run that finds
 * its MV inactive -- used to re-analyze the MV's definition and then rebuild its relationship while holding
 * the MV write lock. Both resolve every base table, the rebuild several times each, and for an external base
 * table each of those is a connector call. They are now resolved before the lock.
 */
public class AlterMVActivateResolveBeforeLockTest extends MVTestBase {

    // Partitioned, so the rebuild analyzes the partition exprs against the hive table as well.
    private static final String HIVE_MV_DEFINITION = "PARTITION BY (`l_shipdate`)\n"
            + "DISTRIBUTED BY RANDOM\n"
            + "REFRESH DEFERRED MANUAL\n"
            + "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par;";

    private LockProbeHiveMetadata probe;
    private ConnectorMetadata originalHiveMetadata;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    /**
     * Samples {@link LockHoldDepth#isUnderLock()} on getTable, OR-accumulated, for calls made on the thread
     * that installed it only: creating an MV hands its definition to the mv-plan-cache executor, which
     * resolves the same hive table at an arbitrary moment on a thread whose lock state is unrelated.
     */
    private static class LockProbeHiveMetadata extends MockedHiveMetadata {
        private final Thread owner = Thread.currentThread();
        private final AtomicBoolean underLock = new AtomicBoolean(false);
        private final AtomicInteger calls = new AtomicInteger();
        // Run once, on the first call made without the lock held.
        private volatile Runnable onUnlockedGetTable;

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            if (Thread.currentThread() == owner) {
                calls.incrementAndGet();
                if (LockHoldDepth.isUnderLock()) {
                    underLock.set(true);
                } else {
                    Runnable hook = onUnlockedGetTable;
                    onUnlockedGetTable = null;
                    if (hook != null) {
                        hook.run();
                    }
                }
            }
            return super.getTable(context, dbName, tblName);
        }
    }

    private void installProbe() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        if (originalHiveMetadata == null) {
            originalHiveMetadata = metadataMgr.getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME)
                    .orElseThrow(() -> new IllegalStateException("hive0 catalog is not registered"));
        }
        probe = new LockProbeHiveMetadata();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, probe);
    }

    @AfterEach
    public void removeProbe() {
        if (originalHiveMetadata != null) {
            MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
            metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, originalHiveMetadata);
            originalHiveMetadata = null;
        }
        // MVActiveChecker.tryToActivate removes the thread's context when it is done.
        connectContext.setThreadLocalInfo();
    }

    private void assertNoConnectorCallUnderTheLock() {
        Assertions.assertTrue(probe.calls.get() > 0, "the activation never reached the connector");
        Assertions.assertFalse(probe.underLock.get(),
                "the activation resolved an external base table while holding the MV lock");
    }

    @Test
    public void testAlterActiveResolvesNoExternalTableUnderTheLock() throws Exception {
        String mvName = "mv_activate_before_lock_user";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" + HIVE_MV_DEFINITION);
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            mv.setInactiveAndReason("test: forced inactive before the activation");

            installProbe();
            starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + mvName + " ACTIVE");

            Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
            assertNoConnectorCallUnderTheLock();
            // The scope is closed with the statement: a lookup on this thread goes to the connector again.
            BaseTableInfo baseTableInfo = mv.getBaseTableInfos().get(0);
            Optional<Table> sentinel = Optional.empty();
            Assertions.assertSame(sentinel, PreResolvedBaseTables.getOrResolve(baseTableInfo, () -> sentinel),
                    "the pre-resolved base tables must not outlive the statement");
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testMVActiveCheckerResolvesNoExternalTableUnderTheLock() throws Exception {
        String mvName = "mv_activate_before_lock_checker";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" + HIVE_MV_DEFINITION);
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            mv.setInactiveAndReason("test: forced inactive before the activation");

            installProbe();
            // Runs the ALTER on the calling thread, so the probe sees it.
            MVActiveChecker.tryToActivate(mv);

            Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
            assertNoConnectorCallUnderTheLock();
        } finally {
            connectContext.setThreadLocalInfo();
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    /**
     * The definition is what the analysis done before the lock was derived from. If it changes before the
     * lock is taken, the analysis no longer describes the MV and must not be applied.
     */
    @Test
    public void testDefinitionChangedBeforeTheLockIsRejected() throws Exception {
        String mvName = "mv_activate_before_lock_stale";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" + HIVE_MV_DEFINITION);
        MaterializedView mv = getMv(DB_NAME, mvName);
        String originalDefineSql = mv.getOriginalViewDefineSql();
        try {
            mv.setInactiveAndReason("test: forced inactive before the activation");

            installProbe();
            probe.onUnlockedGetTable = () -> mv.setOriginalViewDefineSql(originalDefineSql + " ");
            Exception e = Assertions.assertThrows(Exception.class,
                    () -> starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + mvName + " ACTIVE"));

            Assertions.assertTrue(e.getMessage().contains("altered concurrently"), e.getMessage());
            Assertions.assertFalse(mv.isActive());
            Assertions.assertFalse(probe.underLock.get());
        } finally {
            mv.setOriginalViewDefineSql(originalDefineSql);
            starRocksAssert.dropMaterializedView(mvName);
        }
    }
}
