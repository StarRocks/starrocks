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

package com.starrocks.sql.optimizer.function;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code inspect_mv_refresh_info} takes a READ lock on (db, mv) and then, for every base table, resolves
 * it and reads its partition state. An external base table is not in that lock's protection domain --
 * the FE has no identity to lock it by -- so doing that work inside buys a connector round trip in
 * exchange for no protection at all. Half of this method was already hoisted out of the lock for the
 * same reason (the mvToRefreshPartitions block and its comment); these tests cover the other half.
 *
 * <p>The probe samples {@link LockHoldDepth#isUnderLock()} on every connector entry point the method
 * reaches, OR-accumulated: each entry point is called once per base table, so a plain assignment would
 * let a later lock-free call overwrite an earlier violation and turn the test green for the wrong reason.
 *
 * <p>It samples only calls made on the thread that installed it. Creating an MV hands its definition to
 * the {@code mv-plan-cache} executor, which analyzes it asynchronously and resolves the same hive table
 * through the same mocked catalog; that call lands at an arbitrary moment, on a thread whose lock state
 * has nothing to do with the statement under test. Counting it inflates the baseline and makes the
 * call-count assertion below flap, and its lock state would be attributed to {@code inspect_mv_refresh_info}.
 */
public class MetaFunctionsLockConnectorIOTest extends MVTestBase {

    private LockProbeHiveMetadata probe;
    private ConnectorMetadata originalHiveMetadata;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    private static class LockProbeHiveMetadata extends MockedHiveMetadata {
        /** The thread that installed the probe; see the class comment for why the others are ignored. */
        private final Thread owner = Thread.currentThread();
        private final AtomicBoolean underLock = new AtomicBoolean(false);
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger getTableCalls = new AtomicInteger();
        /** Runs on every getTable, so a test can interleave something at a point it controls. */
        private Runnable onGetTable;

        private boolean isObserved() {
            return Thread.currentThread() == owner;
        }

        private void sample() {
            calls.incrementAndGet();
            if (LockHoldDepth.isUnderLock()) {
                underLock.set(true);
            }
        }

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            if (isObserved()) {
                sample();
                getTableCalls.incrementAndGet();
                if (onGetTable != null) {
                    onGetTable.run();
                }
            }
            return super.getTable(context, dbName, tblName);
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName,
                                               ConnectorMetadatRequestContext requestContext) {
            if (isObserved()) {
                sample();
            }
            return super.listPartitionNames(dbName, tableName, requestContext);
        }

        @Override
        public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
            if (isObserved()) {
                sample();
            }
            return super.getPartitions(table, partitionNames);
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
        if (probe != null) {
            probe.onGetTable = null;
        }
        if (originalHiveMetadata != null) {
            MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
            metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, originalHiveMetadata);
            originalHiveMetadata = null;
        }
    }

    private void assertInspectStaysOffTheConnectorUnderTheLock(String mvName) {
        installProbe();
        ConstantOperator result = MetaFunctions.inspectMVRefreshInfo(ConstantOperator.createVarchar(mvName));
        Assertions.assertNotNull(result);
        Assertions.assertTrue(probe.calls.get() > 0,
                "the probe never saw the connector, so this test proves nothing");
        Assertions.assertFalse(probe.underLock.get(),
                "connector metadata was fetched while an FE metadata lock was held");
    }

    @Test
    public void testInspectDoesNotTouchAnExternalBaseTableUnderTheLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_over_hive\n"
                + "PARTITION BY (`l_shipdate`)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par;");
        try {
            assertInspectStaysOffTheConnectorUnderTheLock(DB_NAME + ".mv_over_hive");
        } finally {
            starRocksAssert.dropMaterializedView("mv_over_hive");
        }
    }

    /**
     * The gathering above runs outside the lock and compares the connector's partitions against the MV's
     * own refresh state, which the lock does protect. A refresh committing in that window must not leave
     * the comparison and the visible-version maps describing two different moments.
     *
     * <p>Reproduced without threads: the probe swaps the MV's refresh scheme on the first connector call,
     * which is exactly the moment the gathering is in flight and the lock is not yet held -- the same
     * interleaving a concurrent MVVersionManager commit produces.
     */
    @Test
    public void testAConcurrentRefreshCommitForcesTheGatheringToBeRedoneUnderTheLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_raced\n"
                + "PARTITION BY (`l_shipdate`)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par;");
        try {
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(DB_NAME, "mv_raced");

            // Baseline: the same statement with nothing racing it. Measured rather than hard-coded --
            // the block hoisted out of this lock before this change resolves base tables too, so the
            // absolute call count is not this test's business; the delta is.
            installProbe();
            MetaFunctions.inspectMVRefreshInfo(ConstantOperator.createVarchar(DB_NAME + ".mv_raced"));
            int undisturbedCalls = probe.getTableCalls.get();
            Assertions.assertFalse(probe.underLock.get(),
                    "connector metadata was fetched while an FE metadata lock was held");

            // Now commit a refresh in the window: the probe swaps the MV's refresh scheme on the first
            // connector call, which is where the gathering is in flight and the lock is not yet held.
            installProbe();
            // Swap on every call made outside the lock. The method resolves base tables in an earlier,
            // already-hoisted block too, so "the first call" lands before the reference this branch
            // compares is even captured; swapping on all of them guarantees one lands after it. The redo
            // runs under the lock, where this does nothing, so exactly one redo happens and it terminates.
            probe.onGetTable = () -> {
                if (!LockHoldDepth.isUnderLock()) {
                    mv.setRefreshScheme(mv.getRefreshScheme().copy());
                }
            };
            ConstantOperator result =
                    MetaFunctions.inspectMVRefreshInfo(ConstantOperator.createVarchar(DB_NAME + ".mv_raced"));

            Assertions.assertNotNull(result);
            Assertions.assertTrue(probe.getTableCalls.get() > undisturbedCalls,
                    "the refresh-scheme swap went unnoticed, so the report mixes two moments");
            Assertions.assertTrue(probe.underLock.get(),
                    "the redo is supposed to happen under the lock; if it did not, this test is not "
                            + "exercising the branch it claims to");
        } finally {
            starRocksAssert.dropMaterializedView("mv_raced");
        }
    }

    @Test
    public void testInspectStillReportsInternalBaseTablesAlongsideAnExternalOne() throws Exception {
        // A mixed MV keeps an internal base table on the in-lock path while the external one is
        // gathered before the lock; both must still show up in the result.
        starRocksAssert.withTable("CREATE TABLE mixed_base(l_orderkey int, v int) "
                + "DISTRIBUTED BY HASH(l_orderkey) PROPERTIES('replication_num'='1')");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_mixed\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT a.l_orderkey, b.v FROM hive0.partitioned_db.lineitem_par a "
                + "JOIN mixed_base b ON a.l_orderkey = b.l_orderkey;");
        try {
            installProbe();
            ConstantOperator result =
                    MetaFunctions.inspectMVRefreshInfo(ConstantOperator.createVarchar(DB_NAME + ".mv_mixed"));
            String json = result.getVarchar();
            Assertions.assertTrue(json.contains("lineitem_par"), json);
            Assertions.assertTrue(json.contains("mixed_base"), json);
            Assertions.assertFalse(probe.underLock.get(),
                    "connector metadata was fetched while an FE metadata lock was held");
        } finally {
            starRocksAssert.dropMaterializedView("mv_mixed");
            starRocksAssert.dropTable("mixed_base");
        }
    }
}
