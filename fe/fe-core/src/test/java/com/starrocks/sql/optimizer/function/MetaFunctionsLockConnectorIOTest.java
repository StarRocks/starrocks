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

import com.google.common.collect.Maps;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.LockProbe;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
        private static final String GET_TABLE = "getTable";
        /** Confined to the thread that installed the probe; see the class comment for why. */
        private final LockProbe lock = LockProbe.onCurrentThread();
        /** Runs on every getTable, so a test can interleave something at a point it controls. */
        private Runnable onGetTable;

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            lock.record(GET_TABLE);
            if (lock.isProbedThread() && onGetTable != null) {
                onGetTable.run();
            }
            return super.getTable(context, dbName, tblName);
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName,
                                               ConnectorMetadataRequestContext requestContext) {
            lock.record("listPartitionNames");
            return super.listPartitionNames(dbName, tableName, requestContext);
        }

        @Override
        public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
            lock.record("getPartitions");
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
        probe.lock.assertNothingUnderTheLock("connector metadata was fetched while an FE metadata lock was held");
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
     * The report compares the connector's partitions against the MV's own refresh state. It works on a copy of
     * that state taken under the MV lock, so a refresh committing while the connector is being read neither
     * pulls a connector call under the lock nor makes the report mix two refresh states.
     *
     * <p>Reproduced without threads: the probe swaps the MV's refresh scheme on every connector call, which is
     * exactly where a concurrent MVVersionManager commit would land, adding one more marker partition each time.
     * The connector calls before the copy (the mvToRefreshPartitions block) may land in it; the last one is made
     * while the copy is compared against the connector, so its marker must not be in the report.
     */
    @Test
    public void testAConcurrentRefreshCommitDoesNotPullTheConnectorUnderTheLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_raced\n"
                + "PARTITION BY (`l_shipdate`)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par;");
        try {
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(DB_NAME, "mv_raced");
            BaseTableInfo hiveTable = mv.getBaseTableInfos().get(0);

            installProbe();
            AtomicInteger commits = new AtomicInteger();
            probe.onGetTable = () -> {
                MaterializedView.MvRefreshScheme committed = mv.getRefreshScheme().copy();
                committed.getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap()
                        .computeIfAbsent(hiveTable, k -> Maps.newHashMap())
                        .put("p_commit_" + commits.incrementAndGet() + "_",
                                new MaterializedView.BasePartitionInfo(1L, 1L, 1L));
                mv.setRefreshScheme(committed);
            };
            ConstantOperator result =
                    MetaFunctions.inspectMVRefreshInfo(ConstantOperator.createVarchar(DB_NAME + ".mv_raced"));

            Assertions.assertNotNull(result);
            Assertions.assertTrue(commits.get() > 0, "no connector call was made");
            Assertions.assertFalse(result.getVarchar().contains("p_commit_" + commits.get() + "_"),
                    result.getVarchar());
            probe.lock.assertNothingUnderTheLock("connector metadata was fetched while an FE metadata lock was held");
        } finally {
            starRocksAssert.dropMaterializedView("mv_raced");
        }
    }

    /**
     * An internal base table's partitions must be read under that table's own lock: the (db, mv) lock does not
     * exclude an ADD/DROP PARTITION on another table, whose partition map is a plain HashMap. Probed from another
     * thread while the report iterates the base table's partitions: a WRITE lock on the base table must not be
     * grantable at that moment.
     */
    @Test
    public void testInspectReadsAnInternalBaseTableUnderItsOwnLock() throws Exception {
        starRocksAssert.withTable("CREATE TABLE locked_base(k1 int, v int) "
                + "DISTRIBUTED BY HASH(k1) PROPERTIES('replication_num'='1')");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_over_locked_base\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT k1, v FROM locked_base;");
        try {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
            OlapTable baseTable = (OlapTable) db.getTable("locked_base");
            BaseTableWriteProbe probe = probeBaseTableWritability(db.getId(), baseTable.getId());
            ConstantOperator result = MetaFunctions.inspectMVRefreshInfo(
                    ConstantOperator.createVarchar(DB_NAME + ".mv_over_locked_base"));

            Assertions.assertTrue(result.getVarchar().contains("locked_base"), result.getVarchar());
            Assertions.assertTrue(probe.reads().get() > 0, "the base table's partitions were never read");
            Assertions.assertFalse(probe.writable().get(),
                    "the base table's partitions were read without holding its lock");
        } finally {
            starRocksAssert.dropMaterializedView("mv_over_locked_base");
            starRocksAssert.dropTable("locked_base");
        }
    }

    /**
     * Some statements are planned, and this function constant-folded, under the planner's locks on the tables they
     * reference. One more lock taken there could close a wait cycle: this statement holds the MV and waits for the
     * base table behind a queued writer, while another holds the base table and waits for the MV behind another
     * queued writer. So a caller that already holds a lock gets no further lock taken on its behalf.
     */
    @Test
    public void testInspectTakesNoFurtherLockWhenTheCallerAlreadyHoldsOne() throws Exception {
        starRocksAssert.withTable("CREATE TABLE planner_locked_base(k1 int, v int) "
                + "DISTRIBUTED BY HASH(k1) PROPERTIES('replication_num'='1')");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_over_planner_locked_base\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT k1, v FROM planner_locked_base;");
        try {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
            OlapTable baseTable = (OlapTable) db.getTable("planner_locked_base");
            MaterializedView mv = (MaterializedView) db.getTable("mv_over_planner_locked_base");
            BaseTableWriteProbe probe = probeBaseTableWritability(db.getId(), baseTable.getId());
            // the planner's lock on the table the statement reads FROM
            Locker plannerLocker = new Locker();
            plannerLocker.lockTableWithIntensiveDbLock(db.getId(), mv.getId(), LockType.READ);
            ConstantOperator result;
            try {
                result = MetaFunctions.inspectMVRefreshInfo(
                        ConstantOperator.createVarchar(DB_NAME + ".mv_over_planner_locked_base"));
            } finally {
                plannerLocker.unLockTableWithIntensiveDbLock(db.getId(), mv.getId(), LockType.READ);
            }

            Assertions.assertTrue(result.getVarchar().contains("planner_locked_base"), result.getVarchar());
            Assertions.assertTrue(probe.reads().get() > 0, "the base table's partitions were never read");
            Assertions.assertTrue(probe.writable().get(),
                    "a lock was taken on the base table while the caller already held one");
        } finally {
            starRocksAssert.dropMaterializedView("mv_over_planner_locked_base");
            starRocksAssert.dropTable("planner_locked_base");
        }
    }

    private record BaseTableWriteProbe(AtomicInteger reads, AtomicBoolean writable) {
    }

    /**
     * Each time this thread reads the base table's partitions, checks from another thread whether a WRITE lock on
     * the base table is grantable at that moment.
     */
    private static BaseTableWriteProbe probeBaseTableWritability(long dbId, long baseTableId) {
        Thread inspectThread = Thread.currentThread();
        BaseTableWriteProbe probe = new BaseTableWriteProbe(new AtomicInteger(), new AtomicBoolean());
        new MockUp<OlapTable>() {
            @Mock
            public Collection<Partition> getPartitions(Invocation invocation) {
                OlapTable table = invocation.getInvokedInstance();
                if (Thread.currentThread() == inspectThread && table.getId() == baseTableId) {
                    probe.reads().incrementAndGet();
                    if (canWriteLockFromAnotherThread(dbId, baseTableId)) {
                        probe.writable().set(true);
                    }
                }
                return invocation.proceed();
            }
        };
        return probe;
    }

    private static boolean canWriteLockFromAnotherThread(long dbId, long tableId) {
        AtomicBoolean acquired = new AtomicBoolean();
        Thread prober = new Thread(() -> {
            Locker locker = new Locker();
            if (locker.tryLockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE, 200, TimeUnit.MILLISECONDS)) {
                acquired.set(true);
                locker.unLockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
            }
        });
        prober.start();
        try {
            prober.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return acquired.get();
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
            Assertions.assertFalse(probe.lock.anyUnderLock(),
                    "connector metadata was fetched while an FE metadata lock was held");
        } finally {
            starRocksAssert.dropMaterializedView("mv_mixed");
            starRocksAssert.dropTable("mixed_base");
        }
    }
}
