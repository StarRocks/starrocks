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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.connector.partitiontraits.PrefetchedPartitionInfos;
import com.starrocks.mv.refresh.pct.MVPCTRefreshPlanner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An MV refresh takes FE metadata locks that cover only the MV and its internal base tables. An external base
 * table is outside that protection domain, so resolving it or reading its partitions while one of those locks is
 * held costs a connector round trip under the lock and protects nothing.
 *
 * <p>The probe counts connector calls per lock state, and only on the thread that installed it: creating an MV
 * hands its definition to the {@code mv-plan-cache} executor, which resolves the same hive table asynchronously
 * on a thread whose lock state says nothing about the refresh under test.
 */
public class MVRefreshLockConnectorIOTest extends MVTestBase {

    private Probe probe;
    private LockProbeHiveMetadata hive;
    private final Map<String, ConnectorMetadata> originalMetadata = Maps.newHashMap();

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        // Each mockXxxCatalog installs a fresh metadata manager; mock hive and iceberg into one.
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(temp, "junit").toURI().toString());
    }

    /** Connector calls seen by the probed catalogs, per lock state. */
    private static class Probe {
        private final Thread owner = Thread.currentThread();
        private final AtomicInteger getTableCalls = new AtomicInteger();
        private final AtomicInteger getTableCallsUnderLock = new AtomicInteger();
        /** Calls made from change detection, i.e. with {@link MVPCTRefreshPlanner} on the stack. */
        private final AtomicInteger detectionCalls = new AtomicInteger();
        private final AtomicInteger detectionCallsUnderLock = new AtomicInteger();
        /** Partition reads of any origin, for the scope's own test. */
        private final AtomicInteger partitionCalls = new AtomicInteger();
        /** Where the first call under the lock came from, so a failure names the call site. */
        private volatile Throwable firstUnderLock;

        private boolean isObserved() {
            return Thread.currentThread() == owner;
        }

        private static boolean fromDetection() {
            return StackWalker.getInstance().walk(frames ->
                    frames.anyMatch(f -> f.getClassName().equals(MVPCTRefreshPlanner.class.getName())));
        }

        private void recordFirstUnderLock(String call) {
            if (firstUnderLock == null) {
                firstUnderLock = new Throwable(call + " under lock");
            }
        }

        private void sampleGetTable(String call) {
            if (!isObserved()) {
                return;
            }
            getTableCalls.incrementAndGet();
            if (LockHoldDepth.isUnderLock()) {
                getTableCallsUnderLock.incrementAndGet();
                recordFirstUnderLock(call);
            }
        }

        private void samplePartitionCall(String call) {
            if (!isObserved()) {
                return;
            }
            partitionCalls.incrementAndGet();
            if (!fromDetection()) {
                return;
            }
            detectionCalls.incrementAndGet();
            if (LockHoldDepth.isUnderLock()) {
                detectionCallsUnderLock.incrementAndGet();
                recordFirstUnderLock(call);
            }
        }
    }

    private static class LockProbeHiveMetadata extends MockedHiveMetadata {
        private final Probe probe;

        private LockProbeHiveMetadata(Probe probe) {
            this.probe = probe;
        }

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            probe.sampleGetTable("getTable " + dbName + "." + tblName);
            return super.getTable(context, dbName, tblName);
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName,
                                               ConnectorMetadataRequestContext requestContext) {
            probe.samplePartitionCall("listPartitionNames " + dbName + "." + tableName);
            return super.listPartitionNames(dbName, tableName, requestContext);
        }

        @Override
        public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
            probe.samplePartitionCall("getPartitions " + table.getName());
            return super.getPartitions(table, partitionNames);
        }
    }

    /**
     * Like production, every getTable hands out a new table object over the same native table, so the ref-table
     * check under the lock sees a different object than the one prefetched.
     */
    private static class LockProbeIcebergMetadata extends MockIcebergMetadata {
        private final Probe probe;

        private LockProbeIcebergMetadata(Probe probe) {
            this.probe = probe;
        }

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            probe.sampleGetTable("getTable " + dbName + "." + tblName);
            return super.getTable(context, dbName, tblName);
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName,
                                               ConnectorMetadataRequestContext requestContext) {
            probe.samplePartitionCall("listPartitionNames " + dbName + "." + tableName);
            return super.listPartitionNames(dbName, tableName, requestContext);
        }

        @Override
        public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
            probe.samplePartitionCall("getPartitions " + table.getName());
            return super.getPartitions(table, partitionNames);
        }
    }

    private void installProbe() {
        probe = new Probe();
        hive = new LockProbeHiveMetadata(probe);
        register(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, hive);
        register(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME, new LockProbeIcebergMetadata(probe));
    }

    private void register(String catalog, ConnectorMetadata metadata) {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        originalMetadata.computeIfAbsent(catalog, c -> metadataMgr.getOptionalMetadata(c)
                .orElseThrow(() -> new IllegalStateException(c + " catalog is not registered")));
        metadataMgr.registerMockedMetadata(catalog, metadata);
    }

    @AfterEach
    public void removeProbe() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        originalMetadata.forEach(metadataMgr::registerMockedMetadata);
        originalMetadata.clear();
    }

    private Supplier<String> underLockStack() {
        return () -> "connector metadata was fetched while an FE metadata lock was held:\n"
                + Throwables.getStackTraceAsString(probe.firstUnderLock);
    }

    /**
     * The refresh INSERT is analyzed under a PlannerMetaLocker. Its external relations must already be resolved
     * by then, the way StatementPlanner pre-resolves them for a user INSERT.
     */
    @Test
    public void testRefreshPlanDoesNotResolveAnExternalBaseTableUnderTheLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_refresh_plan_over_hive\n"
                + "PARTITION BY (`l_shipdate`)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_refresh_plan_over_hive");
            // The first refresh also creates the MV's partitions, a DDL that resolves the base table under its
            // own lock (inferDistribution); keep that out of the measurement. A forced refresh then goes
            // straight to building the INSERT plan for every partition, without detecting changes first.
            withMVRefreshTaskRun(DB_NAME, mv);
            installProbe();
            TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
            taskRun.getProperties().put(TaskRun.FORCE, "true");
            initAndExecuteTaskRun(taskRun);
            Assertions.assertTrue(probe.getTableCalls.get() > 0,
                    "the probe never saw the connector, so this test proves nothing");
            Assertions.assertEquals(0, probe.getTableCallsUnderLock.get(), underLockStack());
        } finally {
            starRocksAssert.dropMaterializedView("mv_refresh_plan_over_hive");
        }
    }

    /**
     * The first refresh creates the MV's partitions under the MV's lock, and inferring their bucket number walks
     * the base tables. Only native ones contribute, so an external one must not be resolved there.
     */
    @Test
    public void testAddingPartitionsDoesNotResolveAnExternalBaseTableUnderTheLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_add_partitions_over_hive\n"
                + "PARTITION BY (`l_shipdate`)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_add_partitions_over_hive");
            Assertions.assertTrue(mv.getPartitions().isEmpty());
            installProbe();
            withMVRefreshTaskRun(DB_NAME, mv);
            Assertions.assertFalse(mv.getPartitions().isEmpty(), "the refresh added no partitions");
            Assertions.assertTrue(probe.getTableCalls.get() > 0,
                    "the probe never saw the connector, so this test proves nothing");
            Assertions.assertEquals(0, probe.getTableCallsUnderLock.get(), underLockStack());
        } finally {
            starRocksAssert.dropMaterializedView("mv_add_partitions_over_hive");
        }
    }

    private static Map<String, Long> visibleVersions(MaterializedView mv) {
        return mv.getPartitions().stream().collect(Collectors.toMap(Partition::getName,
                p -> p.getDefaultPhysicalPartition().getVisibleVersion()));
    }

    /**
     * Refresh with the probe installed, assert change detection stayed off the connector under the lock, and
     * return the MV partitions the refresh wrote.
     */
    private Set<String> refreshAndAssertDetectionOffTheLock(MaterializedView mv) throws Exception {
        Map<String, Long> before = visibleVersions(mv);
        installProbe();
        withMVRefreshTaskRun(DB_NAME, mv);
        Assertions.assertTrue(probe.detectionCalls.get() > 0,
                "change detection never reached the connector, so this test proves nothing");
        Assertions.assertEquals(0, probe.detectionCallsUnderLock.get(), underLockStack());
        Map<String, Long> after = visibleVersions(mv);
        return after.keySet().stream()
                .filter(name -> !after.get(name).equals(before.get(name)))
                .collect(Collectors.toSet());
    }

    /** Range-partitioned over a ref table: getMvPartitionNamesToRefresh → getMvBaseTableUpdateInfo. */
    @Test
    public void testRangeMVDetectsRefTableChangesOffTheLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_detect_range\n"
                + "PARTITION BY (`l_shipdate`)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_detect_range");
            withMVRefreshTaskRun(DB_NAME, mv);

            // Nothing changed: detection still reads the connector, and refreshes nothing.
            Assertions.assertEquals(Set.of(), refreshAndAssertDetectionOffTheLock(mv));

            hive.updatePartitions("partitioned_db", "lineitem_par", ImmutableList.of("l_shipdate=1998-01-02"));
            Assertions.assertEquals(Set.of("p19980102"), refreshAndAssertDetectionOffTheLock(mv));
        } finally {
            starRocksAssert.dropMaterializedView("mv_detect_range");
        }
    }

    /** A non-ref base table changing invalidates every partition: needsRefreshBasedOnNonRefTables. */
    @Test
    public void testRangeMVDetectsNonRefTableChangesOffTheLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_detect_non_ref\n"
                + "PARTITION BY (`par_date`)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT t1.c1, t1.c2, par_col, t1_par.par_date FROM hive0.partitioned_db.t1 "
                + "JOIN hive0.partitioned_db.t1_par USING (par_col);");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_detect_non_ref");
            withMVRefreshTaskRun(DB_NAME, mv);
            Assertions.assertEquals(Set.of(), refreshAndAssertDetectionOffTheLock(mv));

            hive.updatePartitions("partitioned_db", "t1", ImmutableList.of("par_col=0"));
            Assertions.assertEquals(visibleVersions(mv).keySet(), refreshAndAssertDetectionOffTheLock(mv));
        } finally {
            starRocksAssert.dropMaterializedView("mv_detect_non_ref");
        }
    }

    @Test
    public void testListMVDetectsChangesOffTheLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_detect_list\n"
                + "PARTITION BY (par_col, par_date)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT c1, c2, par_col, par_date FROM hive0.partitioned_db.t1_par;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_detect_list");
            Assertions.assertTrue(mv.getPartitionInfo().isListPartition());
            withMVRefreshTaskRun(DB_NAME, mv);
            Assertions.assertEquals(Set.of(), refreshAndAssertDetectionOffTheLock(mv));

            hive.updatePartitions("partitioned_db", "t1_par", ImmutableList.of("par_col=0/par_date=2020-01-01"));
            Assertions.assertEquals(1, refreshAndAssertDetectionOffTheLock(mv).size());
        } finally {
            starRocksAssert.dropMaterializedView("mv_detect_list");
        }
    }

    /** isNonPartitionedMVNeedToRefresh, including the dropped-partition check on the unchanged run. */
    @Test
    public void testNonPartitionedMVDetectsChangesOffTheLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_detect_unpartitioned\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_detect_unpartitioned");
            withMVRefreshTaskRun(DB_NAME, mv);
            Assertions.assertEquals(Set.of(), refreshAndAssertDetectionOffTheLock(mv));

            hive.updatePartitions("partitioned_db", "lineitem_par", ImmutableList.of("l_shipdate=1998-01-03"));
            Assertions.assertEquals(Set.of("mv_detect_unpartitioned"), refreshAndAssertDetectionOffTheLock(mv));
        } finally {
            starRocksAssert.dropMaterializedView("mv_detect_unpartitioned");
        }
    }

    /**
     * The shape behind most of the production records: a range MV over an Iceberg ref table. The prefetch and
     * the check under the lock resolve two different table objects at the same snapshot, and must still meet.
     */
    @Test
    public void testIcebergMVDetectsChangesOffTheLock() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_detect_iceberg\n"
                + "PARTITION BY str2date(`date`, '%Y-%m-%d')\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT id, data, date FROM iceberg0.partitioned_db.t1;");
        try {
            MaterializedView mv = getMv(DB_NAME, "mv_detect_iceberg");
            withMVRefreshTaskRun(DB_NAME, mv);
            Assertions.assertEquals(Set.of(), refreshAndAssertDetectionOffTheLock(mv));

            ((MockIcebergMetadata) GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).orElseThrow())
                    .updatePartitions("partitioned_db", "t1", ImmutableList.of("date=2020-01-02"));
            Assertions.assertEquals(Set.of("p20200102_20200103"), refreshAndAssertDetectionOffTheLock(mv));
        } finally {
            starRocksAssert.dropMaterializedView("mv_detect_iceberg");
        }
    }

    /**
     * The scope is a cache: it answers only a call whose table and snapshot match what was prefetched, and a
     * call outside the scope, or with another snapshot, still goes to the connector.
     */
    @Test
    public void testPrefetchedPartitionInfosAnswerOnlyTheSameTableAndSnapshot() throws Exception {
        installProbe();
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(connectContext, MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, "partitioned_db", "lineitem_par");
        Map<String, PartitionInfo> live = ConnectorPartitionTraits.build(table).getPartitionNameWithPartitionInfo();

        try (PrefetchedPartitionInfos prefetched = PrefetchedPartitionInfos.open()) {
            Assertions.assertTrue(prefetched.prefetch(table, null, true));
            int afterPrefetch = probe.partitionCalls.get();

            Assertions.assertEquals(live.keySet(),
                    ConnectorPartitionTraits.build(table).getPartitionNameWithPartitionInfo().keySet());
            Assertions.assertEquals(afterPrefetch, probe.partitionCalls.get(), "a prefetched fetch hit the connector");

            ConnectorPartitionTraits.build(null, table, TvrTableSnapshot.of(Optional.of(1L)))
                    .getPartitionNameWithPartitionInfo();
            Assertions.assertTrue(probe.partitionCalls.get() > afterPrefetch,
                    "a fetch of another snapshot was answered from the prefetched one");
        }

        int afterScope = probe.partitionCalls.get();
        ConnectorPartitionTraits.build(table).getPartitionNameWithPartitionInfo();
        Assertions.assertTrue(probe.partitionCalls.get() > afterScope, "the scope outlived its close()");
    }
}
