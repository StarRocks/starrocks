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

package com.starrocks.alter.reshard;

import com.starrocks.alter.AlterJobException;
import com.starrocks.alter.AlterJobExecutor;
import com.starrocks.alter.reshard.presplit.LoadKind;
import com.starrocks.alter.reshard.presplit.PartitionSamples;
import com.starrocks.alter.reshard.presplit.PreSplitOutcome;
import com.starrocks.alter.reshard.presplit.PreSplitProfile;
import com.starrocks.alter.reshard.presplit.SampleRow;
import com.starrocks.alter.reshard.presplit.ScanContext;
import com.starrocks.alter.reshard.presplit.SkipReason;
import com.starrocks.alter.reshard.presplit.TabletPreSplitCoordinator;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockException;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.journal.JournalTask;
import com.starrocks.journal.JournalWriteException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.load.InsertOverwriteJob;
import com.starrocks.load.InsertOverwriteJobRunner;
import com.starrocks.load.InsertOverwriteJobState;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.EditLogException;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LeaderLease;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTableSink;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TabletReshardConcurrencyTest {
    private static Database db;
    private static OlapTable table;
    private static OlapTable automaticTable;
    private static List<Long> partitions;
    private TabletReshardJobMgr mgr;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // Queued WAL tests replace getEditLog() for every thread. Stop and drain all leader daemons,
        // including heartbeat-mgr and timePrinter, before they can submit unrelated journal entries
        // to a test-owned queue and participate in its WAL fence.
        List<LeaderDaemon> daemons = LeaderDaemon.getRunningInstances();
        daemons.forEach(LeaderDaemon::setStop);
        LeaderDaemon.awaitQuiesced(daemons, 30_000L);
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        Config.enable_range_distribution = true;
        StarRocksAssert starRocksAssert = new StarRocksAssert(context);
        starRocksAssert.withDatabase("reshard_concurrency").useDatabase("reshard_concurrency")
                .withTable("create table t (k int, v int) partition by range(k) "
                        + "(partition p1 values less than ('10'), partition p2 values less than ('20'), "
                        + "partition p3 values less than ('30')) order by(k) "
                        + "properties('replication_num'='1')");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("reshard_concurrency");
        table = (OlapTable) db.getTable("t");
        partitions = table.getAllPhysicalPartitions().stream().map(PhysicalPartition::getId).sorted().toList();
        starRocksAssert.alterTable("alter table t add temporary partition tmp_a values [('10'), ('20'))");
        starRocksAssert.alterTable("alter table t add temporary partition tmp_b values [('20'), ('30'))");
        starRocksAssert.withTable("create table t_auto (dt datetime NOT NULL, v int) "
                + "partition by date_trunc('day', dt) "
                + "(START ('2026-01-01') END ('2026-01-03') EVERY (INTERVAL 1 DAY)) "
                + "order by(dt) properties('replication_num'='1')");
        automaticTable = (OlapTable) db.getTable("t_auto");
    }

    @BeforeEach
    public void beforeEach() {
        mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        mgr.getTabletReshardJobs().clear();
        table.setState(OlapTable.OlapTableState.NORMAL);
        automaticTable.setState(OlapTable.OlapTableState.NORMAL);
    }

    private TabletReshardJob job(boolean merge, long... targets) {
        return jobFor(table, merge, targets);
    }

    private TabletReshardJob jobFor(OlapTable table, boolean merge, long... targets) {
        Map<Long, ReshardingPhysicalPartition> map = new HashMap<>();
        for (long id : targets) {
            MaterializedIndex source = table.getPhysicalPartition(id).getLatestBaseIndex();
            MaterializedIndex target = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                    source.getMetaId(), MaterializedIndex.IndexState.NORMAL, source.getShardGroupId());
            map.put(id, new ReshardingPhysicalPartition(id,
                    Map.of(source.getId(), new ReshardingMaterializedIndex(source.getId(), target, List.of()))));
        }
        long id = GlobalStateMgr.getCurrentState().getNextId();
        return merge ? new MergeTabletJob(id, db.getId(), table.getId(), map)
                : new SplitTabletJob(id, db.getId(), table.getId(), map);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,true"})
    public void disjointJobsAdmittedAndCombinedOverlapRejected(boolean firstMerge, boolean secondMerge) throws Exception {
        TabletReshardJob first = job(firstMerge, partitions.get(0));
        TabletReshardJob second = job(secondMerge, partitions.get(1));
        mgr.addTabletReshardJob(first);
        mgr.addTabletReshardJob(second);
        assertThrows(StarRocksException.class,
                () -> mgr.addTabletReshardJob(job(false, partitions.get(0), partitions.get(2))));
        assertEquals(2, mgr.getTabletReshardJobs().size());
        assertEquals(TabletReshardJob.JobState.PENDING, first.getJobState());
        assertEquals(TabletReshardJob.JobState.PENDING, second.getJobState());
        assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
        try (AutoCloseableLock ignored = new AutoCloseableLock(new Locker(), db.getId(),
                List.of(table.getId()), LockType.READ)) {
            assertEquals(Set.of(partitions.get(0), partitions.get(1)),
                    mgr.getReshardingPhysicalPartitionIdsLocked(table));
            assertFalse(mgr.isPhysicalPartitionReshardingLocked(table, partitions.get(2)));
        }
    }

    private void executeAlter(String clauses) throws Exception {
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE reshard_concurrency.t " + clauses, context);
        new AlterJobExecutor().process(stmt, context);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,true"})
    public void explicitSqlReachesDisjointTargetAfterFirstJobIsActive(boolean firstMerge, boolean secondMerge)
            throws Exception {
        TabletReshardJob first = job(firstMerge, partitions.get(0));
        mgr.addTabletReshardJob(first);
        assertTrue(first.isAdmissionReady());
        PhysicalPartition partition = table.getPhysicalPartition(partitions.get(1));
        MaterializedIndex index = partition.getLatestBaseIndex();
        LakeTablet original = (LakeTablet) index.getTablets().get(0);
        TabletRange originalRange = original.getRange();
        boolean originalShared = original.hasSharedFiles();
        boolean mergeEnabled = Config.tablet_reshard_enable_tablet_merge;
        Config.tablet_reshard_enable_tablet_merge = true;
        LakeTablet sibling = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId(), new TabletRange());
        try {
            String clause;
            if (secondMerge) {
                Tuple cut = new Tuple(List.of(Variant.of(IntegerType.INT, "15")));
                original.setRange(new TabletRange(Range.of(null, cut, false, false)));
                sibling.setRange(new TabletRange(Range.of(cut, null, true, false)));
                original.setHasSharedFiles(false);
                sibling.setHasSharedFiles(false);
                index.addTablet(sibling, new TabletMeta(db.getId(), table.getId(), partition.getId(),
                        index.getId(), TStorageMedium.HDD, true));
                clause = "MERGE TABLETS (" + original.getId() + ", " + sibling.getId() + ")";
            } else {
                clause = "SPLIT TABLET (" + original.getId() + ") PROPERTIES('tablet_reshard_target_size'='-2')";
            }
            executeAlter(clause);
            assertEquals(2, mgr.getTabletReshardJobs().size());
            assertTrue(mgr.getTabletReshardJobs().values().stream().anyMatch(j -> j != first
                    && j.isAdmissionReady() && j.getReshardingPhysicalPartitions().keySet().equals(Set.of(partition.getId()))));
            long occupiedTablet = table.getPhysicalPartition(partitions.get(0))
                    .getLatestBaseIndex().getTablets().get(0).getId();
            RuntimeException conflict = assertThrows(RuntimeException.class, () -> executeAlter(
                    "SPLIT TABLET (" + occupiedTablet + ") PROPERTIES('tablet_reshard_target_size'='-2')"));
            assertTrue(conflict.getMessage().contains(Long.toString(first.getJobId())));
            assertEquals(2, mgr.getTabletReshardJobs().size());
        } finally {
            index.removeTablet(sibling.getId());
            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(sibling.getId());
            original.setRange(originalRange);
            original.setHasSharedFiles(originalShared);
            Config.tablet_reshard_enable_tablet_merge = mergeEnabled;
        }
    }

    @ParameterizedTest
    @EnumSource(value = OlapTable.OlapTableState.class, names = {"SCHEMA_CHANGE", "ROLLUP"})
    public void explicitSqlKeepsOtherAlterStatesBlocked(OlapTable.OlapTableState state) {
        table.setState(state);
        long tabletId = table.getPhysicalPartition(partitions.get(1)).getLatestBaseIndex().getTablets().get(0).getId();
        assertThrows(AlterJobException.class, () -> executeAlter("SPLIT TABLET (" + tabletId + ")"));
        assertTrue(mgr.getTabletReshardJobs().isEmpty());
    }

    @Test
    public void activeReshardKeepsSchemaRollupAndMixedClausesBlocked() throws Exception {
        TabletReshardJob first = job(false, partitions.get(0));
        mgr.addTabletReshardJob(first);
        assertThrows(AlterJobException.class, () -> executeAlter("ADD COLUMN extra int"));
        assertThrows(AlterJobException.class, () -> executeAlter("ADD ROLLUP r(k)"));
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        AlterTableStmt split = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE reshard_concurrency.t SPLIT TABLET", context);
        AlterTableStmt schema = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE reshard_concurrency.t ADD COLUMN extra int", context);
        AlterTableStmt mixed = new AlterTableStmt(split.getTableRef(),
                List.of(split.getAlterClauseList().get(0), schema.getAlterClauseList().get(0)));
        assertThrows(AlterJobException.class, () -> new AlterJobExecutor().process(mixed, context));
        assertEquals(Map.of(first.getJobId(), first), mgr.getTabletReshardJobs());
    }

    @Test
    public void preSplitEligibilityChecksOnlyResolvedPhysicalTarget() throws Exception {
        mgr.addTabletReshardJob(job(false, partitions.get(0)));
        boolean enabled = Config.enable_tablet_pre_split_for_insert_from_files;
        Config.enable_tablet_pre_split_for_insert_from_files = true;
        try {
            assertTrue(TabletPreSplitCoordinator.maybeAct(db, table, partitions.get(1), new ScanContext() { },
                    LoadKind.INSERT_FROM_FILES, true) instanceof PreSplitOutcome.Eligible);
            PreSplitOutcome busy = TabletPreSplitCoordinator.maybeAct(db, table, partitions.get(0), new ScanContext() { },
                    LoadKind.INSERT_FROM_FILES, true);
            assertEquals(SkipReason.TABLE_NOT_NORMAL, ((PreSplitOutcome.Skipped) busy).reason());
        } finally {
            Config.enable_tablet_pre_split_for_insert_from_files = enabled;
        }
    }

    private PartitionSamples samples(int partitionOffset) {
        PhysicalPartition partition = table.getPhysicalPartition(partitions.get(partitionOffset));
        List<SampleRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(SampleRow.ofSortKey(List.of(Variant.of(IntegerType.INT, Integer.toString(i)))));
        }
        return new PartitionSamples(List.of(), table.getPartition(partition.getParentId()).getName(), true,
                partition.getId(), partition.getLatestBaseIndex().getTablets().get(0).getId(), null, rows, Long.MAX_VALUE / 2);
    }

    @Test
    public void preSplitBatchAdmitsFreeTargetsAndSkipsOccupiedTarget() throws Exception {
        TabletReshardJob first = job(false, partitions.get(0));
        mgr.addTabletReshardJob(first);
        PreSplitOutcome result = TabletPreSplitCoordinator.submitForPartitionsCombined(db, table,
                List.of(samples(0), samples(1), samples(2)), 2, UtFrameUtils.createDefaultCtx(), null, Set.of());
        assertTrue(result instanceof PreSplitOutcome.SubmittedCombined);
        PreSplitOutcome.SubmittedCombined submitted = (PreSplitOutcome.SubmittedCombined) result;
        assertEquals(Set.of(partitions.get(1), partitions.get(2)),
                submitted.combinedJob().getReshardingPhysicalPartitions().keySet());
        assertEquals(SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE,
                ((PreSplitOutcome.Skipped) submitted.perPartitionResults().get(0)).reason());
        assertEquals(2, mgr.getTabletReshardJobs().size());
    }

    @Test
    public void preSplitBatchFinalAdmissionRaceRejectsWholeRequest() throws Exception {
        TabletReshardJob first = job(false, partitions.get(0));
        mgr.addTabletReshardJob(first);
        TabletReshardJob racer = job(false, partitions.get(1));
        new MockUp<SplitTabletJobFactory>() {
            @Mock
            public TabletReshardJob forExternalBoundaries(Invocation invocation, Database database, OlapTable olapTable,
                    Map<Long, List<TabletRange>> ranges) throws StarRocksException {
                TabletReshardJob combined = invocation.proceed(database, olapTable, ranges);
                mgr.addTabletReshardJob(racer);
                return combined;
            }
        };
        PreSplitOutcome result = TabletPreSplitCoordinator.submitForPartitionsCombined(db, table,
                List.of(samples(1), samples(2)), 2, UtFrameUtils.createDefaultCtx(), null, Set.of());
        assertEquals(SkipReason.SUBMIT_FAILED, ((PreSplitOutcome.Skipped) result).reason());
        assertEquals(Map.of(first.getJobId(), first, racer.getJobId(), racer), mgr.getTabletReshardJobs());
        try (AutoCloseableLock ignored = new AutoCloseableLock(new Locker(), db.getId(),
                List.of(table.getId()), LockType.READ)) {
            assertFalse(mgr.isPhysicalPartitionReshardingLocked(table, partitions.get(2)));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void failedCleaningLogRetriesBeforeCleanup(boolean merge) throws Exception {
        TabletReshardJob pending = job(merge, partitions.get(0));
        mgr.addTabletReshardJob(pending);
        AtomicBoolean fail = new AtomicBoolean(true);
        AtomicInteger cleaningRuns = new AtomicInteger();
        AtomicInteger installations = new AtomicInteger();
        List<String> payloads = new ArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logUpdateTabletReshardJob(TabletReshardJob logged) {
                payloads.add(GsonUtils.GSON.toJson(logged));
                if (fail.get()) {
                    throw new IllegalStateException("injected CLEANING WAL failure");
                }
            }
        };
        new MockUp<SplitTabletJob>() {
            @Mock
            protected void runRunningJob() {
                installations.incrementAndGet();
                pending.setJobState(TabletReshardJob.JobState.CLEANING);
            }
            @Mock
            protected void runCleaningJob() {
                cleaningRuns.incrementAndGet();
            }
        };
        new MockUp<MergeTabletJob>() {
            @Mock
            protected void runRunningJob() {
                installations.incrementAndGet();
                pending.setJobState(TabletReshardJob.JobState.CLEANING);
            }
            @Mock
            protected void runCleaningJob() {
                cleaningRuns.incrementAndGet();
            }
        };
        pending.jobState = TabletReshardJob.JobState.RUNNING;
        pending.run();
        pending.run();
        assertFalse(pending.isLayoutReady());
        assertEquals(0, cleaningRuns.get(), "cleanup must wait for a successful frozen CLEANING log retry");
        assertEquals(2, payloads.size());
        assertEquals(payloads.get(0), payloads.get(1));
        fail.set(false);
        pending.run();
        assertEquals(1, cleaningRuns.get());
        assertTrue(pending.isLayoutReady());
        assertEquals(3, payloads.size());
        assertEquals(payloads.get(0), payloads.get(2));
        assertEquals(1, installations.get(), "failed CLEANING WAL must not repeat index installation");
    }

    @Test
    public void overwritePlannerAcceptsSupportedTableWhileAnotherPartitionReshards() throws Exception {
        mgr.addTabletReshardJob(job(false, partitions.get(0)));
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        context.setDatabase("reshard_concurrency");
        InsertStmt insert = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(
                "insert overwrite t partition(p2) select 11, 1", context);
        assertNotNull(StatementPlanner.plan(insert, context));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true"})
    public void overwritePrepareAcceptsSupportedReshardTable(boolean dynamic, boolean system) throws Exception {
        OlapTable target = dynamic ? automaticTable : table;
        long occupied = dynamic ? automaticTable.getAllPhysicalPartitions().iterator().next().getId() : partitions.get(0);
        mgr.addTabletReshardJob(jobFor(target, false, occupied));
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        context.setDatabase("reshard_concurrency");
        context.setExecutionId(UUIDUtil.genTUniqueId());
        context.getSessionVariable().setDynamicOverwrite(dynamic);
        InsertStmt insert = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(dynamic
                ? "insert overwrite t_auto select cast('2026-01-02' as datetime), 1"
                : "insert overwrite t partition(p2) select 11, 1", context);
        insert.setSystem(system);
        assertNotNull(StatementPlanner.plan(insert, context));
        assertEquals(dynamic, insert.isDynamicOverwrite());
        InsertOverwriteJob overwrite = new InsertOverwriteJob(GlobalStateMgr.getCurrentState().getNextId(),
                insert, db.getId(), target.getId(), WarehouseManager.DEFAULT_WAREHOUSE_ID, dynamic);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(overwrite, context, new StmtExecutor(context, insert));
        new MockUp<InsertOverwriteJobRunner>() {
            @Mock
            public void handle() {
                // Stop after PREPARE transfers to RUNNING; the unchanged swap path waits for FINISHED.
            }
        };
        try {
            Deencapsulation.invoke(runner, "prepare");
            assertEquals(InsertOverwriteJobState.OVERWRITE_RUNNING, overwrite.getJobState());
            if (dynamic) {
                assertTrue(overwrite.getTxnId() > 0);
            }
        } finally {
            if (overwrite.getTxnId() > 0) {
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .abortTransaction(db.getId(), overwrite.getTxnId(), "test cleanup");
            }
        }
    }

    @Test
    public void dynamicCallersResumeAtOwnDurableLayoutWhileOrdinaryCallerWaitsFinished() throws Exception {
        GlobalTransactionMgr transactions = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        TransactionState.TxnCoordinator coordinator =
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1");
        long txnA = transactions.beginTransaction(db.getId(), List.of(table.getId()), "layout_a",
                coordinator, TransactionState.LoadJobSourceType.INSERT_STREAMING, 60);
        long txnB = transactions.beginTransaction(db.getId(), List.of(table.getId()), "layout_b",
                coordinator, TransactionState.LoadJobSourceType.INSERT_STREAMING, 60);
        PhysicalPartition physicalA = table.getPartition("tmp_a", true).getDefaultPhysicalPartition();
        PhysicalPartition physicalB = table.getPartition("tmp_b", true).getDefaultPhysicalPartition();
        MaterializedIndex sourceA = physicalA.getLatestBaseIndex();
        MaterializedIndex sourceB = physicalB.getLatestBaseIndex();
        SplitTabletJob jobA = splitTemporaryPartition(physicalA, 15);
        SplitTabletJob jobB = splitTemporaryPartition(physicalB, 25);
        MaterializedIndex replacementA = jobA.getReshardingPhysicalPartitions().get(physicalA.getId())
                .getReshardingIndexes().get(sourceA.getId()).getMaterializedIndex();
        MaterializedIndex replacementB = jobB.getReshardingPhysicalPartitions().get(physicalB.getId())
                .getReshardingIndexes().get(sourceB.getId()).getMaterializedIndex();
        assertFalse(sourceA.getId() == replacementA.getId());
        assertFalse(sourceB.getId() == replacementB.getId());
        assertTrue(Collections.disjoint(sourceA.getTabletIdsInOrder(), replacementA.getTabletIdsInOrder()));
        assertTrue(Collections.disjoint(sourceB.getTabletIdsInOrder(), replacementB.getTabletIdsInOrder()));
        jobA.addCleanupExcludedTransactionId(txnA);
        jobB.addCleanupExcludedTransactionId(txnB);
        mgr.addTabletReshardJob(jobA);
        mgr.addTabletReshardJob(jobB);
        Deencapsulation.setField(jobA, "endTransactionId", transactions.getTransactionIDGenerator().peekNextTransactionId());
        Deencapsulation.setField(jobB, "endTransactionId", transactions.getTransactionIDGenerator().peekNextTransactionId());
        CountDownLatch dynamicPolling = new CountDownLatch(2);
        AtomicBoolean polledA = new AtomicBoolean();
        AtomicBoolean polledB = new AtomicBoolean();
        PreSplitProfile profileA = new PreSplitProfile();
        PreSplitProfile profileB = new PreSplitProfile();
        AtomicBoolean abortOrdinary = new AtomicBoolean();
        try {
            CompletableFuture<Void> callerA = CompletableFuture.runAsync(() -> {
                try (PreSplitProfile.Scope ignored = PreSplitProfile.startAttempt(profileA, LoadKind.INSERT_FROM_TABLE)) {
                    TabletPreSplitCoordinator.awaitCombinedJobLayoutReadyAllowingFallback(LoadKind.INSERT_FROM_TABLE,
                            table, jobA, () -> {
                                if (polledA.compareAndSet(false, true)) {
                                    dynamicPolling.countDown();
                                }
                                return false;
                            }, jobA.getAdmissionLease());
                }
                assertTrue(jobA.getCleanupExcludedTransactionIds().isEmpty());
                replanTemporaryInsert("tmp_a", 11, txnA, replacementA);
            });
            CompletableFuture<Void> callerB = CompletableFuture.runAsync(() -> {
                try (PreSplitProfile.Scope ignored = PreSplitProfile.startAttempt(profileB, LoadKind.INSERT_FROM_TABLE)) {
                    TabletPreSplitCoordinator.awaitCombinedJobLayoutReadyAllowingFallback(LoadKind.INSERT_FROM_TABLE,
                            table, jobB, () -> {
                                if (polledB.compareAndSet(false, true)) {
                                    dynamicPolling.countDown();
                                }
                                return false;
                            }, jobB.getAdmissionLease());
                }
                assertTrue(jobB.getCleanupExcludedTransactionIds().isEmpty());
                replanTemporaryInsert("tmp_b", 21, txnB, replacementB);
            });
            assertTrue(dynamicPolling.await(5, TimeUnit.SECONDS));
            // Back replacement tablets with the existing fake StarOS placement, then install both
            // writable generations before either durable CLEANING record can release its caller.
            jobA.createShardsOnStarOS();
            jobB.createShardsOnStarOS();
            try (AutoCloseableLock ignored = new AutoCloseableLock(new Locker(), db.getId(),
                    List.of(table.getId()), LockType.WRITE)) {
                physicalA.addMaterializedIndex(replacementA, true);
                physicalB.addMaterializedIndex(replacementB, true);
                table.lastSchemaUpdateTime.set(System.nanoTime());
            }
            jobA.setJobState(TabletReshardJob.JobState.CLEANING);
            jobB.setJobState(TabletReshardJob.JobState.CLEANING);
            // Each cleanup sees the other caller's open transaction, even while excluding its own.
            jobA.runCleaningJob();
            jobB.runCleaningJob();
            assertEquals(TabletReshardJob.JobState.CLEANING, jobA.getJobState());
            assertEquals(TabletReshardJob.JobState.CLEANING, jobB.getJobState());
            assertTrue(jobA.isLayoutReady());
            assertTrue(jobB.isLayoutReady());
            assertFalse(jobA.isDone());
            assertFalse(jobB.isDone());
            callerA.get(5, TimeUnit.SECONDS);
            callerB.get(5, TimeUnit.SECONDS);
            assertEquals("LAYOUT_READY", profileA.toRuntimeProfile().getInfoString("Outcomes"));
            assertEquals("LAYOUT_READY", profileB.toRuntimeProfile().getInfoString("Outcomes"));
            CountDownLatch ordinaryPolling = new CountDownLatch(1);
            CompletableFuture<Void> ordinary = CompletableFuture.runAsync(() ->
                    TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(LoadKind.INSERT_FROM_TABLE,
                            table, jobA, () -> {
                                ordinaryPolling.countDown();
                                return abortOrdinary.get();
                            }));
            assertTrue(ordinaryPolling.await(5, TimeUnit.SECONDS));
            assertFalse(ordinary.isDone(), "ordinary callers retain FINISHED semantics");
            abortOrdinary.set(true);
            ordinary.get(5, TimeUnit.SECONDS);
        } finally {
            abortOrdinary.set(true);
            transactions.abortTransaction(db.getId(), txnA, "test cleanup");
            transactions.abortTransaction(db.getId(), txnB, "test cleanup");
            try (AutoCloseableLock ignored = new AutoCloseableLock(new Locker(), db.getId(),
                    List.of(table.getId()), LockType.WRITE)) {
                physicalA.deleteMaterializedIndexByIndexId(replacementA.getId());
                physicalB.deleteMaterializedIndexByIndexId(replacementB.getId());
                table.lastSchemaUpdateTime.set(System.nanoTime());
            }
            GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShards(Set.copyOf(replacementA.getTabletIdsInOrder()));
            GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShards(Set.copyOf(replacementB.getTabletIdsInOrder()));
        }
    }

    private SplitTabletJob splitTemporaryPartition(PhysicalPartition partition, int boundary) throws StarRocksException {
        Tuple cut = new Tuple(List.of(Variant.of(IntegerType.INT, Integer.toString(boundary))));
        SplitTabletJob job = (SplitTabletJob) SplitTabletJobFactory.forExternalBoundaries(db, table,
                Map.of(partition.getLatestBaseIndex().getTablets().get(0).getId(), List.of(
                        new TabletRange(Range.of(null, cut, false, false)),
                        new TabletRange(Range.of(cut, null, true, false)))));
        job.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        return job;
    }

    private void replanTemporaryInsert(String partition, int key, long transactionId, MaterializedIndex replacement) {
        try {
            ConnectContext context = UtFrameUtils.createDefaultCtx();
            context.setDatabase("reshard_concurrency");
            InsertStmt insert = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(
                    "insert into t temporary partition(" + partition + ") values(" + key + ",1)", context);
            insert.setTxnId(transactionId);
            ExecPlan plan = StatementPlanner.plan(insert, context);
            OlapTableSink olapSink = assertInstanceOf(OlapTableSink.class, plan.getTopFragment().getSink());
            TOlapTableSink sink = olapSink.toThrift().getOlap_table_sink();
            assertNotNull(sink);
            assertEquals(transactionId, insert.getTxnId());
            assertEquals(transactionId, sink.getTxn_id());
            assertEquals(1, sink.getPartition().getPartitionsSize());
            TOlapTablePartition selected = sink.getPartition().getPartitions().get(0);
            long physicalPartitionId = table.getPartition(partition, true).getDefaultPhysicalPartition().getId();
            assertEquals(physicalPartitionId, selected.getId());
            assertEquals(1, selected.getIndexesSize());
            // Thrift index_id identifies the logical schema; tablets and the transaction's physical
            // index snapshot must independently select the new generation rather than its predecessor.
            assertEquals(replacement.getMetaId(), selected.getIndexes().get(0).getIndex_id());
            Set<Long> expectedTablets = Set.copyOf(replacement.getTabletIdsInOrder());
            assertEquals(2, expectedTablets.size());
            assertEquals(expectedTablets, Set.copyOf(selected.getIndexes().get(0).getTablet_ids()));
            assertEquals(expectedTablets, Set.copyOf(sink.getLocation().getTablets().stream()
                    .map(location -> location.getTablet_id()).toList()));
            TransactionState txn = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                    .getTransactionState(db.getId(), transactionId);
            assertNotNull(txn);
            txn.readLock();
            try {
                assertEquals(List.of(replacement.getId()),
                        txn.getPartitionLoadedIndexIdsWithoutLock(table.getId(), physicalPartitionId));
            } finally {
                txn.readUnlock();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void cleaningWalMustCompleteBeforeLayoutReady() throws Exception {
        TabletReshardJob pending = job(false, partitions.get(0));
        mgr.addTabletReshardJob(pending);
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        useQueuedWal(queue);
        CompletableFuture<Void> transition = CompletableFuture.runAsync(() ->
                pending.setJobState(TabletReshardJob.JobState.CLEANING));
        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(task);
        try {
            assertEquals(TabletReshardJob.JobState.CLEANING, pending.getJobState());
            assertFalse(pending.isLayoutReady());
        } finally {
            task.markSucceed();
        }
        transition.get(5, TimeUnit.SECONDS);
        assertTrue(pending.isLayoutReady());
        pending.jobState = TabletReshardJob.JobState.ABORTING;
        assertFalse(pending.isLayoutReady());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void staleOriginalAdmissionLeaseCannotReportLayoutSuccess(boolean revokeAtReady) throws Exception {
        TabletReshardJob pending = job(false, partitions.get(0));
        mgr.addTabletReshardJob(pending);
        pending.setJobState(TabletReshardJob.JobState.CLEANING);
        LeaderLease original = pending.getAdmissionLease();
        pending.addCleanupExcludedTransactionId(42L);
        LeaderLease replacement = new LeaderLease(original.getHaEpoch(), original.getGeneration() + 1);
        AtomicInteger validations = new AtomicInteger();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public LeaderLease captureLeaderLease() {
                return replacement;
            }
            @Mock
            public boolean isLeaderLeaseValid(LeaderLease expected) {
                if (!revokeAtReady) {
                    return replacement.equals(expected);
                }
                return validations.getAndIncrement() == 0 && original.equals(expected);
            }
        };
        PreSplitProfile profile = new PreSplitProfile();
        try (PreSplitProfile.Scope ignored = PreSplitProfile.startAttempt(profile, LoadKind.INSERT_FROM_TABLE)) {
            TabletPreSplitCoordinator.awaitCombinedJobLayoutReadyAllowingFallback(LoadKind.INSERT_FROM_TABLE,
                    table, pending, () -> false, original);
        }
        assertEquals("RESHARD_LEADER_CHANGED_FALLBACK", profile.toRuntimeProfile().getInfoString("Outcomes"));
        assertTrue(pending.getCleanupExcludedTransactionIds().isEmpty());
    }

    @Test
    public void failedDurableDynamicHandoffKeepsJobAndRevokesTransactionExclusion() throws Exception {
        PhysicalPartition temporary = table.getPartition("tmp_a", true).getDefaultPhysicalPartition();
        PartitionSamples source = samples(1);
        PartitionSamples sample = new PartitionSamples(List.of(), "tmp_a", true, temporary.getId(),
                temporary.getLatestBaseIndex().getTablets().get(0).getId(), null, source.samples(), Long.MAX_VALUE / 2);
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void addTabletReshardJob(Invocation invocation, TabletReshardJob admitted) throws StarRocksException {
                invocation.proceed(admitted);
                throw new IllegalStateException("admission durable, original lease now stale");
            }
        };
        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForTemporaryPartitionsCombined(db, table,
                List.of(sample), 2, UtFrameUtils.createDefaultCtx(), null, Set.of(), 42L);
        assertEquals(SkipReason.SUBMIT_FAILED, ((PreSplitOutcome.Skipped) outcome).reason());
        assertEquals(1, mgr.getTabletReshardJobs().size());
        TabletReshardJob retained = mgr.getTabletReshardJobs().values().iterator().next();
        assertTrue(retained.isAdmissionReady());
        assertTrue(retained.getCleanupExcludedTransactionIds().isEmpty());
        assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
    }

    @ParameterizedTest
    @EnumSource(value = TabletReshardJob.JobState.class, names = {"PENDING", "RUNNING", "CLEANING", "FINISHED", "ABORTED"})
    public void recoveredLayoutReadinessOnlyForDurableCleaningOrFinished(TabletReshardJob.JobState state) {
        TabletReshardJob original = job(false, partitions.get(0));
        original.jobState = state;
        TabletReshardJob recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), TabletReshardJob.class);
        assertFalse(recovered.isLayoutReady());
        recovered.initializeAfterRecovery();
        assertEquals(state == TabletReshardJob.JobState.CLEANING || state == TabletReshardJob.JobState.FINISHED,
                recovered.isLayoutReady());
    }

    private EditLog useQueuedWal(BlockingQueue<JournalTask> queue) {
        EditLog editLog = new EditLog(queue, true);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };
        return editLog;
    }

    private CompletableFuture<Void> submit(TabletReshardJob job) {
        return CompletableFuture.runAsync(() -> {
            try {
                mgr.addTabletReshardJob(job);
            } catch (StarRocksException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void pendingWalDoesNotHoldTableLockOrRunJob() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        useQueuedWal(queue);
        TabletReshardJob pending = job(false, partitions.get(0));
        AtomicInteger runs = new AtomicInteger();
        new MockUp<SplitTabletJob>() {
            @Mock
            protected void runPendingJob() {
                runs.incrementAndGet();
            }
        };
        CompletableFuture<Void> submitted = submit(pending);
        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(task);
        try {
            assertFalse(pending.isAdmissionReady());
            Deencapsulation.invoke(mgr, "runTabletReshardJobs");
            assertEquals(0, runs.get());
            CountDownLatch readAcquired = new CountDownLatch(1);
            CompletableFuture<Void> reader = CompletableFuture.runAsync(() -> {
                try (AutoCloseableLock ignored = new AutoCloseableLock(new Locker(), db.getId(),
                        List.of(table.getId()), LockType.READ)) {
                    assertEquals(Set.of(partitions.get(0)), mgr.getReshardingPhysicalPartitionIdsLocked(table));
                    readAcquired.countDown();
                }
            });
            assertTrue(readAcquired.await(5, TimeUnit.SECONDS));
            reader.get(5, TimeUnit.SECONDS);
        } finally {
            task.markSucceed();
        }
        submitted.get(5, TimeUnit.SECONDS);
        assertTrue(pending.isAdmissionReady());
    }

    @Test
    public void failedAdmissionRemovesOnlyItsOwnReservation() throws Exception {
        TabletReshardJob first = job(false, partitions.get(0));
        mgr.addTabletReshardJob(first);
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        useQueuedWal(queue);
        TabletReshardJob second = job(true, partitions.get(1));
        CompletableFuture<Void> submitted = submit(second);
        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(task);
        task.markAbort(new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED, "injected"));
        assertThrows(ExecutionException.class, () -> submitted.get(5, TimeUnit.SECONDS));
        assertEquals(Map.of(first.getJobId(), first), mgr.getTabletReshardJobs());
        assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,true"})
    public void finalPublicationRetainsOtherJobAndFailedWalIsRetryable(boolean firstMerge, boolean secondMerge)
            throws Exception {
        TabletReshardJob first = job(firstMerge, partitions.get(0));
        TabletReshardJob second = job(secondMerge, partitions.get(1));
        mgr.addTabletReshardJob(first);
        mgr.addTabletReshardJob(second);
        first.jobState = TabletReshardJob.JobState.CLEANING;
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        useQueuedWal(queue);
        CompletableFuture<Void> terminal = CompletableFuture.runAsync(
                () -> first.setJobState(TabletReshardJob.JobState.FINISHED));
        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(task);
        try {
            assertEquals(TabletReshardJob.JobState.CLEANING, first.getJobState());
            assertThrows(StarRocksException.class, () -> mgr.addTabletReshardJob(job(false, partitions.get(0))));
        } finally {
            task.markAbort(new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED, "injected"));
        }
        assertThrows(ExecutionException.class, () -> terminal.get(5, TimeUnit.SECONDS));
        assertEquals(TabletReshardJob.JobState.CLEANING, first.getJobState());
        CompletableFuture<Void> retry = CompletableFuture.runAsync(
                () -> first.setJobState(TabletReshardJob.JobState.FINISHED));
        JournalTask retried = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(retried);
        retried.markSucceed();
        retry.get(5, TimeUnit.SECONDS);
        assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
        second.jobState = TabletReshardJob.JobState.ABORTING;
        CompletableFuture<Void> abort = CompletableFuture.runAsync(second::runAbortingJob);
        JournalTask aborted = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(aborted);
        aborted.markSucceed();
        abort.get(5, TimeUnit.SECONDS);
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void terminalReplayExcludesOldSelfAndRecoveryInitializesReadiness() throws Exception {
        TabletReshardJob first = job(false, partitions.get(0));
        TabletReshardJob second = job(true, partitions.get(1));
        mgr.addTabletReshardJob(first);
        mgr.addTabletReshardJob(second);
        TabletReshardJob replayed = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(first), TabletReshardJob.class);
        replayed.jobState = TabletReshardJob.JobState.ABORTED;
        EditLog editLog = useQueuedWal(new ArrayBlockingQueue<>(4));
        editLog.closeWalGate();
        mgr.replayUpdateTabletReshardJob(replayed);
        assertSame(replayed, mgr.getTabletReshardJob(first.getJobId()));
        assertTrue(replayed.isAdmissionReady());
        assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
        TabletReshardJob recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(second), TabletReshardJob.class);
        recovered.jobState = TabletReshardJob.JobState.ABORTING;
        mgr.getTabletReshardJobs().put(recovered.getJobId(), recovered);
        assertFalse(recovered.isAdmissionReady());
        mgr.gsonPostProcess();
        assertTrue(recovered.isAdmissionReady());
        recovered.jobState = TabletReshardJob.JobState.ABORTED;
        mgr.replayUpdateTabletReshardJob(recovered);
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void differentIndexesOfSamePhysicalPartitionConflict() throws Exception {
        long partitionId = partitions.get(0);
        PhysicalPartition partition = table.getPhysicalPartition(partitionId);
        long rollupId = GlobalStateMgr.getCurrentState().getNextId();
        MaterializedIndex rollup = new MaterializedIndex(rollupId, MaterializedIndex.IndexState.NORMAL);
        partition.createRollupIndex(rollup);
        try {
            TabletReshardJob first = job(false, partitionId);
            mgr.addTabletReshardJob(first);
            MaterializedIndex target = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                    rollup.getMetaId(), MaterializedIndex.IndexState.NORMAL, rollup.getShardGroupId());
            TabletReshardJob otherIndex = new SplitTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                    db.getId(), table.getId(), Map.of(partitionId, new ReshardingPhysicalPartition(partitionId,
                    Map.of(rollupId, new ReshardingMaterializedIndex(rollupId, target, List.of())))));
            assertThrows(StarRocksException.class, () -> mgr.addTabletReshardJob(otherIndex));
            assertEquals(1, mgr.getTabletReshardJobs().size());
        } finally {
            partition.deleteMaterializedIndexByIndexId(rollupId);
        }
    }

    @Test
    public void rejectedSharedFilesAndSupersededSourcePreserveOtherJob() throws Exception {
        TabletReshardJob first = job(false, partitions.get(0));
        mgr.addTabletReshardJob(first);
        long partitionId = partitions.get(1);
        MaterializedIndex source = table.getPhysicalPartition(partitionId).getLatestBaseIndex();
        LakeTablet tablet = (LakeTablet) source.getTablets().get(0);
        boolean oldShared = tablet.hasSharedFiles();
        try {
            TabletReshardJob merge = job(true, partitionId);
            ReshardingMaterializedIndex index = merge.getReshardingPhysicalPartitions().get(partitionId)
                    .getReshardingIndexes().values().iterator().next();
            merge.getReshardingPhysicalPartitions().put(partitionId, new ReshardingPhysicalPartition(partitionId,
                    Map.of(source.getId(), new ReshardingMaterializedIndex(source.getId(), index.getMaterializedIndex(),
                            List.of(new MergingTablet(List.of(tablet.getId()),
                                    GlobalStateMgr.getCurrentState().getNextId()))))));
            tablet.setHasSharedFiles(true);
            assertThrows(StarRocksException.class, () -> mgr.addTabletReshardJob(merge));
            TabletReshardJob stale = job(false, partitionId);
            stale.getReshardingPhysicalPartitions().put(partitionId, new ReshardingPhysicalPartition(partitionId,
                    Map.of(-1L, new ReshardingMaterializedIndex(-1L, index.getMaterializedIndex(), List.of()))));
            assertThrows(StarRocksException.class, () -> mgr.addTabletReshardJob(stale));
            assertEquals(Map.of(first.getJobId(), first), mgr.getTabletReshardJobs());
            assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
        } finally {
            tablet.setHasSharedFiles(oldShared);
        }
    }

    @Test
    public void closedInnerGateRollsBackWithinOuterFence() {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = useQueuedWal(queue);
        TabletReshardJob pending = job(false, partitions.get(0));
        AtomicBoolean rolledBack = new AtomicBoolean();
        new MockUp<EditLog>() {
            @Mock
            public void logUpdateTabletReshardJob(Invocation invocation, TabletReshardJob snapshot, WALApplier applier) {
                assertSame(pending, mgr.getTabletReshardJob(pending.getJobId()));
                assertEquals(1, (int) Deencapsulation.invoke(editLog, "inFlightForTest"));
                editLog.closeWalGate();
                invocation.proceed(snapshot, applier);
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            void refreshTableStateLocked(Invocation invocation, OlapTable lockedTable, long excludedJobId) {
                assertEquals(1, (int) Deencapsulation.invoke(editLog, "inFlightForTest"));
                rolledBack.set(true);
                invocation.proceed(lockedTable, excludedJobId);
            }
        };
        EditLogException failure = assertThrows(EditLogException.class, () -> mgr.addTabletReshardJob(pending));
        assertTrue(failure.getMessage().contains("leader WAL gate is closed"));
        assertTrue(rolledBack.get());
        assertTrue(mgr.getTabletReshardJobs().isEmpty());
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        assertFalse(pending.isAdmissionReady());
        assertTrue(queue.isEmpty());
        assertEquals(0, (int) Deencapsulation.invoke(editLog, "inFlightForTest"));
    }

    @Test
    public void committedApplyFailureNeverRemovesDurableJob() throws Exception {
        TabletReshardJob pending = job(false, partitions.get(0));
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = useQueuedWal(queue);
        new MockUp<TabletReshardJob>() {
            @Mock
            void markAdmissionReady() {
                throw new IllegalStateException("injected apply failure");
            }
        };
        CompletableFuture<Void> submitted = submit(pending);
        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(task);
        task.markSucceed();
        assertThrows(ExecutionException.class, () -> submitted.get(5, TimeUnit.SECONDS));
        assertSame(pending, mgr.getTabletReshardJob(pending.getJobId()));
        assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
        assertEquals(0, (int) Deencapsulation.invoke(editLog, "inFlightForTest"));
    }

    @Test
    public void interruptedContendedCommitStillActivatesAndBalancesLocks() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = useQueuedWal(queue);
        TabletReshardJob pending = job(false, partitions.get(0));
        CountDownLatch finalizing = new CountDownLatch(1);
        AtomicBoolean committed = new AtomicBoolean();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        AtomicInteger remainingDepth = new AtomicInteger(-1);
        CompletableFuture<Void> completion = new CompletableFuture<>();
        Thread submitter = new Thread(() -> {
            try {
                mgr.addTabletReshardJob(pending);
                interruptRestored.set(Thread.currentThread().isInterrupted());
                remainingDepth.set(LockHoldDepth.current());
                completion.complete(null);
            } catch (Throwable e) {
                completion.completeExceptionally(e);
            }
        });
        submitter.setDaemon(true);
        new MockUp<Locker>() {
            @Mock
            public void lock(Invocation invocation, long id, LockType type, long timeout) throws LockException {
                if (Thread.currentThread() == submitter && committed.get() && id == table.getId()) {
                    finalizing.countDown();
                }
                invocation.proceed(id, type, timeout);
            }
        };
        submitter.start();
        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(task);
        try (AutoCloseableLock ignored = new AutoCloseableLock(new Locker(), db.getId(),
                List.of(table.getId()), LockType.WRITE)) {
            committed.set(true);
            task.markSucceed();
            assertTrue(finalizing.await(5, TimeUnit.SECONDS));
            submitter.interrupt();
            assertFalse(pending.isAdmissionReady());
            assertTrue((int) Deencapsulation.invoke(editLog, "inFlightForTest") > 0);
        }
        completion.get(5, TimeUnit.SECONDS);
        assertTrue(pending.isAdmissionReady());
        assertSame(pending, mgr.getTabletReshardJob(pending.getJobId()));
        assertTrue(interruptRestored.get());
        assertEquals(0, remainingDepth.get());
        assertEquals(0, (int) Deencapsulation.invoke(editLog, "inFlightForTest"));
    }

    @Test
    public void staleLeaseAfterCommitDoesNotRemoveDurableJob() throws Exception {
        TabletReshardJob pending = job(false, partitions.get(0));
        LeaderLease original = GlobalStateMgr.getCurrentState().captureLeaderLease();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        useQueuedWal(queue);
        AtomicBoolean revoked = new AtomicBoolean();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public void checkLeaderLease(Invocation invocation, LeaderLease lease) {
                assertEquals(original, lease);
                if (revoked.get()) {
                    throw new IllegalStateException("revoked");
                }
                invocation.proceed(lease);
            }
        };
        CompletableFuture<Void> submitted = submit(pending);
        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(task);
        revoked.set(true);
        task.markSucceed();
        assertThrows(ExecutionException.class, () -> submitted.get(5, TimeUnit.SECONDS));
        assertTrue(pending.isAdmissionReady());
        assertSame(pending, mgr.getTabletReshardJob(pending.getJobId()));
        assertEquals(original, pending.getAdmissionLease());
    }

    @Test
    public void droppedTargetDoesNotReleaseOtherJob() throws Exception {
        TabletReshardJob first = job(false, partitions.get(0));
        TabletReshardJob second = job(true, partitions.get(1));
        mgr.addTabletReshardJob(first);
        mgr.addTabletReshardJob(second);
        PhysicalPartition dropped = table.getPhysicalPartition(partitions.get(0));
        table.removePhysicalPartition(dropped);
        try {
            try (AutoCloseableLock ignored = new AutoCloseableLock(new Locker(), db.getId(),
                    List.of(table.getId()), LockType.READ)) {
                assertTrue(mgr.isPhysicalPartitionReshardingLocked(table, dropped.getId()));
                assertTrue(mgr.hasActiveJobLocked(table.getId(), first.getJobId()));
            }
            first.jobState = TabletReshardJob.JobState.ABORTING;
            first.runAbortingJob();
            assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
            second.jobState = TabletReshardJob.JobState.ABORTING;
            second.runAbortingJob();
            assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        } finally {
            table.addPhysicalPartition(dropped);
        }
    }

    @Test
    public void droppedTableStillActivatesDurableJobAndCanAbort() throws Exception {
        TabletReshardJob pending = job(false, partitions.get(0));
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = useQueuedWal(queue);
        CompletableFuture<Void> submitted = submit(pending);
        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(task);
        new MockUp<LocalMetastore>() {
            @Mock
            public Table getTable(Invocation invocation, long dbId, long tableId) {
                return dbId == db.getId() && tableId == table.getId() ? null : invocation.proceed(dbId, tableId);
            }
        };
        task.markSucceed();
        submitted.get(5, TimeUnit.SECONDS);
        assertTrue(pending.isAdmissionReady());
        pending.jobState = TabletReshardJob.JobState.ABORTING;
        CompletableFuture<Void> abort = CompletableFuture.runAsync(pending::runAbortingJob);
        JournalTask terminal = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(terminal);
        terminal.markSucceed();
        abort.get(5, TimeUnit.SECONDS);
        assertEquals(TabletReshardJob.JobState.ABORTED, pending.getJobState());
        assertEquals(0, (int) Deencapsulation.invoke(editLog, "inFlightForTest"));
    }

    @ParameterizedTest
    @EnumSource(value = TabletReshardJob.JobState.class, names = {"PENDING", "ABORTING"})
    public void oldPayloadRecoveryReadiesDurableNonfinalJobs(TabletReshardJob.JobState state) throws Exception {
        TabletReshardJob original = job(false, partitions.get(0));
        original.jobState = state;
        TabletReshardJob recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), TabletReshardJob.class);
        assertFalse(recovered.isAdmissionReady());
        EditLog editLog = useQueuedWal(new ArrayBlockingQueue<>(4));
        editLog.closeWalGate();
        mgr.replayUpdateTabletReshardJob(recovered);
        assertTrue(recovered.isAdmissionReady());
        assertSame(recovered, mgr.getTabletReshardJob(recovered.getJobId()));
    }
}
