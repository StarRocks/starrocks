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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.alter.reshard.SplitTabletJobFactory;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TabletPreSplitCoordinator#submitForPartitionsCombined}.
 *
 * <p>Each test stands up:
 * <ul>
 *   <li>A mock {@link OlapTable} stubbed to pass the table-level eligibility
 *       gate ({@link PreSplitTargets#findEligibleTable}).</li>
 *   <li>One or more {@link PartitionSamples} entries with synthetic sort-key
 *       samples that produce useful row-quantile boundaries.</li>
 *   <li>{@link MockedStatic} over {@link GlobalStateMgr},
 *       {@link SplitTabletJobFactory}, and (where relevant)
 *       {@link MockedConstruction} over {@link Locker} so the multi-partition
 *       coordinator's collaborators are observable.</li>
 * </ul>
 *
 * <p>Tests do NOT exercise the boundary planner's correctness — that lives in
 * {@link BoundaryPlannerTest}. They DO exercise the coordinator's orchestration:
 * how many factory calls, what shape of {@code oldTabletIdToRanges} reaches the
 * factory, lock acquire/release counts, and per-partition skip routing.
 */
public class TabletPreSplitCoordinatorMultiPartitionTest {

    private static final long DB_ID = 100L;
    private static final long TABLE_ID = 200L;
    private static final long BASE_INDEX_META_ID = 300L;

    private static final long ROLLUP_INDEX_META_ID = 400L;

    private Database database;
    private OlapTable table;
    private MaterializedIndexMeta baseIndexMeta;

    private boolean savedMetricHasInit;
    private LongCounterMetric savedPartitionsTotal;

    private long savedConfigReshardTargetSize;
    private int savedConfigReshardMaxSplitCount;
    private long savedConfigReshardMinSplitSize;

    @BeforeEach
    public void setUp() {
        // Pin tablet-count-selection inputs so the test arithmetic stays valid if defaults move.
        savedConfigReshardTargetSize = Config.tablet_reshard_target_size;
        savedConfigReshardMaxSplitCount = Config.tablet_reshard_max_split_count;
        savedConfigReshardMinSplitSize = Config.tablet_reshard_min_split_size;
        Config.tablet_reshard_target_size = 50L * DebugUtil.MEGABYTE;
        Config.tablet_reshard_max_split_count = 1024;
        // These tests exercise per-partition K selection and orchestration at MB scale, not the
        // min-split-size bound (covered in TabletPreSplitCoordinatorTest). Disable it (1 byte) so
        // compute-node alignment still drives K as the assertions below expect.
        Config.tablet_reshard_min_split_size = 1L;

        // Wire a fresh PARTITIONS_TOTAL counter; MetricRepo.init() is not run inside unit
        // tests so the static field is otherwise null and a hasInit=true bump would NPE.
        // The other labeled counters (PRE_CREATE / ELIGIBILITY_SKIPPED / SAMPLER_FAILED)
        // are `static final MetricWithLabelGroup` instances and are reusable across tests
        // without explicit wiring.
        savedMetricHasInit = MetricRepo.hasInit;
        savedPartitionsTotal = MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_TOTAL;
        MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_TOTAL = new LongCounterMetric(
                "tablet_pre_split_partitions_total", MetricUnit.REQUESTS, "test wire");

        database = mock(Database.class);
        when(database.getId()).thenReturn(DB_ID);

        table = mock(OlapTable.class);
        when(table.getId()).thenReturn(TABLE_ID);
        when(table.getName()).thenReturn("stub_table");
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);

        Column sortKey = new Column("k", IntegerType.BIGINT);
        baseIndexMeta = mock(MaterializedIndexMeta.class);
        when(baseIndexMeta.getIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(baseIndexMeta.getSchema()).thenReturn(List.of(sortKey));
        when(baseIndexMeta.getSortKeyIdxes()).thenReturn(List.of(0));
        when(table.getIndexMetaByMetaId(BASE_INDEX_META_ID)).thenReturn(baseIndexMeta);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(baseIndexMeta));
    }

    @AfterEach
    public void tearDown() {
        MetricRepo.hasInit = savedMetricHasInit;
        MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_TOTAL = savedPartitionsTotal;
        Config.tablet_reshard_target_size = savedConfigReshardTargetSize;
        Config.tablet_reshard_max_split_count = savedConfigReshardMaxSplitCount;
        Config.tablet_reshard_min_split_size = savedConfigReshardMinSplitSize;
    }

    // ---------- Tests ----------

    @Test
    public void accumulatesIntoOneCombinedSubmit() throws Exception {
        // 3 existing-in-catalog partitions, all eligible -> ONE combined submit with 3 entries.
        installExistingPartition("p1", 11_001L, 21_001L, /*rowCount*/ 0L);
        installExistingPartition("p2", 11_002L, 21_002L, 0L);
        installExistingPartition("p3", 11_003L, 21_003L, 0L);

        List<PartitionSamples> entries = List.of(
                existingEntry("p1", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("p2", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("p3", 11_003L, 21_003L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(/*localMetastoreThrows=*/ null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, /*activeComputeNodeCount*/ 3, freshConnectContext(), null, Set.of());

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            Assertions.assertEquals(3, mapCaptor.getValue().size());
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_001L));
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_002L));
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_003L));
            factory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()),
                    times(1));
            verify(GlobalStateMgr.getCurrentState().getTabletReshardJobMgr(), times(1))
                    .addTabletReshardJob(combinedJob);
        }
    }

    @Test
    public void preCreatesMissingPartitionThenIncludesInCombined() throws Exception {
        // pNew is non-existing in the grouper snapshot AND in the catalog at coordinator time.
        // After addPartitions runs, install the partition so the post-create re-resolve sees it.
        AtomicInteger addPartitionsCalls = new AtomicInteger();

        // existing partition pOld is eligible and skips the pre-create branch.
        installExistingPartition("pOld", 11_001L, 21_001L, 0L);

        // For pNew: pre-create + install fresh
        AddPartitionClause clause = mock(AddPartitionClause.class);
        List<PartitionSamples> entries = List.of(
                missingEntry("pNew", clause, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("pOld", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithCustomMetastore(
                (db, name, c) -> {
                    addPartitionsCalls.incrementAndGet();
                    // Simulate the catalog mutation: install pNew so post-create resolve succeeds.
                    installExistingPartition("pNew", 11_002L, 21_002L, 0L);
                });
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            Assertions.assertEquals(1, addPartitionsCalls.get());
            Assertions.assertEquals(2, mapCaptor.getValue().size());
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_002L), "pre-created pNew tablet included");
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_001L), "existing pOld tablet included");
        }
    }

    @Test
    public void continuesAfterPreCreateFailure() throws Exception {
        // pBad: addPartitions throws -> Skipped(PRE_CREATE_FAILED).
        // pGood: existing, eligible -> contributes to combined submit.
        installExistingPartition("pGood", 11_001L, 21_001L, 0L);

        AddPartitionClause badClause = mock(AddPartitionClause.class);
        List<PartitionSamples> entries = List.of(
                missingEntry("pBad", badClause, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("pGood", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        // Only pBad triggers addPartitions (pGood is existing-in-catalog). Unconditional throw
        // is therefore equivalent to "throw only for pBad".
        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(
                new com.starrocks.common.DdlException("synthetic addPartitions failure"));
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            Assertions.assertEquals(1, mapCaptor.getValue().size(), "only pGood feeds the combined submit");
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_001L));

            // perPartitionResults: index 0 = Skipped(PRE_CREATE_FAILED), index 1 = Submitted(null sentinel)
            PreSplitOutcome.SubmittedCombined combined = (PreSplitOutcome.SubmittedCombined) outcome;
            Assertions.assertEquals(2, combined.perPartitionResults().size());
            assertSkippedReason(combined.perPartitionResults().get(0), SkipReason.PRE_CREATE_FAILED);
            Assertions.assertInstanceOf(PreSplitOutcome.Submitted.class, combined.perPartitionResults().get(1));
        }
    }

    @Test
    public void wholeHookFailsOnFactoryReject() throws Exception {
        // Factory throws (synthetic invalid range) -> SUBMIT_FAILED, addTabletReshardJob NOT called.
        installExistingPartition("p1", 11_001L, 21_001L, 0L);
        installExistingPartition("p2", 11_002L, 21_002L, 0L);
        List<PartitionSamples> entries = List.of(
                existingEntry("p1", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("p2", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()))
                    .thenThrow(new StarRocksException("synthetic factory rejection"));

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            assertSkippedReason(outcome, SkipReason.SUBMIT_FAILED);
            verify(GlobalStateMgr.getCurrentState().getTabletReshardJobMgr(), never())
                    .addTabletReshardJob(any());
        }
    }

    @Test
    public void wholeHookFailsOnSubmitReject() throws Exception {
        // Factory succeeds, addTabletReshardJob throws (capacity exceeded, journal failure, etc.).
        installExistingPartition("p1", 11_001L, 21_001L, 0L);
        installExistingPartition("p2", 11_002L, 21_002L, 0L);
        List<PartitionSamples> entries = List.of(
                existingEntry("p1", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("p2", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()))
                    .thenReturn(combinedJob);
            TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
            doThrow(new StarRocksException("capacity exceeded")).when(mgr).addTabletReshardJob(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            assertSkippedReason(outcome, SkipReason.SUBMIT_FAILED);
        }
    }

    @Test
    public void skipsNoUsefulCutsPartition() throws Exception {
        // pAllEqual: every sample row identical -> planner returns NO_SPLIT -> Skipped(NO_USEFUL_CUTS).
        // pUseful: standard -> contributes.
        installExistingPartition("pAllEqual", 11_001L, 21_001L, 0L);
        installExistingPartition("pUseful", 11_002L, 21_002L, 0L);

        List<PartitionSamples> entries = List.of(
                existingEntryAllEqualSamples("pAllEqual", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("pUseful", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            Assertions.assertEquals(1, mapCaptor.getValue().size());
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_002L));

            PreSplitOutcome.SubmittedCombined combined = (PreSplitOutcome.SubmittedCombined) outcome;
            assertSkippedReason(combined.perPartitionResults().get(0), SkipReason.NO_USEFUL_CUTS);
            Assertions.assertInstanceOf(PreSplitOutcome.Submitted.class, combined.perPartitionResults().get(1));
        }
    }

    @Test
    public void selectsPerPartitionKIndependently() throws Exception {
        // Two existing partitions with different estimatedBytes -> selector picks K_i
        // independently for each. With 12 CNs and target_size=50MB:
        //   p1: 1000MB -> ceil(1000/50)=20, rounded up to a multiple of 12 -> 24
        //   p2:   10MB -> ceil(10/50)=1,  rounded up / CN floor -> 12
        // A bug that ignored estimatedBytes would pick K=12 for both, so the differing
        // K_1=24 vs K_2=12 catches it. Provide 100 distinct ascending samples (max
        // useful cuts ~=99) so the planner does not duplicate-collapse below the
        // requested counts.
        installExistingPartition("p1", 11_001L, 21_001L, 0L);
        installExistingPartition("p2", 11_002L, 21_002L, 0L);

        List<PartitionSamples> entries = List.of(
                existingEntry("p1", 11_001L, 21_001L, 100, 1000L * DebugUtil.MEGABYTE),
                existingEntry("p2", 11_002L, 21_002L, 100, 10L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, /*activeComputeNodeCount*/ 12, freshConnectContext(), null, Set.of());

            Map<Long, List<TabletRange>> map = mapCaptor.getValue();
            // K_1 = 24 (ceil(1000MB/50MB)=20 rounded up to a multiple of 12) -> 24 ranges.
            // K_2 = 12 (CN floor dominates over byte target of 1) -> 12 ranges.
            // The two are intentionally different: equal K's would mask a bug that
            // ignores estimatedBytes and just uses activeComputeNodeCount everywhere.
            Assertions.assertEquals(24, map.get(21_001L).size(),
                    "p1 should pick K_1=24 from ceil(1000MB/50MB)=20 rounded up to a multiple of 12 CNs");
            Assertions.assertEquals(12, map.get(21_002L).size(),
                    "p2 should pick K_2=12 from CN floor > byte target of 1");
        }
    }

    @Test
    public void acquiresShortReadLockOnPostCreateRecheck() throws Exception {
        // Verify the coordinator acquires the intensive READ lock and releases it
        // around each post-create re-resolve. Three partitions -> three lock/unlock pairs.
        installExistingPartition("p1", 11_001L, 21_001L, 0L);
        installExistingPartition("p2", 11_002L, 21_002L, 0L);
        installExistingPartition("p3", 11_003L, 21_003L, 0L);

        List<PartitionSamples> entries = List.of(
                existingEntry("p1", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("p2", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("p3", 11_003L, 21_003L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        AtomicInteger lockCalls = new AtomicInteger();
        AtomicInteger unlockCalls = new AtomicInteger();

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class,
                        (mockLocker, ctx) -> {
                            Mockito.doAnswer(inv -> {
                                Long db = inv.getArgument(0);
                                LockType lt = inv.getArgument(2);
                                if (db == DB_ID && lt == LockType.READ) {
                                    lockCalls.incrementAndGet();
                                }
                                return null;
                            }).when(mockLocker).lockTableWithIntensiveDbLock(
                                    any(Long.class), any(Long.class), any(LockType.class));
                            Mockito.doAnswer(inv -> {
                                Long db = inv.getArgument(0);
                                LockType lt = inv.getArgument(2);
                                if (db == DB_ID && lt == LockType.READ) {
                                    unlockCalls.incrementAndGet();
                                }
                                return null;
                            }).when(mockLocker).unLockTableWithIntensiveDbLock(
                                    any(Long.class), any(Long.class), any(LockType.class));
                        })) {
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()))
                    .thenReturn(combinedJob);

            TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            Assertions.assertEquals(3, lockCalls.get(), "one intensive READ lock per partition");
            Assertions.assertEquals(3, unlockCalls.get(), "lock must be released after each partition");
        }
    }

    @Test
    public void tableLevelEligibilityFailsShortCircuits() throws Exception {
        // Table state non-NORMAL -> Skipped(TABLE_NOT_NORMAL) up front; no factory, no addPartitions.
        when(table.getState()).thenReturn(OlapTable.OlapTableState.SCHEMA_CHANGE);
        installExistingPartition("p1", 11_001L, 21_001L, 0L);
        List<PartitionSamples> entries = List.of(
                existingEntry("p1", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            assertSkippedReason(outcome, SkipReason.TABLE_NOT_NORMAL);
            factory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()),
                    never());
        }
    }

    @Test
    public void recordsCombinedMetrics() throws Exception {
        MetricRepo.hasInit = true;
        long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_TOTAL.getValue();

        installExistingPartition("p1", 11_001L, 21_001L, 0L);
        installExistingPartition("p2", 11_002L, 21_002L, 0L);
        installExistingPartition("p3", 11_003L, 21_003L, 0L);

        List<PartitionSamples> entries = List.of(
                existingEntry("p1", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("p2", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("p3", 11_003L, 21_003L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()))
                    .thenReturn(combinedJob);

            TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            Assertions.assertEquals(baseline + 3L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_TOTAL.getValue().longValue(),
                    "PARTITIONS_TOTAL bumped once per input PartitionSamples");
        }
    }

    @Test
    public void wholeHookFailsOnFactoryRuntimeReject() throws Exception {
        // Factory throws a RuntimeException (e.g. IllegalArgumentException for a bad
        // range list) -> the RuntimeException catch arm maps to SUBMIT_FAILED and
        // addTabletReshardJob is never called. Distinct from the StarRocksException
        // arm exercised by wholeHookFailsOnFactoryReject.
        installExistingPartition("p1", 11_001L, 21_001L, 0L);
        installExistingPartition("p2", 11_002L, 21_002L, 0L);
        List<PartitionSamples> entries = List.of(
                existingEntry("p1", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("p2", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()))
                    .thenThrow(new IllegalArgumentException("synthetic bad ranges"));

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            assertSkippedReason(outcome, SkipReason.SUBMIT_FAILED);
            verify(GlobalStateMgr.getCurrentState().getTabletReshardJobMgr(), never())
                    .addTabletReshardJob(any());
        }
    }

    @Test
    public void noUsefulCutsAcrossAllPartitionsSkipsWithoutSubmit() throws Exception {
        // Every entry drops (here: all-equal samples -> NO_SPLIT) so
        // oldTabletIdToRanges stays empty -> the whole hook returns
        // Skipped(NO_USEFUL_CUTS) and never calls the factory.
        installExistingPartition("pAllEqual", 11_001L, 21_001L, 0L);
        List<PartitionSamples> entries = List.of(
                existingEntryAllEqualSamples("pAllEqual", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            assertSkippedReason(outcome, SkipReason.NO_USEFUL_CUTS);
            factory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()),
                    never());
        }
    }

    @Test
    public void missingEntryAlreadyInCatalogRecordsAlreadyExistsAndSkipsAddPartitions() throws Exception {
        // The grouper saw pRaced as missing, but it raced into the catalog before
        // the coordinator's pre-create. The cheap pre-check (table.getPartition != null)
        // records ALREADY_EXISTS and skips addPartitions; the partition still feeds
        // the combined submit because the post-create resolve succeeds.
        installExistingPartition("pRaced", 11_001L, 21_001L, 0L);
        AddPartitionClause clause = mock(AddPartitionClause.class);
        List<PartitionSamples> entries = List.of(
                missingEntry("pRaced", clause, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        AtomicInteger addPartitionsCalls = new AtomicInteger();

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithCustomMetastore(
                (db, name, c) -> addPartitionsCalls.incrementAndGet());
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            Assertions.assertEquals(0, addPartitionsCalls.get(),
                    "raced-into-catalog partition must NOT call addPartitions");
            Assertions.assertEquals(1, mapCaptor.getValue().size());
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_001L));
        }
    }

    @Test
    public void existingEntryThatVanishesPostResolveSkipsNotEligible() throws Exception {
        // resolveUnderReadLock returns null (the partition's getPartition lookup
        // returns null under the READ lock) -> PARTITION_NOT_ELIGIBLE_POST_CREATE.
        // It is the only entry, so oldTabletIdToRanges stays empty and the whole
        // hook returns NO_USEFUL_CUTS.
        // No installExistingPartition call -> table.getPartition("pGone") returns null.
        List<PartitionSamples> entries = List.of(
                existingEntry("pGone", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            // Single entry dropped as PARTITION_NOT_ELIGIBLE_POST_CREATE -> overall NO_USEFUL_CUTS.
            assertSkippedReason(outcome, SkipReason.NO_USEFUL_CUTS);
            factory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()),
                    never());
        }
    }

    @Test
    public void resolveSkipsPartitionWithMultipleTablets() throws Exception {
        // resolveUnderReadLock rejects a base index with != 1 tablet -> null ->
        // PARTITION_NOT_ELIGIBLE_POST_CREATE. pMulti carries 2 tablets; pSolo is
        // a clean single-tablet partition that still contributes.
        installPartitionWithTabletCount("pMulti", 11_001L, /*tabletCount*/ 2, /*rowCount*/ 0L);
        installExistingPartition("pSolo", 11_002L, 21_002L, 0L);
        // The grouper-supplied oldTabletId for pMulti is a stale placeholder (> 0 to
        // satisfy the record invariant); resolveUnderReadLock ignores it and instead
        // walks the installed index, finds 2 tablets, and rejects the partition.
        List<PartitionSamples> entries = List.of(
                existingEntry("pMulti", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("pSolo", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            // Only the single-tablet partition feeds the combined submit.
            Assertions.assertEquals(1, mapCaptor.getValue().size());
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_002L));

            PreSplitOutcome.SubmittedCombined combined = (PreSplitOutcome.SubmittedCombined) outcome;
            assertSkippedReason(combined.perPartitionResults().get(0),
                    SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE);
        }
    }

    @Test
    public void resolveSkipsPartitionWithNonEmptyBaseIndex() throws Exception {
        // resolveUnderReadLock rejects a single-tablet base index whose row count
        // is > 0 (not empty) -> PARTITION_NOT_ELIGIBLE_POST_CREATE.
        installExistingPartition("pNonEmpty", 11_001L, 21_001L, /*rowCount*/ 42L);
        List<PartitionSamples> entries = List.of(
                existingEntry("pNonEmpty", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            // Single non-empty partition dropped -> overall NO_USEFUL_CUTS.
            assertSkippedReason(outcome, SkipReason.NO_USEFUL_CUTS);
            factory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()),
                    never());
        }
    }

    @Test
    public void resolveSkipsPartitionWithNullPhysicalPartition() throws Exception {
        // resolveUnderReadLock returns null when getDefaultPhysicalPartition() is null.
        Partition partition = mock(Partition.class);
        when(partition.getDefaultPhysicalPartition()).thenReturn(null);
        when(table.getPartition("pNoPhys")).thenReturn(partition);
        List<PartitionSamples> entries = List.of(
                existingEntry("pNoPhys", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            assertSkippedReason(outcome, SkipReason.NO_USEFUL_CUTS);
            factory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()),
                    never());
        }
    }

    @Test
    public void resolveSkipsPartitionWithNullBaseIndex() throws Exception {
        // resolveUnderReadLock returns null when the base index lookup is null
        // (an ALTER raced the base index out from under the resolve).
        Partition partition = mock(Partition.class);
        PhysicalPartition physicalPartition = mock(PhysicalPartition.class);
        when(physicalPartition.getId()).thenReturn(11_001L);
        when(physicalPartition.getIndex(BASE_INDEX_META_ID)).thenReturn(null);
        when(partition.getDefaultPhysicalPartition()).thenReturn(physicalPartition);
        when(table.getPartition("pNoIndex")).thenReturn(partition);
        List<PartitionSamples> entries = List.of(
                existingEntry("pNoIndex", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            assertSkippedReason(outcome, SkipReason.NO_USEFUL_CUTS);
            factory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()),
                    never());
        }
    }

    @Test
    public void planOnePartitionRuntimeFailureMapsToSampleFailed() throws Exception {
        // A RuntimeException raised inside planOnePartition (here: buildSampleSet
        // hits a malformed SampleRow whose sortKeyTuple throws) is caught and the
        // entry is dropped as SAMPLE_FAILED — siblings continue.
        installExistingPartition("pBoom", 11_001L, 21_001L, 0L);
        installExistingPartition("pGood", 11_002L, 21_002L, 0L);

        // pBoom: a SampleRow whose sortKeyTuple() throws when buildSampleSet reads it.
        SampleRow explodingRow = mock(SampleRow.class);
        when(explodingRow.sortKeyTuple()).thenThrow(new RuntimeException("synthetic projection failure"));
        PartitionSamples boomEntry = new PartitionSamples(
                List.of("v_pBoom"), "pBoom", /*existsInCatalog*/ true,
                11_001L, 21_001L, /*analyzedClause*/ null,
                List.of(explodingRow), 100L * DebugUtil.MEGABYTE);

        List<PartitionSamples> entries = List.of(
                boomEntry,
                existingEntry("pGood", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            // Only pGood feeds the combined submit; pBoom was dropped.
            Assertions.assertEquals(1, mapCaptor.getValue().size());
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_002L));

            PreSplitOutcome.SubmittedCombined combined = (PreSplitOutcome.SubmittedCombined) outcome;
            assertSkippedReason(combined.perPartitionResults().get(0), SkipReason.SAMPLE_FAILED);
            Assertions.assertInstanceOf(PreSplitOutcome.Submitted.class, combined.perPartitionResults().get(1));
        }
    }

    // ---------- Multi-index (rollup) tests ----------

    @Test
    public void carriesIdTaggedTuplesAndResolvesAllIndexTablets() throws Exception {
        // One partition with a base index + one rollup. The entry's rows carry an
        // id-tagged rollup IndexTuple; resolveUnderReadLock resolves BOTH index
        // tablets and planOnePartition splits both -> the combined map has the base
        // tablet AND the rollup tablet.
        wireVisibleRollups(ROLLUP_INDEX_META_ID);
        installPartitionWithIndices("p1", 11_001L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_001L), 0L),
                mockMaterializedIndex(ROLLUP_INDEX_META_ID, List.of(22_001L), 0L)));
        List<PartitionSamples> entries = List.of(
                rollupEntry("p1", 11_001L, 21_001L, 100L * DebugUtil.MEGABYTE, 100,
                        /*baseCuts*/ true, new IndexSampleSpec(ROLLUP_INDEX_META_ID, true)));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of(ROLLUP_INDEX_META_ID));

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            Assertions.assertEquals(2, mapCaptor.getValue().size(), "base + rollup tablets both split");
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_001L), "base tablet split");
            Assertions.assertTrue(mapCaptor.getValue().containsKey(22_001L), "rollup tablet split");
        }
    }

    @Test
    public void splitsEveryIndexPerPartition() throws Exception {
        // 2 indexes (base + rollup) x 2 partitions -> 4 combined-map entries.
        wireVisibleRollups(ROLLUP_INDEX_META_ID);
        installPartitionWithIndices("p1", 11_001L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_001L), 0L),
                mockMaterializedIndex(ROLLUP_INDEX_META_ID, List.of(22_001L), 0L)));
        installPartitionWithIndices("p2", 11_002L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_002L), 0L),
                mockMaterializedIndex(ROLLUP_INDEX_META_ID, List.of(22_002L), 0L)));
        List<PartitionSamples> entries = List.of(
                rollupEntry("p1", 11_001L, 21_001L, 100L * DebugUtil.MEGABYTE, 100, true,
                        new IndexSampleSpec(ROLLUP_INDEX_META_ID, true)),
                rollupEntry("p2", 11_002L, 21_002L, 100L * DebugUtil.MEGABYTE, 100, true,
                        new IndexSampleSpec(ROLLUP_INDEX_META_ID, true)));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of(ROLLUP_INDEX_META_ID));

            Map<Long, List<TabletRange>> map = mapCaptor.getValue();
            Assertions.assertEquals(4, map.size(), "2 indexes x 2 partitions -> 4 tablet entries");
            Assertions.assertTrue(map.keySet().containsAll(List.of(21_001L, 22_001L, 21_002L, 22_002L)));
        }
    }

    @Test
    public void baseNoCutsRollupCuts_rollupOnlyContribution() throws Exception {
        // Base samples are all equal (no split) but the rollup samples cut -> only the
        // rollup tablet contributes to the combined map.
        wireVisibleRollups(ROLLUP_INDEX_META_ID);
        installPartitionWithIndices("p1", 11_001L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_001L), 0L),
                mockMaterializedIndex(ROLLUP_INDEX_META_ID, List.of(22_001L), 0L)));
        List<PartitionSamples> entries = List.of(
                rollupEntry("p1", 11_001L, 21_001L, 100L * DebugUtil.MEGABYTE, 100,
                        /*baseCuts*/ false, new IndexSampleSpec(ROLLUP_INDEX_META_ID, true)));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of(ROLLUP_INDEX_META_ID));

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            Assertions.assertEquals(1, mapCaptor.getValue().size());
            Assertions.assertTrue(mapCaptor.getValue().containsKey(22_001L), "only the rollup tablet split");
            Assertions.assertFalse(mapCaptor.getValue().containsKey(21_001L), "no-cut base is omitted");
        }
    }

    @Test
    public void allIndexesNoCuts_partitionSkippedNoUsefulCuts() throws Exception {
        // Both base and rollup samples are all equal -> every index no-cut -> the local
        // map is empty -> Skipped(NO_USEFUL_CUTS), the factory is never called.
        wireVisibleRollups(ROLLUP_INDEX_META_ID);
        installPartitionWithIndices("p1", 11_001L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_001L), 0L),
                mockMaterializedIndex(ROLLUP_INDEX_META_ID, List.of(22_001L), 0L)));
        List<PartitionSamples> entries = List.of(
                rollupEntry("p1", 11_001L, 21_001L, 100L * DebugUtil.MEGABYTE, 100,
                        /*baseCuts*/ false, new IndexSampleSpec(ROLLUP_INDEX_META_ID, false)));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of(ROLLUP_INDEX_META_ID));

            assertSkippedReason(outcome, SkipReason.NO_USEFUL_CUTS);
            factory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()),
                    never());
        }
    }

    @Test
    public void secondaryPlanningThrowsAfterBasePlanned_dropsWholePartition() throws Exception {
        // pBad: base cuts and is planned into the LOCAL map, then the rollup planning
        // throws (its rows carry no matching IndexTuple) -> the whole partition is
        // dropped and the base-only remnant NEVER leaks into the combined map.
        wireVisibleRollups(ROLLUP_INDEX_META_ID);
        installPartitionWithIndices("pBad", 11_001L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_001L), 0L),
                mockMaterializedIndex(ROLLUP_INDEX_META_ID, List.of(22_001L), 0L)));
        installPartitionWithIndices("pGood", 11_002L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_002L), 0L),
                mockMaterializedIndex(ROLLUP_INDEX_META_ID, List.of(22_002L), 0L)));
        List<PartitionSamples> entries = List.of(
                // pBad carries NO secondary tuple, so the rollup buildSampleSet throws.
                rollupEntry("pBad", 11_001L, 21_001L, 100L * DebugUtil.MEGABYTE, 100, /*baseCuts*/ true),
                rollupEntry("pGood", 11_002L, 21_002L, 100L * DebugUtil.MEGABYTE, 100, true,
                        new IndexSampleSpec(ROLLUP_INDEX_META_ID, true)));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of(ROLLUP_INDEX_META_ID));

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            Map<Long, List<TabletRange>> map = mapCaptor.getValue();
            Assertions.assertEquals(2, map.size(), "only pGood's base + rollup feed the combined submit");
            Assertions.assertTrue(map.containsKey(21_002L));
            Assertions.assertTrue(map.containsKey(22_002L));
            Assertions.assertFalse(map.containsKey(21_001L), "pBad base-only remnant must NOT leak");
            Assertions.assertFalse(map.containsKey(22_001L));

            PreSplitOutcome.SubmittedCombined combined = (PreSplitOutcome.SubmittedCombined) outcome;
            assertSkippedReason(combined.perPartitionResults().get(0), SkipReason.SAMPLE_FAILED);
            Assertions.assertInstanceOf(PreSplitOutcome.Submitted.class, combined.perPartitionResults().get(1));
        }
    }

    @Test
    public void projectionOrderDiffersFromCatalogOrder_stillCorrect() throws Exception {
        // The catalog resolves the rollups in order [rollupA, rollupB], but each row's
        // IndexTuple list is in the REVERSED order [rollupB, rollupA]. rollupA carries
        // cutting values, rollupB carries all-equal values. Because matching is by
        // indexMetaId (not list position), rollupA still splits and rollupB does not; a
        // positional bug would swap them.
        long rollupA = ROLLUP_INDEX_META_ID;
        long rollupB = 500L;
        wireVisibleRollups(rollupA, rollupB);
        installPartitionWithIndices("p1", 11_001L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_001L), 0L),
                mockMaterializedIndex(rollupA, List.of(22_001L), 0L),
                mockMaterializedIndex(rollupB, List.of(23_001L), 0L)));
        List<PartitionSamples> entries = List.of(
                rollupEntry("p1", 11_001L, 21_001L, 100L * DebugUtil.MEGABYTE, 100, /*baseCuts*/ true,
                        new IndexSampleSpec(rollupB, /*cuts*/ false),
                        new IndexSampleSpec(rollupA, /*cuts*/ true)));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of(rollupA, rollupB));

            Map<Long, List<TabletRange>> map = mapCaptor.getValue();
            Assertions.assertTrue(map.containsKey(22_001L), "rollupA (cutting values, id-matched) must split");
            Assertions.assertFalse(map.containsKey(23_001L), "rollupB (all-equal, id-matched) must not split");
            Assertions.assertTrue(map.containsKey(21_001L), "base splits too");
        }
    }

    @Test
    public void singleIndexMultiPartition_unchanged() throws Exception {
        // Base-only entries (empty secondary tuples) with an empty sampled secondary set
        // exercise the conditional access path and produce today's base-only map.
        installExistingPartition("p1", 11_001L, 21_001L, 0L);
        installExistingPartition("p2", 11_002L, 21_002L, 0L);
        List<PartitionSamples> entries = List.of(
                existingEntry("p1", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("p2", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            Map<Long, List<TabletRange>> map = mapCaptor.getValue();
            Assertions.assertEquals(2, map.size(), "single-index target yields only the base tablets");
            Assertions.assertTrue(map.keySet().containsAll(List.of(21_001L, 21_002L)));
        }
    }

    @Test
    public void rollupMultiTablet_dropsPartition() throws Exception {
        // pBad's rollup carries 2 tablets -> resolveVisibleIndexTargets returns null ->
        // the partition is dropped as PARTITION_NOT_ELIGIBLE_POST_CREATE. pGood's rollup
        // is single-tablet and still contributes.
        wireVisibleRollups(ROLLUP_INDEX_META_ID);
        installPartitionWithIndices("pBad", 11_001L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_001L), 0L),
                mockMaterializedIndex(ROLLUP_INDEX_META_ID, List.of(22_001L, 22_009L), 0L)));
        installPartitionWithIndices("pGood", 11_002L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_002L), 0L),
                mockMaterializedIndex(ROLLUP_INDEX_META_ID, List.of(22_002L), 0L)));
        List<PartitionSamples> entries = List.of(
                rollupEntry("pBad", 11_001L, 21_001L, 100L * DebugUtil.MEGABYTE, 100, true,
                        new IndexSampleSpec(ROLLUP_INDEX_META_ID, true)),
                rollupEntry("pGood", 11_002L, 21_002L, 100L * DebugUtil.MEGABYTE, 100, true,
                        new IndexSampleSpec(ROLLUP_INDEX_META_ID, true)));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of(ROLLUP_INDEX_META_ID));

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            Assertions.assertEquals(2, mapCaptor.getValue().size(), "only pGood's base + rollup contribute");
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_002L));
            Assertions.assertTrue(mapCaptor.getValue().containsKey(22_002L));

            PreSplitOutcome.SubmittedCombined combined = (PreSplitOutcome.SubmittedCombined) outcome;
            assertSkippedReason(combined.perPartitionResults().get(0),
                    SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE);
        }
    }

    @Test
    public void sampledSecondaryIdSetDiffersFromResolved_dropsPartition() throws Exception {
        // The sampled secondary id set is {rollupA=400}, but the partition currently
        // resolves rollupB=500 -> the sets differ -> resolveUnderReadLock drops the
        // partition and the factory is never called.
        long rollupB = 500L;
        wireVisibleRollups(rollupB);
        installPartitionWithIndices("p1", 11_001L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_001L), 0L),
                mockMaterializedIndex(rollupB, List.of(23_001L), 0L)));
        List<PartitionSamples> entries = List.of(
                rollupEntry("p1", 11_001L, 21_001L, 100L * DebugUtil.MEGABYTE, 100, true,
                        new IndexSampleSpec(ROLLUP_INDEX_META_ID, true)));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of(ROLLUP_INDEX_META_ID));

            assertSkippedReason(outcome, SkipReason.NO_USEFUL_CUTS);
            factory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()),
                    never());
        }
    }

    @Test
    public void existingPartitionReResolvedBeforePlanning_staleTargetsRejected() throws Exception {
        // A concurrent split made pStale's base index multi-tablet between the grouper
        // snapshot and planning. resolveUnderReadLock re-resolves (the factory does not
        // enforce one-tablet-per-index) and rejects the stale target; pGood contributes.
        installPartitionWithTabletCount("pStale", 11_001L, /*tabletCount*/ 2, 0L);
        installExistingPartition("pGood", 11_002L, 21_002L, 0L);
        List<PartitionSamples> entries = List.of(
                existingEntry("pStale", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE),
                existingEntry("pGood", 11_002L, 21_002L, 100, 100L * DebugUtil.MEGABYTE));

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = mapCaptor();
            factory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                    eq(database), eq(table), mapCaptor.capture())).thenReturn(combinedJob);

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            Assertions.assertInstanceOf(PreSplitOutcome.SubmittedCombined.class, outcome);
            Assertions.assertEquals(1, mapCaptor.getValue().size(), "only pGood survives the re-resolve");
            Assertions.assertTrue(mapCaptor.getValue().containsKey(21_002L));

            PreSplitOutcome.SubmittedCombined combined = (PreSplitOutcome.SubmittedCombined) outcome;
            assertSkippedReason(combined.perPartitionResults().get(0),
                    SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE);
        }
    }

    @Test
    public void visibleIndexAddedAfterSamplingSnapshot_partitionDropped() throws Exception {
        // The sample saw a single index (empty sampled secondary set), but a rollup
        // became visible by resolveUnderReadLock time -> the resolved secondary set
        // {rollup} differs from {} -> the partition is dropped, NEVER as a base-only
        // combined entry (the factory is never called).
        wireVisibleRollups(ROLLUP_INDEX_META_ID);
        installPartitionWithIndices("p1", 11_001L, List.of(
                mockMaterializedIndex(BASE_INDEX_META_ID, List.of(21_001L), 0L),
                mockMaterializedIndex(ROLLUP_INDEX_META_ID, List.of(22_001L), 0L)));
        List<PartitionSamples> entries = List.of(
                existingEntry("p1", 11_001L, 21_001L, 100, 100L * DebugUtil.MEGABYTE));

        try (MockedStatic<GlobalStateMgr> gsm = mockGlobalStateMgrWithMgrs(null);
                MockedStatic<SplitTabletJobFactory> factory = Mockito.mockStatic(SplitTabletJobFactory.class);
                MockedConstruction<Locker> ignored = noopLockerCtor()) {

            PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                    database, table, entries, 3, freshConnectContext(), null, Set.of());

            assertSkippedReason(outcome, SkipReason.NO_USEFUL_CUTS);
            factory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()),
                    never());
        }
    }

    // ---------- Helpers ----------

    private static void assertSkippedReason(PreSplitOutcome outcome, SkipReason expected) {
        Assertions.assertInstanceOf(PreSplitOutcome.Skipped.class, outcome,
                "expected Skipped(" + expected + "), got: " + outcome);
        Assertions.assertEquals(expected, ((PreSplitOutcome.Skipped) outcome).reason());
    }

    /**
     * Build a mock {@link ConnectContext} for the coordinator's pre-create call.
     * The coordinator only forwards the context to {@code LocalMetastore.addPartitions}
     * (which is itself mocked in every test); constructing a real ConnectContext requires
     * {@code GlobalStateMgr.getVariableMgr()} to be wired and would defeat the static mock.
     */
    private static ConnectContext freshConnectContext() {
        ConnectContext ctx = mock(ConnectContext.class);
        when(ctx.getSessionVariable()).thenReturn(new SessionVariable());
        return ctx;
    }

    /**
     * Create a {@link PartitionSamples} that mirrors a grouper output for an
     * already-existing-in-catalog partition. The sort-key samples are 100
     * distinct ascending BIGINT values so the boundary planner picks K_i - 1
     * useful cuts (the planner's correctness is verified separately).
     */
    private static PartitionSamples existingEntry(String name, long partitionId, long oldTabletId,
                                                  int sampleCount, long estimatedBytes) {
        List<SampleRow> samples = new ArrayList<>(sampleCount);
        for (int i = 0; i < sampleCount; i++) {
            samples.add(SampleRow.ofSortKey(List.of(Variant.of(IntegerType.BIGINT, Long.toString(i)))));
        }
        return new PartitionSamples(
                List.of("v_" + name), name, /*existsInCatalog*/ true,
                partitionId, oldTabletId, /*analyzedClause*/ null,
                samples, estimatedBytes);
    }

    /**
     * Variant that produces all-equal sort-key samples so the boundary
     * planner returns NO_SPLIT. Used to verify the per-partition
     * NO_USEFUL_CUTS branch.
     */
    private static PartitionSamples existingEntryAllEqualSamples(String name, long partitionId, long oldTabletId,
                                                                  int sampleCount, long estimatedBytes) {
        List<SampleRow> samples = new ArrayList<>(sampleCount);
        for (int i = 0; i < sampleCount; i++) {
            samples.add(SampleRow.ofSortKey(List.of(Variant.of(IntegerType.BIGINT, "42"))));
        }
        return new PartitionSamples(
                List.of("v_" + name), name, true,
                partitionId, oldTabletId, null,
                samples, estimatedBytes);
    }

    private static PartitionSamples missingEntry(String name, AddPartitionClause clause,
                                                 int sampleCount, long estimatedBytes) {
        List<SampleRow> samples = new ArrayList<>(sampleCount);
        for (int i = 0; i < sampleCount; i++) {
            samples.add(SampleRow.ofSortKey(List.of(Variant.of(IntegerType.BIGINT, Long.toString(i)))));
        }
        return new PartitionSamples(
                List.of("v_" + name), name, /*existsInCatalog*/ false,
                -1L, -1L, clause,
                samples, estimatedBytes);
    }

    /**
     * Install a {@link Partition} on the {@link #table} stub keyed by name,
     * with a default {@link PhysicalPartition} carrying one base-index tablet
     * of the supplied id and row count. The coordinator's
     * {@code resolveUnderReadLock} walks this exact chain.
     */
    private void installExistingPartition(String name, long physicalPartitionId,
                                          long tabletId, long rowCount) {
        Partition partition = mock(Partition.class);
        PhysicalPartition physicalPartition = mock(PhysicalPartition.class);
        when(physicalPartition.getId()).thenReturn(physicalPartitionId);
        MaterializedIndex baseIndex = mock(MaterializedIndex.class);
        when(baseIndex.getMetaId()).thenReturn(BASE_INDEX_META_ID);
        Tablet tablet = mock(Tablet.class);
        when(tablet.getId()).thenReturn(tabletId);
        when(baseIndex.getTablets()).thenReturn(List.of(tablet));
        when(baseIndex.getRowCount()).thenReturn(rowCount);
        when(physicalPartition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);
        when(physicalPartition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(List.of(baseIndex));
        when(partition.getDefaultPhysicalPartition()).thenReturn(physicalPartition);
        when(table.getPartition(name)).thenReturn(partition);
    }

    /**
     * Install a {@link Partition} whose base index carries {@code tabletCount}
     * tablets. Used to drive {@code resolveUnderReadLock}'s "not exactly one
     * base tablet" rejection (tablet count != 1 -> null -> not eligible).
     */
    private void installPartitionWithTabletCount(String name, long physicalPartitionId,
                                                 int tabletCount, long rowCount) {
        Partition partition = mock(Partition.class);
        PhysicalPartition physicalPartition = mock(PhysicalPartition.class);
        when(physicalPartition.getId()).thenReturn(physicalPartitionId);
        MaterializedIndex baseIndex = mock(MaterializedIndex.class);
        when(baseIndex.getMetaId()).thenReturn(BASE_INDEX_META_ID);
        List<Tablet> tablets = new ArrayList<>(tabletCount);
        for (int i = 0; i < tabletCount; i++) {
            Tablet tablet = mock(Tablet.class);
            when(tablet.getId()).thenReturn(physicalPartitionId * 100 + i);
            tablets.add(tablet);
        }
        when(baseIndex.getTablets()).thenReturn(tablets);
        when(baseIndex.getRowCount()).thenReturn(rowCount);
        when(physicalPartition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);
        when(physicalPartition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(List.of(baseIndex));
        when(partition.getDefaultPhysicalPartition()).thenReturn(physicalPartition);
        when(table.getPartition(name)).thenReturn(partition);
    }

    /** One index's per-row sample shape: which index the tuple is tagged with, and whether it cuts. */
    private record IndexSampleSpec(long indexMetaId, boolean cuts) { }

    /**
     * Build a {@link PartitionSamples} whose rows carry a base sort-key tuple plus
     * one id-tagged {@link IndexTuple} per {@code secondarySpecs}, in the given
     * order (which may deliberately differ from catalog order). {@code cuts} rows
     * carry ascending distinct BIGINT values (the planner produces useful cuts);
     * otherwise every value is identical (the planner returns NO_SPLIT).
     */
    private static PartitionSamples rollupEntry(String name, long partitionId, long oldTabletId,
                                                long estimatedBytes, int sampleCount, boolean baseCuts,
                                                IndexSampleSpec... secondarySpecs) {
        List<SampleRow> rows = new ArrayList<>(sampleCount);
        for (int i = 0; i < sampleCount; i++) {
            List<Variant> baseTuple = List.of(Variant.of(IntegerType.BIGINT, baseCuts ? Long.toString(i) : "42"));
            List<IndexTuple> secondary = new ArrayList<>(secondarySpecs.length);
            for (IndexSampleSpec spec : secondarySpecs) {
                secondary.add(new IndexTuple(spec.indexMetaId(),
                        List.of(Variant.of(IntegerType.BIGINT, spec.cuts() ? Long.toString(i) : "42"))));
            }
            rows.add(new SampleRow(baseTuple, List.of(), secondary));
        }
        return new PartitionSamples(
                List.of("v_" + name), name, /*existsInCatalog*/ true,
                partitionId, oldTabletId, /*analyzedClause*/ null, rows, estimatedBytes);
    }

    /** Mock a {@link MaterializedIndex} with the given meta id, tablet ids, and row count. */
    private static MaterializedIndex mockMaterializedIndex(long metaId, List<Long> tabletIds, long rowCount) {
        MaterializedIndex index = mock(MaterializedIndex.class);
        when(index.getMetaId()).thenReturn(metaId);
        List<Tablet> tablets = new ArrayList<>(tabletIds.size());
        for (Long tabletId : tabletIds) {
            Tablet tablet = mock(Tablet.class);
            when(tablet.getId()).thenReturn(tabletId);
            tablets.add(tablet);
        }
        when(index.getTablets()).thenReturn(tablets);
        when(index.getRowCount()).thenReturn(rowCount);
        return index;
    }

    /**
     * Install a {@link Partition} whose default physical partition exposes the given
     * visible indices (base index first). {@code resolveUnderReadLock} walks both
     * {@code getIndex(baseIndexMetaId)} and {@code getLatestMaterializedIndices}.
     */
    private void installPartitionWithIndices(String name, long physicalPartitionId, List<MaterializedIndex> indices) {
        Partition partition = mock(Partition.class);
        PhysicalPartition physicalPartition = mock(PhysicalPartition.class);
        when(physicalPartition.getId()).thenReturn(physicalPartitionId);
        when(physicalPartition.getIndex(BASE_INDEX_META_ID)).thenReturn(indices.get(0));
        when(physicalPartition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(indices);
        when(partition.getDefaultPhysicalPartition()).thenReturn(physicalPartition);
        when(table.getPartition(name)).thenReturn(partition);
    }

    /**
     * Wire the base index meta plus one {@link MaterializedIndexMeta} per given rollup id
     * (each with its own scalar sort key) so the real {@code MetaUtils.getRangeDistributionColumns}
     * resolves every index and {@code table.getVisibleIndexMetas} reports base + rollups.
     */
    private void wireVisibleRollups(long... rollupMetaIds) {
        List<MaterializedIndexMeta> metas = new ArrayList<>();
        metas.add(baseIndexMeta);
        for (long rollupMetaId : rollupMetaIds) {
            Column rollupSortKey = new Column("k" + rollupMetaId, IntegerType.BIGINT);
            MaterializedIndexMeta rollupMeta = mock(MaterializedIndexMeta.class);
            when(rollupMeta.getIndexMetaId()).thenReturn(rollupMetaId);
            when(rollupMeta.getSchema()).thenReturn(List.of(rollupSortKey));
            when(rollupMeta.getSortKeyIdxes()).thenReturn(List.of(0));
            when(table.getIndexMetaByMetaId(rollupMetaId)).thenReturn(rollupMeta);
            metas.add(rollupMeta);
        }
        when(table.getVisibleIndexMetas()).thenReturn(metas);
    }

    /**
     * Returns a {@code MockedConstruction<Locker>} that turns every
     * {@code lockTableWithIntensiveDbLock} / {@code unLockTableWithIntensiveDbLock}
     * into a no-op so tests that don't care about lock counts run without a
     * real lock framework.
     */
    private static MockedConstruction<Locker> noopLockerCtor() {
        return Mockito.mockConstruction(Locker.class);
    }

    /**
     * Stand up a {@link MockedStatic} of {@link GlobalStateMgr} that returns a
     * fully mocked {@link LocalMetastore} and a fully mocked
     * {@link TabletReshardJobMgr}. {@code localMetastoreThrows} is optional —
     * when non-null, every {@code addPartitions} call throws it.
     */
    @SuppressWarnings("unchecked")
    private static MockedStatic<GlobalStateMgr> mockGlobalStateMgrWithMgrs(
            Throwable localMetastoreThrows) throws Exception {
        MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class);
        GlobalStateMgr mgr = mock(GlobalStateMgr.class);
        LocalMetastore localMetastore = mock(LocalMetastore.class);
        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);
        when(mgr.getLocalMetastore()).thenReturn(localMetastore);
        when(mgr.getTabletReshardJobMgr()).thenReturn(reshardMgr);
        gsm.when(GlobalStateMgr::getCurrentState).thenReturn(mgr);
        if (localMetastoreThrows != null) {
            doThrow(localMetastoreThrows).when(localMetastore)
                    .addPartitions(any(), any(), anyString(), any(AddPartitionClause.class));
        }
        return gsm;
    }

    @FunctionalInterface
    private interface AddPartitionsHandler {
        void handle(Database db, String tableName, AddPartitionClause clause) throws Exception;
    }

    /**
     * Variant of {@link #mockGlobalStateMgrWithMgrs} that delegates each
     * {@code addPartitions} call to {@code handler} so tests can intercept the
     * catalog mutation (e.g., install a partition after pre-create).
     */
    private static MockedStatic<GlobalStateMgr> mockGlobalStateMgrWithCustomMetastore(
            AddPartitionsHandler handler) throws Exception {
        MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class);
        GlobalStateMgr mgr = mock(GlobalStateMgr.class);
        LocalMetastore localMetastore = mock(LocalMetastore.class);
        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);
        when(mgr.getLocalMetastore()).thenReturn(localMetastore);
        when(mgr.getTabletReshardJobMgr()).thenReturn(reshardMgr);
        gsm.when(GlobalStateMgr::getCurrentState).thenReturn(mgr);
        Mockito.doAnswer(inv -> {
            Database db = inv.getArgument(1);
            String tableName = inv.getArgument(2);
            AddPartitionClause clause = inv.getArgument(3);
            handler.handle(db, tableName, clause);
            return null;
        }).when(localMetastore).addPartitions(any(), any(), anyString(), any(AddPartitionClause.class));
        return gsm;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor() {
        return ArgumentCaptor.forClass((Class) Map.class);
    }
}
