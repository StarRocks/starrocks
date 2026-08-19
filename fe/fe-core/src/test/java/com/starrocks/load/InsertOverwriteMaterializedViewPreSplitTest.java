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

package com.starrocks.load;

import com.starrocks.alter.reshard.SplitTabletJobFactory;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.SkipReason;
import com.starrocks.alter.reshard.presplit.TabletPreSplitCoordinator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.mv.RowIdStrategy;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * Wiring coverage for the route a static {@code INSERT OVERWRITE} takes when its target is a
 * range-distributed incremental materialized view: from
 * {@link InsertOverwriteJobRunner#preSplitStaticOverwriteTempPartitions()} through the INSERT
 * pre-split hook and the shared pre-split flow into the row-id boundary derivation, and out as the
 * reshard job that carves the temporary partition's single tablet.
 *
 * <p>Each stage of that path has its own unit test. What only this test covers is that the stages
 * are connected end to end, and that the boundaries which reach the reshard job are the ones the
 * row-id arithmetic dictates. A broken hand-off would leave the feature silently doing nothing —
 * the exact failure it exists to remove — while every per-stage test still passed, so the
 * observation point is {@link SplitTabletJobFactory#forExternalBoundaries} (the one call that turns
 * planned boundaries into a job) and the assertions are on the boundary values themselves.
 *
 * <p>The catalog is mocked rather than created through a real cluster: the path reads a narrow slice
 * of metadata (one temporary partition with one visible index and one sort-key column, plus the
 * table's auto-increment counter), and mocking it pins the two inputs the arithmetic is derived from
 * — the statement's estimate and the compute-node count — exactly. Each skip case below changes ONE
 * thing relative to the happy path, so the happy path is what proves the fixture reaches the factory
 * at all.
 */
public class InsertOverwriteMaterializedViewPreSplitTest {

    private static final long DB_ID = 700L;
    private static final long MV_ID = 800L;
    private static final long BASE_INDEX_META_ID = 10L;
    private static final long FIRST_TEMPORARY_PARTITION_ID = 9001L;
    private static final long FIRST_PHYSICAL_PARTITION_ID = 9101L;
    private static final long FIRST_BASE_TABLET_ID = 9201L;

    /** What the optimizer estimated the refresh writes; both sizing steps below are driven by it. */
    private static final long ESTIMATED_BYTES = 4L * 1024 * 1024;
    private static final long ESTIMATED_ROWS = 4000L;
    private static final long TARGET_TABLET_SIZE = 1024L * 1024;
    private static final int ID_CACHE_SIZE = 25;

    private boolean savedMvRefreshEnabled;
    private int savedIdCacheSize;
    private long savedPreSplitTargetSize;
    private long savedReshardTargetSize;
    private long savedReshardMinSplitSize;
    private int savedReshardMaxSplitCount;
    private long savedSampleByteLimit;

    @BeforeEach
    public void setUp() {
        savedMvRefreshEnabled = Config.enable_tablet_pre_split_for_mv_refresh;
        savedIdCacheSize = Config.auto_increment_cache_size;
        savedPreSplitTargetSize = Config.tablet_pre_split_target_size;
        savedReshardTargetSize = Config.tablet_reshard_target_size;
        savedReshardMinSplitSize = Config.tablet_reshard_min_split_size;
        savedReshardMaxSplitCount = Config.tablet_reshard_max_split_count;
        savedSampleByteLimit = Config.tablet_pre_split_sample_byte_limit;
        Config.enable_tablet_pre_split_for_mv_refresh = true;
        // Pin every input of the two sizing steps so the asserted boundaries stay valid when a
        // default moves.
        Config.auto_increment_cache_size = ID_CACHE_SIZE;
        Config.tablet_pre_split_target_size = TARGET_TABLET_SIZE;
        Config.tablet_reshard_target_size = TARGET_TABLET_SIZE;
        Config.tablet_reshard_min_split_size = TARGET_TABLET_SIZE;
        Config.tablet_reshard_max_split_count = 1024;
        // The derived route reads nothing, but the coordinator still builds the sample request every
        // route shares, and that rejects a non-positive limit.
        Config.tablet_pre_split_sample_byte_limit = 16L * 1024 * 1024;

        // The coordinator's session gate reads the thread-local context, not the one the runner was
        // built with, so bind a real one carrying the session opt-in.
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableTabletPreSplit(true);
        connectContext.setSessionVariable(sessionVariable);
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        Config.enable_tablet_pre_split_for_mv_refresh = savedMvRefreshEnabled;
        Config.auto_increment_cache_size = savedIdCacheSize;
        Config.tablet_pre_split_target_size = savedPreSplitTargetSize;
        Config.tablet_reshard_target_size = savedReshardTargetSize;
        Config.tablet_reshard_min_split_size = savedReshardMinSplitSize;
        Config.tablet_reshard_max_split_count = savedReshardMaxSplitCount;
        Config.tablet_pre_split_sample_byte_limit = savedSampleByteLimit;
    }

    @Test
    public void overwriteOfIncrementalViewSplitsTemporaryPartitionOnDerivedRowIds() {
        // The two sizing steps, for the fixture's 4 MiB / 4000-row estimate on one compute node:
        //   byte-based count  = ceil(4 MiB / tablet_pre_split_target_size 1 MiB)   = 4
        //   row-id usefulness = 4000 / (10 * 1 node * auto_increment_cache_size 25) = 16
        // so K = min(4, 16) = 4. The counter is pristine, so ids start at 1, the stride is
        // ceil(4000 / 4) = 1000, and the cuts land at 1001 / 2001 / 3001.
        try (Fixture fixture = new Fixture(/*temporaryPartitionCount*/ 1)) {
            fixture.runner.preSplitStaticOverwriteTempPartitions();

            Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
            Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID), submitted.keySet(),
                    "the split must target the temporary partition's base tablet, not the visible partition's");
            List<TabletRange> ranges = submitted.get(FIRST_BASE_TABLET_ID);
            Assertions.assertEquals(4, ranges.size(), "three cuts carve four tablets");

            // Both ends stay unbounded: an estimate that turns out low or high then lands in the
            // first or last tablet, instead of leaving rows outside every range.
            Assertions.assertTrue(ranges.get(0).getRange().isMinimum(), "first range must open at -infinity");
            Assertions.assertEquals(rowIdTuple(1001L), ranges.get(0).getRange().getUpperBound());
            Assertions.assertEquals(rowIdTuple(1001L), ranges.get(1).getRange().getLowerBound());
            Assertions.assertEquals(rowIdTuple(2001L), ranges.get(1).getRange().getUpperBound());
            Assertions.assertEquals(rowIdTuple(2001L), ranges.get(2).getRange().getLowerBound());
            Assertions.assertEquals(rowIdTuple(3001L), ranges.get(2).getRange().getUpperBound());
            Assertions.assertEquals(rowIdTuple(3001L), ranges.get(3).getRange().getLowerBound());
            Assertions.assertTrue(ranges.get(3).getRange().isMaximum(), "last range must close at +infinity");
        }
    }

    @Test
    public void nonPristineAutoIncrementCounterSubmitsNothing() {
        // Once an id has been handed out, a compute node may still hold cached ids this planner can
        // neither see nor bound, so the low end of the span is no longer known to be 1 and the whole
        // derivation is guesswork. The gate is cheap and easy to "optimise away", hence this case.
        try (Fixture fixture = new Fixture(/*temporaryPartitionCount*/ 1)) {
            fixture.assignAutoIncrementId(500L);

            fixture.runner.preSplitStaticOverwriteTempPartitions();

            fixture.assertNothingSubmitted();
        }
    }

    @Test
    public void configOffSubmitsNothing() {
        Config.enable_tablet_pre_split_for_mv_refresh = false;
        try (Fixture fixture = new Fixture(/*temporaryPartitionCount*/ 1)) {
            fixture.runner.preSplitStaticOverwriteTempPartitions();

            fixture.assertNothingSubmitted();
        }
    }

    @Test
    public void nonDerivableViewIsReportedAsSuchEvenWithTheFlagOff() {
        // An ordinary async view's full refresh is an INSERT OVERWRITE too, so it reaches this hook. It
        // must be reported as a view the derived tier cannot carve -- not as a victim of this feature's
        // config gate, and not as whatever the target resolver would have rejected it for. Getting that
        // order wrong made every such refresh in the cluster bump one of this feature's buckets, which
        // is exactly what the skip metric exists to rule out.
        Config.enable_tablet_pre_split_for_mv_refresh = false;
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try (Fixture fixture = new Fixture(/*temporaryPartitionCount*/ 1)) {
            when(fixture.materializedView.getRowIdStrategy()).thenReturn(RowIdStrategy.QUERY_COMPUTED);
            long baselineViewTarget = skipCount(SkipReason.MATERIALIZED_VIEW_TARGET);
            long baselineDisabled = skipCount(SkipReason.DISABLED_BY_CONFIG);

            fixture.runner.preSplitStaticOverwriteTempPartitions();

            fixture.assertNothingSubmitted();
            Assertions.assertEquals(baselineViewTarget + 1L, skipCount(SkipReason.MATERIALIZED_VIEW_TARGET),
                    "a view the derived tier cannot carve must be reported as such");
            Assertions.assertEquals(baselineDisabled, skipCount(SkipReason.DISABLED_BY_CONFIG),
                    "and must not be counted against this feature's config gate");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    private static long skipCount(SkipReason reason) {
        return MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                .getMetric(reason.name().toLowerCase()).getValue();
    }

    @Test
    public void severalTemporaryPartitionsAreReportedAsSuchInBothFlagStates() {
        // Attribution matters more than the skip itself here: enabling the flag would NOT make a
        // multi-partition refresh eligible, so charging it to the config gate would tell an operator to
        // flip a switch that cannot help. The reason must therefore be the same whether the flag is on
        // or off.
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            for (boolean flagOn : new boolean[] {true, false}) {
                Config.enable_tablet_pre_split_for_mv_refresh = flagOn;
                try (Fixture fixture = new Fixture(/*temporaryPartitionCount*/ 2)) {
                    long baselineMultiple = skipCount(SkipReason.MULTIPLE_TEMPORARY_PARTITIONS);
                    long baselineDisabled = skipCount(SkipReason.DISABLED_BY_CONFIG);

                    fixture.runner.preSplitStaticOverwriteTempPartitions();

                    fixture.assertNothingSubmitted();
                    Assertions.assertEquals(baselineMultiple + 1L,
                            skipCount(SkipReason.MULTIPLE_TEMPORARY_PARTITIONS),
                            "flag=" + flagOn + ": the partition count is what makes it ineligible");
                    Assertions.assertEquals(baselineDisabled, skipCount(SkipReason.DISABLED_BY_CONFIG),
                            "flag=" + flagOn + ": the config gate must not absorb it");
                }
            }
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void severalTemporaryPartitionsSubmitNothing() {
        // A partitioned view's row ids form a contiguous band per partition, so cuts spanning the
        // whole id space would leave most tablets of most partitions empty and one of each
        // overloaded. Both partitions here are fully wired, so a lost gate shows up as a submit.
        try (Fixture fixture = new Fixture(/*temporaryPartitionCount*/ 2)) {
            fixture.runner.preSplitStaticOverwriteTempPartitions();

            fixture.assertNothingSubmitted();
        }
    }

    @Test
    public void failureInsideTheHookDoesNotFailTheOverwrite() {
        // Pre-split is opportunistic: it runs after the temporary partitions have been cloned and
        // before the INSERT is replanned, so anything escaping here would fail a load that would
        // otherwise have succeeded. The throw is injected at the coordinator, the deepest stage the
        // flow reaches, which is past every inner catch on the way in.
        try (Fixture fixture = new Fixture(/*temporaryPartitionCount*/ 1);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            coordinator.when(() -> TabletPreSplitCoordinator.submitAsynchronously(
                            any(), any(), anyLong(), any(), any(), any(), anyInt()))
                    .thenThrow(new IllegalStateException("injected coordinator failure"));

            Assertions.assertDoesNotThrow(fixture.runner::preSplitStaticOverwriteTempPartitions);
        }
    }

    private static Tuple rowIdTuple(long rowId) {
        return new Tuple(List.of(Variant.of(IntegerType.BIGINT, Long.toString(rowId))));
    }

    private static String temporaryPartitionName(int ordinal) {
        return "p" + (ordinal + 1) + "_overwrite";
    }

    private static String sourcePartitionName(int ordinal) {
        return "p" + (ordinal + 1);
    }

    /**
     * A static overwrite mid-job whose target is an incremental materialized view: the runner, its
     * job with the cloned temporary partitions, and the static scopes the path resolves through
     * (catalog lookups, sort-key resolution, target normalization, and the reshard-job factory the
     * result is observed at). Opened in a try-with-resources block; {@link #close()} releases the
     * static scopes.
     */
    private static final class Fixture implements AutoCloseable {
        private final MockedStatic<GlobalStateMgr> globalStateMgr;
        private final MockedStatic<MetaUtils> metaUtils;
        private final MockedStatic<AnalyzerUtils> analyzerUtils;
        private final MockedStatic<SplitTabletJobFactory> splitTabletJobFactory;
        private final ArgumentCaptor<Map<Long, List<TabletRange>>> rangesCaptor;
        private final LocalMetastore localMetastore;
        private final MaterializedView materializedView;
        private final InsertOverwriteJobRunner runner;

        private Fixture(int temporaryPartitionCount) {
            Database database = mock(Database.class);
            when(database.getId()).thenReturn(DB_ID);
            when(database.getFullName()).thenReturn("mv_db");

            // The one target shape the derived tier serves: an incremental view whose single visible
            // index is keyed by the storage-generated row-id column alone.
            this.materializedView = mock(MaterializedView.class);
            when(materializedView.getId()).thenReturn(MV_ID);
            when(materializedView.getName()).thenReturn("mv_row_id");
            when(materializedView.getRowIdStrategy()).thenReturn(RowIdStrategy.AUTO_INCREMENT);
            when(materializedView.isRangeDistribution()).thenReturn(true);
            when(materializedView.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
            when(materializedView.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
            MaterializedIndexMeta baseIndexMeta = mock(MaterializedIndexMeta.class);
            when(baseIndexMeta.getIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
            when(materializedView.getVisibleIndexMetas()).thenReturn(List.of(baseIndexMeta));

            List<Long> temporaryPartitionIds = new ArrayList<>(temporaryPartitionCount);
            List<String> sourcePartitionNames = new ArrayList<>(temporaryPartitionCount);
            for (int ordinal = 0; ordinal < temporaryPartitionCount; ordinal++) {
                temporaryPartitionIds.add(FIRST_TEMPORARY_PARTITION_ID + ordinal);
                sourcePartitionNames.add(sourcePartitionName(ordinal));
                stubTemporaryPartition(materializedView, ordinal);
            }

            this.globalStateMgr = Mockito.mockStatic(GlobalStateMgr.class);
            this.metaUtils = Mockito.mockStatic(MetaUtils.class);
            this.analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
            this.splitTabletJobFactory = Mockito.mockStatic(SplitTabletJobFactory.class);

            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            globalStateMgr.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);
            this.localMetastore = mock(LocalMetastore.class);
            when(localMetastore.getDb(DB_ID)).thenReturn(database);
            when(localMetastore.getTable(DB_ID, MV_ID)).thenReturn(materializedView);
            // Pristine counter: no id has ever been allocated for this view, which is what pins the
            // low end of the carved id space at 1.
            when(localMetastore.getCurrentAutoIncrementIdByTableId(MV_ID)).thenReturn(null);
            when(globalState.getLocalMetastore()).thenReturn(localMetastore);
            // The runner and both pre-split stages take real intensive table READ locks.
            when(globalState.getLockManager()).thenReturn(new LockManager());
            MetadataMgr metadataMgr = mock(MetadataMgr.class);
            when(metadataMgr.getDb(any(), any(), any())).thenReturn(database);
            when(globalState.getMetadataMgr()).thenReturn(metadataMgr);
            // One compute node, so the tablet-count rounding and the row-id gap headroom are both
            // computed against a known node count.
            WarehouseManager warehouseManager = mock(WarehouseManager.class);
            when(warehouseManager.getAllComputeNodeIds(any(ComputeResource.class))).thenReturn(List.of(1L));
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);

            // The admitted job is reported FINISHED so the flow's fail-safe await returns at once
            // instead of polling out the post-submit budget.
            TabletReshardJob reshardJob = mock(TabletReshardJob.class);
            when(reshardJob.getJobState()).thenReturn(TabletReshardJob.JobState.FINISHED);
            TabletReshardJobMgr reshardJobMgr = mock(TabletReshardJobMgr.class);
            when(reshardJobMgr.getTabletReshardJob(anyLong())).thenReturn(reshardJob);
            when(globalState.getTabletReshardJobMgr()).thenReturn(reshardJobMgr);

            Column rowIdColumn = new Column(IvmOpUtils.COLUMN_ROW_ID, IntegerType.BIGINT);
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(materializedView))
                    .thenReturn(List.of(rowIdColumn));
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(
                            eq(materializedView), eq(BASE_INDEX_META_ID)))
                    .thenReturn(List.of(rowIdColumn));
            metaUtils.when(() -> MetaUtils.getSessionAwareTable(any(), eq(database), any()))
                    .thenReturn(materializedView);

            TableRef normalizedTableRef = mock(TableRef.class);
            when(normalizedTableRef.getCatalogName()).thenReturn("default_catalog");
            when(normalizedTableRef.getDbName()).thenReturn("mv_db");
            when(normalizedTableRef.getTableName()).thenReturn("mv_row_id");
            analyzerUtils.when(() -> AnalyzerUtils.normalizedTableRef(any(), any()))
                    .thenReturn(normalizedTableRef);

            this.rangesCaptor = rangesCaptor();
            splitTabletJobFactory.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                            eq(database), eq(materializedView), rangesCaptor.capture()))
                    .thenReturn(reshardJob);

            InsertStmt insertStmt = mock(InsertStmt.class);
            when(insertStmt.isOverwrite()).thenReturn(true);
            when(insertStmt.hasOverwriteJob()).thenReturn(true);
            when(insertStmt.getTxnId()).thenReturn(DmlStmt.INVALID_TXN_ID);
            when(insertStmt.getUserSpecifiedPropertyKeys()).thenReturn(Set.of());
            when(insertStmt.getTableRef()).thenReturn(mock(TableRef.class));

            // The runner is handed a stubbed context so the load's compute resource is fixed whatever
            // run mode the JVM ended up in; it carries the same session variable as the thread-local
            // context that the coordinator's own session gate reads.
            ConnectContext context = mock(ConnectContext.class);
            when(context.getSessionVariable()).thenReturn(ConnectContext.get().getSessionVariable());
            when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));

            InsertOverwriteJob job = new InsertOverwriteJob(
                    /*jobId*/ 501L, insertStmt, DB_ID, MV_ID, WarehouseManager.DEFAULT_WAREHOUSE_ID,
                    /*dynamicOverwrite*/ false);
            job.setTmpPartitionIds(temporaryPartitionIds);
            job.setSourcePartitionNames(sourcePartitionNames);
            this.runner = new InsertOverwriteJobRunner(job, context, mock(StmtExecutor.class),
                    new Estimates(ESTIMATED_BYTES, ESTIMATED_ROWS));
        }

        /**
         * Wires one cloned temporary partition: resolvable both by id (the runner maps the job's ids
         * to names) and by temp-scoped name (the flow resolves its target), with a single empty
         * base-index tablet.
         */
        private static void stubTemporaryPartition(MaterializedView materializedView, int ordinal) {
            Tablet tablet = mock(Tablet.class);
            when(tablet.getId()).thenReturn(FIRST_BASE_TABLET_ID + ordinal);
            MaterializedIndex baseIndex = mock(MaterializedIndex.class);
            when(baseIndex.getMetaId()).thenReturn(BASE_INDEX_META_ID);
            when(baseIndex.getTablets()).thenReturn(List.of(tablet));
            // A freshly cloned temporary partition holds no rows yet; a non-empty one is not
            // pre-splittable.
            when(baseIndex.getRowCount()).thenReturn(0L);

            long physicalPartitionId = FIRST_PHYSICAL_PARTITION_ID + ordinal;
            PhysicalPartition physicalPartition = mock(PhysicalPartition.class);
            when(physicalPartition.getId()).thenReturn(physicalPartitionId);
            when(physicalPartition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);
            when(physicalPartition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                    .thenReturn(List.of(baseIndex));

            Partition partition = mock(Partition.class);
            when(partition.getName()).thenReturn(temporaryPartitionName(ordinal));
            when(partition.getSubPartitions()).thenReturn(List.of(physicalPartition));

            when(materializedView.getPartition(FIRST_TEMPORARY_PARTITION_ID + ordinal)).thenReturn(partition);
            when(materializedView.getPartition(temporaryPartitionName(ordinal), true)).thenReturn(partition);
            when(materializedView.getPhysicalPartition(physicalPartitionId)).thenReturn(physicalPartition);
        }

        @SuppressWarnings("unchecked")
        private static ArgumentCaptor<Map<Long, List<TabletRange>>> rangesCaptor() {
            return ArgumentCaptor.forClass(Map.class);
        }

        /** Hand out an id for the view, making the row-id space no longer pristine. */
        private void assignAutoIncrementId(long currentAutoIncrementId) {
            when(localMetastore.getCurrentAutoIncrementIdByTableId(MV_ID)).thenReturn(currentAutoIncrementId);
        }

        /**
         * The {@code oldTabletId -> ranges} map the reshard job was built from. Fails unless exactly
         * one job was built: a second one would mean a partition was carved twice.
         */
        private Map<Long, List<TabletRange>> submittedRanges() {
            splitTabletJobFactory.verify(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()));
            return rangesCaptor.getValue();
        }

        /** No reshard job was built — the observable effect of a skip anywhere along the path. */
        private void assertNothingSubmitted() {
            splitTabletJobFactory.verify(
                    () -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()), never());
        }

        @Override
        public void close() {
            splitTabletJobFactory.close();
            analyzerUtils.close();
            metaUtils.close();
            globalStateMgr.close();
        }
    }
}
