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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.TimeoutException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.DUMMY_CONTEXT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TabletPreSplitCoordinatorTest {

    private static final long PARTITION_ID = 10001L;
    private static final long BASE_INDEX_META_ID = 200L;

    private Database database;
    private OlapTable table;
    private PhysicalPartition partition;
    private MaterializedIndex baseIndex;

    private boolean savedConfigInsertFromFiles;
    private boolean savedConfigBrokerLoad;
    private long savedConfigReshardTargetSize;
    private int savedConfigReshardMaxSplitCount;

    @BeforeEach
    public void setUp() {
        savedConfigInsertFromFiles = Config.enable_tablet_pre_split_for_insert_from_files;
        savedConfigBrokerLoad = Config.enable_tablet_pre_split_for_broker_load;
        savedConfigReshardTargetSize = Config.tablet_reshard_target_size;
        savedConfigReshardMaxSplitCount = Config.tablet_reshard_max_split_count;
        Config.enable_tablet_pre_split_for_insert_from_files = true;
        Config.enable_tablet_pre_split_for_broker_load = false;
        // Pin tablet-count-selection inputs so the test arithmetic stays valid if defaults move.
        Config.tablet_reshard_target_size = 10L * DebugUtil.GIGABYTE;
        Config.tablet_reshard_max_split_count = 1024;

        // Bind a fresh ConnectContext so the coordinator's session-var check finds one.
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableTabletPreSplit(true);
        connectContext.setSessionVariable(sessionVariable);
        connectContext.setThreadLocalInfo();

        database = mock(Database.class);
        baseIndex = mock(MaterializedIndex.class);
        when(baseIndex.getTablets()).thenReturn(List.of(mock(Tablet.class)));
        when(baseIndex.getRowCount()).thenReturn(0L);

        partition = mock(PhysicalPartition.class);
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);

        table = mock(OlapTable.class);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(mock(MaterializedIndexMeta.class)));
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(table.getPhysicalPartition(PARTITION_ID)).thenReturn(partition);
        Column scalarKey = mock(Column.class);
        Type scalarType = IntegerType.BIGINT;
        when(scalarKey.getType()).thenReturn(scalarType);
        when(table.getKeyColumnsInOrder()).thenReturn(List.of(scalarKey));
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        Config.enable_tablet_pre_split_for_insert_from_files = savedConfigInsertFromFiles;
        Config.enable_tablet_pre_split_for_broker_load = savedConfigBrokerLoad;
        Config.tablet_reshard_target_size = savedConfigReshardTargetSize;
        Config.tablet_reshard_max_split_count = savedConfigReshardMaxSplitCount;
    }

    private PreSplitOutcome invokeMaybeAct() {
        return TabletPreSplitCoordinator.maybeAct(
                database, table, PARTITION_ID, DUMMY_CONTEXT, LoadKind.INSERT_FROM_FILES);
    }

    private static void assertSkipped(PreSplitOutcome outcome, SkipReason expected) {
        Assertions.assertInstanceOf(PreSplitOutcome.Skipped.class, outcome,
                "expected Skipped(" + expected + "), got: " + outcome);
        Assertions.assertEquals(expected, ((PreSplitOutcome.Skipped) outcome).reason());
    }

    @Test
    public void testHappyPathReturnsEligible() {
        Assertions.assertInstanceOf(PreSplitOutcome.Eligible.class, invokeMaybeAct());
    }

    @Test
    public void testInsertCallSkippedWhenInsertConfigOff() {
        Config.enable_tablet_pre_split_for_insert_from_files = false;
        Config.enable_tablet_pre_split_for_broker_load = false;

        assertSkipped(invokeMaybeAct(), SkipReason.DISABLED_BY_CONFIG);
    }

    @Test
    public void testInsertCallSkippedWhenOnlyBrokerConfigOn() {
        // Operator enabled Broker Load only; an INSERT-from-FILES call must still be gated off.
        Config.enable_tablet_pre_split_for_insert_from_files = false;
        Config.enable_tablet_pre_split_for_broker_load = true;

        assertSkipped(invokeMaybeAct(), SkipReason.DISABLED_BY_CONFIG);
    }

    @Test
    public void testBrokerLoadCallReturnsEligibleWithOnlyBrokerConfigOn() {
        Config.enable_tablet_pre_split_for_insert_from_files = false;
        Config.enable_tablet_pre_split_for_broker_load = true;

        PreSplitOutcome outcome = TabletPreSplitCoordinator.maybeAct(
                database, table, PARTITION_ID, DUMMY_CONTEXT, LoadKind.BROKER_LOAD);
        Assertions.assertInstanceOf(PreSplitOutcome.Eligible.class, outcome);
    }

    @Test
    public void testBrokerLoadCallSkippedWhenOnlyInsertConfigOn() {
        // And the inverse: a Broker Load call is gated off if only INSERT is enabled.
        Config.enable_tablet_pre_split_for_insert_from_files = true;
        Config.enable_tablet_pre_split_for_broker_load = false;

        PreSplitOutcome outcome = TabletPreSplitCoordinator.maybeAct(
                database, table, PARTITION_ID, DUMMY_CONTEXT, LoadKind.BROKER_LOAD);
        assertSkipped(outcome, SkipReason.DISABLED_BY_CONFIG);
    }

    @Test
    public void testSessionVariableOffSkipped() {
        ConnectContext.get().getSessionVariable().setEnableTabletPreSplit(false);

        assertSkipped(invokeMaybeAct(), SkipReason.DISABLED_BY_SESSION);
    }

    @Test
    public void testNotRangeDistributionSkipped() {
        when(table.isRangeDistribution()).thenReturn(false);

        assertSkipped(invokeMaybeAct(), SkipReason.NOT_RANGE_DISTRIBUTION);
    }

    @Test
    public void testNonNormalTableSkipped() {
        when(table.getState()).thenReturn(OlapTable.OlapTableState.SCHEMA_CHANGE);

        assertSkipped(invokeMaybeAct(), SkipReason.TABLE_NOT_NORMAL);
    }

    @Test
    public void testMaterializedViewOrRollupSkipped() {
        // visibleIndexMetas.size() > 1 means at least one MV or rollup is attached.
        when(table.getVisibleIndexMetas()).thenReturn(
                List.of(mock(MaterializedIndexMeta.class), mock(MaterializedIndexMeta.class)));

        assertSkipped(invokeMaybeAct(), SkipReason.HAS_MATERIALIZED_VIEW_OR_ROLLUP);
    }

    @Test
    public void testUnsupportedSortKeyColumnTypeSkipped() {
        Column nonScalarKey = mock(Column.class);
        Type nonScalarType = mock(Type.class);
        when(nonScalarType.isScalarType()).thenReturn(false);
        when(nonScalarKey.getType()).thenReturn(nonScalarType);
        when(table.getKeyColumnsInOrder()).thenReturn(List.of(nonScalarKey));

        assertSkipped(invokeMaybeAct(), SkipReason.UNSUPPORTED_SORT_KEY);
    }

    @Test
    public void testEmptyKeyColumnsSkipped() {
        when(table.getKeyColumnsInOrder()).thenReturn(List.of());

        assertSkipped(invokeMaybeAct(), SkipReason.UNSUPPORTED_SORT_KEY);
    }

    @Test
    public void testMultipleBaseIndexTabletsSkipped() {
        when(baseIndex.getTablets()).thenReturn(List.of(mock(Tablet.class), mock(Tablet.class)));

        assertSkipped(invokeMaybeAct(), SkipReason.MULTIPLE_BASE_INDEX_TABLETS);
    }

    @Test
    public void testPartitionNotEmptySkipped() {
        when(baseIndex.getRowCount()).thenReturn(42L);

        assertSkipped(invokeMaybeAct(), SkipReason.PARTITION_NOT_EMPTY);
    }

    @Test
    public void testMissingPartitionSkipped() {
        when(table.getPhysicalPartition(PARTITION_ID)).thenReturn(null);

        assertSkipped(invokeMaybeAct(), SkipReason.METADATA_NOT_RESOLVED);
    }

    @Test
    public void testMissingBaseIndexSkipped() {
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(null);

        assertSkipped(invokeMaybeAct(), SkipReason.METADATA_NOT_RESOLVED);
    }

    // ---- selectTabletCount (B2) ----

    private static int selectTabletCount(long totalBytes, int activeComputeNodeCount) {
        return TabletPreSplitCoordinator.selectTabletCount(
                new Estimates(totalBytes, /*totalRows*/ 0L), activeComputeNodeCount);
    }

    @Test
    public void testTabletCountSmallLoadOnThreeComputeNodes() {
        // 1 GB / 10 GB target rounds up to 1 tablet by bytes; compute-node floor of 3 wins.
        Assertions.assertEquals(3, selectTabletCount(DebugUtil.GIGABYTE, 3));
    }

    @Test
    public void testTabletCountLargeLoadOnThreeComputeNodes() {
        // 100 GB / 10 GB target = 10 tablets by bytes; beats the compute-node floor of 3.
        Assertions.assertEquals(10, selectTabletCount(100L * DebugUtil.GIGABYTE, 3));
    }

    @Test
    public void testTabletCountSmallLoadOnTwelveComputeNodes() {
        Assertions.assertEquals(12, selectTabletCount(DebugUtil.GIGABYTE, 12));
    }

    @Test
    public void testTabletCountLargeLoadOnTwelveComputeNodes() {
        // 100 GB / 10 GB target = 10 tablets by bytes; compute-node floor of 12 wins.
        Assertions.assertEquals(12, selectTabletCount(100L * DebugUtil.GIGABYTE, 12));
    }

    @Test
    public void testTabletCountSaturatesAtMaxSplitCount() {
        // 10 PB / 10 GB target ≈ 1M tablets by bytes; clamps to tablet_reshard_max_split_count.
        Assertions.assertEquals(Config.tablet_reshard_max_split_count,
                selectTabletCount(10L * 1024L * DebugUtil.TERABYTE, 1));
    }

    @Test
    public void testTabletCountFloorsAtTwo() {
        // Zero bytes + single compute node would give 1; clamp lifts it to the minimum 2.
        Assertions.assertEquals(2, selectTabletCount(0L, 1));
    }

    @Test
    public void testTabletCountRejectsNonPositiveComputeNodeCount() {
        Estimates anyEstimates = new Estimates(DebugUtil.GIGABYTE, 0L);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletPreSplitCoordinator.selectTabletCount(anyEstimates, 0));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletPreSplitCoordinator.selectTabletCount(anyEstimates, -1));
    }

    @Test
    public void testTabletCountRejectsNonPositiveTargetSize() {
        Estimates anyEstimates = new Estimates(DebugUtil.GIGABYTE, 0L);
        Config.tablet_reshard_target_size = 0L;
        Assertions.assertThrows(IllegalStateException.class,
                () -> TabletPreSplitCoordinator.selectTabletCount(anyEstimates, 3));
        Config.tablet_reshard_target_size = -1L;
        Assertions.assertThrows(IllegalStateException.class,
                () -> TabletPreSplitCoordinator.selectTabletCount(anyEstimates, 3));
    }

    @Test
    public void testTabletCountRejectsMaxSplitCountBelowTwo() {
        Estimates anyEstimates = new Estimates(DebugUtil.GIGABYTE, 0L);
        Config.tablet_reshard_max_split_count = 1;
        Assertions.assertThrows(IllegalStateException.class,
                () -> TabletPreSplitCoordinator.selectTabletCount(anyEstimates, 3));
    }

    // ---- runPreSplit pipeline orchestration (B3) ----

    private static final PreSplitPipeline.PreparedReshardJob FAKE_PREPARED_JOB =
            new PreSplitPipeline.PreparedReshardJob(new Object());

    private static class FakePipeline implements PreSplitPipeline {
        Optional<PreparedReshardJob> preSubmitReturn = Optional.of(FAKE_PREPARED_JOB);
        StarRocksException preSubmitThrow;
        StarRocksException submitThrow;
        StarRocksException awaitThrow;
        int preSubmitCalls;
        int submitCalls;
        int awaitCalls;

        @Override
        public Optional<PreparedReshardJob> preSubmit(SampleRequest request, int activeComputeNodeCount,
                                                       Duration timeout) throws StarRocksException {
            preSubmitCalls++;
            if (preSubmitThrow != null) {
                throw preSubmitThrow;
            }
            return preSubmitReturn;
        }

        @Override
        public void submit(PreparedReshardJob preparedJob) throws StarRocksException {
            submitCalls++;
            if (submitThrow != null) {
                throw submitThrow;
            }
        }

        @Override
        public void awaitFinished(PreparedReshardJob preparedJob, Duration timeout) throws StarRocksException {
            awaitCalls++;
            if (awaitThrow != null) {
                throw awaitThrow;
            }
        }
    }

    private PreSplitOutcome invokeRunPreSplit(FakePipeline pipeline) throws PreSplitPostSubmitTimeoutException {
        return TabletPreSplitCoordinator.runPreSplit(
                database, table, PARTITION_ID, DUMMY_CONTEXT,
                LoadKind.INSERT_FROM_FILES, pipeline, /*activeComputeNodeCount*/ 3);
    }

    @Test
    public void testRunPreSplitSkipsWhenEligibilityFails() throws Exception {
        // Eligibility fails → pipeline must not be invoked.
        when(table.isRangeDistribution()).thenReturn(false);
        FakePipeline pipeline = new FakePipeline();

        PreSplitOutcome outcome = invokeRunPreSplit(pipeline);

        assertSkipped(outcome, SkipReason.NOT_RANGE_DISTRIBUTION);
        Assertions.assertEquals(0, pipeline.preSubmitCalls, "pipeline.preSubmit should not run when eligibility fails");
    }

    @Test
    public void testRunPreSplitHappyPathReturnsFinished() throws Exception {
        FakePipeline pipeline = new FakePipeline();

        PreSplitOutcome outcome = invokeRunPreSplit(pipeline);

        Assertions.assertInstanceOf(PreSplitOutcome.Finished.class, outcome);
        Assertions.assertEquals(1, pipeline.preSubmitCalls);
        Assertions.assertEquals(1, pipeline.submitCalls);
        Assertions.assertEquals(1, pipeline.awaitCalls);
    }

    @Test
    public void testRunPreSplitPreSubmitTimeoutMapsToSkipped() throws Exception {
        FakePipeline pipeline = new FakePipeline();
        pipeline.preSubmitThrow = new PreSplitPreSubmitTimeoutException("sampler exceeded budget");

        PreSplitOutcome outcome = invokeRunPreSplit(pipeline);

        assertSkipped(outcome, SkipReason.TIMEOUT_PRE_SUBMIT);
        Assertions.assertEquals(0, pipeline.submitCalls);
        Assertions.assertEquals(0, pipeline.awaitCalls);
    }

    @Test
    public void testRunPreSplitSampleFailureMapsToSkipped() throws Exception {
        FakePipeline pipeline = new FakePipeline();
        pipeline.preSubmitThrow = new StarRocksException("connector unavailable");

        PreSplitOutcome outcome = invokeRunPreSplit(pipeline);

        assertSkipped(outcome, SkipReason.SAMPLE_FAILED);
        Assertions.assertEquals(0, pipeline.submitCalls);
    }

    @Test
    public void testRunPreSplitNoUsefulCutsSkipsBeforeSubmit() throws Exception {
        FakePipeline pipeline = new FakePipeline();
        pipeline.preSubmitReturn = Optional.empty();

        PreSplitOutcome outcome = invokeRunPreSplit(pipeline);

        assertSkipped(outcome, SkipReason.NO_USEFUL_CUTS);
        Assertions.assertEquals(0, pipeline.submitCalls);
        Assertions.assertEquals(0, pipeline.awaitCalls);
    }

    @Test
    public void testRunPreSplitSubmitFailureMapsToSkipped() throws Exception {
        FakePipeline pipeline = new FakePipeline();
        pipeline.submitThrow = new StarRocksException("table state changed during submit");

        PreSplitOutcome outcome = invokeRunPreSplit(pipeline);

        assertSkipped(outcome, SkipReason.SUBMIT_FAILED);
        Assertions.assertEquals(0, pipeline.awaitCalls);
    }

    @Test
    public void testRunPreSplitPostSubmitTimeoutPropagates() {
        FakePipeline pipeline = new FakePipeline();
        pipeline.awaitThrow = new PreSplitPostSubmitTimeoutException("job did not finish in 300s");

        // Caller (load executor) is expected to catch this and abort the load txn.
        Assertions.assertThrows(PreSplitPostSubmitTimeoutException.class,
                () -> invokeRunPreSplit(pipeline));
    }

    @Test
    public void testRunPreSplitPostSubmitTimeoutRecordsHardCapMetric() {
        withHardCapCounter(counter -> {
            FakePipeline pipeline = new FakePipeline();
            pipeline.awaitThrow = new PreSplitPostSubmitTimeoutException("job did not finish in 300s");

            Assertions.assertThrows(PreSplitPostSubmitTimeoutException.class,
                    () -> invokeRunPreSplit(pipeline));
            Assertions.assertEquals(1L, counter.getValue().longValue(),
                    "post-submit timeout should record one hard-cap event");
        });
    }

    /**
     * Wire the counter in and execute {@code body}; restore previous state in a finally block
     * so other tests are not affected by the touched static fields.
     */
    private static void withHardCapCounter(java.util.function.Consumer<LongCounterMetric> body) {
        LongCounterMetric counter = new LongCounterMetric(
                "tablet_pre_split_post_submit_hard_cap", MetricUnit.REQUESTS, "hard-cap events");
        LongCounterMetric savedCounter = MetricRepo.COUNTER_TABLET_PRE_SPLIT_POST_SUBMIT_HARD_CAP;
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.COUNTER_TABLET_PRE_SPLIT_POST_SUBMIT_HARD_CAP = counter;
        MetricRepo.hasInit = true;
        try {
            body.accept(counter);
        } finally {
            MetricRepo.hasInit = savedHasInit;
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_POST_SUBMIT_HARD_CAP = savedCounter;
        }
    }

    @Test
    public void testRunPreSplitSubmitFailureDoesNotRecordHardCapMetric() {
        withHardCapCounter(counter -> {
            FakePipeline pipeline = new FakePipeline();
            pipeline.submitThrow = new StarRocksException("submit rejected");

            Assertions.assertDoesNotThrow(() -> invokeRunPreSplit(pipeline));
            Assertions.assertEquals(0L, counter.getValue().longValue(),
                    "submit failure must not touch the hard-cap counter");
        });
    }

    @Test
    public void testRunPreSplitJobFailureBeforeFinishDoesNotRecordHardCapMetric() {
        withHardCapCounter(counter -> {
            FakePipeline pipeline = new FakePipeline();
            pipeline.awaitThrow = new StarRocksException("job CANCELLED");

            Assertions.assertDoesNotThrow(() -> invokeRunPreSplit(pipeline));
            Assertions.assertEquals(0L, counter.getValue().longValue(),
                    "non-timeout terminal-state failure must not touch the hard-cap counter");
        });
    }

    @Test
    public void testRunPreSplitJobFailureBeforeFinishMapsToSkipped() throws Exception {
        // The admitted job entered a terminal-error state (e.g. CANCELLED) before reaching FINISHED.
        FakePipeline pipeline = new FakePipeline();
        pipeline.awaitThrow = new StarRocksException("job CANCELLED");

        PreSplitOutcome outcome = invokeRunPreSplit(pipeline);

        assertSkipped(outcome, SkipReason.JOB_FAILED_BEFORE_FINISH);
    }

    @Test
    public void testRunPreSplitRejectsNullPipeline() {
        Assertions.assertThrows(NullPointerException.class, () -> TabletPreSplitCoordinator.runPreSplit(
                database, table, PARTITION_ID, DUMMY_CONTEXT, LoadKind.INSERT_FROM_FILES, null, 3));
    }

    @Test
    public void testRunPreSplitRejectsNonPositiveComputeNodeCount() {
        FakePipeline pipeline = new FakePipeline();
        Assertions.assertThrows(IllegalArgumentException.class, () -> TabletPreSplitCoordinator.runPreSplit(
                database, table, PARTITION_ID, DUMMY_CONTEXT, LoadKind.INSERT_FROM_FILES, pipeline, 0));
    }

    @Test
    public void testRunPreSplitBareTimeoutExceptionMapsToHardCap() {
        // Defense in depth: even if the pipeline throws a generic TimeoutException (the
        // parent class) rather than the typed PreSplitPostSubmitTimeoutException, the
        // coordinator must treat it as a post-submit hard-cap event and propagate (load
        // txn aborts). Otherwise the bare timeout would fall into the StarRocksException
        // catch and the load would proceed against unfinished tablet metadata.
        withHardCapCounter(counter -> {
            FakePipeline pipeline = new FakePipeline();
            pipeline.awaitThrow = new TimeoutException("bare timeout from deeper await");

            PreSplitPostSubmitTimeoutException thrown = Assertions.assertThrows(
                    PreSplitPostSubmitTimeoutException.class, () -> invokeRunPreSplit(pipeline));
            Assertions.assertSame(pipeline.awaitThrow, thrown.getCause(),
                    "the original timeout should be the wrapped exception's cause");
            Assertions.assertEquals(1L, counter.getValue().longValue(),
                    "bare TimeoutException must record one hard-cap event");
        });
    }
}
