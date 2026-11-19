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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.starrocks.alter.reshard.SplitTabletJobFactory;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.PreSplitProfile;
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
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric.MetricUnit;
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
import com.starrocks.type.StringType;
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
 * reshard job that carves the single tablet of each temporary partition.
 *
 * <p>Each stage of that path has its own unit test. What only this test covers is that the stages
 * are connected end to end, and that the boundaries which reach the reshard job are the ones the
 * row-id arithmetic dictates. A broken hand-off would leave the feature silently doing nothing —
 * the exact failure it exists to remove — while every per-stage test still passed, so the
 * observation point is {@link SplitTabletJobFactory#forExternalBoundaries} (the one call that turns
 * planned boundaries into a job) and the assertions are on the boundary values themselves.
 *
 * <p>The catalog is mocked rather than created through a real cluster: the path reads a narrow slice
 * of metadata (the temporary partitions, each with one base tablet, one visible index and one
 * sort-key column, plus the table's auto-increment counter), and mocking it pins the two inputs the
 * arithmetic is derived from — the statement's estimate and the compute-node count — exactly. Each
 * skip case below changes ONE thing relative to the happy path, so the happy path is what proves the
 * fixture reaches the factory at all.
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
    private int savedMaxPartitionsPerLoad;

    @BeforeEach
    public void setUp() {
        savedMvRefreshEnabled = Config.enable_tablet_pre_split_for_mv_refresh;
        savedIdCacheSize = Config.auto_increment_cache_size;
        savedPreSplitTargetSize = Config.tablet_pre_split_target_size;
        savedReshardTargetSize = Config.tablet_reshard_target_size;
        savedReshardMinSplitSize = Config.tablet_reshard_min_split_size;
        savedReshardMaxSplitCount = Config.tablet_reshard_max_split_count;
        savedSampleByteLimit = Config.tablet_pre_split_sample_byte_limit;
        savedMaxPartitionsPerLoad = Config.tablet_pre_split_max_partitions_per_load;
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
        Config.tablet_pre_split_max_partitions_per_load = savedMaxPartitionsPerLoad;
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

            RuntimeProfile root = new RuntimeProfile("Load");
            PreSplitProfile.appendTo(root, fixture.preSplitProfile);
            RuntimeProfile profile = root.getChild(PreSplitProfile.PROFILE_NAME);
            Assertions.assertNotNull(profile, "an attempted MV refresh pre-split must attach its profile node");
            Assertions.assertEquals(1L, profile.getCounter(PreSplitProfile.ATTEMPTS).getValue());
            Assertions.assertEquals(ESTIMATED_BYTES,
                    profile.getCounter(PreSplitProfile.ESTIMATED_INPUT_BYTES).getValue());
            Assertions.assertEquals(1L, profile.getCounter(PreSplitProfile.TARGET_PARTITIONS).getValue());
            Assertions.assertEquals(3L, profile.getCounter(PreSplitProfile.BOUNDARIES_PLANNED).getValue());
            Assertions.assertEquals("incremental MV refresh", profile.getInfoString("LoadKinds"));
            Assertions.assertEquals("mv_row_id", profile.getInfoString("Tables"));
            Assertions.assertEquals("derived_tier", profile.getInfoString("SourceTiers"));
            Assertions.assertEquals("SUBMITTED, FINISHED", profile.getInfoString("Outcomes"));
            Assertions.assertEquals("9301", profile.getInfoString("ReshardJobIds"));
        }
    }

    @Test
    public void overwriteOfFingerprintViewSplitsOnTheBase64Space() {
        // A fingerprint-encoded row id is a VARCHAR that is uniform over the base64 alphabet, so the
        // cuts carve the symbol space instead of a numeric span. The 4 MiB estimate against the 1 MiB
        // pre-split target requests 4 tablets, and the 1 MiB minimum split size also allows 4, so the
        // three cuts are the quarter points of the first symbol pair: E+, U+ and k+.
        Column varcharRowId = new Column(IvmOpUtils.COLUMN_ROW_ID, StringType.DEFAULT_STRING);
        try (Fixture fixture = new Fixture(/*temporaryPartitionCount*/ 1, varcharRowId)) {
            when(fixture.materializedView.getRowIdStrategy()).thenReturn(RowIdStrategy.QUERY_COMPUTED);
            when(fixture.materializedView.getEncodeRowIdVersion())
                    .thenReturn(IvmOpUtils.ENCODE_ROW_ID_VERSION_FINGERPRINT_V2);

            fixture.runner.preSplitStaticOverwriteTempPartitions();

            Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
            Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID), submitted.keySet());
            List<TabletRange> ranges = submitted.get(FIRST_BASE_TABLET_ID);
            Assertions.assertEquals(4, ranges.size(), "three cuts carve four tablets");

            Assertions.assertTrue(ranges.get(0).getRange().isMinimum(), "first range must open at -infinity");
            Assertions.assertEquals(stringTuple("E+"), ranges.get(0).getRange().getUpperBound());
            Assertions.assertEquals(stringTuple("E+"), ranges.get(1).getRange().getLowerBound());
            Assertions.assertEquals(stringTuple("U+"), ranges.get(1).getRange().getUpperBound());
            Assertions.assertEquals(stringTuple("U+"), ranges.get(2).getRange().getLowerBound());
            Assertions.assertEquals(stringTuple("k+"), ranges.get(2).getRange().getUpperBound());
            Assertions.assertEquals(stringTuple("k+"), ranges.get(3).getRange().getLowerBound());
            Assertions.assertTrue(ranges.get(3).getRange().isMaximum(), "last range must close at +infinity");

            // Variant equality compares the value only, so the boundary type is checked on its own: a
            // cut typed differently from the sort-key column would not match the BE's key encoding.
            for (int i = 0; i < 3; i++) {
                Tuple cut = ranges.get(i).getRange().getUpperBound();
                Assertions.assertEquals(1, cut.getValues().size());
                Assertions.assertEquals(varcharRowId.getType(), cut.getValues().get(0).getType(),
                        "cut " + i + " must carry the row-id column's own type");
            }
        }
    }

    private static Tuple stringTuple(String value) {
        return new Tuple(List.of(Variant.of(StringType.DEFAULT_STRING, value)));
    }

    @Test
    public void usedAutoIncrementCounterCutsAboveTheCounter() {
        // Every id a later grant hands out is at or above the counter, so the span starts there: same
        // 4 tablets and 1000-row stride as the pristine case, shifted to 500 + 1000 * k. The doubled gap
        // mass (2 * 1 node * cache size 25 = 50) still affords 4000 / (10 * 50) = 8 tablets, so the
        // requested count of 4 survives without touching auto_increment_cache_size.
        try (Fixture fixture = new Fixture(/*temporaryPartitionCount*/ 1)) {
            fixture.assignAutoIncrementId(500L);

            fixture.runner.preSplitStaticOverwriteTempPartitions();

            List<TabletRange> ranges = fixture.submittedRanges().get(FIRST_BASE_TABLET_ID);
            Assertions.assertEquals(4, ranges.size(), "three cuts carve four tablets");
            Assertions.assertEquals(rowIdTuple(1500L), ranges.get(0).getRange().getUpperBound());
            Assertions.assertEquals(rowIdTuple(2500L), ranges.get(1).getRange().getUpperBound());
            Assertions.assertEquals(rowIdTuple(3500L), ranges.get(2).getRange().getUpperBound());
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
            when(fixture.materializedView.getEncodeRowIdVersion()).thenReturn(0);
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
    public void severalTemporaryPartitionsOfAnAutoIncrementViewAreReportedAsSuchInBothFlagStates() {
        // Attribution matters more than the skip itself here: an auto-increment view's ids come from one
        // counter, so each partition's ids sit in a narrow band, and enabling the flag would NOT make its
        // multi-partition refresh eligible. Charging it to the config gate would tell an operator to flip
        // a switch that cannot help, so the reason must be the same whether the flag is on or off.
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
    public void severalTemporaryPartitionsOfAnAutoIncrementViewSubmitNothing() {
        // An auto-increment view's row ids come from one counter, so they form a contiguous band per
        // partition, and cuts spanning the whole id space would leave most tablets of most partitions
        // empty and one of each overloaded. Both partitions here are fully wired, so a lost gate shows
        // up as a submit.
        try (Fixture fixture = new Fixture(/*temporaryPartitionCount*/ 2)) {
            fixture.runner.preSplitStaticOverwriteTempPartitions();

            fixture.assertNothingSubmitted();
        }
    }

    @Test
    public void overwriteOfFingerprintViewSplitsEveryTemporaryPartitionInOneJob() {
        // Fingerprints are uniform in every partition, so the same cuts serve all of them. The 4 MiB
        // estimate against the 1 MiB pre-split target budgets 4 tablets for the whole refresh; each of the
        // two partitions takes half of the tablets and half of the bytes, so each is cut once, at U+.
        withTestLocalPreSplitMetrics("every_partition", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2)) {
                long baselineMultiple = skipCount(SkipReason.MULTIPLE_TEMPORARY_PARTITIONS);

                fixture.runner.preSplitStaticOverwriteTempPartitions();

                Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
                Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID, FIRST_BASE_TABLET_ID + 1), submitted.keySet(),
                        "one job must carve the base tablet of every temporary partition");
                submitted.values().forEach(InsertOverwriteMaterializedViewPreSplitTest::assertCutOnceAtTheMidpoint);
                Assertions.assertEquals(baselineMultiple, skipCount(SkipReason.MULTIPLE_TEMPORARY_PARTITIONS),
                        "a fingerprint view's partition count is not a reason to skip");

                RuntimeProfile profile = preSplitProfileOf(fixture);
                Assertions.assertEquals(2L, profile.getCounter(PreSplitProfile.TARGET_PARTITIONS).getValue());
                Assertions.assertEquals(2L, profile.getCounter(PreSplitProfile.BOUNDARIES_PLANNED).getValue(),
                        "one cut in each of the two partitions");
                Assertions.assertEquals(ESTIMATED_BYTES,
                        profile.getCounter(PreSplitProfile.ESTIMATED_INPUT_BYTES).getValue(),
                        "the profile reports the refresh's whole estimate, not one partition's share");
                Assertions.assertEquals("derived_tier", profile.getInfoString("SourceTiers"));
                Assertions.assertEquals("SUBMITTED, FINISHED", profile.getInfoString("Outcomes"));
                Assertions.assertEquals("9301", profile.getInfoString("ReshardJobIds"));
            }
        });
    }

    @Test
    public void perTabletSplitCapDoesNotLimitTheWholeRefresh() {
        // tablet_reshard_max_split_count bounds how many tablets ONE old tablet becomes. The refresh's
        // budget of 4 is shared out first, and each partition's 2 fits the cap; capping the budget at 2
        // before sharing it would leave each partition one tablet and nothing to cut.
        Config.tablet_reshard_max_split_count = 2;
        withTestLocalPreSplitMetrics("split_cap", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2)) {
                fixture.runner.preSplitStaticOverwriteTempPartitions();

                Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
                Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID, FIRST_BASE_TABLET_ID + 1), submitted.keySet());
                submitted.values().forEach(InsertOverwriteMaterializedViewPreSplitTest::assertCutOnceAtTheMidpoint);
            }
        });
    }

    @Test
    public void ineligibleFingerprintPartitionIsDroppedAlone() {
        // A temporary partition that cannot be resolved must not cost its siblings their split. It still
        // receives its share of the refresh's rows, so the share stays a half and the remaining partition
        // is cut once, exactly as it would be alongside its sibling -- not three times, as it would be if
        // it were given the whole refresh.
        withTestLocalPreSplitMetrics("dropped_alone", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2)) {
                when(fixture.materializedView.getPartition(temporaryPartitionName(1), true)).thenReturn(null);
                long baselineUnresolved = skipCount(SkipReason.METADATA_NOT_RESOLVED);

                fixture.runner.preSplitStaticOverwriteTempPartitions();

                Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
                Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID), submitted.keySet());
                assertCutOnceAtTheMidpoint(submitted.get(FIRST_BASE_TABLET_ID));
                Assertions.assertEquals(baselineUnresolved + 1L, skipCount(SkipReason.METADATA_NOT_RESOLVED),
                        "the dropped partition records its own reason");
                Assertions.assertEquals(1L,
                        preSplitProfileOf(fixture).getCounter(PreSplitProfile.TARGET_PARTITIONS).getValue());
            }
        });
    }

    @Test
    public void nonEmptyFingerprintPartitionIsDroppedAlone() {
        // Emptiness is a per-partition check, like resolvability: a partition that already holds rows is
        // dropped with its own reason, and the other one is still cut, sized for its half of the refresh.
        withTestLocalPreSplitMetrics("not_empty_later", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2)) {
                fixture.holdRows(1);
                long baselineNotEmpty = skipCount(SkipReason.PARTITION_NOT_EMPTY);

                fixture.runner.preSplitStaticOverwriteTempPartitions();

                Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
                Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID), submitted.keySet(),
                        "the partition that holds rows must not be carved");
                assertCutOnceAtTheMidpoint(submitted.get(FIRST_BASE_TABLET_ID));
                Assertions.assertEquals(baselineNotEmpty + 1L, skipCount(SkipReason.PARTITION_NOT_EMPTY));
            }
        });
    }

    @Test
    public void nonEmptyFirstFingerprintPartitionDoesNotStopTheOthers() {
        // The partition that comes first by name holds rows. It is dropped before the coordinator's gate, so the
        // gate runs on the next one and the refresh is still carved rather than skipped as a whole.
        withTestLocalPreSplitMetrics("not_empty_first", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2)) {
                fixture.holdRows(0);
                long baselineNotEmpty = skipCount(SkipReason.PARTITION_NOT_EMPTY);

                fixture.runner.preSplitStaticOverwriteTempPartitions();

                Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
                Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID + 1), submitted.keySet());
                assertCutOnceAtTheMidpoint(submitted.get(FIRST_BASE_TABLET_ID + 1));
                Assertions.assertEquals(baselineNotEmpty + 1L, skipCount(SkipReason.PARTITION_NOT_EMPTY),
                        "one reason, for the partition that holds rows");
            }
        });
    }

    @Test
    public void nonEmptyFingerprintPartitionDoesNotTakeAPlaceUnderTheCap() {
        // With a cap of one, the later partition must be dropped as not empty rather than counted as capped. It
        // has been written to (its visible version moved past the initial one) while its row count still reads
        // zero, as it does until the tablet statistics are next refreshed.
        Config.tablet_pre_split_max_partitions_per_load = 1;
        withTestLocalPreSplitMetrics("not_empty_cap", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2)) {
                fixture.commitVersion(1);
                long baselineNotEmpty = skipCount(SkipReason.PARTITION_NOT_EMPTY);

                fixture.runner.preSplitStaticOverwriteTempPartitions();

                Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
                Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID), submitted.keySet());
                assertCutOnceAtTheMidpoint(submitted.get(FIRST_BASE_TABLET_ID));
                Assertions.assertEquals(baselineNotEmpty + 1L, skipCount(SkipReason.PARTITION_NOT_EMPTY));
                Assertions.assertEquals(0L, MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED.getValue(),
                        "a partition dropped as not empty is not also counted against the cap");
            }
        });
    }

    @Test
    public void fingerprintPartitionGoneBeforeTheEmptinessCheckIsReportedAsUnresolved() {
        // The partition was resolved, but is gone by the time its emptiness is read under a fresh lock. That is
        // missing metadata, not a partition holding rows, and its sibling is still carved.
        withTestLocalPreSplitMetrics("gone_before_check", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2)) {
                when(fixture.materializedView.getPhysicalPartition(FIRST_PHYSICAL_PARTITION_ID + 1)).thenReturn(null);
                long baselineUnresolved = skipCount(SkipReason.METADATA_NOT_RESOLVED);
                long baselineNotEmpty = skipCount(SkipReason.PARTITION_NOT_EMPTY);

                fixture.runner.preSplitStaticOverwriteTempPartitions();

                Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID), fixture.submittedRanges().keySet());
                Assertions.assertEquals(baselineUnresolved + 1L, skipCount(SkipReason.METADATA_NOT_RESOLVED));
                Assertions.assertEquals(baselineNotEmpty, skipCount(SkipReason.PARTITION_NOT_EMPTY));
            }
        });
    }

    @Test
    public void fingerprintPartitionsTooSmallForTwoMinimumSizeTabletsSubmitNothing() {
        // Three partitions share the budget of 4 as 2 tablets each, but each one's share of the bytes,
        // ceil(4 MiB / 3) = 1,398,102 B, fills only one tablet of the 1 MiB minimum split size. Such a
        // refresh already writes through one tablet per partition, so nothing is cut.
        withTestLocalPreSplitMetrics("too_small", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 3)) {
                long baselineTooSmall = skipCount(SkipReason.ROW_ID_SPAN_TOO_SMALL);
                long baselineMultiple = skipCount(SkipReason.MULTIPLE_TEMPORARY_PARTITIONS);

                fixture.runner.preSplitStaticOverwriteTempPartitions();

                fixture.assertNothingSubmitted();
                Assertions.assertEquals(baselineTooSmall + 1L, skipCount(SkipReason.ROW_ID_SPAN_TOO_SMALL),
                        "one reason for the statement, not one per partition");
                Assertions.assertEquals(baselineMultiple, skipCount(SkipReason.MULTIPLE_TEMPORARY_PARTITIONS));
                // Exactly the skip, named as the metric names it: a failure on the way, such as an uninitialized
                // metric, would add a FAILED_FALLBACK here while still submitting nothing.
                Assertions.assertEquals("SKIPPED: ROW_ID_SPAN_TOO_SMALL",
                        preSplitProfileOf(fixture).getInfoString("Outcomes"));
            }
        });
    }

    @Test
    public void partitionCapLimitsTheJobButStillSharesByEveryPartition() {
        // tablet_pre_split_max_partitions_per_load bounds how many partitions one job carves, as on the sampled
        // route. The partition over the cap still receives its half of the refresh's rows, so the kept one is
        // sized for half and cut once -- not three times, as it would be if it were given the whole refresh.
        Config.tablet_pre_split_max_partitions_per_load = 1;
        withTestLocalPreSplitMetrics("partition_cap", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2)) {
                fixture.runner.preSplitStaticOverwriteTempPartitions();

                Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
                Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID), submitted.keySet(),
                        "the job keeps p1_overwrite, first by name, and drops the partition over the cap");
                assertCutOnceAtTheMidpoint(submitted.get(FIRST_BASE_TABLET_ID));
                Assertions.assertEquals(1L, MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED.getValue(),
                        "one count per partition dropped by the cap");
                RuntimeProfile profile = preSplitProfileOf(fixture);
                Assertions.assertEquals(1L, profile.getCounter(PreSplitProfile.TARGET_PARTITIONS).getValue());
                Assertions.assertEquals("SUBMITTED, FINISHED", profile.getInfoString("Outcomes"));
            }
        });
    }

    @Test
    public void partitionCapKeepsThePartitionsThatComeFirstByName() {
        // The scope hands the partitions over in an order that changes from one JVM to the next (it copies its
        // map with Map.copyOf), so the flow sorts them by name before the cap picks the ones it keeps. Ten
        // partitions make that order visible: by name p10_overwrite sorts before p1_overwrite, so it is the one
        // kept, whatever order the scope used. The sizes are lowered so that a tenth of the 4 MiB estimate still
        // splits: the 128 KiB target budgets 32 tablets, 4 per partition, and each partition's ceil(4 MiB / 10)
        // = 419,431 B fills 3 tablets of the 128 KiB minimum, so the kept partition becomes 3 tablets.
        Config.tablet_pre_split_max_partitions_per_load = 1;
        Config.tablet_pre_split_target_size = 128L * 1024;
        Config.tablet_reshard_min_split_size = 128L * 1024;
        withTestLocalPreSplitMetrics("partition_cap_by_name", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 10)) {
                fixture.runner.preSplitStaticOverwriteTempPartitions();

                Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
                Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID + 9), submitted.keySet(),
                        "p10_overwrite is first by name");
                Assertions.assertEquals(3, submitted.get(FIRST_BASE_TABLET_ID + 9).size(),
                        "sized for a tenth of the refresh, not for all of it");
                Assertions.assertEquals(9L, MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED.getValue());
            }
        });
    }

    @Test
    public void firstEligiblePartitionGatesTheJobWhenAnEarlierOneIsIneligible() {
        // The coordinator's gate runs on one partition of the job. The flow walks the partitions by name, so it
        // reaches p1_overwrite first; when that one cannot be resolved, the gate must move to p2_overwrite rather
        // than give up on the job.
        withTestLocalPreSplitMetrics("first_ineligible", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2);
                    MockedStatic<TabletPreSplitCoordinator> coordinator =
                            Mockito.mockStatic(TabletPreSplitCoordinator.class, Mockito.CALLS_REAL_METHODS)) {
                when(fixture.materializedView.getPartition(temporaryPartitionName(0), true)).thenReturn(null);

                fixture.runner.preSplitStaticOverwriteTempPartitions();

                Map<Long, List<TabletRange>> submitted = fixture.submittedRanges();
                Assertions.assertEquals(Set.of(FIRST_BASE_TABLET_ID + 1), submitted.keySet());
                assertCutOnceAtTheMidpoint(submitted.get(FIRST_BASE_TABLET_ID + 1));
                coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                        any(), any(), eq(FIRST_PHYSICAL_PARTITION_ID + 1), any(), any(), any(), anyInt(), any()));
            }
        });
    }

    @Test
    public void everyFingerprintPartitionIneligibleSubmitsNothingAndThrowsNothing() {
        withTestLocalPreSplitMetrics("all_ineligible", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2)) {
                when(fixture.materializedView.getPartition(temporaryPartitionName(0), true)).thenReturn(null);
                when(fixture.materializedView.getPartition(temporaryPartitionName(1), true)).thenReturn(null);
                long baselineUnresolved = skipCount(SkipReason.METADATA_NOT_RESOLVED);

                fixture.runner.preSplitStaticOverwriteTempPartitions();

                fixture.assertNothingSubmitted();
                Assertions.assertEquals(baselineUnresolved + 2L, skipCount(SkipReason.METADATA_NOT_RESOLVED),
                        "each partition records its own reason");
                // The hook swallows a throw and records FAILED_FALLBACK, so an empty outcome list is what
                // shows the flow returned on its own.
                Assertions.assertNull(preSplitProfileOf(fixture).getInfoString("Outcomes"));
            }
        });
    }

    @Test
    public void severalTemporaryPartitionsOfAFingerprintViewAreChargedToTheConfigGateWhenItIsOff() {
        // Unlike an auto-increment view, a fingerprint view's multi-partition refresh is a candidate, so with
        // the flag off the flag is what stops it, and turning it on is what an operator should be told to do.
        Config.enable_tablet_pre_split_for_mv_refresh = false;
        withTestLocalPreSplitMetrics("fingerprint_flag_off", () -> {
            try (Fixture fixture = fingerprintFixture(/*temporaryPartitionCount*/ 2)) {
                long baselineMultiple = skipCount(SkipReason.MULTIPLE_TEMPORARY_PARTITIONS);
                long baselineDisabled = skipCount(SkipReason.DISABLED_BY_CONFIG);

                fixture.runner.preSplitStaticOverwriteTempPartitions();

                fixture.assertNothingSubmitted();
                Assertions.assertEquals(baselineDisabled + 1L, skipCount(SkipReason.DISABLED_BY_CONFIG));
                Assertions.assertEquals(baselineMultiple, skipCount(SkipReason.MULTIPLE_TEMPORARY_PARTITIONS));
            }
        });
    }

    /** The fixture for a view whose query computes its row id as a fingerprint of the group-by keys. */
    private static Fixture fingerprintFixture(int temporaryPartitionCount) {
        Fixture fixture = new Fixture(temporaryPartitionCount,
                new Column(IvmOpUtils.COLUMN_ROW_ID, StringType.DEFAULT_STRING));
        when(fixture.materializedView.getRowIdStrategy()).thenReturn(RowIdStrategy.QUERY_COMPUTED);
        when(fixture.materializedView.getEncodeRowIdVersion()).thenReturn(IvmOpUtils.ENCODE_ROW_ID_VERSION_FINGERPRINT_V2);
        return fixture;
    }

    /** Two tablets meeting at U+, the midpoint of the base64 space. */
    private static void assertCutOnceAtTheMidpoint(List<TabletRange> ranges) {
        Assertions.assertEquals(2, ranges.size(), "one cut carves two tablets");
        Assertions.assertTrue(ranges.get(0).getRange().isMinimum(), "first range must open at -infinity");
        Assertions.assertEquals(stringTuple("U+"), ranges.get(0).getRange().getUpperBound());
        Assertions.assertEquals(stringTuple("U+"), ranges.get(1).getRange().getLowerBound());
        Assertions.assertTrue(ranges.get(1).getRange().isMaximum(), "last range must close at +infinity");
    }

    private static RuntimeProfile preSplitProfileOf(Fixture fixture) {
        RuntimeProfile root = new RuntimeProfile("Load");
        PreSplitProfile.appendTo(root, fixture.preSplitProfile);
        RuntimeProfile profile = root.getChild(PreSplitProfile.PROFILE_NAME);
        Assertions.assertNotNull(profile, "an attempted MV refresh pre-split must attach its profile node");
        return profile;
    }

    /**
     * Runs {@code body} with metric recording switched on and the pre-split histograms and partition-cap
     * counter a derived submit writes replaced by test-local instances. {@code MetricRepo.init()} never runs
     * in this bare test, so those globals are null, and dereferencing one would end the hook in its
     * catch-all: a success path would then fall back without pre-split while its skip-free assertions
     * still held.
     */
    private static void withTestLocalPreSplitMetrics(String name, Runnable body) {
        boolean savedHasInit = MetricRepo.hasInit;
        Histogram savedBoundariesPlanned = MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED;
        Histogram savedPreSubmitWait = MetricRepo.HISTO_TABLET_PRE_SPLIT_PRE_SUBMIT_WAIT_MS;
        Histogram savedPostSubmitWait = MetricRepo.HISTO_TABLET_PRE_SPLIT_POST_SUBMIT_WAIT_MS;
        LongCounterMetric savedPartitionsCapped = MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED;
        MetricRepo.hasInit = true;
        MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED =
                new MetricRegistry().histogram("mv_presplit_" + name + "_boundaries_planned_test");
        MetricRepo.HISTO_TABLET_PRE_SPLIT_PRE_SUBMIT_WAIT_MS =
                new MetricRegistry().histogram("mv_presplit_" + name + "_pre_submit_wait_test");
        MetricRepo.HISTO_TABLET_PRE_SPLIT_POST_SUBMIT_WAIT_MS =
                new MetricRegistry().histogram("mv_presplit_" + name + "_post_submit_wait_test");
        MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED = new LongCounterMetric(
                "mv_presplit_" + name + "_partitions_capped_test", MetricUnit.REQUESTS, "test wire");
        try {
            body.run();
        } finally {
            MetricRepo.hasInit = savedHasInit;
            MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED = savedBoundariesPlanned;
            MetricRepo.HISTO_TABLET_PRE_SPLIT_PRE_SUBMIT_WAIT_MS = savedPreSubmitWait;
            MetricRepo.HISTO_TABLET_PRE_SPLIT_POST_SUBMIT_WAIT_MS = savedPostSubmitWait;
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED = savedPartitionsCapped;
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
                            any(), any(), anyLong(), any(), any(), any(), anyInt(), any()))
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
        private final PreSplitProfile preSplitProfile;
        private final InsertOverwriteJobRunner runner;

        private Fixture(int temporaryPartitionCount) {
            this(temporaryPartitionCount, new Column(IvmOpUtils.COLUMN_ROW_ID, IntegerType.BIGINT));
        }

        private Fixture(int temporaryPartitionCount, Column rowIdColumn) {
            Database database = mock(Database.class);
            when(database.getId()).thenReturn(DB_ID);
            when(database.getFullName()).thenReturn("mv_db");

            // The target shapes the derived tier serves: an incremental view whose single visible
            // index is keyed by the row-id column alone, built here with either a storage-generated
            // row id or a fingerprint computed by the view's query.
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
            when(reshardJob.getJobId()).thenReturn(9301L);
            when(reshardJob.getJobState()).thenReturn(TabletReshardJob.JobState.FINISHED);
            TabletReshardJobMgr reshardJobMgr = mock(TabletReshardJobMgr.class);
            when(reshardJobMgr.getTabletReshardJob(anyLong())).thenReturn(reshardJob);
            when(globalState.getTabletReshardJobMgr()).thenReturn(reshardJobMgr);

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
            when(insertStmt.getTableRef()).thenReturn(mock(TableRef.class));

            // The runner is handed a stubbed context so the load's compute resource is fixed whatever
            // run mode the JVM ended up in; it carries the same session variable as the thread-local
            // context that the coordinator's own session gate reads.
            ConnectContext context = mock(ConnectContext.class);
            when(context.getSessionVariable()).thenReturn(ConnectContext.get().getSessionVariable());
            when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));
            // The thread-local context's profile, not a fresh one: the hook's catch-all records its
            // FAILED_FALLBACK after the attempt scope has closed, where only the thread-local context is
            // found, so sharing the profile is what lets an assertion on the outcomes see a swallowed throw.
            this.preSplitProfile = ConnectContext.get().getOrCreatePreSplitProfile();
            when(context.getOrCreatePreSplitProfile()).thenReturn(preSplitProfile);
            when(context.getPreSplitProfile()).thenReturn(preSplitProfile);

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

        /** Make the temporary partition with this ordinal hold rows, as one that was written to would. */
        private void holdRows(int ordinal) {
            PhysicalPartition physicalPartition =
                    materializedView.getPhysicalPartition(FIRST_PHYSICAL_PARTITION_ID + ordinal);
            when(physicalPartition.getIndex(BASE_INDEX_META_ID).getRowCount()).thenReturn(100L);
        }

        /** Move the temporary partition with this ordinal past its initial version, as a committed load would. */
        private void commitVersion(int ordinal) {
            PhysicalPartition physicalPartition =
                    materializedView.getPhysicalPartition(FIRST_PHYSICAL_PARTITION_ID + ordinal);
            when(physicalPartition.getVisibleVersion()).thenReturn(PhysicalPartition.PARTITION_INIT_VERSION + 1);
        }

        /** Hand out an id for the view so the next span starts above it. */
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
