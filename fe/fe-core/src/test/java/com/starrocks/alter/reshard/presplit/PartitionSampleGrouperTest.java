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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricRepo;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PartitionSampleGrouper} and {@link PartitionValueFormatter}.
 *
 * <p>The grouper has three load-bearing behaviors verified here:
 * <ol>
 *   <li>Type-aware {@link Variant} -> String formatting for the partition
 *       source tuple (the regression rebut for DATE/DATETIME's ISO-instant
 *       {@code getStringValue}).</li>
 *   <li>Per-distinct-value invocation of
 *       {@link AnalyzerUtils#getAddPartitionClauseFromPartitionValues} +
 *       {@link AlterTableClauseAnalyzer#analyze} so the resulting clause
 *       carries a populated {@code resolvedPartitionDescList}.</li>
 *   <li>Catalog reads under a single intensive table READ lock that is
 *       released before {@code group} returns.</li>
 * </ol>
 */
public class PartitionSampleGrouperTest {

    private static final long DB_ID = 100L;
    private static final long TABLE_ID = 200L;
    private static final long BASE_INDEX_META_ID = 300L;
    private static final long TOTAL_FILE_BYTES = 1_000_000L;

    private int savedCap;
    private boolean savedMetricHasInit;
    private LongCounterMetric savedPartitionsCappedCounter;

    @BeforeEach
    public void setUp() {
        savedCap = Config.tablet_pre_split_max_partitions_per_load;
        savedMetricHasInit = MetricRepo.hasInit;
        savedPartitionsCappedCounter = MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED;
        // Wire a fresh counter for tests that read it; MetricRepo.init() is not
        // run inside this unit test, so the static field is otherwise null.
        MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED = new LongCounterMetric(
                "tablet_pre_split_partitions_capped", MetricUnit.REQUESTS, "test wire");
    }

    @AfterEach
    public void tearDown() {
        Config.tablet_pre_split_max_partitions_per_load = savedCap;
        MetricRepo.hasInit = savedMetricHasInit;
        MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED = savedPartitionsCappedCounter;
    }

    // ---------- PartitionValueFormatter ----------

    @Test
    public void formatsDateVariantWithSqlFormat() {
        // Formatter must emit the day-granularity SQL form "yyyy-MM-dd" the analyzer accepts.
        Column col = new Column("d", DateType.DATE);
        PartitionValueFormatter formatter = new PartitionValueFormatter(col);
        Variant cell = Variant.of(DateType.DATE, "2026-05-26");
        assertEquals("2026-05-26", formatter.format(cell));
    }

    @Test
    public void formatsDatetimeVariantWithSqlFormat() {
        Column col = new Column("dt", DateType.DATETIME);
        PartitionValueFormatter formatter = new PartitionValueFormatter(col);
        Variant cell = Variant.of(DateType.DATETIME, "2026-05-26 12:34:56");
        assertEquals("2026-05-26 12:34:56", formatter.format(cell));
    }

    @Test
    public void formatsIntVariantAsPlainNumber() {
        Column col = new Column("n", IntegerType.INT);
        PartitionValueFormatter formatter = new PartitionValueFormatter(col);
        Variant cell = Variant.of(IntegerType.INT, "42");
        assertEquals("42", formatter.format(cell));
    }

    @Test
    public void formatsStringVariantVerbatim() {
        Column col = new Column("s", StringType.DEFAULT_STRING);
        PartitionValueFormatter formatter = new PartitionValueFormatter(col);
        Variant cell = Variant.of(StringType.DEFAULT_STRING, "my-tag");
        assertEquals("my-tag", formatter.format(cell));
    }

    @Test
    public void formatsNullCellReturnsNull() {
        Column col = new Column("n", IntegerType.INT);
        PartitionValueFormatter formatter = new PartitionValueFormatter(col);
        assertNull(formatter.format(null));
        assertNull(formatter.format(Variant.nullVariant(IntegerType.INT)));
    }

    @Test
    public void isSupportedColumnTypeAcceptsDateAndIntegers() {
        assertTrue(PartitionValueFormatter.isSupportedColumnType(new Column("d", DateType.DATE)));
        assertTrue(PartitionValueFormatter.isSupportedColumnType(new Column("dt", DateType.DATETIME)));
        assertTrue(PartitionValueFormatter.isSupportedColumnType(new Column("i", IntegerType.INT)));
        assertTrue(PartitionValueFormatter.isSupportedColumnType(new Column("b", IntegerType.BIGINT)));
        assertTrue(PartitionValueFormatter.isSupportedColumnType(new Column("s", StringType.DEFAULT_STRING)));
    }

    @Test
    public void isSupportedColumnTypeRejectsArray() {
        Column arrayCol = new Column("a", new ArrayType(IntegerType.INT));
        assertFalse(PartitionValueFormatter.isSupportedColumnType(arrayCol));
    }

    // ---------- PartitionSampleGrouper.group ----------

    @Test
    public void rejectsUnsupportedPartitionColumnType() {
        // STRUCT / ARRAY partition columns aren't reachable in production today,
        // but the grouper must refuse them defensively + bump the bvar.
        MetricRepo.hasInit = true;
        long baseline = eligibilitySkipCount(SkipReason.UNSUPPORTED_PARTITION_COLUMN_TYPE);

        Column unsupportedCol = new Column("a", new ArrayType(IntegerType.INT));
        OlapTable table = stubTable(List.of(unsupportedCol));
        SampleSet samples = buildSampleSet(List.of(
                rowWithPartitionCells(Variant.of(IntegerType.INT, "1"))));

        try (MockedConstruction<Locker> ignored = Mockito.mockConstruction(Locker.class)) {
            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);
            assertTrue(out.isEmpty());
        }

        assertEquals(baseline + 1L, eligibilitySkipCount(SkipReason.UNSUPPORTED_PARTITION_COLUMN_TYPE),
                "unsupported partition column must bump the bvar");
    }

    @Test
    public void groupsByDistinctPartitionValue() {
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));

        List<Tuple> partitionTuples = new ArrayList<>();
        // 3 distinct dates with 5/3/2 rows each
        for (int i = 0; i < 5; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-26")));
        }
        for (int i = 0; i < 3; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-27")));
        }
        for (int i = 0; i < 2; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-28")));
        }
        SampleSet samples = sampleSetOf(partitionTuples);

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-27", "p20260527");
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-28", "p20260528");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(3, out.size());
            assertEquals(5, out.get(0).samples().size());
            assertEquals(3, out.get(1).samples().size());
            assertEquals(2, out.get(2).samples().size());
            assertEquals("p20260526", out.get(0).partitionName());
            assertEquals(List.of("2026-05-26"), out.get(0).partitionValues());
        }
    }

    @Test
    public void singlePartitionValueProducesOneGroup() {
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        List<Tuple> partitionTuples = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-26")));
        }
        SampleSet samples = sampleSetOf(partitionTuples);

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(1, out.size());
            assertEquals(10, out.get(0).samples().size());
            // Sample of 10 of 10 rows -> ratio 1.0 -> all of totalFileBytes.
            assertEquals(TOTAL_FILE_BYTES, out.get(0).estimatedBytes());
        }
    }

    @Test
    public void analyzerInvokedOncePerDistinctPartitionValue() {
        // 100 rows but only 2 distinct values -> analyzer called exactly twice.
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));

        List<Tuple> partitionTuples = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-26")));
        }
        for (int i = 0; i < 40; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-27")));
        }
        SampleSet samples = sampleSetOf(partitionTuples);

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-27", "p20260527");

            PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            analyzerUtils.verify(() -> AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                    eq(table), eq(List.of(List.of("2026-05-26"))), eq(false), any()), times(1));
            analyzerUtils.verify(() -> AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                    eq(table), eq(List.of(List.of("2026-05-27"))), eq(false), any()), times(1));
        }
    }

    @Test
    public void analyzedClauseCarriesResolvedPartitionDescList() {
        // The coordinator calls clause.getResolvedPartitionDescList(); confirm it's non-empty.
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        SampleSet samples = sampleSetOf(List.of(tuple(Variant.of(DateType.DATE, "2026-05-26"))));

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(1, out.size());
            AddPartitionClause clause = out.get(0).analyzedClause();
            assertNotNull(clause);
            assertNotNull(clause.getResolvedPartitionDescList());
            assertFalse(clause.getResolvedPartitionDescList().isEmpty());
            assertEquals("p20260526", clause.getResolvedPartitionDescList().get(0).getPartitionName());
        }
    }

    @Test
    public void marksExistingPartitionWhenCatalogHasIt() {
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        // Catalog has p20260526 with 1 tablet, 0 rows -> eligible existing partition
        installPartitionWithTablets(table, "p20260526", /*physicalId*/ 9001L,
                /*tabletIds*/ List.of(8001L), /*rowCount*/ 0L);

        SampleSet samples = sampleSetOf(List.of(tuple(Variant.of(DateType.DATE, "2026-05-26"))));

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(1, out.size());
            PartitionSamples ps = out.get(0);
            assertTrue(ps.existsInCatalog());
            assertEquals(9001L, ps.partitionIdIfExists());
            assertEquals(8001L, ps.oldTabletIdIfExists());
            assertNull(ps.analyzedClause());
        }
    }

    @Test
    public void marksMissingPartitionForPreCreate() {
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        // No partition installed -> getPartition returns null -> grouper marks as pre-create.

        SampleSet samples = sampleSetOf(List.of(tuple(Variant.of(DateType.DATE, "2026-05-26"))));

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(1, out.size());
            PartitionSamples ps = out.get(0);
            assertFalse(ps.existsInCatalog());
            assertNotNull(ps.analyzedClause());
        }
    }

    @Test
    public void dropsNonEmptyExistingPartitions() {
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        // existing partition has rowCount > 0 -> drop
        installPartitionWithTablets(table, "p20260526", 9001L, List.of(8001L), /*rowCount*/ 1L);

        SampleSet samples = sampleSetOf(List.of(tuple(Variant.of(DateType.DATE, "2026-05-26"))));

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertTrue(out.isEmpty(), "non-empty existing partition must be dropped");
        }
    }

    @Test
    public void dropsMultiTabletExistingPartitions() {
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        installPartitionWithTablets(table, "p20260526", 9001L,
                List.of(8001L, 8002L), /*rowCount*/ 0L);

        MetricRepo.hasInit = true;
        long ineligibleBaseline = eligibilitySkipCount(SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE);

        SampleSet samples = sampleSetOf(List.of(tuple(Variant.of(DateType.DATE, "2026-05-26"))));

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertTrue(out.isEmpty(), "multi-tablet existing partition must be dropped");
            assertEquals(ineligibleBaseline + 1L,
                    eligibilitySkipCount(SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE),
                    "multi-tablet existing partition must bump PARTITION_NOT_ELIGIBLE_POST_CREATE");
        }
    }

    @Test
    public void dropsAndBumpsStaleCatalogOnNullPhysicalPartition() {
        // Catalog race: getPartition returned a Partition, but its physical
        // partition vanished by the time we read it. Grouper must drop the
        // group AND bump SkipReason.STALE_CATALOG_STATE.
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        Partition partition = mock(Partition.class);
        when(partition.getDefaultPhysicalPartition()).thenReturn(null);
        when(table.getPartition("p20260526")).thenReturn(partition);

        MetricRepo.hasInit = true;
        long baseline = eligibilitySkipCount(SkipReason.STALE_CATALOG_STATE);

        SampleSet samples = sampleSetOf(List.of(tuple(Variant.of(DateType.DATE, "2026-05-26"))));
        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertTrue(out.isEmpty(), "stale catalog must drop the group");
            assertEquals(baseline + 1L, eligibilitySkipCount(SkipReason.STALE_CATALOG_STATE));
        }
    }

    @Test
    public void dropsAndBumpsStaleCatalogOnNullBaseIndex() {
        // Catalog race: physical partition exists but its base index was dropped.
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        Partition partition = mock(Partition.class);
        PhysicalPartition physicalPartition = mock(PhysicalPartition.class);
        when(physicalPartition.getIndex(BASE_INDEX_META_ID)).thenReturn(null);
        when(partition.getDefaultPhysicalPartition()).thenReturn(physicalPartition);
        when(table.getPartition("p20260526")).thenReturn(partition);

        MetricRepo.hasInit = true;
        long baseline = eligibilitySkipCount(SkipReason.STALE_CATALOG_STATE);

        SampleSet samples = sampleSetOf(List.of(tuple(Variant.of(DateType.DATE, "2026-05-26"))));
        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertTrue(out.isEmpty(), "stale catalog must drop the group");
            assertEquals(baseline + 1L, eligibilitySkipCount(SkipReason.STALE_CATALOG_STATE));
        }
    }

    @Test
    public void resultIsSortedByHeaviestSampleCountWithoutCap() {
        // Cap not exceeded -> ordering must still be heaviest-first
        // (insertion order would be 2/5/3, but expected output is 5/3/2).
        Config.tablet_pre_split_max_partitions_per_load = 0; // disabled cap
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));

        List<Tuple> partitionTuples = new ArrayList<>();
        // Insert lightest first to ensure sort actually reorders.
        for (int i = 0; i < 2; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-28")));
        }
        for (int i = 0; i < 5; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-26")));
        }
        for (int i = 0; i < 3; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-27")));
        }
        SampleSet samples = sampleSetOf(partitionTuples);

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-27", "p20260527");
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-28", "p20260528");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(3, out.size());
            assertEquals(5, out.get(0).samples().size());
            assertEquals(3, out.get(1).samples().size());
            assertEquals(2, out.get(2).samples().size());
        }
    }

    @Test
    public void analyzerFailureSkipsThatGroupAndBumpsBvar() {
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        MetricRepo.hasInit = true;
        long baseline = eligibilitySkipCount(SkipReason.INVALID_PARTITION_VALUE);

        // Row 1's analyzer throws; row 2's succeeds -> 1 result, baseline+1 INVALID counter.
        SampleSet samples = sampleSetOf(List.of(
                tuple(Variant.of(DateType.DATE, "2026-05-26")),
                tuple(Variant.of(DateType.DATE, "2026-05-27"))));

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            // 2026-05-26 -> analyzer throws
            analyzerUtils.when(() -> AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                    eq(table), eq(List.of(List.of("2026-05-26"))), anyBoolean(), any()))
                    .thenThrow(new RuntimeException("synthetic analyzer failure"));
            // 2026-05-27 -> succeeds
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-27", "p20260527");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(1, out.size());
            assertEquals("p20260527", out.get(0).partitionName());
            assertEquals(baseline + 1L, eligibilitySkipCount(SkipReason.INVALID_PARTITION_VALUE));
        }
    }

    @Test
    public void emptyInputReturnsEmpty() {
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        SampleSet samples = sampleSetOf(Collections.emptyList());

        try (MockedConstruction<Locker> ignored = Mockito.mockConstruction(Locker.class)) {
            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);
            assertTrue(out.isEmpty());
        }
    }

    @Test
    public void unpartitionedSampleSetShortCircuits() {
        // SampleSet with no partitionSourceTuples (sampler ran without partition projection).
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        Tuple sortOnly = tuple(Variant.of(IntegerType.INT, "1"));
        SampleSet samples = new SampleSet(List.of(sortOnly), Estimates.ZERO);

        List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);
        assertTrue(out.isEmpty());
    }

    @Test
    public void appliesMaxPartitionsCap() {
        Config.tablet_pre_split_max_partitions_per_load = 2;
        MetricRepo.hasInit = true;
        long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED.getValue();

        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        // 3 distinct values with samples 5/3/2 -> cap=2 keeps the top 2 (5 and 3).
        List<Tuple> partitionTuples = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-26")));
        }
        for (int i = 0; i < 3; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-27")));
        }
        for (int i = 0; i < 2; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-28")));
        }
        SampleSet samples = sampleSetOf(partitionTuples);

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-27", "p20260527");
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-28", "p20260528");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(2, out.size());
            assertEquals(5, out.get(0).samples().size());
            assertEquals(3, out.get(1).samples().size());
            assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED.getValue().longValue());
        }
    }

    @Test
    public void capDisabledWhenConfigNonPositive() {
        Config.tablet_pre_split_max_partitions_per_load = 0;
        MetricRepo.hasInit = true;
        long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED.getValue();

        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        List<Tuple> partitionTuples = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-26")));
        }
        partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-27")));
        SampleSet samples = sampleSetOf(partitionTuples);

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-27", "p20260527");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(2, out.size());
            assertEquals(baseline,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED.getValue().longValue());
        }
    }

    @Test
    public void acquiresIntensiveReadLockAroundCatalogReads() {
        // Verify lockTablesWithIntensiveDbLock(...) is called for READ on this dbId+tableId
        // and unLockTablesWithIntensiveDbLock is called before group() returns.
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        SampleSet samples = sampleSetOf(List.of(tuple(Variant.of(DateType.DATE, "2026-05-26"))));

        AtomicInteger lockCalls = new AtomicInteger();
        AtomicInteger unlockCalls = new AtomicInteger();

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class,
                        (mockLocker, ctx) -> {
                            Mockito.doAnswer(inv -> {
                                Long db = inv.getArgument(0);
                                LockType lt = inv.getArgument(2);
                                if (db == DB_ID && lt == LockType.READ) {
                                    lockCalls.incrementAndGet();
                                }
                                return null;
                            }).when(mockLocker).lockTablesWithIntensiveDbLock(
                                    any(Long.class), any(), any(LockType.class));
                            Mockito.doAnswer(inv -> {
                                Long db = inv.getArgument(0);
                                LockType lt = inv.getArgument(2);
                                if (db == DB_ID && lt == LockType.READ) {
                                    unlockCalls.incrementAndGet();
                                }
                                return null;
                            }).when(mockLocker).unLockTablesWithIntensiveDbLock(
                                    any(Long.class), any(), any(LockType.class));
                        })) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");

            PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);
        }

        assertEquals(1, lockCalls.get(), "exactly one intensive READ lock must be acquired");
        assertEquals(1, unlockCalls.get(), "intensive READ lock must be released before return");
    }

    @Test
    public void mergesRawValuesResolvingToSamePartition() {
        // W1 rebut: two DIFFERENT raw partition-source values resolve to the SAME
        // partition (the expression-partitioning case, e.g.
        // PARTITION BY date_trunc('day', ts) folding two timestamps into one day).
        // The grouper must merge them into ONE PartitionSamples carrying every
        // row, not two entries sharing one oldTabletId (which the coordinator's
        // oldTabletIdToRanges.put would silently overwrite).
        Column dtCol = new Column("dt", DateType.DATETIME);
        OlapTable table = stubTable(List.of(dtCol));

        List<Tuple> partitionTuples = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATETIME, "2026-05-26 01:00:00")));
        }
        for (int i = 0; i < 20; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATETIME, "2026-05-26 23:00:00")));
        }
        SampleSet samples = sampleSetOf(partitionTuples);

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            // Both raw values resolve to the SAME partition name regardless of input.
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26 01:00:00", "p20260526");
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26 23:00:00", "p20260526");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(1, out.size(), "two raw values resolving to one partition must merge into one entry");
            assertEquals("p20260526", out.get(0).partitionName());
            assertEquals(50, out.get(0).samples().size(),
                    "merged group must carry ALL 50 rows (30 + 20), not one raw value's subset");
        }
    }

    @Test
    public void analyzerRunsOutsideReadLock() {
        // W2 rebut: the analyzer (AnalyzerUtils.getAddPartitionClauseFromPartitionValues)
        // must run BEFORE the intensive READ lock is acquired, matching the runtime
        // auto-create path. Capture whether the lock was held when each analyze call
        // ran, and verify the lock is acquired exactly once.
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));

        List<Tuple> partitionTuples = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-26")));
        }
        for (int i = 0; i < 3; i++) {
            partitionTuples.add(tuple(Variant.of(DateType.DATE, "2026-05-27")));
        }
        SampleSet samples = sampleSetOf(partitionTuples);

        AtomicBoolean lockHeld = new AtomicBoolean(false);
        AtomicInteger lockCalls = new AtomicInteger();
        AtomicInteger analyzeCallsWhileLocked = new AtomicInteger();
        AtomicInteger analyzeCalls = new AtomicInteger();

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class,
                        (mockLocker, ctx) -> {
                            Mockito.doAnswer(inv -> {
                                lockCalls.incrementAndGet();
                                lockHeld.set(true);
                                return null;
                            }).when(mockLocker).lockTablesWithIntensiveDbLock(
                                    any(Long.class), any(), any(LockType.class));
                            Mockito.doAnswer(inv -> {
                                lockHeld.set(false);
                                return null;
                            }).when(mockLocker).unLockTablesWithIntensiveDbLock(
                                    any(Long.class), any(), any(LockType.class));
                        })) {
            analyzerUtils.when(() -> AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                            eq(table), any(), anyBoolean(), any()))
                    .thenAnswer(inv -> {
                        analyzeCalls.incrementAndGet();
                        if (lockHeld.get()) {
                            analyzeCallsWhileLocked.incrementAndGet();
                        }
                        List<List<String>> values = inv.getArgument(1);
                        String formatted = values.get(0).get(0);
                        String partitionName = "p" + formatted.replace("-", "");
                        AddPartitionClause clause = new AddPartitionClause(null, null, null, false);
                        PartitionDesc desc = mock(PartitionDesc.class);
                        when(desc.getPartitionName()).thenReturn(partitionName);
                        clause.setResolvedPartitionDescList(List.of(desc));
                        return clause;
                    });

            PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);
        }

        assertEquals(2, analyzeCalls.get(), "analyze must run for both distinct raw values");
        assertEquals(0, analyzeCallsWhileLocked.get(),
                "analyzer must NOT run while the intensive READ lock is held");
        assertEquals(1, lockCalls.get(), "intensive READ lock must be acquired exactly once");
    }

    @Test
    public void capAppliesBeforeCatalogLookup() {
        // W2 rebut: with cap=2 and 5 distinct partitions, only 2 partitions may
        // reach the catalog-lookup phase (table.getPartition <= 2 invocations) and
        // recordPartitionCapped must fire 3 times for the dropped groups.
        Config.tablet_pre_split_max_partitions_per_load = 2;
        MetricRepo.hasInit = true;
        long cappedBaseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED.getValue();

        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        AtomicInteger getPartitionCalls = new AtomicInteger();
        when(table.getPartition(any(String.class))).thenAnswer(inv -> {
            getPartitionCalls.incrementAndGet();
            return null;
        });

        // 5 distinct values with descending sample counts so the cap keeps the
        // two heaviest deterministically.
        String[] dates = {"2026-05-21", "2026-05-22", "2026-05-23", "2026-05-24", "2026-05-25"};
        int[] counts = {9, 7, 5, 3, 1};
        List<Tuple> partitionTuples = new ArrayList<>();
        for (int g = 0; g < dates.length; g++) {
            for (int i = 0; i < counts[g]; i++) {
                partitionTuples.add(tuple(Variant.of(DateType.DATE, dates[g])));
            }
        }
        SampleSet samples = sampleSetOf(partitionTuples);

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            for (String date : dates) {
                stubAnalyzerToReturnClauseFor(analyzerUtils, table, date, "p" + date.replace("-", ""));
            }

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertEquals(2, out.size(), "cap=2 must keep exactly the two heaviest groups");
            assertTrue(getPartitionCalls.get() <= 2,
                    "only capped (<= 2) groups may reach the catalog-lookup phase, got "
                            + getPartitionCalls.get());
            assertEquals(cappedBaseline + 3L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED.getValue().longValue(),
                    "3 dropped groups must each bump recordPartitionCapped");
        }
    }

    @Test
    public void recordsPartitionNotEligiblePostCreateOnNonEmptyExisting() {
        // SUGGESTION fix: a non-empty existing partition is dropped, and the drop
        // now records PARTITION_NOT_ELIGIBLE_POST_CREATE at the drop site instead
        // of being silently folded into a trailing GROUPER_EMPTY.
        Column dateCol = new Column("d", DateType.DATE);
        OlapTable table = stubTable(List.of(dateCol));
        installPartitionWithTablets(table, "p20260526", 9001L, List.of(8001L), /*rowCount*/ 5L);

        MetricRepo.hasInit = true;
        long ineligibleBaseline = eligibilitySkipCount(SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE);
        long emptyBaseline = eligibilitySkipCount(SkipReason.GROUPER_EMPTY);

        SampleSet samples = sampleSetOf(List.of(tuple(Variant.of(DateType.DATE, "2026-05-26"))));

        try (MockedStatic<AnalyzerUtils> analyzerUtils = Mockito.mockStatic(AnalyzerUtils.class);
                MockedConstruction<AlterTableClauseAnalyzer> alterCtor =
                        Mockito.mockConstruction(AlterTableClauseAnalyzer.class, (mockObj, ctx) -> { });
                MockedConstruction<Locker> lockerCtor = Mockito.mockConstruction(Locker.class)) {
            stubAnalyzerToReturnClauseFor(analyzerUtils, table, "2026-05-26", "p20260526");

            List<PartitionSamples> out = PartitionSampleGrouper.group(samples, table, null, DB_ID, TOTAL_FILE_BYTES);

            assertTrue(out.isEmpty(), "non-empty existing partition must be dropped");
            assertEquals(ineligibleBaseline + 1L,
                    eligibilitySkipCount(SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE),
                    "non-empty existing partition must bump PARTITION_NOT_ELIGIBLE_POST_CREATE");
            assertEquals(emptyBaseline, eligibilitySkipCount(SkipReason.GROUPER_EMPTY),
                    "an ineligible-but-present group must NOT bump GROUPER_EMPTY");
        }
    }

    // ---------- helpers ----------

    private static long eligibilitySkipCount(SkipReason reason) {
        return MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                .getMetric(reason.name().toLowerCase()).getValue();
    }

    /**
     * Build a stub {@link OlapTable} that returns the given partition columns
     * via {@code getPartitionInfo().getPartitionColumns(getIdToColumn())}.
     */
    private static OlapTable stubTable(List<Column> partitionColumns) {
        OlapTable table = mock(OlapTable.class);
        when(table.getId()).thenReturn(TABLE_ID);
        when(table.getName()).thenReturn("stub_table");
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(table.getIdToColumn()).thenReturn(Collections.emptyMap());
        PartitionInfo info = mock(PartitionInfo.class);
        when(info.getPartitionColumns(any())).thenReturn(partitionColumns);
        when(table.getPartitionInfo()).thenReturn(info);
        return table;
    }

    /**
     * Stub {@code AnalyzerUtils.getAddPartitionClauseFromPartitionValues} so
     * that the call with {@code [[formattedValue]]} returns a pre-populated
     * {@link AddPartitionClause} whose resolved partition desc reports
     * {@code partitionName}.
     */
    private static void stubAnalyzerToReturnClauseFor(MockedStatic<AnalyzerUtils> analyzerUtils,
                                                      OlapTable table,
                                                      String formattedValue,
                                                      String partitionName) {
        analyzerUtils.when(() -> AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                        eq(table), eq(List.of(List.of(formattedValue))), anyBoolean(), any()))
                .thenAnswer(inv -> {
                    AddPartitionClause clause = new AddPartitionClause(null, null, null, false);
                    PartitionDesc desc = mock(PartitionDesc.class);
                    when(desc.getPartitionName()).thenReturn(partitionName);
                    clause.setResolvedPartitionDescList(List.of(desc));
                    return clause;
                });
    }

    /**
     * Install an existing partition on {@code table} that reports the given
     * physical partition id, tablet ids, and row count for the table's base
     * index. Used by the existing-partition eligibility tests.
     */
    private static void installPartitionWithTablets(OlapTable table, String partitionName,
                                                    long physicalPartitionId,
                                                    List<Long> tabletIds, long rowCount) {
        Partition partition = mock(Partition.class);
        PhysicalPartition physicalPartition = mock(PhysicalPartition.class);
        when(physicalPartition.getId()).thenReturn(physicalPartitionId);
        MaterializedIndex baseIndex = mock(MaterializedIndex.class);
        List<Tablet> tablets = new ArrayList<>();
        for (Long tabletId : tabletIds) {
            Tablet tablet = mock(Tablet.class);
            when(tablet.getId()).thenReturn(tabletId);
            tablets.add(tablet);
        }
        when(baseIndex.getTablets()).thenReturn(tablets);
        when(baseIndex.getRowCount()).thenReturn(rowCount);
        when(physicalPartition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);
        when(partition.getDefaultPhysicalPartition()).thenReturn(physicalPartition);
        when(table.getPartition(partitionName)).thenReturn(partition);
    }

    private static Tuple tuple(Variant... values) {
        return new Tuple(List.of(values));
    }

    private static Tuple rowWithPartitionCells(Variant... cells) {
        return new Tuple(List.of(cells));
    }

    /**
     * Build a {@link SampleSet} with the same sort-key tuple for every row
     * (the sort key is not load-bearing for grouper tests) and the given
     * partition source tuples.
     */
    private static SampleSet sampleSetOf(List<Tuple> partitionSourceTuples) {
        List<Tuple> sortTuples = new ArrayList<>(partitionSourceTuples.size());
        for (int i = 0; i < partitionSourceTuples.size(); i++) {
            sortTuples.add(tuple(Variant.of(IntegerType.INT, String.valueOf(i))));
        }
        return new SampleSet(sortTuples, partitionSourceTuples, Estimates.ZERO);
    }

    /**
     * Build a {@link SampleSet} for one explicit partition tuple. Convenience
     * wrapper for the simpler one-row tests.
     */
    private static SampleSet buildSampleSet(List<Tuple> partitionTuples) {
        return sampleSetOf(partitionTuples);
    }
}
