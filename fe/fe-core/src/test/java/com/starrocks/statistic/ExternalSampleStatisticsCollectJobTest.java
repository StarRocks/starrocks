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

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExternalSampleStatisticsCollectJobTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDefaultCtx();
    }

    private static ExternalSampleStatisticsCollectJob newJob(List<String> partitionNames, List<String> columnNames,
                                                               List<com.starrocks.type.Type> columnTypes,
                                                               int allPartitionSize) {
        Database database = new Database(1, "test_db");
        Table table = HiveTable.builder().setTableName("test_table").build();
        return new ExternalSampleStatisticsCollectJob("test_catalog", database, table, partitionNames, columnNames,
                columnTypes, StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.ONCE,
                Maps.newHashMap(), allPartitionSize);
    }

    private void clearAnalyzeStatusMap() {
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeStatusMap().clear();
    }

    @Test
    public void testLowCardinalityColumnConverges() {
        // A boolean-like column: NDV settles at 2 after the first round and never moves again.
        Map<String, Long> prev = Map.of("is_active", 2L);
        Map<String, Long> curr = Map.of("is_active", 2L);
        assertTrue(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testHighCardinalityColumnKeepsGrowingDoesNotConverge() {
        // A near-unique id column: doubling the sample roughly doubles the observed NDV, so the
        // round loop must keep going instead of stopping early.
        Map<String, Long> prev = Map.of("id", 2_000_000L);
        Map<String, Long> curr = Map.of("id", 3_900_000L);
        assertFalse(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testWithinThresholdConverges() {
        Map<String, Long> prev = Map.of("c1", 1000L);
        Map<String, Long> curr = Map.of("c1", 1020L);
        assertTrue(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testSingleNonConvergingColumnBlocksWholeRound() {
        // Even if most columns have settled, one still-changing column (typically the
        // highest-cardinality one) must keep the round loop from stopping early.
        Map<String, Long> prev = Map.of("stable", 5L, "growing", 1000L);
        Map<String, Long> curr = Map.of("stable", 5L, "growing", 2000L);
        assertFalse(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testMissingPreviousColumnTreatedAsNotConverged() {
        Map<String, Long> prev = Map.of();
        Map<String, Long> curr = Map.of("new_column", 10L);
        assertFalse(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testZeroCardinalityBothRoundsConverges() {
        // An all-null column: cardinality stays 0 across rounds, must not divide by zero.
        Map<String, Long> prev = Map.of("all_null", 0L);
        Map<String, Long> curr = Map.of("all_null", 0L);
        assertTrue(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testGetSampledPartitionsHashValueAndAllPartitionSizeDefaultToLegacyPartitionSelection() {
        // Before Iceberg file sampling has run (or for a non-Iceberg SAMPLE job that never sets
        // usedFileSampling), these must report the factory's real partition pre-selection, since
        // that's what Hive-style partition sampling actually depends on.
        ExternalSampleStatisticsCollectJob job = newJob(Lists.newArrayList("p1", "p2"),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 5);
        assertEquals(5, job.getAllPartitionSize());
        assertEquals(2, job.getSampledPartitionsHashValue().size());
    }

    @Test
    public void testGetSampledPartitionsHashValueAndAllPartitionSizeAreNeutralAfterFileSampling() {
        // Once Iceberg's whole-table file sampling has run, row_count/data_size/null_count are
        // already rescaled to a full-table estimate during collection (see
        // collectSinglePassStatisticSync); StatisticsUtils#estimateColumnStatistics must not apply
        // its own allPartitionSize / sampledPartitionsHashValue().size() rescale on top of that, or
        // a partitioned Iceberg table's row count gets inflated a second time. Reporting a fixed
        // 1-of-1 split keeps that downstream rescale a no-op.
        ExternalSampleStatisticsCollectJob job = newJob(Lists.newArrayList("p1", "p2"),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 5);
        job.usedFileSampling = true;
        assertEquals(1, job.getAllPartitionSize());
        Set<Long> sampled = job.getSampledPartitionsHashValue();
        assertEquals(1, sampled.size());

        // A second, independently-constructed job must report the exact same neutral hash value so
        // StatisticExecutor's cross-run union (sampledPartitions.addAll(...)) never grows past size
        // 1 -- growing past 1 would silently reintroduce the double-rescale this is meant to avoid.
        ExternalSampleStatisticsCollectJob otherJob = newJob(Lists.newArrayList("p3"),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 9);
        otherJob.usedFileSampling = true;
        assertEquals(sampled, otherJob.getSampledPartitionsHashValue());
    }

    @Test
    public void testBuildSinglePassSQLEmitsSixAggregatesPerStatisticableColumn() {
        ExternalSampleStatisticsCollectJob job = newJob(List.of(),
                Lists.newArrayList("c1", "c2"),
                Lists.newArrayList(IntegerType.INT, IntegerType.BIGINT), 1);
        String sql = job.buildSinglePassSQL(List.of(0, 1));

        assertTrue(sql.startsWith("SELECT CAST(COUNT(1) AS BIGINT)"), sql);
        // One data_size + hll + null_count + max + min + cardinality aggregate per column, in order.
        assertTrue(sql.contains("hll_serialize(IFNULL(hll_raw(`c1`), hll_empty()))"), sql);
        assertTrue(sql.contains("COUNT(1) - COUNT(`c1`)"), sql);
        assertTrue(sql.contains("hll_cardinality(IFNULL(hll_raw(`c1`), hll_empty()))"), sql);
        assertTrue(sql.contains("hll_serialize(IFNULL(hll_raw(`c2`), hll_empty()))"), sql);
        assertTrue(sql.endsWith("FROM `test_catalog`.`test_db`.`test_table`"), sql);
    }

    @Test
    public void testBuildSinglePassSQLUsesConstantPlaceholdersForNonStatisticableColumns() {
        // JSON columns can't be statistic'd (Type#canStatistic()); the wide row must still emit the
        // same six aggregate slots (via constant placeholders) so the caller's fixed base-offset
        // decoding (base, base+1, ..., base+5) lines up regardless of column type.
        ExternalSampleStatisticsCollectJob job = newJob(List.of(),
                Lists.newArrayList("j1"), Lists.newArrayList(JsonType.JSON), 1);
        String sql = job.buildSinglePassSQL(List.of(0));

        assertFalse(sql.contains("hll_raw(`j1`)"), sql);
        assertTrue(sql.contains("hex(hll_serialize(hll_empty()))"), sql);
        // COUNT(1) + 6 placeholder aggregates => 7 SELECT items => 6 separating commas.
        assertEquals(6, sql.split(",").length - 1);
    }

    @Test
    public void testFindPersistedRoundNoPriorRuns() {
        clearAnalyzeStatusMap();
        ExternalSampleStatisticsCollectJob job = newJob(List.of(),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 1);
        assertEquals(0, job.findPersistedRound());
    }

    @Test
    public void testFindPersistedRoundSkipsNativeStatus() {
        clearAnalyzeStatusMap();
        ExternalAnalyzeStatus nativeStatus = new ExternalAnalyzeStatus(1, "test_catalog", "test_db", "test_table",
                "uuid", Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        nativeStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
        nativeStatus.setEndTime(LocalDateTime.now());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(nativeStatus);

        ExternalSampleStatisticsCollectJob job = newJob(List.of(),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 1);
        assertEquals(0, job.findPersistedRound());
    }

    @Test
    public void testFindPersistedRoundSkipsNonSampleType() {
        clearAnalyzeStatusMap();
        ExternalAnalyzeStatus fullStatus = new ExternalAnalyzeStatus(1, "test_catalog", "test_db", "test_table",
                "uuid", Lists.newArrayList("c1"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        fullStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
        fullStatus.setEndTime(LocalDateTime.now());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(fullStatus);

        ExternalSampleStatisticsCollectJob job = newJob(List.of(),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 1);
        assertEquals(0, job.findPersistedRound());
    }

    @Test
    public void testFindPersistedRoundSkipsNonFinishedStatus() {
        clearAnalyzeStatusMap();
        ExternalAnalyzeStatus runningStatus = new ExternalAnalyzeStatus(1, "test_catalog", "test_db", "test_table",
                "uuid", Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        runningStatus.setStatus(StatsConstants.ScheduleStatus.RUNNING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(runningStatus);

        ExternalSampleStatisticsCollectJob job = newJob(List.of(),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 1);
        assertEquals(0, job.findPersistedRound());
    }

    @Test
    public void testFindPersistedRoundReturnsMostRecentRound() {
        clearAnalyzeStatusMap();
        Map<String, String> olderProps = Maps.newHashMap();
        olderProps.put("external_sample_round", "2");
        ExternalAnalyzeStatus older = new ExternalAnalyzeStatus(1, "test_catalog", "test_db", "test_table",
                "uuid", Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                StatsConstants.ScheduleType.ONCE, olderProps, LocalDateTime.now().minusDays(1));
        older.setStatus(StatsConstants.ScheduleStatus.FINISH);
        older.setEndTime(LocalDateTime.now().minusHours(1));

        Map<String, String> newerProps = Maps.newHashMap();
        newerProps.put("external_sample_round", "3");
        ExternalAnalyzeStatus newer = new ExternalAnalyzeStatus(2, "test_catalog", "test_db", "test_table",
                "uuid", Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                StatsConstants.ScheduleType.ONCE, newerProps, LocalDateTime.now());
        newer.setStatus(StatsConstants.ScheduleStatus.FINISH);
        newer.setEndTime(LocalDateTime.now());

        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(older);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(newer);

        ExternalSampleStatisticsCollectJob job = newJob(List.of(),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 1);
        assertEquals(3, job.findPersistedRound());
    }

    @Test
    public void testFindPersistedRoundMissingPropertyDefaultsZero() {
        clearAnalyzeStatusMap();
        ExternalAnalyzeStatus noProp = new ExternalAnalyzeStatus(1, "test_catalog", "test_db", "test_table",
                "uuid", Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                StatsConstants.ScheduleType.ONCE, null, LocalDateTime.now());
        noProp.setStatus(StatsConstants.ScheduleStatus.FINISH);
        noProp.setEndTime(LocalDateTime.now());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(noProp);

        ExternalSampleStatisticsCollectJob job = newJob(List.of(),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 1);
        assertEquals(0, job.findPersistedRound());
    }

    @Test
    public void testFindPersistedRoundBadFormatDefaultsZero() {
        clearAnalyzeStatusMap();
        Map<String, String> props = Maps.newHashMap();
        props.put("external_sample_round", "not_a_number");
        ExternalAnalyzeStatus badRound = new ExternalAnalyzeStatus(1, "test_catalog", "test_db", "test_table",
                "uuid", Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                StatsConstants.ScheduleType.ONCE, props, LocalDateTime.now());
        badRound.setStatus(StatsConstants.ScheduleStatus.FINISH);
        badRound.setEndTime(LocalDateTime.now());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(badRound);

        ExternalSampleStatisticsCollectJob job = newJob(List.of(),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 1);
        assertEquals(0, job.findPersistedRound());
    }

    @Test
    public void testFindPersistedRoundSkipsDifferentTable() {
        clearAnalyzeStatusMap();
        Map<String, String> props = Maps.newHashMap();
        props.put("external_sample_round", "5");
        ExternalAnalyzeStatus otherTable = new ExternalAnalyzeStatus(1, "other_catalog", "test_db", "test_table",
                "uuid", Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                StatsConstants.ScheduleType.ONCE, props, LocalDateTime.now());
        otherTable.setStatus(StatsConstants.ScheduleStatus.FINISH);
        otherTable.setEndTime(LocalDateTime.now());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(otherTable);

        ExternalSampleStatisticsCollectJob job = newJob(List.of(),
                Lists.newArrayList("c1"), Lists.newArrayList(IntegerType.INT), 1);
        assertEquals(0, job.findPersistedRound());
    }
}
