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
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.type.Type;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

// Statistics of partitions that no longer exist must be deleted at the end of a collection, and only those:
// a partition that merely was not collected this time, or whose rows are still within the grace period, keeps
// its statistics. See ExternalFullStatisticsCollectJob#cleanupDroppedPartitionStats.
public class ExternalStatsDroppedPartitionCleanupTest extends PlanTestNoneDBBase {
    private static final String CATALOG = "hive0";
    private static final String DB = "partitioned_db";
    private static final String TABLE = "t1";

    @TempDir
    public static File temp;

    private static Database hiveDb;
    private static Table hiveTable;

    private static final long GRACE_SECONDS = 7200;
    private static final long RUNNING_ANALYZE_ID = 987654321L;
    private final int savedExprChildrenLimit = Config.expr_children_limit;
    private final int savedMaxInDelete = Config.max_allowed_in_element_num_of_delete;
    private final long savedGrace = Config.connector_table_analyze_dropped_partition_stats_grace_second;
    private final long savedCleanupInterval =
            Config.connector_table_analyze_dropped_partition_stats_cleanup_interval_second;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(temp, "junit").toURI().toString());
        hiveDb = connectContext.getGlobalStateMgr().getMetadataMgr().getDb(connectContext, CATALOG, DB);
        hiveTable = connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, CATALOG, DB, TABLE);
    }

    @BeforeEach
    public void setGrace() {
        Config.connector_table_analyze_dropped_partition_stats_grace_second = GRACE_SECONDS;
    }

    @AfterEach
    public void restoreConfig() {
        Config.expr_children_limit = savedExprChildrenLimit;
        Config.max_allowed_in_element_num_of_delete = savedMaxInDelete;
        Config.connector_table_analyze_dropped_partition_stats_grace_second = savedGrace;
        Config.connector_table_analyze_dropped_partition_stats_cleanup_interval_second = savedCleanupInterval;
        AnalyzeMgr analyzeMgr = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
        analyzeMgr.getExternalBasicStatsMetaMap().clear();
        analyzeMgr.getAnalyzeStatusMap().remove(RUNNING_ANALYZE_ID);
        analyzeMgr.resetExternalDroppedPartitionCleanupRound();
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }

    private static List<String> columns() {
        return Lists.newArrayList("c1", "c2");
    }

    private static List<Type> columnTypes() {
        return Lists.newArrayList(com.starrocks.type.IntegerType.INT, com.starrocks.type.StringType.STRING);
    }

    // A job whose table currently has `livePartitionNames`; null stands for a listing that fails.
    private static ExternalFullStatisticsCollectJob newJob(List<String> livePartitionNames) {
        return new ExternalFullStatisticsCollectJob(CATALOG, hiveDb, hiveTable,
                Lists.newArrayList("par_col=0"), columns(), columnTypes(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap()) {
            @Override
            protected List<String> listLivePartitionNames() {
                if (livePartitionNames == null) {
                    throw new RuntimeException("listing failed");
                }
                return livePartitionNames;
            }
        };
    }

    // Stands in for the statistics table: `stale` is what the grace-filtered query returns, every delete is
    // recorded batch by batch.
    private static class FakeStatsTable {
        final List<List<String>> deletedBatches = new ArrayList<>();
        final List<List<String>> deletedColumnBatches = new ArrayList<>();
        final AtomicInteger queries = new AtomicInteger();

        FakeStatsTable(List<String> stale, boolean deleteSucceeds) {
            new MockUp<StatisticExecutor>() {
                @Mock
                public List<String> queryExternalStatPartitionsNotUpdatedSince(ConnectContext ctx, String tableUUID,
                                                                              List<String> columnNames,
                                                                              long graceSeconds) {
                    queries.incrementAndGet();
                    Assertions.assertEquals(hiveTable.getUUID(), tableUUID);
                    // Only the job's own columns: their cache is the one the job refreshes.
                    Assertions.assertEquals(columns(), columnNames);
                    Assertions.assertEquals(GRACE_SECONDS,
                            graceSeconds);
                    return stale;
                }

                @Mock
                public boolean dropExternalStatDroppedPartitions(ConnectContext ctx, String tableUUID,
                                                                 List<String> partitionNames,
                                                                 List<String> columnNames) {
                    Assertions.assertEquals(hiveTable.getUUID(), tableUUID);
                    Assertions.assertTrue(columns().containsAll(columnNames), columnNames.toString());
                    deletedBatches.add(new ArrayList<>(partitionNames));
                    deletedColumnBatches.add(new ArrayList<>(columnNames));
                    return deleteSucceeds;
                }
            };
        }

        List<String> deleted() {
            List<String> all = new ArrayList<>();
            deletedBatches.forEach(all::addAll);
            return all;
        }
    }

    @Test
    public void testDeletesOnlyPartitionsMissingFromTheLiveList() {
        // par_col=1 still exists (only its stats are old, it was simply not re-collected): kept.
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=1", "par_col=7", "par_col=8"), true);
        newJob(Lists.newArrayList("par_col=0", "par_col=1", "par_col=2")).cleanupDroppedPartitionStats(connectContext, 1L);
        Assertions.assertEquals(Lists.newArrayList("par_col=7", "par_col=8"), stats.deleted());
    }

    @Test
    public void testLiveNullPartitionMatchesItsNormalizedStoredName() {
        // The collection stores a Hive NULL partition as par_col=NULL; the connector lists it with its placeholder.
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=NULL", "par_col=7"), true);
        newJob(Lists.newArrayList("par_col=0", "par_col=__HIVE_DEFAULT_PARTITION__"))
                .cleanupDroppedPartitionStats(connectContext, 1L);
        Assertions.assertEquals(Lists.newArrayList("par_col=7"), stats.deleted());
    }

    @Test
    public void testNothingDeletedWhenNoPartitionIsMissing() {
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=1"), true);
        newJob(Lists.newArrayList("par_col=0", "par_col=1")).cleanupDroppedPartitionStats(connectContext, 1L);
        Assertions.assertEquals(1, stats.queries.get());
        Assertions.assertTrue(stats.deletedBatches.isEmpty());
    }

    @Test
    public void testBatchesAlsoBoundedByTheDeleteInListLimit() {
        Config.max_allowed_in_element_num_of_delete = 1;
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=10", "par_col=11", "par_col=12"), true);
        newJob(Lists.newArrayList("par_col=0")).cleanupDroppedPartitionStats(connectContext, 1L);
        // The column list is an IN predicate as well: 3 partition batches x 2 column batches.
        Assertions.assertEquals(6, stats.deletedBatches.size());
        stats.deletedColumnBatches.forEach(batch -> Assertions.assertEquals(1, batch.size(), batch.toString()));
        Assertions.assertEquals(3, stats.deletedColumnBatches.stream().filter(b -> b.equals(List.of("c1"))).count());
    }

    @Test
    public void testDeletesInBatchesOfHalfTheExprChildrenLimit() {
        Config.expr_children_limit = 4;
        FakeStatsTable stats = new FakeStatsTable(
                Lists.newArrayList("par_col=10", "par_col=11", "par_col=12", "par_col=13", "par_col=14"), true);
        newJob(Lists.newArrayList("par_col=0")).cleanupDroppedPartitionStats(connectContext, 1L);
        Assertions.assertEquals(3, stats.deletedBatches.size());
        stats.deletedBatches.forEach(batch -> Assertions.assertTrue(batch.size() <= 2, batch.toString()));
        Assertions.assertEquals(5, stats.deleted().size());
    }

    @Test
    public void testSkippedWithoutAPartitionList() {
        // Listing failed or came back empty: no way to tell a dropped partition from a live one.
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=7"), true);
        newJob(null).cleanupDroppedPartitionStats(connectContext, 1L);
        newJob(Lists.newArrayList()).cleanupDroppedPartitionStats(connectContext, 1L);
        Assertions.assertEquals(0, stats.queries.get());
        Assertions.assertTrue(stats.deletedBatches.isEmpty());
    }

    @Test
    public void testDisabledByANonPositiveGrace() {
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=7"), true);
        Config.connector_table_analyze_dropped_partition_stats_grace_second = 0;
        newJob(Lists.newArrayList("par_col=0")).cleanupDroppedPartitionStats(connectContext, 1L);
        Assertions.assertEquals(0, stats.queries.get());
        Assertions.assertTrue(stats.deletedBatches.isEmpty());
    }

    @Test
    public void testSkippedAfterAToleratedFailure() {
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=7"), true);
        new MockUp<ExternalFullStatisticsCollectJob>() {
            @Mock
            public boolean hasToleratedFailures() {
                return true;
            }
        };
        newJob(Lists.newArrayList("par_col=0")).cleanupDroppedPartitionStats(connectContext, 1L);
        Assertions.assertEquals(0, stats.queries.get());
        Assertions.assertTrue(stats.deletedBatches.isEmpty());
    }

    @Test
    public void testFailuresNeverFailTheJob() {
        Config.expr_children_limit = 2;
        // A failed delete batch does not stop the remaining batches.
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=7", "par_col=8"), false);
        Assertions.assertDoesNotThrow(() ->
                newJob(Lists.newArrayList("par_col=0")).cleanupDroppedPartitionStats(connectContext, 1L));
        // Batch size 1: 2 partitions x 2 columns.
        Assertions.assertEquals(4, stats.deletedBatches.size());

        new MockUp<StatisticExecutor>() {
            @Mock
            public List<String> queryExternalStatPartitionsNotUpdatedSince(ConnectContext ctx, String tableUUID,
                                                                          List<String> columnNames,
                                                                          long graceSeconds) {
                throw new RuntimeException("statistics table unavailable");
            }
        };
        Assertions.assertDoesNotThrow(() ->
                newJob(Lists.newArrayList("par_col=0")).cleanupDroppedPartitionStats(connectContext, 1L));
    }

    @Test
    public void testSkippedOnceTheAnalyzeIsCancelled() {
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=7"), true);
        new MockUp<AnalyzeMgr>() {
            @Mock
            public boolean isAnalyzeCancelled(long analyzeID) {
                return analyzeID == 42L;
            }
        };
        newJob(Lists.newArrayList("par_col=0")).cleanupDroppedPartitionStats(connectContext, 42L);
        Assertions.assertEquals(0, stats.queries.get());
    }

    @Test
    public void testListsTheRealPartitionsByDefault() {
        // Without the test override, the list comes from the connector: the mocked table has par_col=0..2.
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=1", "par_col=7"), true);
        new ExternalFullStatisticsCollectJob(CATALOG, hiveDb, hiveTable, Lists.newArrayList("par_col=0"), columns(),
                columnTypes(), StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap())
                .cleanupDroppedPartitionStats(connectContext, 1L);
        Assertions.assertEquals(Lists.newArrayList("par_col=7"), stats.deleted());
    }

    // ---- Periodic pass (AnalyzeMgr#clearStatisticFromExternalDroppedPartition): no collection involved. ----

    // A table whose columns c1, c2 were last collected FULL.
    private static void addStatsMeta(String table) {
        ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta(CATALOG, DB, table, columns(),
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), Maps.newHashMap());
        for (String column : columns()) {
            meta.addColumnStatsMeta(new ColumnStatsMeta(column, StatsConstants.AnalyzeType.FULL, LocalDateTime.now()));
        }
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddExternalBasicStatsMeta(meta);
    }

    private static void runPeriodic() {
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().clearStatisticFromExternalDroppedPartition();
    }

    @Test
    public void testPeriodicDeletesDroppedPartitionsWithoutACollection() {
        addStatsMeta(TABLE);
        // The mocked table lists par_col=0..2: par_col=1 is live, par_col=7 is gone.
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=1", "par_col=7"), true);
        runPeriodic();
        Assertions.assertEquals(Lists.newArrayList("par_col=7"), stats.deleted());
    }

    @Test
    public void testPeriodicRunsAtMostOncePerInterval() {
        addStatsMeta(TABLE);
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=7"), true);
        runPeriodic();
        runPeriodic();
        Assertions.assertEquals(1, stats.queries.get());
    }

    @Test
    public void testPeriodicDisabled() {
        addStatsMeta(TABLE);
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=7"), true);
        Config.connector_table_analyze_dropped_partition_stats_cleanup_interval_second = 0;
        runPeriodic();
        Config.connector_table_analyze_dropped_partition_stats_cleanup_interval_second = 3600;
        Config.connector_table_analyze_dropped_partition_stats_grace_second = 0;
        runPeriodic();
        Assertions.assertEquals(0, stats.queries.get());
    }

    @Test
    public void testPeriodicSkipsATableUnderAnalyze() {
        addStatsMeta(TABLE);
        ExternalAnalyzeStatus status = new ExternalAnalyzeStatus(RUNNING_ANALYZE_ID, CATALOG, DB, TABLE,
                hiveTable.getUUID(), columns(), StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE,
                Maps.newHashMap(), LocalDateTime.now());
        status.setStatus(StatsConstants.ScheduleStatus.RUNNING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(status);
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=7"), true);
        runPeriodic();
        Assertions.assertEquals(0, stats.queries.get());
    }

    @Test
    public void testPeriodicSkipsUnresolvableTablesAndSurvivesFailures() {
        // A table the catalog no longer has is left alone (the dropped-table cleanup owns it).
        addStatsMeta("no_such_table");
        addStatsMeta(TABLE);
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<String> queryExternalStatPartitionsNotUpdatedSince(ConnectContext ctx, String tableUUID,
                                                                          List<String> columnNames,
                                                                          long graceSeconds) {
                throw new RuntimeException("statistics table unavailable");
            }
        };
        Assertions.assertDoesNotThrow(ExternalStatsDroppedPartitionCleanupTest::runPeriodic);
    }

    @Test
    public void testPeriodicCleansOnlyFullColumns() {
        // c1 was last collected by SAMPLE: its rows are scaled up by the partition count its metadata recorded,
        // which only a collection rewrites. A sample whose coverage is incomplete may also be resumed from its
        // recorded partition set. Either way only its own collection may delete its rows.
        for (ColumnStatsMeta c1 : List.of(
                new ColumnStatsMeta("c1", StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(),
                        new HashSet<>(List.of(1L, 2L, 3L)), 3, 3),
                new ColumnStatsMeta("c1", StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(),
                        new HashSet<>(List.of(1L)), 3, 3))) {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().resetExternalDroppedPartitionCleanupRound();
            ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta(CATALOG, DB, TABLE, columns(),
                    StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), Maps.newHashMap());
            meta.addColumnStatsMeta(c1);
            meta.addColumnStatsMeta(new ColumnStatsMeta("c2", StatsConstants.AnalyzeType.FULL, LocalDateTime.now()));
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddExternalBasicStatsMeta(meta);
            FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=7"), true);
            new MockUp<StatisticExecutor>() {
                @Mock
                public List<String> queryExternalStatPartitionsNotUpdatedSince(ConnectContext ctx, String tableUUID,
                                                                              List<String> columnNames,
                                                                              long graceSeconds) {
                    Assertions.assertEquals(List.of("c2"), columnNames);
                    stats.queries.incrementAndGet();
                    return Lists.newArrayList("par_col=7");
                }
            };
            runPeriodic();
            Assertions.assertEquals(1, stats.queries.get());
            Assertions.assertEquals(List.of(List.of("c2")), stats.deletedColumnBatches);
        }
    }

    @Test
    public void testPeriodicSkipsATableWithoutFullColumns() {
        // Legacy metadata without per-column entries: the optimizer ignores such rows, nothing to clean.
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddExternalBasicStatsMeta(
                new ExternalBasicStatsMeta(CATALOG, DB, TABLE, columns(), StatsConstants.AnalyzeType.FULL,
                        LocalDateTime.now(), Maps.newHashMap()));
        FakeStatsTable stats = new FakeStatsTable(Lists.newArrayList("par_col=7"), true);
        runPeriodic();
        Assertions.assertEquals(0, stats.queries.get());
    }
}
