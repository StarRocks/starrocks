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
import java.util.ArrayList;
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
    private final int savedExprChildrenLimit = Config.expr_children_limit;
    private final int savedMaxInDelete = Config.max_allowed_in_element_num_of_delete;
    private final long savedGrace = Config.connector_table_analyze_dropped_partition_stats_grace_second;

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
                    Assertions.assertEquals(columns(), columnNames);
                    deletedBatches.add(new ArrayList<>(partitionNames));
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
        Assertions.assertEquals(3, stats.deletedBatches.size());
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
        Assertions.assertEquals(2, stats.deletedBatches.size());

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
}
