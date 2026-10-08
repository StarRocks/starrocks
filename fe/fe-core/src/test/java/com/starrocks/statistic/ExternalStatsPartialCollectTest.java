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
import com.google.common.hash.Hashing;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

// A collection query that fails must not take the rest of the job (or the metadata describing what the job
// did manage to collect) down with it. Covers both halves of that: the tolerance rules in
// ExternalFullStatisticsCollectJob#executeCollectSQLList, and the read path that decides whether rows in
// external_column_statistics may be trusted as whole-table values.
public class ExternalStatsPartialCollectTest extends PlanTestNoneDBBase {
    private static final String CATALOG = "hive0";
    private static final String DB = "partitioned_db";
    private static final String TABLE = "t1";

    @TempDir
    public static File temp;

    private static Database hiveDb;
    private static Table hiveTable;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(temp, "junit").toURI().toString());
        hiveDb = connectContext.getGlobalStateMgr().getMetadataMgr().getDb(connectContext, CATALOG, DB);
        hiveTable = connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, CATALOG, DB, TABLE);
    }

    @AfterEach
    public void cleanupMeta() {
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().removeExternalBasicStatsMeta(CATALOG, DB, TABLE);
    }

    // The backend's wording for "this whole node is out of memory", which is transient and retried rather
    // than counted as a failed partition. Verbatim from a production failure.
    private static final String BACKEND_OUT_OF_MEMORY =
            "Memory of process exceed limit. Start execute plan fragment. " +
                    "Backend: 172.26.80.14, Used: 54716154728, Limit: 53402157342";
    // Anything else is the query's own problem and is not retried.
    private static final String PERMANENT_FAILURE = "Statistics query fail | Error Message [bad column]";

    // Runs the real collection loop while standing in for everything that would touch a backend: the
    // collection query itself, the buffered INSERT, and the stale-row cleanup.
    private static class RecordingJob extends ExternalFullStatisticsCollectJob {
        private final Set<Integer> failAtIndexes;
        private String failureMessage = PERMANENT_FAILURE;
        private int executedCount = 0;
        private int collectedCount = 0;
        private int forceFlushCount = 0;
        private int backoffCount = 0;

        RecordingJob(List<String> partitionNames, List<String> columnNames, List<Type> columnTypes,
                     Set<Integer> failAtIndexes) {
            super(CATALOG, hiveDb, hiveTable, partitionNames, columnNames, columnTypes,
                    StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());
            this.failAtIndexes = failAtIndexes;
        }

        @Override
        public void collectStatisticSync(String sql, ConnectContext context, AnalyzeStatus analyzeStatus)
                throws Exception {
            int index = executedCount++;
            if (failAtIndexes.contains(index)) {
                throw new RuntimeException(failureMessage);
            }
            collectedCount++;
        }

        @Override
        protected void flushInsertStatisticsData(ConnectContext context, boolean force) throws Exception {
            if (force) {
                forceFlushCount++;
            }
        }

        @Override
        protected void cleanupStaleRawKeyedRows(ConnectContext context, long jobId) {
        }

        // Keep the retry logic, drop the waiting.
        @Override
        protected long retryBackoffMillis(int attempt) {
            backoffCount++;
            return 0;
        }

        int getExecutedCount() {
            return executedCount;
        }
    }

    private static List<String> partitions(int count) {
        List<String> names = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            names.add("par_col=" + i);
        }
        return names;
    }

    private static List<String> columns() {
        return Lists.newArrayList("c1", "c2");
    }

    private static List<Type> columnTypes() {
        return Lists.newArrayList(com.starrocks.type.IntegerType.INT, com.starrocks.type.StringType.STRING);
    }

    private static AnalyzeStatus newAnalyzeStatus() {
        return new NativeAnalyzeStatus(1L, 1L, 1L, columns(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
    }

    @Test
    public void testFailedQueryDoesNotAbortRemainingPartitions() throws Exception {
        // 200 partitions x 1 column group = 200 queries; the 6th fails.
        RecordingJob job = new RecordingJob(partitions(200), columns(), columnTypes(), Set.of(5));
        AnalyzeStatus analyzeStatus = newAnalyzeStatus();

        job.runCollectPhases(connectContext, analyzeStatus, 1L);

        // Every remaining query still ran, and the rows they produced were force-flushed - before this,
        // the exception escaped the loop and the only force-flush was never reached, so a job that died
        // on query 6 wrote nothing at all.
        Assertions.assertEquals(200, job.executedCount);
        Assertions.assertEquals(199, job.collectedCount);
        Assertions.assertEquals(1, job.forceFlushCount);

        Assertions.assertTrue(job.hasToleratedFailures());
        Assertions.assertTrue(analyzeStatus.getReason().contains("partially failed but tolerated 1/200"),
                analyzeStatus.getReason());

        // The failed partition is absent from every column's coverage, so the metadata committed for
        // these rows describes 199 partitions, not the 200 that were requested.
        for (String column : columns()) {
            Set<Long> collected = job.getCollectedPartitionsHashByColumn().get(column);
            Assertions.assertEquals(199, collected.size());
            Assertions.assertFalse(collected.contains(hash("par_col=5")));
            Assertions.assertTrue(collected.contains(hash("par_col=0")));
        }
    }

    @Test
    public void testColumnGroupsOfOnePartitionFailIndependently() throws Exception {
        // 4 columns at parallelism 1 split into 2 groups of 2, so each partition is scanned twice. Failing
        // the second group of the first partition must not evict that partition from the first group's
        // columns: their rows for it were written and are correct.
        List<String> columnNames = Lists.newArrayList("c1", "c2", "c3", "par_col");
        List<Type> types = Lists.newArrayList(com.starrocks.type.IntegerType.INT, com.starrocks.type.StringType.STRING,
                com.starrocks.type.StringType.STRING, com.starrocks.type.StringType.STRING);
        RecordingJob job = new RecordingJob(partitions(100), columnNames, types, Set.of(1));

        job.runCollectPhases(connectContext, newAnalyzeStatus(), 1L);

        Assertions.assertEquals(100, job.getCollectedPartitionsHashByColumn().get("c1").size());
        Assertions.assertEquals(100, job.getCollectedPartitionsHashByColumn().get("c2").size());
        Assertions.assertEquals(99, job.getCollectedPartitionsHashByColumn().get("c3").size());
        Assertions.assertEquals(99, job.getCollectedPartitionsHashByColumn().get("par_col").size());
    }

    @Test
    public void testTooManyFailuresFailsTheJob() {
        // 20 failures out of 200 is far past statistic_full_statistics_failure_tolerance_ratio (5%): what is
        // left is not a sample worth keeping, so the job fails instead of committing it.
        Set<Integer> failures = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            failures.add(i);
        }
        RecordingJob job = new RecordingJob(partitions(200), columns(), columnTypes(), failures);

        Exception e = Assertions.assertThrows(Exception.class,
                () -> job.runCollectPhases(connectContext, newAnalyzeStatus(), 1L));
        Assertions.assertTrue(e.getMessage().contains("too many failed tasks"), e.getMessage());
        // It gave up as soon as the ratio was exceeded rather than running all 200.
        Assertions.assertTrue(job.executedCount < 200);
    }

    @Test
    public void testTooFewQueriesAreNotTolerated() {
        // With only a handful of partitions each one carries too much of the table for the rest to stand in
        // for it, so nothing is tolerated and the original error surfaces unchanged.
        RecordingJob job = new RecordingJob(partitions(3), columns(), columnTypes(), Set.of(1));

        Exception e = Assertions.assertThrows(Exception.class,
                () -> job.runCollectPhases(connectContext, newAnalyzeStatus(), 1L));
        Assertions.assertTrue(e.getMessage().contains(PERMANENT_FAILURE), e.getMessage());
    }

    @Test
    public void testBackendOutOfMemoryIsWaitedOutNotCountedAsFailure() throws Exception {
        // The backend was out of memory because of an unrelated query, not because of this one. Counting each
        // rejection against the failure budget is what killed the production job: the rejections arrive as
        // fast as the queries are submitted, so a budget of 15 was gone in 13 seconds - one second before the
        // neighbouring query finished and freed the memory.
        Set<Integer> failures = new HashSet<>();
        for (int i = 0; i < 40; i++) {
            failures.add(i * 3);
        }
        RecordingJob job = new RecordingJob(partitions(200), columns(), columnTypes(), failures);
        job.failureMessage = BACKEND_OUT_OF_MEMORY;
        AnalyzeStatus analyzeStatus = newAnalyzeStatus();

        job.runCollectPhases(connectContext, analyzeStatus, 1L);

        // 40 rejections - far past the 5% budget - yet nothing was lost: each was retried until it went
        // through, and the job collected every partition.
        Assertions.assertFalse(job.hasToleratedFailures());
        Assertions.assertNull(analyzeStatus.getReason());
        Assertions.assertEquals(40, job.backoffCount);
        Assertions.assertEquals(200, job.collectedCount);
        Assertions.assertEquals(200, job.getCollectedPartitionsHashByColumn().get("c1").size());
    }

    @Test
    public void testBackendOutOfMemoryStillFailsAfterRetriesAreExhausted() {
        // A backend that never recovers must not pin the job there forever: once the retries are spent the
        // query counts as failed and the usual tolerance rules take over.
        Set<Integer> failures = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            failures.add(i);
        }
        RecordingJob job = new RecordingJob(partitions(200), columns(), columnTypes(), failures);
        job.failureMessage = BACKEND_OUT_OF_MEMORY;

        Exception e = Assertions.assertThrows(Exception.class,
                () -> job.runCollectPhases(connectContext, newAnalyzeStatus(), 1L));
        Assertions.assertTrue(e.getMessage().contains("too many failed tasks"), e.getMessage());
        // 5% of 200 is a budget of 10, so the 11th counted failure ends the job. Each of those 11 was tried
        // three times (two backoffs) before counting, and none of them collected anything.
        Assertions.assertEquals(33, job.getExecutedCount());
        Assertions.assertEquals(22, job.backoffCount);
        Assertions.assertEquals(0, job.collectedCount);
    }

    @Test
    public void testOneQueryCannotHoldTheWholeJobBudget() throws Exception {
        // The job budget is an hour by default; without a cap each individual query inherits whatever is left
        // of it, so one stuck query would prevent every partition after it from being attempted at all.
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) {
                return Lists.newArrayList();
            }
        };
        long savedJobTimeout = Config.statistic_collect_query_timeout;
        long savedQueryCap = Config.connector_table_analyze_query_timeout;
        int savedQueryTimeout = connectContext.getSessionVariable().getQueryTimeoutS();
        try {
            ExternalFullStatisticsCollectJob job = new ExternalFullStatisticsCollectJob(CATALOG, hiveDb, hiveTable,
                    partitions(1), columns(), columnTypes(), StatsConstants.AnalyzeType.FULL,
                    StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

            Config.statistic_collect_query_timeout = 3600;
            job.collectStatisticSync("select 1", connectContext, newAnalyzeStatus());
            Assertions.assertEquals(360, connectContext.getSessionVariable().getQueryTimeoutS());

            // Tunable at runtime, for storage slow enough that the default is genuinely too tight.
            Config.connector_table_analyze_query_timeout = 900;
            job.collectStatisticSync("select 1", connectContext, newAnalyzeStatus());
            Assertions.assertEquals(900, connectContext.getSessionVariable().getQueryTimeoutS());

            // The job's remaining budget still wins when it is the smaller of the two.
            Config.statistic_collect_query_timeout = 30;
            job.collectStatisticSync("select 1", connectContext, newAnalyzeStatus());
            Assertions.assertEquals(30, connectContext.getSessionVariable().getQueryTimeoutS());

            // Turned off: back to the old behaviour of handing over the whole remaining budget.
            Config.statistic_collect_query_timeout = 3600;
            Config.connector_table_analyze_query_timeout = 0;
            job.collectStatisticSync("select 1", connectContext, newAnalyzeStatus());
            Assertions.assertEquals(3600, connectContext.getSessionVariable().getQueryTimeoutS());
        } finally {
            Config.statistic_collect_query_timeout = savedJobTimeout;
            Config.connector_table_analyze_query_timeout = savedQueryCap;
            connectContext.getSessionVariable().setQueryTimeoutS(savedQueryTimeout);
        }
    }

    // Fills a job's row buffer with one collected row, then force-flushes it with the insert failing
    // `failures` times before succeeding. Returns how many inserts were attempted.
    private int flushWithFailingInserts(int failures, String errorMessage) throws Exception {
        TStatisticData data = new TStatisticData();
        data.setPartitionName("par_col=0");
        data.setColumnName("c1");
        data.setRowCount(10);
        data.setDataSize(80);
        data.setNullCount(0);
        data.setHll("AA".getBytes(StandardCharsets.UTF_8));
        data.setMax("9");
        data.setMin("0");
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) {
                return Lists.newArrayList(data);
            }
        };
        AtomicInteger inserts = new AtomicInteger();
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() {
                if (inserts.incrementAndGet() <= failures) {
                    connectContext.getState().setError(errorMessage);
                } else {
                    connectContext.getState().setOk();
                }
            }
        };

        ExternalFullStatisticsCollectJob job = new ExternalFullStatisticsCollectJob(CATALOG, hiveDb, hiveTable,
                partitions(1), columns(), columnTypes(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap()) {
            @Override
            protected long retryBackoffMillis(int attempt) {
                return 0;
            }
        };
        job.collectStatisticSync("select 1", connectContext, newAnalyzeStatus());
        job.flushInsertStatisticsData(connectContext, true);
        return inserts.get();
    }

    @Test
    public void testWriteFailureIsNotBlamedOnThePartitionThatFilledTheBuffer() throws Exception {
        // The buffer holds rows from many partitions, so a failed write says nothing about whichever
        // partition happened to trigger it. Counting it as that partition's failure used to drop the
        // partition from the recorded coverage while its rows stayed buffered and landed on the next
        // successful write - leaving the read path dividing those rows by a denominator missing them.
        AtomicInteger flushes = new AtomicInteger();
        RecordingJob job = new RecordingJob(partitions(200), columns(), columnTypes(), Set.of()) {
            @Override
            protected void flushInsertStatisticsData(ConnectContext context, boolean force) throws Exception {
                super.flushInsertStatisticsData(context, force);
                if (flushes.incrementAndGet() == 5) {
                    throw new RuntimeException(BACKEND_OUT_OF_MEMORY);
                }
            }
        };

        // The write failure ends the job rather than being absorbed as one partition's failure...
        Assertions.assertThrows(Exception.class,
                () -> job.runCollectPhases(connectContext, newAnalyzeStatus(), 1L));
        // ...and every partition queried before it stayed on the record, including the one being written.
        Assertions.assertEquals(job.collectedCount, job.getCollectedPartitionsHashByColumn().get("c1").size());
        Assertions.assertFalse(job.hasToleratedFailures());
    }

    @Test
    public void testStatisticsInsertWaitsOutBackendOutOfMemory() throws Exception {
        // The collected rows only reach storage after every partition has been read, so this is the worst
        // possible moment to give up: a backend momentarily out of memory here throws away the whole job's
        // work. Observed in production - 300 partitions collected without a single failure, then the final
        // insert was killed and all of it was lost.
        Assertions.assertEquals(3, flushWithFailingInserts(2, BACKEND_OUT_OF_MEMORY));
    }

    @Test
    public void testStatisticsInsertStillGivesUpEventually() {
        // A backend that never recovers must not hold the job forever.
        Exception e = Assertions.assertThrows(Exception.class,
                () -> flushWithFailingInserts(Integer.MAX_VALUE, BACKEND_OUT_OF_MEMORY));
        Assertions.assertTrue(e.getMessage().contains("Memory of process exceed limit"), e.getMessage());
    }

    @Test
    public void testStatisticsInsertFailureOfAnotherKindIsNotRetried() throws Exception {
        Assertions.assertThrows(Exception.class, () -> flushWithFailingInserts(1, "Unknown column 'c1'"));
    }

    @Test
    public void testQueryLevelFailureIsNotRetried() {
        // "This query is too big for its own memory limit" reproduces exactly on a retry, so it is counted
        // immediately - only backend-wide exhaustion is worth waiting out.
        RecordingJob job = new RecordingJob(partitions(200), columns(), columnTypes(), Set.of(5));
        job.failureMessage = "Memory of query exceed limit. Used: 100, Limit: 50";

        Assertions.assertDoesNotThrow(() -> job.runCollectPhases(connectContext, newAnalyzeStatus(), 1L));
        Assertions.assertEquals(0, job.backoffCount);
        Assertions.assertTrue(job.hasToleratedFailures());
        Assertions.assertEquals(199, job.collectedCount);
    }

    @Test
    public void testSkippedPartitionIsNotCountedAsCollected() throws Exception {
        // The iceberg default partition is silently skipped when building queries (no partition predicate can
        // be generated for it). Coverage is accumulated from executed queries, so a partition that was never
        // queried cannot slip into the denominator just because nothing reported it as failed.
        List<String> partitionNames = partitions(150);
        partitionNames.add(PartitionUtil.ICEBERG_DEFAULT_PARTITION);
        RecordingJob job = new RecordingJob(partitionNames, columns(), columnTypes(), Set.of());

        job.runCollectPhases(connectContext, newAnalyzeStatus(), 1L);

        Assertions.assertFalse(job.hasToleratedFailures());
        Assertions.assertEquals(150, job.executedCount);
        Set<Long> collected = job.getCollectedPartitionsHashByColumn().get("c1");
        Assertions.assertEquals(150, collected.size());
        Assertions.assertFalse(collected.contains(hash(PartitionUtil.ICEBERG_DEFAULT_PARTITION)));
    }

    @Test
    public void testCancelDuringTheLastQueryIsNotTolerated() {
        // A KILL lands while a query is running, so it surfaces as that query's failure. On the last query
        // there is no next loop iteration to notice it, so it has to be re-checked where the failure is
        // caught - otherwise the job reports success and commits statistics the user asked it to abandon.
        AtomicBoolean cancelled = new AtomicBoolean(false);
        new MockUp<AnalyzeMgr>() {
            @Mock
            public boolean isAnalyzeCancelled(long analyzeId) {
                return cancelled.get();
            }
        };
        RecordingJob job = new RecordingJob(partitions(200), columns(), columnTypes(), Set.of(199)) {
            @Override
            public void collectStatisticSync(String sql, ConnectContext context, AnalyzeStatus status)
                    throws Exception {
                if (getExecutedCount() == 199) {
                    cancelled.set(true);
                }
                super.collectStatisticSync(sql, context, status);
            }
        };

        Exception e = Assertions.assertThrows(Exception.class,
                () -> job.runCollectPhases(connectContext, newAnalyzeStatus(), 1L));
        Assertions.assertTrue(e.getMessage().contains("USER_CANCEL"), e.getMessage());
    }

    @Test
    public void testFailureToleranceRatioIsConfigurable() throws Exception {
        double saved = Config.statistic_full_statistics_failure_tolerance_ratio;
        try {
            Config.statistic_full_statistics_failure_tolerance_ratio = 0.5;
            Set<Integer> failures = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                failures.add(i);
            }
            RecordingJob job = new RecordingJob(partitions(200), columns(), columnTypes(), failures);
            job.runCollectPhases(connectContext, newAnalyzeStatus(), 1L);
            Assertions.assertEquals(180, job.collectedCount);
        } finally {
            Config.statistic_full_statistics_failure_tolerance_ratio = saved;
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Commit path: the metadata written for a partial run has to describe the partial run.
    // ---------------------------------------------------------------------------------------------------

    @Test
    public void testPartialFullJobIsCommittedAsSample() throws Exception {
        // FULL metadata carries no partition set, so it cannot describe a run that lost partitions. Leaving
        // the previous metadata alone is worse than useless when that metadata is a SAMPLE entry: its small
        // partition set would become the denominator for this run's near-complete rows.
        Set<Long> staleSampled = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            staleSampled.add(hash("par_col=" + i));
        }
        putMeta(new ColumnStatsMeta("c1", StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), staleSampled, 200));

        // Fail one partition the previous run had covered (par_col=5) and one it had not (par_col=50).
        RecordingJob job = new RecordingJob(partitions(200), columns(), columnTypes(), Set.of(5, 50)) {
            @Override
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
                runCollectPhases(context, analyzeStatus, 1L);
            }
        };
        AnalyzeStatus analyzeStatus = newAnalyzeStatus();
        new StatisticExecutor().collectStatistics(connectContext, job, analyzeStatus, false, false);

        ExternalBasicStatsMeta meta =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getExternalTableBasicStatsMeta(CATALOG, DB, TABLE);
        ColumnStatsMeta columnMeta = meta.getColumnStatsMeta("c1");
        Assertions.assertEquals(StatsConstants.AnalyzeType.SAMPLE, columnMeta.getType());
        Assertions.assertEquals(200, columnMeta.getAllPartitionSize());
        // The recorded coverage mirrors the rows on disk: the 198 this run wrote, plus par_col=5, whose row
        // the previous run left behind. par_col=50 has no row from either run, so it stays out. The stale
        // 20-partition denominator the old entry claimed is gone - keeping it would have inflated the table
        // ten-fold against this run's near-complete rows.
        Assertions.assertEquals(199, columnMeta.getSampledPartitionsHashValue().size());
        Assertions.assertTrue(columnMeta.getSampledPartitionsHashValue().contains(hash("par_col=5")));
        Assertions.assertFalse(columnMeta.getSampledPartitionsHashValue().contains(hash("par_col=50")));
    }

    @Test
    public void testPartialRunLeavesAFullyCollectedColumnAlone() {
        // The failure budget counts queries across the whole job, but coverage is per column: a wide table
        // splits each partition into many column-group queries, so one group can lose nearly every partition
        // while the job stays well inside the budget. If that column is already recorded as fully collected,
        // its previous run's rows still cover what this one missed - recording this run's handful of
        // partitions would leave the read path dividing whole-table rows by that handful.
        putMeta(new ColumnStatsMeta("c1", StatsConstants.AnalyzeType.FULL, LocalDateTime.now()));
        Set<Long> barelyAnything = new HashSet<>();
        barelyAnything.add(hash("par_col=0"));

        new StatisticExecutor().commitExternalColumnStatsMeta(hiveDb, hiveTable, CATALOG,
                Lists.newArrayList("c1"), Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.now(), Maps.newHashMap(), new HashSet<>(), Collections.emptySet(), 100,
                Maps.newHashMap(java.util.Map.of("c1", barelyAnything)), false);

        ColumnStatsMeta columnMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                .getExternalTableBasicStatsMeta(CATALOG, DB, TABLE).getColumnStatsMeta("c1");
        Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, columnMeta.getType());
        Assertions.assertTrue(columnMeta.getSampledPartitionsHashValue().isEmpty());
    }

    @Test
    public void testPartialRunStillNarrowsASampledColumn() {
        // The same partial run against a column that was only ever sampled: here the recorded set is the
        // union of both runs, which is exactly the set of partitions with rows on disk.
        Set<Long> previouslySampled = new HashSet<>();
        previouslySampled.add(hash("par_col=7"));
        putMeta(new ColumnStatsMeta("c1", StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(),
                previouslySampled, 100));
        Set<Long> collected = new HashSet<>();
        collected.add(hash("par_col=0"));

        new StatisticExecutor().commitExternalColumnStatsMeta(hiveDb, hiveTable, CATALOG,
                Lists.newArrayList("c1"), Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.now(), Maps.newHashMap(), new HashSet<>(), Collections.emptySet(), 100,
                Maps.newHashMap(java.util.Map.of("c1", collected)), false);

        ColumnStatsMeta columnMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                .getExternalTableBasicStatsMeta(CATALOG, DB, TABLE).getColumnStatsMeta("c1");
        Assertions.assertEquals(StatsConstants.AnalyzeType.SAMPLE, columnMeta.getType());
        Assertions.assertEquals(2, columnMeta.getSampledPartitionsHashValue().size());
    }

    @Test
    public void testPartialDirectValueColumnIsNotRecordedAsFullCoverage() {
        // A direct-value column normally records "every partition was scanned". After a tolerated failure
        // that is no longer true, and claiming it would tell the read path to trust an incomplete row count
        // and NDV as the whole table's.
        new MockUp<StatisticUtils>() {
            @Mock
            public boolean isDirectValuePartitionColumn(Table table, String columnName) {
                return true;
            }
        };
        Set<Long> collected = new HashSet<>();
        collected.add(hash("par_col=0"));
        collected.add(hash("par_col=1"));
        Set<Long> allPartitions = new HashSet<>(collected);
        allPartitions.add(hash("par_col=2"));

        new StatisticExecutor().commitExternalColumnStatsMeta(hiveDb, hiveTable, CATALOG,
                Lists.newArrayList("c1"), Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.now(), Maps.newHashMap(), new HashSet<>(), allPartitions, 3,
                Maps.newHashMap(java.util.Map.of("c1", collected)), false);

        ColumnStatsMeta columnMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                .getExternalTableBasicStatsMeta(CATALOG, DB, TABLE).getColumnStatsMeta("c1");
        Assertions.assertEquals(StatsConstants.AnalyzeType.SAMPLE, columnMeta.getType());
        Assertions.assertEquals(2, columnMeta.getSampledPartitionsHashValue().size());
        Assertions.assertFalse(columnMeta.getSampledPartitionsHashValue().contains(hash("par_col=2")));
    }

    @Test
    public void testFullyCollectedDirectValueColumnKeepsFullCoverage() {
        // The unchanged path: with no per-column coverage reported, a direct-value column is still recorded
        // as covering every partition.
        new MockUp<StatisticUtils>() {
            @Mock
            public boolean isDirectValuePartitionColumn(Table table, String columnName) {
                return true;
            }
        };
        Set<Long> allPartitions = new HashSet<>();
        allPartitions.add(hash("par_col=0"));
        allPartitions.add(hash("par_col=1"));

        new StatisticExecutor().commitExternalColumnStatsMeta(hiveDb, hiveTable, CATALOG,
                Lists.newArrayList("c1"), Lists.newArrayList("c1"), StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.now(), Maps.newHashMap(), new HashSet<>(), allPartitions, 2, false);

        ColumnStatsMeta columnMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                .getExternalTableBasicStatsMeta(CATALOG, DB, TABLE).getColumnStatsMeta("c1");
        Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, columnMeta.getType());
        Assertions.assertEquals(2, columnMeta.getSampledPartitionsHashValue().size());
    }

    // ---------------------------------------------------------------------------------------------------
    // Read path: rows are only as good as the metadata that says how much of the table they cover.
    // ---------------------------------------------------------------------------------------------------

    private static long hash(String partitionName) {
        return Hashing.murmur3_128().hashUnencodedChars(partitionName).asLong();
    }

    private static ConnectorTableColumnStats sampledStats(long rowCount) {
        return new ConnectorTableColumnStats(ColumnStatistic.builder().setDistinctValuesCount(100).build(),
                rowCount, "");
    }

    private static void putMeta(ColumnStatsMeta columnStatsMeta) {
        ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta(CATALOG, DB, TABLE, Lists.newArrayList("c1"),
                StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), Maps.newHashMap());
        if (columnStatsMeta != null) {
            meta.addColumnStatsMeta(columnStatsMeta);
        }
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddExternalBasicStatsMeta(meta);
    }

    @Test
    public void testRowsWithoutAnyMetaAreUnknown() {
        // A job that died before committing metadata leaves rows for an unknown subset of partitions behind.
        // Using them as whole-table values is a silent underestimate, so they are not used at all.
        ConnectorTableColumnStats estimated = StatisticsUtils.estimateColumnStatistics(hiveTable, "c1", sampledStats(1000));
        Assertions.assertTrue(estimated.isUnknown());
    }

    @Test
    public void testRowsWithoutColumnMetaAreUnknown() {
        putMeta(null);
        ConnectorTableColumnStats estimated = StatisticsUtils.estimateColumnStatistics(hiveTable, "c1", sampledStats(1000));
        Assertions.assertTrue(estimated.isUnknown());
    }

    @Test
    public void testSampleMetaExtrapolatesByCollectedPartitions() {
        // 10 of 300 partitions collected: the aggregate over those 10 scales up by 30x. This is what a
        // partial collection commits, so the recorded partition set has to be the one actually collected.
        Set<Long> sampled = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            sampled.add(hash("par_col=" + i));
        }
        putMeta(new ColumnStatsMeta("c1", StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), sampled, 300));

        ConnectorTableColumnStats estimated = StatisticsUtils.estimateColumnStatistics(hiveTable, "c1", sampledStats(1000));
        Assertions.assertFalse(estimated.isUnknown());
        Assertions.assertEquals(30000, estimated.getRowCount());
    }

    @Test
    public void testZeroCollectedPartitionsIsUnknown() {
        // Defensive: a SAMPLE entry covering no partition has nothing to scale by and used to divide by zero.
        putMeta(new ColumnStatsMeta("c1", StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), new HashSet<>(), 300));

        ConnectorTableColumnStats estimated = StatisticsUtils.estimateColumnStatistics(hiveTable, "c1", sampledStats(1000));
        Assertions.assertTrue(estimated.isUnknown());
    }

    @Test
    public void testFullMetaIsUsedAsIs() {
        putMeta(new ColumnStatsMeta("c1", StatsConstants.AnalyzeType.FULL, LocalDateTime.now()));

        ConnectorTableColumnStats estimated = StatisticsUtils.estimateColumnStatistics(hiveTable, "c1", sampledStats(1000));
        Assertions.assertFalse(estimated.isUnknown());
        Assertions.assertEquals(1000, estimated.getRowCount());
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        File result = new File(root, String.join("/", subDirs));
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }
}
