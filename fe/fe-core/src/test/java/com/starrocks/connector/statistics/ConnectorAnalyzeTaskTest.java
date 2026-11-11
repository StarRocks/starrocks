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

package com.starrocks.connector.statistics;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.ColumnStatsMeta;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.ExternalFullStatisticsCollectJob;
import com.starrocks.statistic.ExternalSampleStatisticsCollectJob;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTriggerExternalAnalyzeRequest;
import com.starrocks.thrift.TTriggerExternalAnalyzeResponse;
import com.starrocks.thrift.TTriggerExternalAnalyzeTable;
import com.starrocks.utframe.UtFrameUtils;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ConnectorAnalyzeTaskTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(ctx);
    }

    @AfterEach
    public void tearDown() {
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeStatusMap().clear();
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getExternalBasicStatsMetaMap().clear();
    }

    @Test
    public void testMergeTask() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_custkey", "o_orderstatus"));
        task1.mergeTask(task2);
        Assertions.assertEquals(3, task1.getColumns().size());
    }

    @Test
    public void testTaskRun() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        ExternalAnalyzeStatus externalAnalyzeStatus = new ExternalAnalyzeStatus(1, "hive0", "partitioned_db", "orders",
                tableUUID, List.of("o_custkey", "o_orderstatus"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        externalAnalyzeStatus.setStatus(StatsConstants.ScheduleStatus.RUNNING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(externalAnalyzeStatus);

        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));
        Optional<AnalyzeStatus> result = task1.run();
        Assertions.assertTrue(result.isEmpty());

        new MockUp<ExternalSampleStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
                // do nothing
            }
        };

        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return LocalDateTime.now().minusDays(1);
            }
        };
        // execute analyze when last analyze status is finish
        externalAnalyzeStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
        // add ExternalBasicStatsMeta when analyze finish
        ExternalBasicStatsMeta externalBasicStatsMeta = new ExternalBasicStatsMeta("hive0", "partitioned_db",
                "orders", List.of("o_custkey", "o_orderstatus"), StatsConstants.AnalyzeType.FULL,
                LocalDateTime.now(), Maps.newHashMap());
        externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("o_custkey", StatsConstants.AnalyzeType.FULL,
                LocalDateTime.now()));
        externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("o_orderstatus", StatsConstants.AnalyzeType.FULL,
                LocalDateTime.now()));

        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addExternalBasicStatsMeta(externalBasicStatsMeta);

        result = task1.run();
        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        ExternalAnalyzeStatus externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assertions.assertEquals(List.of("o_orderkey"), externalAnalyzeStatusResult.getColumns());
    }

    @Test
    public void testTaskRunWithStructSubfield() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "subfield_db", "subfield");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("col_int", "col_struct.c0"));
        Optional<AnalyzeStatus> result = task.run();
        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        ExternalAnalyzeStatus externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assertions.assertEquals(List.of("col_int", "col_struct.c0"), externalAnalyzeStatusResult.getColumns());
    }

    @Test
    public void testTaskRunWithTableUpdate() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();

        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));
        new MockUp<ExternalFullStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
                // do nothing
            }
        };

        Optional<AnalyzeStatus> result = task1.run();
        Assertions.assertTrue(result.isPresent());
        Map<String, ColumnStatsMeta>  columnStatsMetaMap = GlobalStateMgr.getCurrentState().getAnalyzeMgr().
                getExternalTableBasicStatsMeta("hive0", "partitioned_db", "orders").getColumnStatsMetaMap();
        Assertions.assertEquals(2, columnStatsMetaMap.size());
        Assertions.assertTrue(columnStatsMetaMap.containsKey("o_orderkey"));
        Assertions.assertTrue(columnStatsMetaMap.containsKey("o_custkey"));

        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return LocalDateTime.now().minusDays(1);
            }
        };
        // table not update, skip analyze
        task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));
        result = task1.run();
        Assertions.assertTrue(result.isEmpty());

        // table update, analyze again
        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return LocalDateTime.now().plusMinutes(10);
            }
        };

        task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));
        result = task1.run();
        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        ExternalAnalyzeStatus externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assertions.assertEquals(2, externalAnalyzeStatusResult.getColumns().size());
    }

    @Test
    public void testIncrementalFullRunIsCommittedAsSample() {
        // A FULL run that refreshes only the updated partitions records SAMPLE over the table's partition count,
        // taken from the partition list the task already fetched.
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        int tablePartitions = ConnectorPartitionTraits.build(table).getPartitionNames().size();
        Assertions.assertTrue(tablePartitions > 2);
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, table.getUUID());
        ConnectorAnalyzeTask task = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey"));
        new MockUp<ExternalFullStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
                // do nothing
            }
        };
        int oldLimit = Config.statistic_sample_collect_partition_size;
        Config.statistic_sample_collect_partition_size = 2000;
        try {
            Optional<AnalyzeStatus> result = task.run();
            Assertions.assertTrue(result.isPresent());
            ColumnStatsMeta meta = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                    .getExternalTableBasicStatsMeta("hive0", "partitioned_db", "orders").getColumnStatsMeta("o_orderkey");
            // every partition counts as updated on the first run: the whole table was scanned
            Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, meta.getType());

            new MockUp<StatisticUtils>() {
                @Mock
                Set<String> getUpdatedPartitionNames(Table table, LocalDateTime checkTime) {
                    return Sets.newHashSet("o_orderdate=1992-04-19", "o_orderdate=1992-04-18");
                }
            };
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeStatusMap().clear();
            task.executeAnalyze(LocalDateTime.MIN);
            meta = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                    .getExternalTableBasicStatsMeta("hive0", "partitioned_db", "orders").getColumnStatsMeta("o_orderkey");
            Assertions.assertEquals(StatsConstants.AnalyzeType.SAMPLE, meta.getType());
            Assertions.assertEquals(tablePartitions, meta.getAllPartitionSize());
            Assertions.assertTrue(meta.getSampledPartitionsHashValue().isEmpty());
        } finally {
            Config.statistic_sample_collect_partition_size = oldLimit;
        }
    }

    @Test
    public void testSampleTaskRun() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();

        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));
        new MockUp<ExternalSampleStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
                // do nothing
            }
        };

        new MockUp<ExternalFullStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
                // do nothing
            }
        };

        Optional<AnalyzeStatus> result = task1.run();
        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        ExternalAnalyzeStatus externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assertions.assertSame(externalAnalyzeStatusResult.getType(), StatsConstants.AnalyzeType.SAMPLE);

        Config.statistic_sample_collect_partition_size = 2000;
        result = task1.run();
        Assertions.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assertions.assertSame(externalAnalyzeStatusResult.getType(), StatsConstants.AnalyzeType.FULL);
        Config.statistic_sample_collect_partition_size = 1000;

        // mock only two partitions updated
        new MockUp<StatisticUtils>() {
            @Mock
            Set<String> getUpdatedPartitionNames(Table table, LocalDateTime checkTime) {
                return Sets.newHashSet("o_orderdate=1992-04-19", "o_orderdate=1992-04-18");
            }
        };
        result = task1.run();
        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assertions.assertSame(externalAnalyzeStatusResult.getType(), StatsConstants.AnalyzeType.SAMPLE);
    }

    @Test
    public void testAnIncompleteColumnIsCollectedAgainEvenOnAQuietTable() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        new MockUp<ExternalFullStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
            }
        };
        // The table has not been written since the statistics were collected, which normally means there
        // is nothing to do.
        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return LocalDateTime.now().minusDays(1);
            }
        };

        // A complete column is left alone, as before.
        putColumnMeta(tableUUID, "o_orderkey", 100, 100);
        Assertions.assertTrue(new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey")).run().isEmpty());

        // A column whose last collection reached only part of what it asked for is collected again: a
        // table that is never written to again would otherwise keep its incomplete statistics forever.
        putColumnMeta(tableUUID, "o_orderkey", 40, 100);
        Optional<AnalyzeStatus> result = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey")).run();
        Assertions.assertTrue(result.isPresent());
    }

    private static void putColumnMeta(String tableUUID, String column, int collected, int requested) {
        Set<Long> collectedPartitions = new HashSet<>();
        for (int i = 0; i < collected; i++) {
            collectedPartitions.add((long) i);
        }
        ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta("hive0", "partitioned_db", "orders",
                Lists.newArrayList(column), StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), Maps.newHashMap());
        meta.setTableUUID(tableUUID);
        meta.addColumnStatsMeta(new ColumnStatsMeta(column, StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(),
                collectedPartitions, requested, requested));
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddExternalBasicStatsMeta(meta);
    }

    @Test
    public void testUnreadableUpdateTimeDoesNotStopTheWholeTableFromBeingTriggered() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();

        // One column carries a timestamp that cannot be parsed - an unset thrift field reaches this as
        // null. Letting the parse throw would abandon the whole batch, so no column of this table would
        // ever be triggered, and the exception would disappear into the async callback that calls this.
        Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> columnStats = Maps.newHashMap();
        columnStats.put(new ConnectorTableColumnKey(tableUUID, "o_orderkey"),
                Optional.of(new ConnectorTableColumnStats(ColumnStatistic.unknown(), 100, null)));
        columnStats.put(new ConnectorTableColumnKey(tableUUID, "o_custkey"),
                Optional.of(new ConnectorTableColumnStats(ColumnStatistic.unknown(), 100, "not a timestamp")));

        ConnectorTableTriggerAnalyzeMgr mgr = new ConnectorTableTriggerAnalyzeMgr();
        Assertions.assertDoesNotThrow(() -> mgr.checkAndUpdateTableStats(columnStats));
        // Both columns are treated as due rather than silently dropped.
        Assertions.assertEquals(1, mgr.getConnectorAnalyzeTaskQueue().getPendingTaskSize());
    }

    @Test
    public void testFollowerForwardsTriggeredColumnsToLeader() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> columnStats = Maps.newHashMap();
        columnStats.put(new ConnectorTableColumnKey(tableUUID, "o_orderkey"), Optional.empty());

        List<TTriggerExternalAnalyzeRequest> sent = Lists.newArrayList();
        ConnectorTableTriggerAnalyzeMgr mgr = new ConnectorTableTriggerAnalyzeMgr() {
            @Override
            TTriggerExternalAnalyzeResponse sendToLeader(TTriggerExternalAnalyzeRequest request) throws TException {
                sent.add(request);
                if (sent.size() > 1) {
                    throw new TException("leader unreachable");
                }
                return new TTriggerExternalAnalyzeResponse();
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return false;
            }
        };

        // a follower cannot run the analyze: it buffers the columns instead of queueing a task nobody drains
        mgr.checkAndUpdateTableStats(columnStats);
        Assertions.assertEquals(0, mgr.getConnectorAnalyzeTaskQueue().getPendingTaskSize());
        Assertions.assertEquals(1, mgr.getTriggeredColumnsSize());

        // and sends them to the leader on the next round
        mgr.forwardTriggeredColumnsToLeader();
        Assertions.assertEquals(1, sent.size());
        Assertions.assertEquals(tableUUID, sent.get(0).getTables().get(0).getTable_uuid());
        Assertions.assertEquals(List.of("o_orderkey"), sent.get(0).getTables().get(0).getColumns());
        Assertions.assertEquals(0, mgr.getTriggeredColumnsSize());

        // nothing buffered, nothing sent
        mgr.forwardTriggeredColumnsToLeader();
        Assertions.assertEquals(1, sent.size());

        // a lost request is dropped, the next query raises it again
        mgr.checkAndUpdateTableStats(columnStats);
        mgr.forwardTriggeredColumnsToLeader();
        Assertions.assertEquals(2, sent.size());
        Assertions.assertEquals(0, mgr.getTriggeredColumnsSize());
    }

    @Test
    public void testLeaderQueuesForwardedColumns() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        ConnectorTableTriggerAnalyzeMgr mgr = new ConnectorTableTriggerAnalyzeMgr();
        mgr.addTriggeredColumns(tableUUID, List.of("o_orderkey"));
        mgr.addTriggeredColumns(tableUUID, List.of("o_custkey"));
        mgr.addTriggeredColumns("hive0.partitioned_db.no_such_table.1", List.of("c1"));
        Assertions.assertEquals(2, mgr.getTriggeredColumnsSize());

        mgr.queueTriggeredColumns();
        Assertions.assertEquals(0, mgr.getTriggeredColumnsSize());
        // the unknown table is skipped, the two requests for orders are one task
        Assertions.assertEquals(1, mgr.getConnectorAnalyzeTaskQueue().getPendingTaskSize());
    }

    @Test
    public void testForwardBufferBoundAndRejectedResponse() {
        long oldMax = Config.connector_table_query_trigger_analyze_max_pending_task_num;
        Config.connector_table_query_trigger_analyze_max_pending_task_num = 1;
        try {
            List<TTriggerExternalAnalyzeRequest> sent = Lists.newArrayList();
            ConnectorTableTriggerAnalyzeMgr mgr = new ConnectorTableTriggerAnalyzeMgr() {
                @Override
                TTriggerExternalAnalyzeResponse sendToLeader(TTriggerExternalAnalyzeRequest request) {
                    sent.add(request);
                    TTriggerExternalAnalyzeResponse response = new TTriggerExternalAnalyzeResponse();
                    TStatus status = new TStatus(TStatusCode.CANCELLED);
                    status.addToError_msgs("current node is not leader");
                    response.setStatus(status);
                    return response;
                }
            };
            // nothing to buffer
            mgr.addTriggeredColumns(null, List.of("c1"));
            mgr.addTriggeredColumns("t1", List.of());
            Assertions.assertEquals(0, mgr.getTriggeredColumnsSize());

            // columns of a buffered table still merge when the buffer is full, a new table is dropped
            mgr.addTriggeredColumns("t1", List.of("c1"));
            mgr.addTriggeredColumns("t1", List.of("c2"));
            mgr.addTriggeredColumns("t2", List.of("c1"));
            Assertions.assertEquals(1, mgr.getTriggeredColumnsSize());

            // a rejected request is dropped like a failed one
            mgr.forwardTriggeredColumnsToLeader();
            Assertions.assertEquals(1, sent.size());
            Assertions.assertEquals(Set.of("c1", "c2"), new HashSet<>(sent.get(0).getTables().get(0).getColumns()));
            Assertions.assertEquals(0, mgr.getTriggeredColumnsSize());
        } finally {
            Config.connector_table_query_trigger_analyze_max_pending_task_num = oldMax;
        }
    }

    @Test
    public void testTriggerExternalAnalyzeRpc() throws TException {
        Map<String, Collection<String>> received = Maps.newHashMap();
        new MockUp<ConnectorTableTriggerAnalyzeMgr>() {
            @Mock
            public void addTriggeredColumns(String tableUUID, Collection<String> columns) {
                received.put(tableUUID, columns);
            }
        };
        TTriggerExternalAnalyzeTable t = new TTriggerExternalAnalyzeTable();
        t.setTable_uuid("t1");
        t.setColumns(List.of("c1", "c2"));
        TTriggerExternalAnalyzeRequest request = new TTriggerExternalAnalyzeRequest();
        request.addToTables(t);
        FrontendServiceImpl impl = new FrontendServiceImpl(null);

        // the leader buffers the request
        TTriggerExternalAnalyzeResponse response = impl.triggerExternalAnalyze(request);
        Assertions.assertEquals(TStatusCode.OK, response.getStatus().getStatus_code());
        Assertions.assertEquals(Map.of("t1", List.of("c1", "c2")), received);

        // an empty request is fine
        received.clear();
        response = impl.triggerExternalAnalyze(new TTriggerExternalAnalyzeRequest());
        Assertions.assertEquals(TStatusCode.OK, response.getStatus().getStatus_code());
        Assertions.assertTrue(received.isEmpty());

        // a node that is no longer the leader rejects it
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return false;
            }
        };
        response = impl.triggerExternalAnalyze(request);
        Assertions.assertEquals(TStatusCode.CANCELLED, response.getStatus().getStatus_code());
        Assertions.assertTrue(received.isEmpty());
    }
}
