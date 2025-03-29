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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.ColumnStatsMeta;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.ExternalFullStatisticsCollectJob;
import com.starrocks.statistic.ExternalSampleStatisticsCollectJob;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.UtFrameUtils;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ConnectorAnalyzeTaskTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(ctx);
    }

    @After
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
        Assert.assertEquals(3, task1.getColumns().size());
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
        Assert.assertTrue(result.isEmpty());

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
        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        ExternalAnalyzeStatus externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assert.assertEquals(List.of("o_orderkey"), externalAnalyzeStatusResult.getColumns());
    }

    @Test
    public void testTaskRunWithStructSubfield() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "subfield_db", "subfield");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("col_int", "col_struct.c0"));
        Optional<AnalyzeStatus> result = task.run();
        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        ExternalAnalyzeStatus externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assert.assertEquals(List.of("col_int", "col_struct.c0"), externalAnalyzeStatusResult.getColumns());
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
        Assert.assertTrue(result.isPresent());
        Map<String, ColumnStatsMeta>  columnStatsMetaMap = GlobalStateMgr.getCurrentState().getAnalyzeMgr().
                getExternalTableBasicStatsMeta("hive0", "partitioned_db", "orders").getColumnStatsMetaMap();
        Assert.assertEquals(2, columnStatsMetaMap.size());
        Assert.assertTrue(columnStatsMetaMap.containsKey("o_orderkey"));
        Assert.assertTrue(columnStatsMetaMap.containsKey("o_custkey"));

        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return LocalDateTime.now().minusDays(1);
            }
        };
        // table not update, skip analyze
        task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));
        result = task1.run();
        Assert.assertTrue(result.isEmpty());

        // table update, analyze again
        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return LocalDateTime.now().plusMinutes(10);
            }
        };

        task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));
        result = task1.run();
        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        ExternalAnalyzeStatus externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assert.assertEquals(2, externalAnalyzeStatusResult.getColumns().size());
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
        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        ExternalAnalyzeStatus externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assert.assertSame(externalAnalyzeStatusResult.getType(), StatsConstants.AnalyzeType.SAMPLE);

        Config.statistic_sample_collect_partition_size = 2000;
        result = task1.run();
        Assert.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assert.assertSame(externalAnalyzeStatusResult.getType(), StatsConstants.AnalyzeType.FULL);
        Config.statistic_sample_collect_partition_size = 1000;

        // mock only two partitions updated
        new MockUp<StatisticUtils>() {
            @Mock
            Set<String> getUpdatedPartitionNames(Table table, LocalDateTime checkTime) {
                return Sets.newHashSet("o_orderdate=1992-04-19", "o_orderdate=1992-04-18");
            }
        };
        result = task1.run();
        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get() instanceof ExternalAnalyzeStatus);
        externalAnalyzeStatusResult = (ExternalAnalyzeStatus) result.get();
        Assert.assertSame(externalAnalyzeStatusResult.getType(), StatsConstants.AnalyzeType.SAMPLE);
    }
}
