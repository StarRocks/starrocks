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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TStatisticData;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class StatisticsExecutorTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);

        starRocksAssert.withTable("CREATE TABLE `t0_stats` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL,\n" +
                "  `v4` date NULL,\n" +
                "  `v5` datetime NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0_stats");
        Partition partition = new ArrayList<>(t0.getPartitions()).get(0);
        partition.getDefaultPhysicalPartition()
                .updateVisibleVersion(2, LocalDateTime.of(2022, 1, 1, 1, 1, 1)
                        .atZone(Clock.systemDefaultZone().getZone()).toEpochSecond() * 1000);
        setTableStatistics(t0, 20000000);
    }

    @Test
    public void testCollectStatisticSync(@Mocked StmtExecutor executor) throws Exception {
        // mock
        MockUp<StmtExecutor> mock = new MockUp<StmtExecutor>() {
            @Mock
            public void execute() {
            }
        };

        Database database = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t0_stats");
        List<Long> partitionIdList =
                table.getAllPartitions().stream().map(Partition::getId).collect(Collectors.toList());

        SampleStatisticsCollectJob collectJob = new SampleStatisticsCollectJob(database, table,
                Lists.newArrayList("v1", "v2", "v3", "v4", "v5"),
                StatsConstants.AnalyzeType.SAMPLE,
                StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap()
        );

        String sql = "insert into test.t0 values(1,2,3)";
        ConnectContext context = StatisticUtils.buildConnectContext();

        QueryState errorState = new QueryState();
        errorState.setStateType(QueryState.MysqlStateType.ERR);
        errorState.setError("Too many versions");

        QueryState okState = new QueryState();
        okState.setStateType(QueryState.MysqlStateType.OK);

        new Expectations(context) {
            {
                context.getState().getStateType();
                result = QueryState.MysqlStateType.ERR;

                context.getState().getErrorMessage();
                result = "Error";
            }
        };

        Assert.assertThrows(DdlException.class, () -> collectJob.collectStatisticSync(sql, context));

        new Expectations(context) {
            {
                context.getState().getStateType();
                result = QueryState.MysqlStateType.OK;
            }
        };

        collectJob.collectStatisticSync(sql, context);
    }

    @Test
    public void testQueryStatisticSync() throws AnalysisException {
        String res;
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) {
                Assert.assertEquals(
                        "SELECT cast(8 as INT), column_name, sum(row_count), cast(sum(data_size) as bigint), " +
                                "hll_union_agg(ndv), sum(null_count),  cast(max(cast(max as string)) as string), " +
                                "cast(min(cast(min as string)) as string), max(update_time) FROM external_column_statistics " +
                                "WHERE table_uuid = \"hive0.partitioned_db.t1.0\" " +
                                "and column_name in (\"c2\") GROUP BY table_uuid, column_name UNION ALL " +
                                "SELECT cast(8 as INT), column_name, sum(row_count), cast(sum(data_size) as bigint), " +
                                "hll_union_agg(ndv), sum(null_count),  cast(max(cast(max as bigint)) as string), " +
                                "cast(min(cast(min as bigint)) as string), max(update_time) " +
                                "FROM external_column_statistics WHERE table_uuid = \"hive0.partitioned_db.t1.0\"" +
                                " and column_name in (\"c1\") GROUP BY table_uuid, column_name", sql);
                return Lists.newArrayList();
            }
        };

        ConnectContext context = StatisticUtils.buildConnectContext();
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "partitioned_db",
                "t1");
        String tableUUID = table.getUUID();
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.queryStatisticSync(context, tableUUID, table, ImmutableList.of("c1", "c2"));
    }

    public static StatementBase parseSql(String originStmt) {
        return SqlParser.parse(originStmt, connectContext.getSessionVariable()).get(0);
    }

    public static StatementBase analyzeSuccess(String originStmt) {
        try {
            StatementBase statementBase = parseSql(originStmt);
            Analyzer.analyze(statementBase, connectContext);

            return statementBase;
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail();
            throw ex;
        }
    }

    @Test
    public void testExecuteAnalyze(@Mocked StatisticExecutor statisticExecutor) {
        ExternalAnalyzeStatus status = new ExternalAnalyzeStatus(1, "test_catalog",
                "test_db", "test_table",
                "test123", Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(), LocalDateTime.MIN);
        new Expectations() {
            {
                statisticExecutor.collectStatistics((ConnectContext) any, (StatisticsCollectJob) any,
                        (AnalyzeStatus) any,
                        anyBoolean);
                minTimes = 0;
                result = status;
            }
        };
        String sql = "analyze table hive0.partitioned_db.t1 update histogram on c1";
        AnalyzeStmt stmt = (AnalyzeStmt) analyzeSuccess(sql);
        StmtExecutor executor = new StmtExecutor(connectContext, stmt);
        AnalyzeStatus pendingStatus = new ExternalAnalyzeStatus(1, "", "", "",
                "test123", Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(), LocalDateTime.MIN);

        Database db = connectContext.getGlobalStateMgr().getMetadataMgr().getDb(connectContext, "hive0", "partitioned_db");
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "partitioned_db", "t1");

        Deencapsulation.invoke(executor, "executeAnalyze", connectContext, stmt, pendingStatus, db, table);
        Assert.assertTrue(stmt.isExternal());
        Assert.assertTrue(stmt.getAnalyzeTypeDesc().isHistogram());

        sql = "analyze table hive0.partitioned_db.t1";
        stmt = (AnalyzeStmt) analyzeSuccess(sql);
        executor = new StmtExecutor(connectContext, stmt);
        pendingStatus = new ExternalAnalyzeStatus(1, "", "", "",
                "test123", Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(), LocalDateTime.MIN);
        Deencapsulation.invoke(executor, "executeAnalyze", connectContext, stmt, pendingStatus, db, table);
        Assert.assertTrue(stmt.isExternal());
        Assert.assertFalse(stmt.getAnalyzeTypeDesc().isHistogram());
    }

    @Test
    public void testCollectStatistics() {
        ExternalAnalyzeStatus status = new ExternalAnalyzeStatus(1, "test_catalog",
                "test_db", "test_table",
                "test123", Lists.newArrayList("col1", "col2"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.MIN);

        Database database = new Database(1, "test_db");
        Table table = HiveTable.builder().setTableName("test_table").build();
        StatisticsCollectJob statisticsCollectJob = new ExternalFullStatisticsCollectJob("test_catalog",
                database, table, List.of(), Lists.newArrayList("col1", "col2"),
                Lists.newArrayList(Type.INT, Type.INT),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

        new MockUp<ExternalFullStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
            }
        };

        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.collectStatistics(connectContext, statisticsCollectJob, status, false);

        ExternalBasicStatsMeta externalBasicStatsMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().
                getExternalTableBasicStatsMeta("test_catalog", "test_db", "test_table");
        Assert.assertEquals(externalBasicStatsMeta.getColumnStatsMetaMap().size(), 2);
        Assert.assertTrue(externalBasicStatsMeta.getColumnStatsMetaMap().containsKey("col1"));
        Assert.assertTrue(externalBasicStatsMeta.getColumnStatsMetaMap().containsKey("col2"));

        status = new ExternalAnalyzeStatus(1, "test_catalog",
                "test_db", "test_table",
                "test123", Lists.newArrayList("col1", "col3"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.MIN);
        statisticsCollectJob = new ExternalFullStatisticsCollectJob("test_catalog",
                database, table, List.of(), Lists.newArrayList("col1", "col3"),
                Lists.newArrayList(Type.INT, Type.STRING),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());
        statisticExecutor.collectStatistics(connectContext, statisticsCollectJob, status, false);
        externalBasicStatsMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().
                getExternalTableBasicStatsMeta("test_catalog", "test_db", "test_table");
        Assert.assertEquals(externalBasicStatsMeta.getColumns(), Lists.newArrayList("col1", "col3"));
        Assert.assertEquals(externalBasicStatsMeta.getColumnStatsMetaMap().size(), 3);
    }

    @Test
    public void testDropHistogramStmt() {
        new MockUp<StatisticExecutor>() {
            @Mock
            public void dropExternalHistogram(ConnectContext statsConnectCtx, String tableUUID,
                                              List<String> columnNames) {
                return;
            }
        };
        String sql = "analyze table hive0.partitioned_db.t1 drop histogram on c1";
        DropHistogramStmt stmt = (DropHistogramStmt) analyzeSuccess(sql);
        StmtExecutor executor = new StmtExecutor(connectContext, stmt);
        Deencapsulation.invoke(executor, "handleDropHistogramStmt");
        Assert.assertTrue(stmt.isExternal());

        sql = "analyze table test.t0_stats drop histogram on v1, v2";
        stmt = (DropHistogramStmt) analyzeSuccess(sql);
        executor = new StmtExecutor(connectContext, stmt);
        Deencapsulation.invoke(executor, "handleDropHistogramStmt");
        Assert.assertFalse(stmt.isExternal());
    }

    @Test
    public void testSessionVariableInStats() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setStatisticCollectParallelism(5);
        context.setThreadLocalInfo();

        ConnectContext statsContext = StatisticUtils.buildConnectContext();
        Assert.assertEquals(1, statsContext.getSessionVariable().getParallelExecInstanceNum());
    }
}
