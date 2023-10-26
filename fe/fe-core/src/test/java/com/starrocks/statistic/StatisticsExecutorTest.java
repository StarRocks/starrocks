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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
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

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0_stats");
        Partition partition = new ArrayList<>(t0.getPartitions()).get(0);
        partition.updateVisibleVersion(2, LocalDateTime.of(2022, 1, 1, 1, 1, 1)
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

        Database database = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) database.getTable("t0_stats");
        List<Long> partitionIdList =
                table.getAllPartitions().stream().map(Partition::getId).collect(Collectors.toList());

        SampleStatisticsCollectJob collectJob = new SampleStatisticsCollectJob(database, table,
                Lists.newArrayList("v1", "v2", "v3", "v4", "v5"),
                StatsConstants.AnalyzeType.SAMPLE,
                StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap());

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
    public void testQueryStatisticSync() {
        String res;
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) {
                Assert.assertEquals("SELECT cast(6 as INT), column_name, sum(row_count), cast(sum(data_size) as bigint), " +
                            "hll_union_agg(ndv), sum(null_count),  cast(max(cast(max as int(11))) as string), " +
                            "cast(min(cast(min as int(11))) as string) " +
                            "FROM external_column_statistics " +
                            "WHERE table_uuid = \"hive0.partitioned_db.t1.0\" " +
                            "and column_name = \"c1\" " +
                            "GROUP BY table_uuid, column_name UNION ALL " +
                            "SELECT cast(6 as INT), column_name, sum(row_count), cast(sum(data_size) as bigint), " +
                            "hll_union_agg(ndv), sum(null_count),  cast(max(cast(max as varchar(1073741824))) as string), " +
                            "cast(min(cast(min as varchar(1073741824))) as string) " +
                            "FROM external_column_statistics " +
                            "WHERE table_uuid = \"hive0.partitioned_db.t1.0\"" +
                            " and column_name = \"c2\" " +
                            "GROUP BY table_uuid, column_name", sql);
                return Lists.newArrayList();
            }
        };

        ConnectContext context = StatisticUtils.buildConnectContext();
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db",
                "t1");
        String tableUUID = table.getUUID();
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.queryStatisticSync(context, tableUUID, table, ImmutableList.of("c1", "c2"));
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
