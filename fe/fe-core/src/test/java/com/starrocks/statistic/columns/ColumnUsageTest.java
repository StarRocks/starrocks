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

package com.starrocks.statistic.columns;

import com.google.common.base.Splitter;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.NativeAnalyzeJob;
import com.starrocks.statistic.StatisticsCollectJob;
import com.starrocks.statistic.StatisticsCollectJobFactory;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

class ColumnUsageTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() {
        StatisticsMetaManager statistic = new StatisticsMetaManager();
        statistic.createStatisticsTablesForTest();
        TableKeeper keeper = PredicateColumnsStorage.createKeeper();
        keeper.run();
        FeConstants.runningUnitTest = true;
        Config.statistic_partition_healthy_v2 = false;
    }

    @BeforeEach
    public void before() {
        PredicateColumnsMgr.getInstance().reset();
    }

    @Test
    public void testColumnUsage() throws Exception {
        // normal predicate
        starRocksAssert.query("select * from t0 where v1 > 1").explainQuery();
        starRocksAssert.query("select * from information_schema.column_stats_usage where table_name = 't0'")
                .explainContains("constant exprs", "'v1' | 'normal,predicate'");

        starRocksAssert.query("select * from test_all_type where lower(t1a) = '123' and t1e < 1.1").explainQuery();
        starRocksAssert.query("select * from information_schema.column_stats_usage where table_name = 'test_all_type'")
                .explainContains("constant exprs", "'t1a' | 'normal,predicate'");

        // group by
        starRocksAssert.query("select v2, v3, count(*) from t0 group by v2, v3").explainQuery();
        starRocksAssert.query("select * from information_schema.column_stats_usage where table_name = 't0'")
                .explainContains("constant exprs", "'v1' | 'normal,predicate'");

        // join
        starRocksAssert.query("select * from t0 join t1 on t0.v2 = t1.v4").explainQuery();
        starRocksAssert.query("select * from information_schema.column_stats_usage where table_name = 't0'")
                .explainContains(" 'v3' | 'normal,group_by'", "'v2' | 'normal,predicate,join,group_by'",
                        "'v1' | 'normal,predicate'");
        starRocksAssert.query("select * from information_schema.column_stats_usage where table_name = 't1'")
                .explainContains("constant exprs", "'v4' | 'normal,predicate,join'");
    }

    @ParameterizedTest
    @CsvSource(delimiterString = "|", value = {
            // empty query with various analyze syntax
            "|analyze table t0 predicate columns| ",
            "|analyze table t0 all columns|v1,v2,v3",
            "|analyze table t0(v1,v3)|v1,v3",

            // simple query
            "select * from t0 where v1 > 1|analyze table t0 predicate columns|v1",
            "select * from t0 where v1 > 1 and v2 < 10|analyze table t0 predicate columns|v2,v1",
            "select * from t0 where v1 > 1 and v2 < 10|analyze table t0 update histogram on predicate columns|v2,v1",
            "select * from t0 order by v1 limit 1000|analyze table t0 predicate columns|",
            "select min(v1), max(v2), count(v3) from t0|analyze table t0 predicate columns|",
            "select case when v1 > 1 then v1 else 'small' end as v1_case, " +
                    "count(*) from t0 group by 1" +
                    "|analyze table t0 predicate columns|v1",

            // expressions
            "select * from t0 where abs(v1) > 1|analyze table t0 predicate columns|v1",
            "select * from t0 where abs(v1) > v2 + 100|analyze table t0 predicate columns|v2,v1",
            "select * from t0 where cast(v1 as string) = 'a'|analyze table t0 predicate columns|v1",

            // complex query:
            // multi-stage aggregation
            "select v1, count(*) from t0 group by v1|analyze table t0 predicate columns|v1",
            "select v1,v2, count(*) from t0 group by v1,v2|analyze table t0 predicate columns|v2,v1",
            "select v1, count(distinct v2) from t0 group by v1|analyze table t0 predicate columns|v2,v1",
            "select count(distinct v1), count(distinct v2), count(distinct v3) from t0 group by v1" +
                    "|analyze table t0 predicate columns|v3,v2,v1",

            "select count(distinct v1) from (select * from t0 order by v1 limit 100) r" +
                    "|analyze table t0 predicate columns|v1",
            "select v1, count(v2) from (select * from t0 order by v1 limit 100) r group by v1" +
                    "|analyze table t0 predicate columns|v1",
            "select case when get_json_string(v_json, 'a') > 1 " +
                    "   then get_json_string(v_json, 'b') else 'small' end as v1_case, " +
                    "count(*) from tjson " +
                    "group by 1" +
                    "|analyze table tjson predicate columns|v_json",
            "select case when get_json_string(vvv, 'a') > 1 " +
                    "   then get_json_string(vvv, 'b') else 'small' end as v1_case, " +
                    "count(*) from (" +
                    "   select json_object('a', get_json_string(v_json, 'a'), " +
                    "               'b', get_json_int(v_json, 'b')) as vvv " +
                    "   from tjson) r " +
                    "group by 1" +
                    "|analyze table tjson predicate columns|v_json",

            // with join
            "select * from t0 join t1 on t0.v1 = t1.v4" +
                    "|analyze table t0 predicate columns|v1",
            "select * from t0 join t1 on t0.v1 = t1.v4" +
                    "|analyze table t1 predicate columns|v4",
            "select v4,count(*) from (select *  from t0 join t1 on t0.v1 = t1.v4 ) r group by v4" +
                    "|analyze table t0 predicate columns|v1",
            "select v4,count(*) from (select *  from t0 join t1 on t0.v1 = t1.v4 ) r group by v4" +
                    "|analyze table t1 predicate columns|v4",
            "select v2 + v5 as k, count(*) from (select * from t0 join t1 on t0.v1 = t1.v4 ) r group by 1" +
                    "|analyze table t1 predicate columns|v5,v4",
            "select v2 + v5 as k, count(*) from (select * from t0 join t1 on t0.v1 = t1.v4 ) r group by 1" +
                    "|analyze table t0 predicate columns|v2,v1",

            // window
            "select max(v1) over (partition by v2 order by v3) from t0" +
                    "|analyze table t0 predicate columns|v2",

            // union
            "select max(v1) from (select v1,v2 from t0 union all select v4,v5 from t1) r group by v2" +
                    "| analyze table t0 predicate columns | v2",
            "select max(v1) from (select v1,v2 from t0 union all select v4,v5 from t1) r group by v2" +
                    "| analyze table t1 predicate columns | v5",

            // cte
            "with cte1 as (select max(v1) v1 from t0 group by v2), cte2 as (select max(v4) v4 from t1 group by v5) " +
                    "select * from cte1 join cte2 on cte1.v1 = cte2.v4 " +
                    "| analyze table t0 predicate columns | v2",
            "with cte1 as (select max(v1) v1 from t0 group by v2), cte2 as (select max(v4) v4 from t1 group by v5) " +
                    "select * from cte1 join cte2 on cte1.v1 = cte2.v4 " +
                    "| analyze table t1 predicate columns | v5",
    })
    public void testAnalyzePredicateColumns(String query, String analyzeStmt, String expectedColumns) throws Exception {
        AnalyzeTestUtil.init();
        if (StringUtils.isNotEmpty(query)) {
            starRocksAssert.query(query).explainQuery();
        }
        AnalyzeStmt stmt = (AnalyzeStmt) AnalyzeTestUtil.analyzeSuccess(analyzeStmt);
        List<String> expect =
                StringUtils.isNotEmpty(expectedColumns) ? Splitter.on(",").splitToList(expectedColumns) : List.of();
        Assertions.assertEquals(expect.stream().sorted().collect(Collectors.toList()),
                stmt.getColumnNames().stream().sorted().collect(Collectors.toList()));
    }

    @Test
    public void testAutoAnalyzePredicateColumns() throws Exception {
        AnalyzeTestUtil.init();
        Table table = starRocksAssert.getTable(connectContext.getDatabase(), "t0");
        setTableStatistics((OlapTable) table, Config.statistic_auto_collect_small_table_rows + 1);
        setPartitionStatistics((OlapTable) table, "t0", Config.statistic_auto_collect_small_table_rows + 1);

        starRocksAssert.query("select * from t0 where v1 > 1").explainQuery();
        starRocksAssert.getCtx().executeSql("analyze table t0 predicate columns with sync mode");

        String analyzeSql = "create analyze table t0";
        starRocksAssert.ddl(analyzeSql);

        List<AnalyzeJob> allAnalyzeJobList =
                starRocksAssert.getCtx().getGlobalStateMgr().getAnalyzeMgr().getAllAnalyzeJobList()
                        .stream().filter(x -> {
                            try {
                                return x.getTableName().equalsIgnoreCase("t0");
                            } catch (MetaNotFoundException e) {
                                return false;
                            }
                        })
                        .collect(Collectors.toList());
        NativeAnalyzeJob analyzeJob = (NativeAnalyzeJob) allAnalyzeJobList.get(0);
        AnalyzeMgr analyzeMgr = GlobalStateMgr.getCurrentState().getAnalyzeMgr();

        // mock a small table, with staled stats
        LocalDateTime mockUpdate = LocalDateTime.now().minusDays(1);
        BasicStatsMeta statsMeta = analyzeMgr.getTableBasicStatsMeta(table.getId());
        statsMeta = Mockito.mock(BasicStatsMeta.class);
        Mockito.when(statsMeta.getTableId()).thenReturn(table.getId());
        analyzeMgr.addBasicStatsMeta(statsMeta);

        Mockito.when(statsMeta.getUpdateTime()).thenReturn(mockUpdate);
        Mockito.when(statsMeta.isUpdatedAfterLoad(Mockito.any())).thenReturn(false);
        Mockito.when(statsMeta.getHealthy()).thenReturn(0.1);

        // enable the predicate-columns strategy
        {
            int defaultValue = Config.statistic_auto_collect_predicate_columns_threshold;
            Config.statistic_auto_collect_predicate_columns_threshold = 2;

            List<StatisticsCollectJob> collectJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(analyzeJob);
            Assertions.assertEquals(1, collectJobs.size());
            StatisticsCollectJob job0 = collectJobs.get(0);
            Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, job0.getType());
            Assertions.assertEquals(List.of("v1"), job0.getColumnNames());

            Config.statistic_auto_collect_predicate_columns_threshold = defaultValue;
        }

        // disable the strategy
        {
            int defaultValue = Config.statistic_auto_collect_predicate_columns_threshold;
            Config.statistic_auto_collect_predicate_columns_threshold = 0;
            List<StatisticsCollectJob> collectJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(analyzeJob);
            Assertions.assertEquals(1, collectJobs.size());
            StatisticsCollectJob job0 = collectJobs.get(0);
            Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, job0.getType());
            Assertions.assertEquals(List.of("v1", "v2", "v3"), job0.getColumnNames());
            Config.statistic_auto_collect_predicate_columns_threshold = defaultValue;
        }
    }
}