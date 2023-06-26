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


package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.FullStatisticsCollectJob;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.statistic.StatisticSQLBuilder;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.regex.Pattern;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getConnectContext;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeStmtTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = getStarRocksAssert();

        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);

        createTblStmtStr = "create table db.tb2(kk1 int, kk2 json) "
                + "DUPLICATE KEY(kk1) distributed by hash(kk1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testAllColumns() {
        String sql = "analyze table db.tbl";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertNull(analyzeStmt.getColumnNames());
    }

    @Test
    public void testShowUserProperty() {
        String sql = "SHOW PROPERTY FOR 'jack' LIKE '%load_cluster%'";
        ShowUserPropertyStmt showUserPropertyStmt = (ShowUserPropertyStmt) analyzeSuccess(sql);
        Assert.assertEquals("jack", showUserPropertyStmt.getUser());
    }

    @Test
    public void testSetUserProperty() {
        String sql = "SET PROPERTY FOR 'tom' 'max_user_connections' = 'value', 'test' = 'true'";
        SetUserPropertyStmt setUserPropertyStmt = (SetUserPropertyStmt) analyzeSuccess(sql);
        Assert.assertEquals("tom", setUserPropertyStmt.getUser());
    }

    @Test
    public void testSelectedColumns() {
        String sql = "analyze table db.tbl (kk1, kk2)";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);

        Assert.assertTrue(!analyzeStmt.isSample());
        Assert.assertEquals(2, analyzeStmt.getColumnNames().size());

        sql = "analyze table test.t0";
        analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertNull(analyzeStmt.getColumnNames());
    }

    @Test
    public void testProperties() {
        String sql = "analyze full table db.tbl properties('expire_sec' = '30')";
        analyzeFail(sql, "Property 'expire_sec' is not valid");

        sql = "analyze full table db.tbl";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertFalse(analyzeStmt.isAsync());

        sql = "analyze full table db.tbl with sync mode";
        analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertFalse(analyzeStmt.isAsync());

        sql = "analyze full table db.tbl with async mode";
        analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertTrue(analyzeStmt.isAsync());
    }

    @Test
    public void testShow() throws MetaNotFoundException {
        String sql = "show analyze";
        ShowAnalyzeJobStmt showAnalyzeJobStmt = (ShowAnalyzeJobStmt) analyzeSuccess(sql);
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = testDb.getTable("t0");

        AnalyzeJob analyzeJob = new AnalyzeJob(testDb.getId(), table.getId(), Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.FINISH, LocalDateTime.MIN);
        Assert.assertEquals("[-1, test, t0, ALL, FULL, ONCE, {}, FINISH, None, ]",
                ShowAnalyzeJobStmt.showAnalyzeJobs(getConnectContext(), analyzeJob).toString());

        sql = "show analyze job";
        showAnalyzeJobStmt = (ShowAnalyzeJobStmt) analyzeSuccess(sql);

        sql = "show analyze status";
        ShowAnalyzeStatusStmt showAnalyzeStatusStatement = (ShowAnalyzeStatusStmt) analyzeSuccess(sql);

        AnalyzeStatus analyzeStatus = new NativeAnalyzeStatus(-1, testDb.getId(), table.getId(), Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.of(
                        2020, 1, 1, 1, 1));
        analyzeStatus.setEndTime(LocalDateTime.of(2020, 1, 1, 1, 1));
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
        analyzeStatus.setReason("Test Failed");
        Assert.assertEquals("[-1, default_catalog.test, t0, ALL, FULL, ONCE, FAILED, " +
                        "2020-01-01 01:01:00, 2020-01-01 01:01:00, " +
                        "{}, Test Failed]",
                ShowAnalyzeStatusStmt.showAnalyzeStatus(getConnectContext(), analyzeStatus).toString());

        sql = "show stats meta";
        ShowBasicStatsMetaStmt showAnalyzeMetaStmt = (ShowBasicStatsMetaStmt) analyzeSuccess(sql);

        BasicStatsMeta basicStatsMeta = new BasicStatsMeta(testDb.getId(), table.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1), Maps.newHashMap());
        Assert.assertEquals("[test, t0, ALL, FULL, 2020-01-01 01:01:00, {}, 100%]",
                ShowBasicStatsMetaStmt.showBasicStatsMeta(getConnectContext(), basicStatsMeta).toString());

        sql = "show histogram meta";
        ShowHistogramStatsMetaStmt showHistogramStatsMetaStmt = (ShowHistogramStatsMetaStmt) analyzeSuccess(sql);
        HistogramStatsMeta histogramStatsMeta = new HistogramStatsMeta(testDb.getId(), table.getId(), "v1",
                StatsConstants.AnalyzeType.HISTOGRAM, LocalDateTime.of(
                        2020, 1, 1, 1, 1),
                Maps.newHashMap());
        Assert.assertEquals("[test, t0, v1, HISTOGRAM, 2020-01-01 01:01:00, {}]",
                ShowHistogramStatsMetaStmt.showHistogramStatsMeta(getConnectContext(), histogramStatsMeta).toString());
    }

    @Test
    public void testStatisticsSqlBuilder() throws Exception {
        Database database = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) database.getTable("t0");
        System.out.println(table.getPartitions());
        Partition partition = (new ArrayList<>(table.getPartitions())).get(0);

        Column v1 = table.getColumn("v1");
        Column v2 = table.getColumn("v2");

        Assert.assertEquals(String.format(new String("SELECT cast(1 as INT), now(), " +
                        "db_id, table_id, column_name," +
                        " sum(row_count), " +
                        "cast(sum(data_size) as bigint), hll_union_agg(ndv), sum(null_count),  " +
                        "cast(max(cast(max as bigint(20))) as string), " +
                        "cast(min(cast(min as bigint(20))) as string) FROM column_statistics " +
                        "WHERE table_id = %d and column_name = \"v1\" GROUP BY db_id, table_id, column_name " +
                        "UNION ALL SELECT cast(1 as INT), now(), db_id, table_id, column_name, sum(row_count), " +
                        "cast(sum(data_size) as bigint), " +
                        "hll_union_agg(ndv), sum(null_count),  cast(max(cast(max as bigint(20))) as string), " +
                        "cast(min(cast(min as bigint(20))) as string) " +
                        "FROM column_statistics WHERE table_id = %d and column_name = \"v2\" " +
                        "GROUP BY db_id, table_id, column_name"), table.getId(), table.getId()),
                StatisticSQLBuilder.buildQueryFullStatisticsSQL(database.getId(), table.getId(),
                        Lists.newArrayList(v1, v2)));

        Assert.assertEquals(String.format(
                "SELECT cast(1 as INT), update_time, db_id, table_id, column_name, row_count, " +
                        "data_size, distinct_count, null_count, max, min " +
                        "FROM table_statistic_v1 WHERE db_id = %d and table_id = %d and column_name in ('v1', 'v2')",
                        database.getId(), table.getId()),
                StatisticSQLBuilder.buildQuerySampleStatisticsSQL(database.getId(),
                        table.getId(), Lists.newArrayList("v1", "v2")));

        FullStatisticsCollectJob collectJob = new FullStatisticsCollectJob(database, table,
                Lists.newArrayList(partition.getId()),
                Lists.newArrayList("v1", "v2"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap());
    }

    @Test
    public void testHistogram() {
        String sql = "analyze table t0 update histogram on v1,v2 with 256 buckets";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertTrue(analyzeStmt.getAnalyzeTypeDesc() instanceof AnalyzeHistogramDesc);
        Assert.assertEquals(((AnalyzeHistogramDesc) (analyzeStmt.getAnalyzeTypeDesc())).getBuckets(), 256);

        sql = "analyze table t0 drop histogram on v1";
        DropHistogramStmt dropHistogramStmt = (DropHistogramStmt) analyzeSuccess(sql);
        Assert.assertEquals(dropHistogramStmt.getTableName().toSql(), "`test`.`t0`");
        Assert.assertEquals(dropHistogramStmt.getColumnNames().toString(), "[v1]");
    }

    @Test
    public void testHistogramSampleRatio() {
        OlapTable t0 = (OlapTable) starRocksAssert.getCtx().getGlobalStateMgr()
                .getDb("db").getTable("tbl");
        for (Partition partition : t0.getAllPartitions()) {
            partition.getBaseIndex().setRowCount(10000);
        }

        String sql = "analyze table db.tbl update histogram on kk1 with 256 buckets " +
                "properties(\"histogram_sample_ratio\"=\"0.1\")";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertEquals("1", analyzeStmt.getProperties().get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));

        for (Partition partition : t0.getAllPartitions()) {
            partition.getBaseIndex().setRowCount(400000);
        }

        sql = "analyze table db.tbl update histogram on kk1 with 256 buckets " +
                "properties(\"histogram_sample_ratio\"=\"0.2\")";
        analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertEquals("0.5", analyzeStmt.getProperties().get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));

        for (Partition partition : t0.getAllPartitions()) {
            partition.getBaseIndex().setRowCount(20000000);
        }
        sql = "analyze table db.tbl update histogram on kk1 with 256 buckets " +
                "properties(\"histogram_sample_ratio\"=\"0.9\")";
        analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertEquals("0.5", analyzeStmt.getProperties().get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));
    }

    @Test
    public void testDropStats() {
        String sql = "drop stats t0";
        DropStatsStmt dropStatsStmt = (DropStatsStmt) analyzeSuccess(sql);
        Assert.assertEquals("t0", dropStatsStmt.getTableName().getTbl());

        Assert.assertEquals("DELETE FROM table_statistic_v1 WHERE TABLE_ID = 10004",
                StatisticSQLBuilder.buildDropStatisticsSQL(10004L, StatsConstants.AnalyzeType.SAMPLE));
        Assert.assertEquals("DELETE FROM column_statistics WHERE TABLE_ID = 10004",
                StatisticSQLBuilder.buildDropStatisticsSQL(10004L, StatsConstants.AnalyzeType.FULL));
    }

    @Test
    public void testKillAnalyze() {
        String sql = "kill analyze 1";
        KillAnalyzeStmt killAnalyzeStmt = (KillAnalyzeStmt) analyzeSuccess(sql);

        GlobalStateMgr.getCurrentAnalyzeMgr().registerConnection(1, getConnectContext());
        Assert.assertThrows(SemanticException.class,
                () -> GlobalStateMgr.getCurrentAnalyzeMgr().unregisterConnection(2, true));
        GlobalStateMgr.getCurrentAnalyzeMgr().unregisterConnection(1, true);
        Assert.assertThrows(SemanticException.class,
                () -> GlobalStateMgr.getCurrentAnalyzeMgr().unregisterConnection(1, true));
    }

    @Test
    public void testDropAnalyzeTest() {
        String sql = "drop analyze 10";
        StatementBase stmt = analyzeSuccess(sql);
        try {
            DDLStmtExecutor.execute(stmt, getConnectContext());
        } catch (Exception ignore) {
            Assert.fail();
        }
    }

    @Test
    public void testAnalyzeStatus() throws MetaNotFoundException {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = testDb.getTable("t0");
        AnalyzeStatus analyzeStatus = new NativeAnalyzeStatus(-1, testDb.getId(), table.getId(), Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.of(
                        2020, 1, 1, 1, 1));
        analyzeStatus.setEndTime(LocalDateTime.of(2020, 1, 1, 1, 1));
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.RUNNING);
        Assert.assertEquals(
                "[-1, default_catalog.test, t0, ALL, FULL, ONCE, RUNNING (0%), 2020-01-01 01:01:00, 2020-01-01 01:01:00," +
                " {}, ]", ShowAnalyzeStatusStmt.showAnalyzeStatus(getConnectContext(), analyzeStatus).toString());

        analyzeStatus.setProgress(50);
        Assert.assertEquals(
                "[-1, default_catalog.test, t0, ALL, FULL, ONCE, RUNNING (50%), 2020-01-01 01:01:00, 2020-01-01 01:01:00," +
                " {}, ]", ShowAnalyzeStatusStmt.showAnalyzeStatus(getConnectContext(), analyzeStatus).toString());

        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
        Assert.assertEquals(
                "[-1, default_catalog.test, t0, ALL, FULL, ONCE, SUCCESS, 2020-01-01 01:01:00, 2020-01-01 01:01:00," +
                " {}, ]", ShowAnalyzeStatusStmt.showAnalyzeStatus(getConnectContext(), analyzeStatus).toString());

        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
        Assert.assertEquals(
                "[-1, default_catalog.test, t0, ALL, FULL, ONCE, FAILED, 2020-01-01 01:01:00, 2020-01-01 01:01:00," +
                " {}, ]", ShowAnalyzeStatusStmt.showAnalyzeStatus(getConnectContext(), analyzeStatus).toString());
    }

    @Test
    public void testObjectColumns() {
        Database database = GlobalStateMgr.getCurrentState().getDb("db");
        OlapTable table = (OlapTable) database.getTable("tb2");

        Column kk1 = table.getColumn("kk1");
        Column kk2 = table.getColumn("kk2");

        String pattern = "SELECT cast\\(1 as INT\\), now\\(\\), db_id, table_id, column_name, sum\\(row_count\\), " +
                "cast\\(sum\\(data_size\\) as bigint\\), hll_union_agg\\(ndv\\), sum\\(null_count\\), " +
                " cast\\(max\\(cast\\(max as int\\(11\\)\\)\\) as string\\), cast\\(min\\(cast\\(min " +
                "as int\\(11\\)\\)\\) as string\\) " +
                "FROM column_statistics WHERE table_id = (\\d+) and column_name = \"kk1\" " +
                "GROUP BY db_id, table_id, column_name " +
                "UNION ALL SELECT cast\\(1 as INT\\), now\\(\\), db_id, table_id, column_name, " +
                "sum\\(row_count\\), " +
                "cast\\(sum\\(data_size\\) as bigint\\), hll_union_agg\\(ndv\\), sum\\(null_count\\),  " +
                "cast\\(max\\(cast\\(max as string\\)\\) as string\\), cast\\(min\\(cast\\(min as" +
                " string\\)\\) as string\\) " +
                "FROM column_statistics WHERE table_id = (\\d+) and column_name = \"kk2\" " +
                "GROUP BY db_id, table_id, column_name";
        String content = StatisticSQLBuilder.buildQueryFullStatisticsSQL(database.getId(), table.getId(),
                Lists.newArrayList(kk1, kk2));
        Assert.assertTrue(Pattern.matches(pattern, content));
    }

    @Test
    public void testQueryDict() throws Exception {
        String column = "case";
        String catalogName = "default_catalog";
        String dbName = "select";
        String tblName = "insert";
        String sql = "select cast(" + 1 + " as Int), " +
                "cast(" + 2 + " as bigint), " +
                "dict_merge(" +  StatisticUtils.quoting(column) + ") as _dict_merge_" + column +
                " from " + StatisticUtils.quoting(catalogName, dbName, tblName) + " [_META_]";
        QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, getConnectContext());
        Assert.assertEquals("select.insert",
                ((SelectRelation) stmt.getQueryRelation()).getRelation().getResolveTableName().toString());
    }

    @Test
    public void testTypeKeys() throws Exception {
        analyzeSuccess("select count(*) from tarray group by v4");
        analyzeSuccess("select distinct v4 from tarray");
        analyzeSuccess("select * from tarray order by v4");
        analyzeSuccess("select DENSE_RANK() OVER(partition by v3 order by v4) from tarray");
        analyzeSuccess("select * from tarray join tarray y using(v4)");
        analyzeFail("select * from tarray join tarray y using(v5)");
        analyzeFail("select avg(v4) from tarray");
        analyzeFail("select count(*) from tarray group by v5");
        analyzeFail("select distinct v5 from tarray");
        analyzeFail("select * from tarray order by v5");
        analyzeFail("select DENSE_RANK() OVER(partition by v5 order by v4) from tarray");
    }
}
