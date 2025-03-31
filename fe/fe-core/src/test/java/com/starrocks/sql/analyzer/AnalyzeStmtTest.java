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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeMultiColumnDesc;
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
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.ExternalAnalyzeJob;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.FullStatisticsCollectJob;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.statistic.NativeAnalyzeJob;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.statistic.StatisticSQLBuilder;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.statistic.columns.PredicateColumnsStorage;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        ConnectorPlanTestBase.mockHiveCatalog(getConnectContext());

        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);

        createTblStmtStr = "create table db.tb2(kk1 int, kk2 json) "
                + "DUPLICATE KEY(kk1) distributed by hash(kk1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTblStmtStr);

        String createStructTableSql = "CREATE TABLE struct_a(\n" +
                "a INT, \n" +
                "b STRUCT<a INT, c INT> COMMENT 'smith',\n" +
                "c STRUCT<a INT, b DOUBLE>,\n" +
                "d STRUCT<a INT, b ARRAY<STRUCT<a INT, b DOUBLE>>, c STRUCT<a INT>>,\n" +
                "struct_a STRUCT<struct_a STRUCT<struct_a INT>, other INT> COMMENT 'alias test'\n" +
                ") DISTRIBUTED BY HASH(`a`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createStructTableSql);

        String upperColumnTableSql = "CREATE TABLE upper_tbl(\n" +
                "Ka1 int, \n" +
                "Kb2 varchar(32), \n" +
                "Kc3 int, \n" +
                "Kd4 int" +
                ") DISTRIBUTED BY HASH(`Ka1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(upperColumnTableSql);
    }

    @Test
    public void testAllColumns() {
        String sql = "analyze table db.tbl";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertEquals(4, analyzeStmt.getColumnNames().size());
    }

    @Test
    public void testShowUserProperty() {
        String sql = "SHOW PROPERTY FOR 'jack' LIKE '%load_cluster%'";
        ShowUserPropertyStmt showUserPropertyStmt = (ShowUserPropertyStmt) analyzeSuccess(sql);
        Assert.assertEquals("jack", showUserPropertyStmt.getUser());
    }

    @Test
    public void testSetUserProperty() {
        String sql = "SET PROPERTY FOR 'tom' 'max_user_connections' = '100'";
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
        Assert.assertEquals(3, analyzeStmt.getColumnNames().size());
    }

    @Test
    public void testStructColumns() {
        String sql = "analyze table db.struct_a (b.a, b.c)";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertEquals("[b.a, b.c]", analyzeStmt.getColumnNames().toString());

        sql = "analyze table db.struct_a (d.c.a)";
        analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertEquals("[d.c.a]", analyzeStmt.getColumnNames().toString());

        sql = "analyze table db.struct_a update histogram on b.a, b.c, d.c.a with 256 buckets";
        analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertEquals("[b.a, b.c, d.c.a]", analyzeStmt.getColumnNames().toString());

        sql = "analyze table db.struct_a drop histogram on b.a, b.c, d.c.a";
        DropHistogramStmt dropHistogramStmt = (DropHistogramStmt) analyzeSuccess(sql);
        Assert.assertEquals("[b.a, b.c, d.c.a]", dropHistogramStmt.getColumnNames().toString());
    }

    @Test
    public void testAnalyzeHiveTable() {
        String sql = "analyze table hive0.tpch.customer";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);

        Assert.assertTrue(analyzeStmt.isExternal());

        sql = "analyze table hive0.tpch.customer(C_NAME, C_PHONE)";
        analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertEquals("[c_name, c_phone]", analyzeStmt.getColumnNames().toString());
    }

    @Test
    public void testAnalyzeHiveResource() {
        new MockUp<MetaUtils>() {
            @Mock
            public Table getSessionAwareTable(ConnectContext session, Database database, TableName tableName) {
                return new HiveTable(1, "customer", Lists.newArrayList(), "resource_name",
                        CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName("resource_name", "hive"),
                        "hive", "tpch", "", "",
                        0, Lists.newArrayList(), Lists.newArrayList(), Maps.newHashMap(),
                        Maps.newHashMap(), null, null);
            }
        };
        String sql = "analyze table tpch.customer";
        analyzeFail(sql, "Don't support analyze external table created by resource mapping.");
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

        sql = "analyze full table db.tbl partition(`tbl`) with async mode";
        analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertTrue(analyzeStmt.isAsync());
    }

    @Test
    public void testShow() throws MetaNotFoundException {
        String sql = "show analyze";
        analyzeSuccess(sql);
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "t0");

        NativeAnalyzeJob nativeAnalyzeJob = new NativeAnalyzeJob(testDb.getId(), table.getId(), Lists.newArrayList(),
                Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.FINISH, LocalDateTime.MIN);
        Assert.assertEquals("[-1, default_catalog, test, t0, ALL, FULL, ONCE, {}, FINISH, None, ]",
                ShowAnalyzeJobStmt.showAnalyzeJobs(getConnectContext(), nativeAnalyzeJob).toString());

        ExternalAnalyzeJob externalAnalyzeJob = new ExternalAnalyzeJob("hive0", "partitioned_db",
                "t1", Lists.newArrayList(), Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), StatsConstants.ScheduleStatus.FINISH,
                LocalDateTime.MIN);
        Assert.assertEquals("[-1, hive0, partitioned_db, t1, ALL, FULL, ONCE, {}, FINISH, None, ]",
                ShowAnalyzeJobStmt.showAnalyzeJobs(getConnectContext(), externalAnalyzeJob).toString());

        sql = "show analyze job";
        analyzeSuccess(sql);

        sql = "show analyze job where catalog = 'hive0'";
        ShowAnalyzeJobStmt stmt = (ShowAnalyzeJobStmt) analyzeSuccess(sql);
        getConnectContext().getGlobalStateMgr().getAnalyzeMgr().addAnalyzeJob(externalAnalyzeJob);
        String res = ShowExecutor.execute(stmt, getConnectContext()).getResultRows().toString();
        Assert.assertTrue(res.contains("hive0, partitioned_db, t1, ALL, FULL, ONCE, {}, FINISH, None"));

        sql = "show analyze job where catalog = 'xxxx'";
        stmt = (ShowAnalyzeJobStmt) analyzeSuccess(sql);
        getConnectContext().getGlobalStateMgr().getAnalyzeMgr().addAnalyzeJob(externalAnalyzeJob);
        res = ShowExecutor.execute(stmt, getConnectContext()).getResultRows().toString();
        Assert.assertEquals("[]", res);

        sql = "show analyze status where id = 1";
        analyzeFail(sql, "Detail message: Invalid right operator in predicate 'id = 1'. Right side must be a string literal." +
                " Example: column = 'value' or column LIKE 'pattern%'");
        analyzeSuccess("show analyze status where id = '1'");

        sql = "show analyze status where id > 1";
        analyzeFail(sql, "Invalid predicate in SHOW statement. Only '=' and 'LIKE' operators are supported. Found: 'id > 1'");

        sql = "show analyze status where max(id) = 6";
        analyzeFail(sql, "Invalid left operator in predicate 'max(id) = 6'. " +
                "Left side must be a column reference.");

        analyzeSuccess("show analyze status where db_name like '%tpc%'");
        analyzeSuccess("show analyze status where db_name like '%tpc%' and id = '123456'");

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

        AnalyzeStatus extenalAnalyzeStatus = new ExternalAnalyzeStatus(-1, "hive0", "partitioned_db",
                "tx", "tx:xxxx", Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.MIN);
        Assert.assertNull(ShowAnalyzeStatusStmt.showAnalyzeStatus(getConnectContext(), extenalAnalyzeStatus));

        BasicStatsMeta basicStatsMeta = new BasicStatsMeta(testDb.getId(), table.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1), Maps.newHashMap());
        Assert.assertEquals("[test, t0, ALL, FULL, 2020-01-01 01:01:00, {}, 100%, ]",
                ShowBasicStatsMetaStmt.showBasicStatsMeta(getConnectContext(), basicStatsMeta).toString());

        sql = "show stats meta where `database` like '%te%'";
        ShowBasicStatsMetaStmt showAnalyzeMetaStmt = (ShowBasicStatsMetaStmt) analyzeSuccess(sql);
        getConnectContext().getGlobalStateMgr().getAnalyzeMgr().addBasicStatsMeta(basicStatsMeta);
        res = ShowExecutor.execute(showAnalyzeMetaStmt, getConnectContext()).getResultRows().toString();
        Assert.assertEquals("[[test, t0, ALL, FULL, 2020-01-01 01:01:00, {}, 100%, ]]", res);
        getConnectContext().getGlobalStateMgr().getAnalyzeMgr().dropBasicStatsMetaAndData(
                getConnectContext(), Set.of(table.getId()));

        sql = "show histogram meta where updateTime = '2020-01-01 01:01:00'";
        ShowHistogramStatsMetaStmt showHistogramStatsMetaStmt = (ShowHistogramStatsMetaStmt) analyzeSuccess(sql);
        HistogramStatsMeta histogramStatsMeta = new HistogramStatsMeta(testDb.getId(), table.getId(), "v1",
                StatsConstants.AnalyzeType.HISTOGRAM, LocalDateTime.of(
                2020, 1, 1, 1, 1),
                Maps.newHashMap());
        Assert.assertEquals("[test, t0, v1, HISTOGRAM, 2020-01-01 01:01:00, {}]",
                ShowHistogramStatsMetaStmt.showHistogramStatsMeta(getConnectContext(), histogramStatsMeta).toString());

        getConnectContext().getGlobalStateMgr().getAnalyzeMgr().addHistogramStatsMeta(histogramStatsMeta);
        res = ShowExecutor.execute(showHistogramStatsMetaStmt, getConnectContext()).getResultRows().toString();
        Assert.assertEquals("[[test, t0, v1, HISTOGRAM, 2020-01-01 01:01:00, {}]]", res);
    }

    @Test
    public void testShowWithOrderByLimit() {
        Database db = getConnectContext().getGlobalStateMgr().getLocalMetastore().getDb("db");

        Table tbl = db.getTable("tbl");
        Table tb2 = db.getTable("tb2");
        Table structA = db.getTable("struct_a");
        Table upperTbl = db.getTable("upper_tbl");

        AnalyzeMgr analyzeMgr = getConnectContext().getGlobalStateMgr().getAnalyzeMgr();
        analyzeMgr.dropBasicStatsMetaAndData(getConnectContext(),
                Set.of(tbl.getId(), tb2.getId(), structA.getId(), upperTbl.getId()));


        BasicStatsMeta meta1 = new BasicStatsMeta(db.getId(), tbl.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2022, 1, 1, 10, 0),
                Maps.newHashMap(),
                1000);

        Map<String, String> sampleProps = new HashMap<>();
        sampleProps.put("sample_ratio", "0.3");

        BasicStatsMeta meta2 = new BasicStatsMeta(db.getId(), tb2.getId(),
                Arrays.asList("kk1", "kk2"),
                StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.of(2022, 3, 15, 14, 30),
                sampleProps,
                500);

        BasicStatsMeta meta3 = new BasicStatsMeta(db.getId(), structA.getId(),
                Arrays.asList("a", "b"),
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2021, 12, 31, 23, 59),
                Maps.newHashMap(),
                10000);

        Map<String, String> sampleProps2 = new HashMap<>();
        sampleProps2.put("sample_ratio", "0.5");
        sampleProps2.put("time_cost", "45s");

        BasicStatsMeta meta4 = new BasicStatsMeta(db.getId(), upperTbl.getId(),
                Arrays.asList("Ka1", "Kb2"),
                StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.of(2023, 5, 20, 8, 45),
                sampleProps2,
                2500);

        analyzeMgr.addBasicStatsMeta(meta1);
        analyzeMgr.addBasicStatsMeta(meta2);
        analyzeMgr.addBasicStatsMeta(meta3);
        analyzeMgr.addBasicStatsMeta(meta4);

        // case 1: Sort by update time in descending order
        String sql1 = "show stats meta order by `UpdateTime` desc";
        ShowBasicStatsMetaStmt stmt1 = (ShowBasicStatsMetaStmt) analyzeSuccess(sql1);
        List<List<String>> res1 = ShowExecutor.execute(stmt1, getConnectContext()).getResultRows();

        // should be meta4, meta2, meta1, meta3
        Assert.assertEquals(4, res1.size());

        // First row should be meta4 (2023-05-20)
        Assert.assertEquals("db", res1.get(0).get(0)); // Database
        Assert.assertEquals("upper_tbl", res1.get(0).get(1)); // Table
        Assert.assertEquals("SAMPLE", res1.get(0).get(3)); // Type
        Assert.assertEquals("2023-05-20 08:45:00", res1.get(0).get(4)); // UpdateTime
        Assert.assertTrue(res1.get(0).get(5).contains("sample_ratio")); // Properties
        Assert.assertTrue(res1.get(0).get(5).contains("time_cost")); // Properties

        // Second row should be meta2 (2022-03-15)
        Assert.assertEquals("db", res1.get(1).get(0));
        Assert.assertEquals("tb2", res1.get(1).get(1));
        Assert.assertEquals("SAMPLE", res1.get(1).get(3));
        Assert.assertEquals("2022-03-15 14:30:00", res1.get(1).get(4));
        Assert.assertTrue(res1.get(1).get(5).contains("sample_ratio")); // Properties

        // Third row should be meta1 (2022-01-01)
        Assert.assertEquals("db", res1.get(2).get(0));
        Assert.assertEquals("tbl", res1.get(2).get(1));
        Assert.assertEquals("FULL", res1.get(2).get(3));
        Assert.assertEquals("2022-01-01 10:00:00", res1.get(2).get(4));

        // Fourth row should be meta3 (2021-12-31)
        Assert.assertEquals("db", res1.get(3).get(0));
        Assert.assertEquals("struct_a", res1.get(3).get(1));
        Assert.assertEquals("FULL", res1.get(3).get(3));
        Assert.assertEquals("2021-12-31 23:59:00", res1.get(3).get(4));

        // case 2: Sort by table name and limit results
        String sql2 = "show stats meta order by `Table` asc limit 2";
        ShowBasicStatsMetaStmt stmt2 = (ShowBasicStatsMetaStmt) analyzeSuccess(sql2);
        List<List<String>> res2 = ShowExecutor.execute(stmt2, getConnectContext()).getResultRows();

        // Should return the first 2 records sorted by table name
        Assert.assertEquals(2, res2.size());

        // First row should be struct_a table
        Assert.assertEquals("struct_a", res2.get(0).get(1)); // Table

        // Second row should be tb2 table
        Assert.assertEquals("tb2", res2.get(1).get(1)); // Table

        // Test case 3: Sort by type
        String sql3 = "show stats meta order by `Type` asc";
        ShowBasicStatsMetaStmt stmt3 = (ShowBasicStatsMetaStmt) analyzeSuccess(sql3);
        List<List<String>> res3 = ShowExecutor.execute(stmt3, getConnectContext()).getResultRows();

        // should be sorted by type in alphabetical order
        Assert.assertEquals(4, res3.size());

        // Types should be in alphabetical order: FULL, SAMPLE
        String[] expectedTypeOrder = {"FULL", "FULL", "SAMPLE", "SAMPLE"};
        for (int i = 0; i < res3.size(); i++) {
            Assert.assertEquals(expectedTypeOrder[i], res3.get(i).get(3)); // Type
        }

        // case 4: Sort by type and update time
        String sql4 = "show stats meta order by `Type` asc, `UpdateTime` desc";
        ShowBasicStatsMetaStmt stmt4 = (ShowBasicStatsMetaStmt) analyzeSuccess(sql4);
        List<List<String>> res4 = ShowExecutor.execute(stmt4, getConnectContext()).getResultRows();

        // should be sorted by type ascending, and within each type by time descending
        Assert.assertEquals(4, res4.size());

        // First and second rows should be FULL type, sorted by time descending
        Assert.assertEquals("FULL", res4.get(0).get(3));
        Assert.assertEquals("FULL", res4.get(1).get(3));

        LocalDateTime time1 = LocalDateTime.parse(res4.get(0).get(4),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime time2 = LocalDateTime.parse(res4.get(1).get(4),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        Assert.assertTrue(time1.isAfter(time2));

        Assert.assertEquals("SAMPLE", res4.get(2).get(3));
        Assert.assertEquals("SAMPLE", res4.get(3).get(3));

        LocalDateTime time3 = LocalDateTime.parse(res4.get(2).get(4),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime time4 = LocalDateTime.parse(res4.get(3).get(4),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        Assert.assertTrue(time3.isAfter(time4));

        // case 5: Filter by property content and sort
        String sql6 = "show stats meta where `Properties` like '%sample_ratio%' order by `Table` desc";
        ShowBasicStatsMetaStmt stmt6 = (ShowBasicStatsMetaStmt) analyzeSuccess(sql6);
        List<List<String>> res6 = ShowExecutor.execute(stmt6, getConnectContext()).getResultRows();

        // should have meta2 and meta4, sorted by table name in descending order
        Assert.assertEquals(2, res6.size());

        // First row should be upper_tbl table
        Assert.assertEquals("upper_tbl", res6.get(0).get(1)); // Table
        Assert.assertEquals("SAMPLE", res6.get(0).get(3)); // Type

        // Second row should be tb2 table
        Assert.assertEquals("tb2", res6.get(1).get(1)); // Table
        Assert.assertEquals("SAMPLE", res6.get(1).get(3)); // Type

        // case 6: Complex condition with limit
        String sql7 = "show stats meta where `Type` = 'FULL' order by `UpdateTime` desc limit 1";
        ShowBasicStatsMetaStmt stmt7 = (ShowBasicStatsMetaStmt) analyzeSuccess(sql7);
        List<List<String>> res7 = ShowExecutor.execute(stmt7, getConnectContext()).getResultRows();

        // should only have meta1
        Assert.assertEquals(1, res7.size());
        Assert.assertEquals("db", res7.get(0).get(0)); // Database
        Assert.assertEquals("tbl", res7.get(0).get(1)); // Table
        Assert.assertEquals("ALL", res7.get(0).get(2)); // Columns
        Assert.assertEquals("FULL", res7.get(0).get(3)); // Type
        Assert.assertEquals("2022-01-01 10:00:00", res7.get(0).get(4)); // UpdateTime

        // case 7: Test limit only
        String sql8 = "show stats meta limit 3";
        ShowBasicStatsMetaStmt stmt8 = (ShowBasicStatsMetaStmt) analyzeSuccess(sql8);
        List<List<String>> res8 = ShowExecutor.execute(stmt8, getConnectContext()).getResultRows();

        Assert.assertEquals(3, res8.size());

        // case 8: Test ORDER BY multiple columns and LIMIT combination
        String sql9 = "show stats meta order by `Database` asc, `Table` desc, `UpdateTime` desc limit 2";
        ShowBasicStatsMetaStmt stmt9 = (ShowBasicStatsMetaStmt) analyzeSuccess(sql9);
        List<List<String>> res9 = ShowExecutor.execute(stmt9, getConnectContext()).getResultRows();

        Assert.assertEquals(2, res9.size());
        Assert.assertEquals("upper_tbl", res9.get(0).get(1)); // Table
        Assert.assertEquals("tbl", res9.get(1).get(1)); // Table
    }

    @Test
    public void testStatisticsSqlBuilder() {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t0");
        System.out.println(table.getPartitions());
        Partition partition = (new ArrayList<>(table.getPartitions())).get(0);

        Column v1 = table.getColumn("v1");
        Column v2 = table.getColumn("v2");

        Assert.assertEquals(String.format("SELECT cast(10 as INT), now(), " +
                        "db_id, table_id, column_name," +
                        " sum(row_count), " +
                        "cast(sum(data_size) as bigint), hll_union_agg(ndv), sum(null_count),  " +
                        "cast(max(cast(max as bigint)) as string), " +
                        "cast(min(cast(min as bigint)) as string), cast(avg(collection_size) as bigint) FROM column_statistics " +
                        "WHERE table_id = %d and column_name in (\"v1\", \"v2\") " +
                        "GROUP BY db_id, table_id, column_name", table.getId()),
                StatisticSQLBuilder.buildQueryFullStatisticsSQL(table.getId(),
                        Lists.newArrayList("v1", "v2"), Lists.newArrayList(v1.getType(), v2.getType())));

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
                .getLocalMetastore().getDb("db").getTable("tbl");
        for (Partition partition : t0.getAllPartitions()) {
            partition.getDefaultPhysicalPartition().getBaseIndex().setRowCount(10000);
        }

        String sql = "analyze table db.tbl update histogram on kk1 with 256 buckets " +
                "properties(\"histogram_sample_ratio\"=\"0.1\")";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertEquals("1", analyzeStmt.getProperties().get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));

        for (Partition partition : t0.getAllPartitions()) {
            partition.getDefaultPhysicalPartition().getBaseIndex().setRowCount(400000);
        }

        sql = "analyze table db.tbl update histogram on kk1 with 256 buckets " +
                "properties(\"histogram_sample_ratio\"=\"0.2\")";
        analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertEquals("0.5", analyzeStmt.getProperties().get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));

        for (Partition partition : t0.getAllPartitions()) {
            partition.getDefaultPhysicalPartition().getBaseIndex().setRowCount(20000000);
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
    public void testDropTableOnMultiColumnStats() {
        String sql = "drop multi_columns stats t0";
        DropStatsStmt dropStatsStmt = (DropStatsStmt) analyzeSuccess(sql);
        Assert.assertEquals("t0", dropStatsStmt.getTableName().getTbl());
        Assert.assertTrue(dropStatsStmt.isMultiColumn());
        Assert.assertEquals("DELETE FROM multi_column_statistics WHERE TABLE_ID = 10004",
                StatisticSQLBuilder.buildDropMultipleStatisticsSQL(10004L));
    }

    @Test
    public void testKillAnalyze() {
        String sql = "kill analyze 1";
        KillAnalyzeStmt killAnalyzeStmt = (KillAnalyzeStmt) analyzeSuccess(sql);

        GlobalStateMgr.getCurrentState().getAnalyzeMgr().registerConnection(1, getConnectContext());
        Assert.assertThrows(SemanticException.class,
                () -> GlobalStateMgr.getCurrentState().getAnalyzeMgr().unregisterConnection(2, true));
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().unregisterConnection(1, true);
        Assert.assertThrows(SemanticException.class,
                () -> GlobalStateMgr.getCurrentState().getAnalyzeMgr().unregisterConnection(1, true));
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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "t0");
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
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db");
        OlapTable table =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "tb2");

        Column kk1 = table.getColumn("kk1");
        Column kk2 = table.getColumn("kk2");

        String pattern = String.format("SELECT cast(10 as INT), now(), db_id, table_id, column_name, sum(row_count), " +
                "cast(sum(data_size) as bigint), hll_union_agg(ndv), sum(null_count),  " +
                "cast(max(cast(max as string)) as string), cast(min(cast(min as string)) as string), " +
                "cast(avg(collection_size) as bigint) " +
                "FROM column_statistics WHERE table_id = %d and column_name in (\"kk2\") " +
                "GROUP BY db_id, table_id, column_name " +
                "UNION ALL SELECT cast(10 as INT), now(), db_id, table_id, column_name, " +
                "sum(row_count), cast(sum(data_size) as bigint), hll_union_agg(ndv), sum(null_count),  " +
                "cast(max(cast(max as bigint)) as string), cast(min(cast(min as bigint)) as string), " +
                "cast(avg(collection_size) as bigint) " +
                "FROM column_statistics WHERE table_id = %d and column_name in (\"kk1\") " +
                "GROUP BY db_id, table_id, column_name", table.getId(), table.getId());
        String content = StatisticSQLBuilder.buildQueryFullStatisticsSQL(table.getId(),
                Lists.newArrayList("kk1", "kk2"), Lists.newArrayList(kk1.getType(), kk2.getType()));
        Assert.assertEquals(pattern, content);
    }

    @Test
    public void testQueryDict() throws Exception {
        String column = "case";
        String catalogName = "default_catalog";
        String dbName = "select";
        String tblName = "insert";
        String sql = "select cast(" + 1 + " as Int), " +
                "cast(" + 2 + " as bigint), " +
                "dict_merge(" + StatisticUtils.quoting(column) + ", 255) as _dict_merge_" + column +
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

    @Test
    public void testAnalyzePredicateColumns() {
        StatisticsMetaManager statistic = new StatisticsMetaManager();
        statistic.createStatisticsTablesForTest();
        TableKeeper keeper = PredicateColumnsStorage.createKeeper();
        keeper.run();

        AnalyzeStmt stmt = (AnalyzeStmt) analyzeSuccess("analyze table db.tbl all columns");
        Assert.assertTrue(stmt.isAllColumns());
        stmt = (AnalyzeStmt) analyzeSuccess("analyze table db.tbl predicate columns");
        Assert.assertTrue(stmt.isUsePredicateColumns());
    }

    @Test
    public void testAnalyzeTableWithSampleRatio() {
        analyzeSuccess("analyze sample table db.tbl properties(\"high_weight_sample_ratio\" = \"0.6\")");
        analyzeSuccess("analyze sample table db.tbl properties(\"medium_high_weight_sample_ratio\" = \"0.6\")");
        analyzeSuccess("analyze sample table db.tbl properties(\"medium_low_weight_sample_ratio\" = \"0.6\")");
        analyzeSuccess("analyze sample table db.tbl properties(\"low_weight_sample_ratio\" = \"0.6\")");
    }

    @Test
    public void testUpperColumn() {
        try {
            AnalyzeTestUtil.connectContext.getSessionVariable().setEnableAnalyzePhasePruneColumns(true);
            analyzeSuccess("select Ka1 from db.upper_tbl");
        } finally {
            AnalyzeTestUtil.connectContext.getSessionVariable().setEnableAnalyzePhasePruneColumns(false);
        }
    }

    @Test
    public void testAnalyzeMultiColumnStats() {
        analyzeFail("analyze table db.tbl multi_columns");
        analyzeFail("analyze table db.tbl multi_columns (kk1)",
                "must greater than 1 column on multi-column combined analyze statement");
        analyzeFail("analyze table db.tbl multi_columns (kk1, kk2) partition(`tbl`)",
                "not support specify partition names on multi-column analyze statement");
        analyzeFail("analyze table db.tbl multi_columns (k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11)",
                "column size 11 exceeded max size of 10 on multi-column combined analyze statement");
        analyzeFail("analyze table hive0.tpch.customer multi_columns (C_NAME, C_PHONE)",
                "Don't support analyze multi-columns combined statistics on external table");
        analyzeFail("analyze table hive0.tpch.customer multi_columns (C_NAME, C_PHONE) with async mode",
                "not support async analyze on multi-column analyze statement");

        analyzeSuccess("analyze full table db.tbl multi_columns (kk1, kk2)");
        analyzeSuccess("analyze sample table db.tbl multi_columns (kk1, kk2)");

        AnalyzeStmt stmt = (AnalyzeStmt) analyzeSuccess("analyze table db.tbl multi_columns (kk1, kk2)");
        Assert.assertFalse(stmt.isAsync());
        Assert.assertTrue(stmt.isSample());
        Assert.assertTrue(stmt.getAnalyzeTypeDesc() instanceof AnalyzeMultiColumnDesc);
        Assert.assertTrue(stmt.getAnalyzeTypeDesc().getStatsTypes().contains(StatsConstants.StatisticsType.MCDISTINCT));
    }
}
