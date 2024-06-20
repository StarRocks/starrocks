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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.stream.Collectors;

public class StatisticsSQLTest extends PlanTestBase {
    private static long t0StatsTableId = 0;

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @BeforeClass
    public static void beforeClass() throws Exception {

        PlanTestBase.beforeClass();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, temp.newFolder().toURI().toString());

        StatisticsMetaManager m = new StatisticsMetaManager();
        m.createStatisticsTablesForTest();

        starRocksAssert.withTable("CREATE TABLE `stat0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL,\n" +
                "  `v4` date NULL,\n" +
                "  `v5` datetime NULL,\n" +
                "  `s1` String NULL,\n" +
                "  `j1` JSON NULL" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `escape0['abc']` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2['+']` bigint NULL COMMENT \"\",\n" +
                "  `v3[\" / \"]` bigint NULL,\n" +
                "  `v4[99]` String NULL,\n" +
                "  `v5('1' + '2')` String NULL,\n" +
                "  `v6['''+''']` String NULL,\n" +
                "  `v7[''''''+'''''']` JSON NULL" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

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

        starRocksAssert.withTable("CREATE TABLE `complex_table` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2.a2.b2['+']` bigint NULL COMMENT \"\",\n" +
                "  `struct_a.c3.d3` STRUCT<struct_b int, " +
                "                          `struct_c.e3` int, " +
                "                          `struct_d.f4` struct<struct_e int, struct_f int, `struct_g.h` int>" +
                "                          > COMMENT ''\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("stat0");
        t0StatsTableId = t0.getId();
    }

    @Test
    public void testSampleStatisticsSQL() throws Exception {
        Table t0 = GlobalStateMgr.getCurrentState().getDb("test").getTable("stat0");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");

        List<String> columnNames = Lists.newArrayList("v3", "j1", "s1");
        SampleStatisticsCollectJob job = new SampleStatisticsCollectJob(db, t0, columnNames,
                StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

        String sql = job.buildSampleInsertSQL(db.getId(), t0StatsTableId, columnNames, job.columnTypes, 200);
        starRocksAssert.useDatabase("_statistics_");
        String except = String.format("SELECT %s, '%s', %s, '%s', '%s'",
                t0.getId(), "v3", db.getId(), "test.stat0", "test");
        assertCContains(sql, except);

        String plan = getFragmentPlan(sql);

        Assert.assertEquals(3, StringUtils.countMatches(plan, "OlapScanNode"));
        assertCContains(plan, "left(");
        assertCContains(plan, "count * 1024");
    }

    @Test
    public void testFullStatisticsSQL() throws Exception {
        Table t0 = GlobalStateMgr.getCurrentState().getDb("test").getTable("stat0");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        List<Long> pids = t0.getPartitions().stream().map(Partition::getId).collect(Collectors.toList());

        List<String> columnNames = Lists.newArrayList("j1", "s1");
        FullStatisticsCollectJob job = new FullStatisticsCollectJob(db, t0, pids, columnNames,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

        List<List<String>> sqls = job.buildCollectSQLList(1);
        Assert.assertEquals(2, sqls.size());
        Assert.assertEquals(1, sqls.get(0).size());
        Assert.assertEquals(1, sqls.get(1).size());
        starRocksAssert.useDatabase("_statistics_");
        String plan = getFragmentPlan(sqls.get(0).get(0));
        assertCContains(plan, "count * 1024");

        plan = getFragmentPlan(sqls.get(1).get(0));
        assertCContains(plan, "left(");
        assertCContains(plan, "char_length(");
        assertCContains(plan, "hex(hll_serialize(");
    }

    @Test
    public void testFullStatisticsSQLWithStruct() throws Exception {
        Table t0 = GlobalStateMgr.getCurrentState().getDb("test").getTable("struct_a");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        List<Long> pids = t0.getPartitions().stream().map(Partition::getId).collect(Collectors.toList());

        List<String> columnNames = Lists.newArrayList("b.a", "b.c", "d.c.a");

        FullStatisticsCollectJob job = new FullStatisticsCollectJob(db, t0, pids, columnNames, ImmutableList.of(Type.INT,
                Type.INT, Type.INT), StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

        List<List<String>> sqls = job.buildCollectSQLList(1);
        Assert.assertEquals(3, sqls.size());
        for (int i = 0; i < sqls.size(); i++) {
            Assert.assertEquals(1, sqls.get(i).size());
            String sql = sqls.get(i).get(0);
            starRocksAssert.useDatabase("_statistics_");
            ExecPlan plan = getExecPlan(sql);
            List<Expr> output = plan.getOutputExprs();
            Assert.assertEquals(output.get(2).getType().getPrimitiveType(), Type.STRING.getPrimitiveType());
            assertCContains(plan.getColNames().get(2).replace("\\", ""), columnNames.get(i));
        }
    }

    @Test
    public void testHistogramStatisticsSQLWithStruct() throws Exception {
        Table t0 = GlobalStateMgr.getCurrentState().getDb("test").getTable("struct_a");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");

        List<String> columnNames = Lists.newArrayList("b.a", "b.c", "d.c.a");
        HistogramStatisticsCollectJob histogramStatisticsCollectJob = new HistogramStatisticsCollectJob(
                db, t0, Lists.newArrayList("b.a", "b.c", "d.c.a"),
                Lists.newArrayList(Type.INT, Type.INT, Type.INT),
                StatsConstants.AnalyzeType.HISTOGRAM, StatsConstants.ScheduleType.ONCE,
                Maps.newHashMap());
        for (String col : columnNames) {
            String sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectMCV",
                    db, t0, 3L, col);
            starRocksAssert.useDatabase("_statistics_");
            String plan = getFragmentPlan(sql);
            assertCContains(plan, "0:OlapScanNode\n" +
                    "     TABLE: struct_a");
        }

        for (String col : columnNames) {
            String sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectHistogram",
                    db, t0, 0.1, 10L, ImmutableMap.of("d.c.a", "100"), col, Type.INT);
            sql = sql.substring(sql.indexOf("SELECT"));
            starRocksAssert.useDatabase("_statistics_");
            String plan = getFragmentPlan(sql);
            assertCContains(plan, "4:AGGREGATE (update finalize)\n" +
                    "  |  output: histogram");
        }
    }

    @Test
    public void testHiveHistogramStatisticsSQLWithStruct() throws Exception {
        Table t0 = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0", "subfield_db",
                "subfield");
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb("hive0", "subfield_db");

        List<String> columnNames = Lists.newArrayList("col_struct.c0", "col_struct.c1.c11");
        ExternalHistogramStatisticsCollectJob hiveHistogramStatisticsCollectJob = new ExternalHistogramStatisticsCollectJob(
                "hive0", db, t0, columnNames, Lists.newArrayList(Type.INT, Type.INT),
                StatsConstants.AnalyzeType.HISTOGRAM, StatsConstants.ScheduleType.ONCE,
                Maps.newHashMap());
        for (String col : columnNames) {
            String sql = Deencapsulation.invoke(hiveHistogramStatisticsCollectJob, "buildCollectMCV",
                    db, t0, 3L, col);
            starRocksAssert.useDatabase("_statistics_");
            String plan = getFragmentPlan(sql);
            assertCContains(plan, " 0:HdfsScanNode\n" +
                    "     TABLE: subfield");
        }

        for (String col : columnNames) {
            String sql = Deencapsulation.invoke(hiveHistogramStatisticsCollectJob, "buildCollectHistogram",
                    db, t0, 0.1, 10L, ImmutableMap.of("col_struct.c1.c11", "100"), col, Type.INT);
            sql = sql.substring(sql.indexOf("SELECT"));
            starRocksAssert.useDatabase("_statistics_");
            String plan = getFragmentPlan(sql);
            assertCContains(plan, "4:AGGREGATE (update finalize)\n" +
                    "  |  output: histogram");
        }
    }

    @Test
    public void testEscapeFullSQL() throws Exception {
        Table t0 = GlobalStateMgr.getCurrentState().getDb("test").getTable("escape0['abc']");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        List<Long> pids = t0.getPartitions().stream().map(Partition::getId).collect(Collectors.toList());

        List<String> columnNames = t0.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        FullStatisticsCollectJob job = new FullStatisticsCollectJob(db, t0, pids, columnNames,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

        List<List<String>> sqls = job.buildCollectSQLList(1);
        Assert.assertEquals(7, sqls.size());

        for (int i = 0; i < sqls.size(); i++) {
            Assert.assertEquals(1, sqls.get(i).size());
            String sql = sqls.get(i).get(0);
            starRocksAssert.useDatabase("_statistics_");
            ExecPlan plan = getExecPlan(sql);
            List<Expr> output = plan.getOutputExprs();
            Assert.assertEquals(output.get(2).getType().getPrimitiveType(), Type.STRING.getPrimitiveType());
            assertCContains(plan.getColNames().get(2).replace("\\", ""), columnNames.get(i));
        }
    }

    @Test
    public void testEscapeSampleSQL() throws Exception {
        Table t0 = GlobalStateMgr.getCurrentState().getDb("test").getTable("escape0['abc']");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");

        List<String> columnNames = t0.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        SampleStatisticsCollectJob job = new SampleStatisticsCollectJob(db, t0, columnNames,
                StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

        for (String column : columnNames) {
            String sql = job.buildSampleInsertSQL(db.getId(), t0.getId(), Lists.newArrayList(column),
                    Lists.newArrayList(t0.getColumn(column).getType()), 200);
            starRocksAssert.useDatabase("_statistics_");
            ExecPlan plan = getExecPlan(sql);
            List<Expr> output = plan.getOutputExprs();
            Assert.assertEquals(output.get(1).getType().getPrimitiveType(), Type.STRING.getPrimitiveType());
            Assert.assertEquals(output.get(3).getType().getPrimitiveType(), Type.STRING.getPrimitiveType());
            Assert.assertEquals(output.get(4).getType().getPrimitiveType(), Type.STRING.getPrimitiveType());

            assertCContains(plan.getColNames().get(1).replace("\\", ""), column);
            assertCContains(plan.getColNames().get(3).replace("\\", ""), "escape0['abc']");
        }
    }

    @Test
    public void testDropPartitionSQL() throws Exception {
        starRocksAssert.useDatabase("_statistics_");
        String sql = StatisticSQLBuilder.buildDropPartitionSQL(Lists.newArrayList(1L, 2L, 3L));
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "partition_id IN (1, 2, 3)");

    }

    @Test
    public void testDropInvalidPartitionSQL() throws Exception {
        starRocksAssert.useDatabase("_statistics_");
        String sql = StatisticSQLBuilder.buildDropTableInvalidPartitionSQL(Lists.newArrayList(4L, 5L, 6L),
                Lists.newArrayList(1L, 2L, 3L));
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "table_id IN (4, 5, 6)");
        assertCContains(plan, "partition_id NOT IN (1, 2, 3)");
    }

    @Test
    public void testCacheQueryColumnStatics() {
        String sql = StatisticSQLBuilder.buildQueryFullStatisticsSQL(1L, 2L, Lists.newArrayList("col1", "col2"),
                Lists.newArrayList(Type.INT, Type.INT));
        assertContains(sql, "table_id = 2 and column_name in (\"col1\", \"col2\")");
        Assert.assertEquals(0, StringUtils.countMatches(sql, "UNION ALL"));

        sql = StatisticSQLBuilder.buildQueryFullStatisticsSQL(1L, 2L,
                Lists.newArrayList("col1", "col2", "col3"),
                Lists.newArrayList(Type.INT, Type.BIGINT, Type.LARGEINT));
        assertContains(sql, "table_id = 2 and column_name in (\"col1\", \"col2\")");
        assertContains(sql, "table_id = 2 and column_name in (\"col3\")");
        Assert.assertEquals(1, StringUtils.countMatches(sql, "UNION ALL"));

        sql = StatisticSQLBuilder.buildQueryFullStatisticsSQL(1L, 2L,
                Lists.newArrayList("col1", "col2", "col3", "col4", "col5", "col6", "col7"),
                Lists.newArrayList(Type.INT, Type.BIGINT, Type.LARGEINT, Type.STRING, Type.VARCHAR, Type.ARRAY_DATE,
                        Type.DATE));
        assertContains(sql, "table_id = 2 and column_name in (\"col1\", \"col2\")");
        assertContains(sql, "table_id = 2 and column_name in (\"col3\")");
        assertContains(sql, "table_id = 2 and column_name in (\"col4\", \"col5\", \"col6\")");
        assertContains(sql, "table_id = 2 and column_name in (\"col7\")");
        Assert.assertEquals(3, StringUtils.countMatches(sql, "UNION ALL"));

        sql = StatisticSQLBuilder.buildQueryFullStatisticsSQL(1L, 2L,
                Lists.newArrayList("col1", "col2", "col3", "col4", "col5", "col6", "col7"),
                Lists.newArrayList(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 4, 3),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 4, 3),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 5, 2),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 14, 3),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 8, 3),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 21, 6),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 22, 7),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 23, 8)));
        assertContains(sql, "table_id = 2 and column_name in (\"col1\", \"col2\")");
        Assert.assertEquals(5, StringUtils.countMatches(sql, "UNION ALL"));
    }

    @Test
    public void testCacheExternalQueryColumnStatics() {
        String sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL("a", Lists.newArrayList("col1", "col2"),
                Lists.newArrayList(Type.INT, Type.INT));
        assertContains(sql, "table_uuid = \"a\" and column_name in (\"col1\", \"col2\")");
        Assert.assertEquals(0, StringUtils.countMatches(sql, "UNION ALL"));

        sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL("a",
                Lists.newArrayList("col1", "col2", "col3"),
                Lists.newArrayList(Type.INT, Type.BIGINT, Type.LARGEINT));
        assertContains(sql, "table_uuid = \"a\" and column_name in (\"col1\", \"col2\")");
        assertContains(sql, "table_uuid = \"a\" and column_name in (\"col3\")");
        Assert.assertEquals(1, StringUtils.countMatches(sql, "UNION ALL"));

        sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL("a",
                Lists.newArrayList("col1", "col2", "col3", "col4", "col5", "col6", "col7"),
                Lists.newArrayList(Type.INT, Type.BIGINT, Type.LARGEINT, Type.STRING, Type.VARCHAR, Type.ARRAY_DATE,
                        Type.DATE));
        assertContains(sql, "column_name in (\"col1\", \"col2\")");
        assertContains(sql, "column_name in (\"col3\")");
        assertContains(sql, "column_name in (\"col4\", \"col5\", \"col6\")");
        assertContains(sql, "column_name in (\"col7\")");
        Assert.assertEquals(3, StringUtils.countMatches(sql, "UNION ALL"));

        sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL("a",
                Lists.newArrayList("col1", "col2", "col3", "col4", "col5", "col6", "col7"),
                Lists.newArrayList(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 4, 3),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 4, 3),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 5, 2),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 14, 3),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 8, 3),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 21, 6),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 22, 7),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 23, 8)));
        assertContains(sql, "column_name in (\"col1\", \"col2\")");
        Assert.assertEquals(5, StringUtils.countMatches(sql, "UNION ALL"));
    }

    @Test
    public void testQuota() {
        Table t0 = GlobalStateMgr.getCurrentState().getDb("test").getTable("complex_table");
        assertContains(StatisticUtils.quoting(t0, "v2.a2.b2['+']"), "`v2.a2.b2['+']`");
        assertContains(StatisticUtils.quoting(t0, "struct_a.c3.d3"), "`struct_a.c3.d3`");
        assertContains(StatisticUtils.quoting(t0, "struct_a.c3.d3.struct_b"), "`struct_a.c3.d3`.`struct_b`");
        assertContains(StatisticUtils.quoting(t0, "struct_a.c3.d3.struct_c.e3"), "`struct_a.c3.d3`.`struct_c.e3`");
        assertContains(StatisticUtils.quoting(t0, "struct_a.c3.d3.struct_d.f4"), "`struct_a.c3.d3`.`struct_d.f4`");
        assertContains(StatisticUtils.quoting(t0, "struct_a.c3.d3.struct_d.f4.struct_e"),
                "`struct_a.c3.d3`.`struct_d.f4`.`struct_e`");
        assertContains(StatisticUtils.quoting(t0, "struct_a.c3.d3.struct_d.f4.struct_g.h"),
                "`struct_a.c3.d3`.`struct_d.f4`.`struct_g.h`");
    }
}
