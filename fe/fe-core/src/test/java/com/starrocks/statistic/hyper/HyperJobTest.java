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

package com.starrocks.statistic.hyper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.DistributedEnvPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.HyperStatisticsCollectJob;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.statistic.base.ColumnClassifier;
import com.starrocks.statistic.base.ColumnStats;
import com.starrocks.statistic.base.PartitionSampler;
import com.starrocks.statistic.base.PrimitiveTypeColumnStats;
import com.starrocks.statistic.base.SubFieldColumnStats;
import com.starrocks.statistic.base.TabletSampler;
import com.starrocks.statistic.sample.SampleInfo;
import com.starrocks.statistic.sample.TabletStats;
import com.starrocks.type.AnyStructType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.utframe.StarRocksAssert;
import mockit.Mock;
import mockit.MockUp;
import org.apache.velocity.VelocityContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HyperJobTest extends DistributedEnvPlanTestBase {

    private static Database db;

    private static Table table;

    private static PartitionSampler sampler;

    private static long pid;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("create table t_struct(c0 INT, " +
                "c1 date," +
                "c2 varchar(255)," +
                "c3 decimal(10, 2)," +
                "c4 struct<a int, b array<struct<a int, b int>>>," +
                "c5 struct<a int, b int>," +
                "c6 struct<a int, b int, c struct<a int, b int>, d array<int>>," +
                "c7 array<int>) " +
                "duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t_struct");
        pid = table.getPartition("t_struct").getId();
        sampler = PartitionSampler.create(table, List.of(pid), Maps.newHashMap(), null);

        for (Partition partition : ((OlapTable) table).getAllPartitions()) {
            partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(10000);
        }
    }

    @Test
    public void generateComplexTypeColumnTask() throws Exception {
        List<String> columnNames = table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<Type> columnTypes = table.getColumns().stream().map(Column::getType).collect(Collectors.toList());

        List<HyperQueryJob> jobs =
                HyperQueryJob.createFullQueryJobs(connectContext, db, table, columnNames, columnTypes, List.of(pid), 1, false,
                        Map.of());
        Assertions.assertEquals(2, jobs.size());
        assertInstanceOf(ConstQueryJob.class, jobs.get(1));
        for (final var job : jobs) {
            List<String> sqls = job.buildQuerySQL();
            for (String sql : sqls) {
                Assertions.assertNotNull(getFragmentPlan(sql)); // Make sure the query can be executed.
            }
        }

    }

    @Test
    public void generatePrimitiveTypeColumnTask() {
        List<String> columnNames = table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<Type> columnTypes = table.getColumns().stream().map(Column::getType).collect(Collectors.toList());

        ColumnClassifier cc = ColumnClassifier.of(columnNames, columnTypes, table, false, Map.of());
        ColumnStats columnStat = cc.getColumnStats().stream().filter(c -> c instanceof PrimitiveTypeColumnStats)
                .findAny().orElse(null);

        VelocityContext context = HyperStatisticSQLs.buildBaseContext(db, table, table.getPartition(pid), columnStat);
        context.put("dataSize", columnStat.getFullDataSize());
        context.put("countNullFunction", columnStat.getFullNullCount());
        context.put("hllFunction", columnStat.getNDV());
        context.put("maxFunction", columnStat.getMax());
        context.put("minFunction", columnStat.getMin());
        context.put("fromTable", "`" + db.getOriginName() + "`.`" + table.getName() +
                "` partition `" + table.getPartition(pid).getName() + "`");
        context.put("laterals", columnStat.getLateralJoin());
        context.put("collectionSizeFunction", columnStat.getCollectionSize());
        String sql = HyperStatisticSQLs.build(context, HyperStatisticSQLs.BATCH_FULL_STATISTIC_TEMPLATE);
        assertContains(sql, "hex(hll_serialize(IFNULL(hll_raw(`c0`)");
        List<StatementBase> stmt = SqlParser.parse(sql, connectContext.getSessionVariable());
        assertInstanceOf(QueryStatement.class, stmt.get(0));
        assertDoesNotThrow(() -> getFragmentPlan(sql));
    }

    @Test
    public void generateSubFieldTypeColumnTask() {
        List<String> columnNames = Lists.newArrayList("c1", "c4.b", "c6.c.b");
        List<Type> columnTypes = Lists.newArrayList(DateType.DATE,
                new ArrayType(AnyStructType.ANY_STRUCT), IntegerType.INT);

        ColumnClassifier cc = ColumnClassifier.of(columnNames, columnTypes, table, false, Map.of());
        List<ColumnStats> columnStat = cc.getColumnStats().stream().filter(c -> c instanceof SubFieldColumnStats)
                .collect(Collectors.toList());
        String sql = HyperStatisticSQLs.buildSampleSQL(db, table, table.getPartition(pid), columnStat, sampler,
                HyperStatisticSQLs.BATCH_SAMPLE_STATISTIC_SELECT_TEMPLATE);
        Assertions.assertEquals(2, columnStat.size());
        List<StatementBase> stmt = SqlParser.parse(sql, connectContext.getSessionVariable());
        assertInstanceOf(QueryStatement.class, stmt.get(0));
        assertDoesNotThrow(() -> getFragmentPlan(sql));
    }

    public Pair<List<String>, List<Type>> initColumn(List<String> cols) {
        List<String> columnNames = Lists.newArrayList();
        List<Type> columnTypes = Lists.newArrayList();
        for (String col : cols) {
            Column c = table.getColumn(col);
            columnNames.add(c.getName());
            columnTypes.add(c.getType());
        }
        return Pair.create(columnNames, columnTypes);
    }

    @Test
    public void testConstQueryJobs() {
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() throws Exception {
            }
        };
        Pair<List<String>, List<Type>> pair = initColumn(List.of("c4", "c5", "c6"));

        HyperStatisticsCollectJob job = new HyperStatisticsCollectJob(db, table, List.of(pid), pair.first, pair.second,
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), false);

        ConnectContext context = StatisticUtils.buildConnectContext();
        AnalyzeStatus status = new NativeAnalyzeStatus(1, 1, 1, pair.first, StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        try {
            job.collect(context, status);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testFullJobs() {
        Pair<List<String>, List<Type>> pair = initColumn(List.of("c1", "c2", "c3"));

        List<HyperQueryJob> jobs = HyperQueryJob.createFullQueryJobs(connectContext, db, table, pair.first,
                pair.second, List.of(pid), 1, false, Map.of());

        Assertions.assertEquals(1, jobs.size());

        List<String> sqls = jobs.get(0).buildQuerySQL();
        Assertions.assertEquals(3, sqls.size());

        assertContains(sqls.get(0), "hex(hll_serialize(IFNULL(hll_raw(`c1`),");
        assertContains(sqls.get(1), "FROM `test`.`t_struct` partition `t_struct`");

        for (final var sql : sqls) {
            assertDoesNotThrow(() -> getFragmentPlan(sql));
        }
    }

    @Test
    public void testSampleJobs() {
        Pair<List<String>, List<Type>> pair = initColumn(List.of("c1", "c2", "c3"));
        final var tabletId = getAnyTabletId(table);
        new MockUp<SampleInfo>() {
            @Mock
            public List<TabletStats> getMediumHighWeightTablets() {
                return List.of(new TabletStats(tabletId, pid, 5000000));
            }
        };

        List<HyperQueryJob> jobs = HyperQueryJob.createSampleQueryJobs(connectContext, db, table, pair.first,
                pair.second, List.of(pid), 1, sampler, false, Map.of());

        Assertions.assertEquals(2, jobs.size());
        assertInstanceOf(MetaQueryJob.class, jobs.get(0));
        assertInstanceOf(SampleQueryJob.class, jobs.get(1));

        List<String> sqls = jobs.get(1).buildQuerySQL();
        Assertions.assertEquals(1, sqls.size());

        assertContains(sqls.get(0), "with base_cte_table as ( SELECT * FROM (SELECT * FROM `test`.`t_struct` " +
                "TABLET(" + tabletId + ") SAMPLE('percent'='10')) t_medium_high)");
        assertContains(sqls.get(0), "cast(IFNULL(SUM(CHAR_LENGTH(`c2`)) * 0/ COUNT(*), 0) as BIGINT), " +
                "hex(hll_serialize(IFNULL(hll_raw(`c2`), hll_empty())))," +
                " cast((COUNT(*) - COUNT(`c2`)) * 0 / COUNT(*) as BIGINT), " +
                "IFNULL(MAX(LEFT(`c2`, 200)), ''), IFNULL(MIN(LEFT(`c2`, 200)), ''), cast(-1.0 as BIGINT) " +
                "FROM (SELECT * FROM base_cte_table ) from_cte ");

        for (final var sql : sqls) {
            assertDoesNotThrow(() -> getFragmentPlan(sql));
        }
    }

    @Test
    public void testArrayNDV() {
        Pair<List<String>, List<Type>> pair = initColumn(List.of("c7"));

        List<HyperQueryJob> jobs = HyperQueryJob.createFullQueryJobs(connectContext, db, table, pair.first,
                pair.second, List.of(pid), 1, true, Map.of());

        Assertions.assertEquals(1, jobs.size());

        List<String> sqls = jobs.get(0).buildQuerySQL();
        assertContains(sqls.get(0), "'00'");
        Config.enable_manual_collect_array_ndv = true;
        sqls = jobs.get(0).buildQuerySQL();
        assertContains(sqls.get(0), "hex(hll_serialize(IFNULL(hll_raw(crc32_hash(`c7`)), hll_empty())))");
        Config.enable_manual_collect_array_ndv = false;

        for (final var sql : sqls) {
            assertDoesNotThrow(() -> getFragmentPlan(sql));
        }
    }

    @Test
    public void testSubfieldSampleJobs() {
        List<String> columnNames = Lists.newArrayList("c4.b", "c6.c.b");
        List<Type> columnTypes = Lists.newArrayList(new ArrayType(AnyStructType.ANY_STRUCT), IntegerType.INT);
        final var tabletId = getAnyTabletId(table);

        new MockUp<SampleInfo>() {
            @Mock
            public List<TabletStats> getMediumHighWeightTablets() {
                return List.of(new TabletStats(tabletId, pid, 5000000));
            }
        };

        List<HyperQueryJob> jobs = HyperQueryJob.createSampleQueryJobs(connectContext, db, table, columnNames,
                columnTypes, List.of(pid), 1, sampler, false, Map.of());

        Assertions.assertEquals(1, jobs.size());
        assertInstanceOf(SampleQueryJob.class, jobs.get(0));

        List<String> sqls = jobs.get(0).buildQuerySQL();
        Assertions.assertEquals(2, sqls.size());

        assertContains(sqls.get(1),
                "with base_cte_table as ( SELECT * FROM (SELECT * FROM `test`.`t_struct` TABLET(" + tabletId + ") SAMPLE" +
                        "('percent'='10'))");
        assertContains(sqls.get(1), "'c6.c.b', cast(0 as BIGINT), cast(4 * 0 as BIGINT), ");
        assertContains(sqls.get(1), "hex(hll_serialize(IFNULL(hll_raw(`c6`.`c`.`b`), hll_empty()))), ");
        assertContains(sqls.get(1), "cast((COUNT(*) - COUNT(`c6`.`c`.`b`)) * 0 / COUNT(*) as BIGINT), " +
                "IFNULL(MAX(`c6`.`c`.`b`), ''), IFNULL(MIN(`c6`.`c`.`b`), ''), cast(-1.0 as BIGINT) FROM " +
                "(SELECT * FROM base_cte_table ) from_cte");

        for (final var sql : sqls) {
            assertDoesNotThrow(() -> getFragmentPlan(sql));
        }
    }

    @Test
    public void testSampleRows() {
        new MockUp<TabletSampler>() {
            @Mock
            public List<TabletStats> sample() {
                return List.of(new TabletStats(1, pid, 5000000));
            }

        };
        PartitionSampler sampler = PartitionSampler.create(table, List.of(pid), Maps.newHashMap(), null);
        Assertions.assertEquals(5550000, sampler.getSampleInfo(pid).getSampleRowCount());
    }

    @Test
    public void testHyperQueryJobContextStartTime() {
        Pair<List<String>, List<Type>> pair = initColumn(List.of("c1", "c2", "c3"));

        new MockUp<SampleInfo>() {
            @Mock
            public List<TabletStats> getMediumHighWeightTablets() {
                return List.of(new TabletStats(1, pid, 5000000));
            }
        };

        List<HyperQueryJob> jobs = HyperQueryJob.createSampleQueryJobs(connectContext, db, table, pair.first,
                pair.second, List.of(pid), 1, sampler, false, Map.of());
        long startTime = connectContext.getStartTime();
        for (HyperQueryJob job : jobs) {
            job.queryStatistics();
            Assertions.assertNotEquals(startTime, connectContext.getStartTime());
        }
    }

    @Test
    public void testVirtualStatisticWithFullStatistics() throws Exception {
        // GIVEN
        final var namesAndTypes = initColumn(List.of("c7")); // c7 is array<int>
        Map<String, String> properties = Map.of(StatsConstants.UNNEST_VIRTUAL_STATISTICS, "true");

        // WHEN
        final var jobs = HyperQueryJob.createFullQueryJobs(connectContext, db, table, namesAndTypes.first,
                namesAndTypes.second, List.of(pid), 1, false, properties);

        // THEN
        assertFalse(jobs.isEmpty());

        boolean containsArrayColumn = false;
        boolean containsVirtualStat = false;

        for (final var job : jobs) {
            List<String> sqls = job.buildQuerySQL();
            assertEquals(2, sqls.size());
            for (String sql : sqls) {
                Assertions.assertNotNull(getFragmentPlan(sql)); // Make sure the query can be executed.

                List<StatementBase> stmt = SqlParser.parse(sql, connectContext.getSessionVariable());

                assertFalse(stmt.isEmpty());

                // Check for regular array column
                if (sql.contains("'c7'")) {
                    containsArrayColumn = true;
                }

                // Check for virtual statistic with unnest
                if (sql.contains("unnest(`c7`)") && sql.contains("VIRTUAL_STATISTIC_c7_UNNEST")) {
                    containsVirtualStat = true;
                }
            }
        }

        assertTrue(containsArrayColumn);
        assertTrue(containsVirtualStat);
    }

    private static long getAnyTabletId(Table table) {
        final var partition = table.getPartitions().stream().findFirst().get().getDefaultPhysicalPartition();
        final var tablet = partition.getLatestBaseIndex().getTablets().get(0);
        return tablet.getId();
    }

    @Test
    public void testVirtualStatisticWithSampleStatistics() throws Exception {
        // GIVEN
        final var namesAndTypes = initColumn(List.of("c7")); // c7 is array<int>

        new MockUp<SampleInfo>() {
            @Mock
            public List<TabletStats> getMediumHighWeightTablets() {
                return List.of(new TabletStats(getAnyTabletId(table), pid, 5000000));
            }

            @Mock
            public int getMaxSampleTabletNum() {
                return 1;
            }
        };

        Map<String, String> properties = Map.of(StatsConstants.UNNEST_VIRTUAL_STATISTICS, "true");
        List<HyperQueryJob> jobs = HyperQueryJob.createSampleQueryJobs(connectContext, db, table, namesAndTypes.first,
                namesAndTypes.second, List.of(pid), 1, sampler, false, properties);

        // Should have jobs for both regular and virtual columns
        assertFalse(jobs.isEmpty());

        boolean containsArrayColumn = false;
        boolean containsVirtualStat = false;

        for (HyperQueryJob job : jobs) {
            List<String> sqls = job.buildQuerySQL();
            for (String sql : sqls) {
                Assertions.assertNotNull(getFragmentPlan(sql)); // Make sure the query can be executed.
                List<StatementBase> stmt = SqlParser.parse(sql, connectContext.getSessionVariable());
                assertFalse(stmt.isEmpty());

                // Check for regular array column (column name in the SQL as string literal or backticked)
                if (sql.contains("'c7'") || sql.contains("`c7`")) {
                    containsArrayColumn = true;
                }

                // Check for virtual statistic with unnest - in sample mode it uses subquery
                if (sql.contains("unnest(`c7`)") && sql.contains("VIRTUAL_STATISTIC_c7_UNNEST")) {
                    containsVirtualStat = true;
                }
            }
        }

        assertTrue(containsArrayColumn);
        assertTrue(containsVirtualStat);
    }

    @Test
    public void testVirtualStatisticNotAddedWhenDisabled() throws Exception {
        // GIVEN
        final var namesAndTypes = initColumn(List.of("c7")); // c7 is array<int>
        Map<String, String> properties = Map.of(StatsConstants.UNNEST_VIRTUAL_STATISTICS, "false");

        // WHEN
        List<HyperQueryJob> jobs = HyperQueryJob.createFullQueryJobs(connectContext, db, table, namesAndTypes.first,
                namesAndTypes.second, List.of(pid), 1, false, properties);

        // THEN
        for (HyperQueryJob job : jobs) {
            List<String> sqls = job.buildQuerySQL();
            for (String sql : sqls) {
                Assertions.assertNotNull(getFragmentPlan(sql)); // Make sure the query can be executed.
                assertFalse(sql.contains("VIRTUAL_STATISTIC"));
                assertFalse(sql.contains("unnest"));
            }
        }
    }

    @AfterAll
    public static void afterClass() {
        FeConstants.runningUnitTest = false;
    }
}
