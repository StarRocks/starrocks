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


package com.starrocks.sql.optimizer.statistics;


import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;

public class CachedStatisticStorageTest {
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    public static final String DEFAULT_CREATE_TABLE_TEMPLATE = ""
            + "CREATE TABLE IF NOT EXISTS `table_statistic_v1` (\n"
            + "  `table_id` bigint NOT NULL,\n"
            + "  `column_name` varchar(65530) NOT NULL,\n"
            + "  `db_id` bigint NOT NULL,\n"
            + "  `table_name` varchar(65530) NOT NULL,\n"
            + "  `db_name` varchar(65530) NOT NULL,\n"
            + "  `row_count` bigint NOT NULL,\n"
            + "  `data_size` bigint NOT NULL,\n"
            + "  `distinct_count` bigint NOT NULL,\n"
            + "  `null_count` bigint NOT NULL,\n"
            + "  `max` varchar(65530) NOT NULL,\n"
            + "  `min` varchar(65530) NOT NULL,\n"
            + "  `update_time` datetime NOT NULL\n"
            + "  )\n"
            + "ENGINE=OLAP\n"
            + "UNIQUE KEY(`table_id`,  `column_name`, `db_id`)\n"
            + "DISTRIBUTED BY HASH(`table_id`, `column_name`, `db_id`) BUCKETS 2\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"1\",\n"
            + "\"in_memory\" = \"false\",\n"
            + "\"storage_format\" = \"V2\"\n"
            + ");";

    public static void createStatisticsTable() throws Exception {
        CreateDbStmt dbStmt = new CreateDbStmt(false, StatsConstants.STATISTICS_DB_NAME);
        try {
            GlobalStateMgr.getCurrentState().getMetadata().createDb(dbStmt.getFullDbName());
        } catch (DdlException e) {
            return;
        }
        starRocksAssert.useDatabase(StatsConstants.STATISTICS_DB_NAME);
        starRocksAssert.withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        createStatisticsTable();
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL COMMENT \"\",\n" +
                "  `v4` date NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
    }

    @Mocked
    StatisticExecutor statisticExecutor;

    @Test
    public void testGetColumnStatistic(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) db.getTable("t0");

        new Expectations() {{
                cachedStatisticStorage.getColumnStatistic(table, "v1");
                result = ColumnStatistic.builder().setDistinctValuesCount(888).build();
                minTimes = 0;

                cachedStatisticStorage.getColumnStatistic(table, "v2");
                result = ColumnStatistic.builder().setDistinctValuesCount(999).build();
                minTimes = 0;

                cachedStatisticStorage.getColumnStatistic(table, "v3");
                result = ColumnStatistic.builder().setDistinctValuesCount(666).build();
                minTimes = 0;
            }};
        ColumnStatistic columnStatistic1 =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v1");
        Assert.assertEquals(888, columnStatistic1.getDistinctValuesCount(), 0.001);

        ColumnStatistic columnStatistic2 =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v2");
        Assert.assertEquals(999, columnStatistic2.getDistinctValuesCount(), 0.001);

        ColumnStatistic columnStatistic3 =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v3");
        Assert.assertEquals(666, columnStatistic3.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testGetColumnStatistics(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) db.getTable("t0");

        ColumnStatistic columnStatistic1 = ColumnStatistic.builder().setDistinctValuesCount(888).build();
        ColumnStatistic columnStatistic2 = ColumnStatistic.builder().setDistinctValuesCount(999).build();

        new Expectations() {{
                cachedStatisticStorage.getColumnStatistics(table, ImmutableList.of("v1", "v2"));
                result = ImmutableList.of(columnStatistic1, columnStatistic2);
                minTimes = 0;
            }};
        List<ColumnStatistic> columnStatistics = Deencapsulation
                .invoke(cachedStatisticStorage, "getColumnStatistics", table, ImmutableList.of("v1", "v2"));
        Assert.assertEquals(2, columnStatistics.size());
        Assert.assertEquals(888, columnStatistics.get(0).getDistinctValuesCount(), 0.001);
        Assert.assertEquals(999, columnStatistics.get(1).getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testLoadCacheLoadEmpty(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        Table table = db.getTable("t0");

        new Expectations() {{
                cachedStatisticStorage.getColumnStatistic(table, "v1");
                result = ColumnStatistic.unknown();
                minTimes = 0;
            }};
        ColumnStatistic columnStatistic =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v1");
        Assert.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(0.0, columnStatistic.getNullsFraction(), 0.001);
        Assert.assertEquals(1.0, columnStatistic.getAverageRowSize(), 0.001);
        Assert.assertEquals(1.0, columnStatistic.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testConvert2ColumnStatistics() {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) db.getTable("t0");
        ColumnBasicStatsCacheLoader cachedStatisticStorage = Deencapsulation.newInstance(ColumnBasicStatsCacheLoader.class);

        TStatisticData statisticData = new TStatisticData();
        statisticData.setDbId(db.getId());
        statisticData.setTableId(table.getId());
        statisticData.setColumnName("v1");
        statisticData.setMax("123");
        statisticData.setMin("0");

        ColumnStatistic columnStatistic =
                Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(123, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(0, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v4");
        statisticData.setMax("2021-05-21");
        statisticData.setMin("2021-05-20");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2021, 5, 21, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2021, 5, 20, 0, 0, 0)),
                columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v1");
        statisticData.setMin("aa");
        statisticData.setMax("bb");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v1");
        statisticData.setMin("");
        statisticData.setMax("");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v4");
        statisticData.setMin("");
        statisticData.setMax("");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v4");
        statisticData.setMin("");
        statisticData.setMax("");
        statisticData.setRowCount(0);
        statisticData.setDataSize(0);
        statisticData.setNullCount(0);
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(0, columnStatistic.getAverageRowSize(), 0.001);
        Assert.assertEquals(0, columnStatistic.getNullsFraction(), 0.001);
    }
}
