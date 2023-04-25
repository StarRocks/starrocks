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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.Bucket;
import com.starrocks.sql.optimizer.statistics.ColumnBasicStatsCacheLoader;
import com.starrocks.sql.optimizer.statistics.ColumnHistogramStatsCacheLoader;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CacheLoaderTest {
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL COMMENT \"\",\n" +
                "  `v4` date NULL,\n" +
                "  `v5` datetime NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testCovertBasicStatistics() {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) db.getTable("t0");
        ColumnBasicStatsCacheLoader basicStatsCacheLoader
                = Deencapsulation.newInstance(ColumnBasicStatsCacheLoader.class);

        TStatisticData statisticData = new TStatisticData();
        statisticData.setDbId(db.getId());
        statisticData.setTableId(table.getId());
        statisticData.setColumnName("v4");
        statisticData.setMin("0000-01-01");
        statisticData.setMax("9999-12-31");
        statisticData.setCountDistinct(10);
        statisticData.setDataSize(10);
        statisticData.setRowCount(10);
        statisticData.setNullCount(5);

        ColumnStatistic columnStatistic =
                Deencapsulation.invoke(basicStatsCacheLoader, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(-6.2167248343E10, columnStatistic.getMinValue(), 0.1);
        Assert.assertEquals(2.534021856E11, columnStatistic.getMaxValue(), 0.1);


        statisticData = new TStatisticData();
        statisticData.setDbId(db.getId());
        statisticData.setTableId(table.getId());
        statisticData.setColumnName("v5");
        statisticData.setMin("0000-01-01 00:00:00");
        statisticData.setMax("9999-12-31 00:00:00");
        statisticData.setCountDistinct(10);
        statisticData.setDataSize(10);
        statisticData.setRowCount(10);
        statisticData.setNullCount(5);

        columnStatistic =
                Deencapsulation.invoke(basicStatsCacheLoader, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(-6.2167248343E10, columnStatistic.getMinValue(), 0.1);
        Assert.assertEquals(2.534021856E11, columnStatistic.getMaxValue(), 0.1);
    }

    @Test
    public void testCovertHistogramStatistics() {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) db.getTable("t0");
        ColumnHistogramStatsCacheLoader columnHistogramStatsCacheLoader
                = Deencapsulation.newInstance(ColumnHistogramStatsCacheLoader.class);

        TStatisticData statisticData = new TStatisticData();
        statisticData.setDbId(db.getId());
        statisticData.setTableId(table.getId());
        statisticData.setColumnName("v1");
        statisticData.setHistogram("{ \"buckets\" : [[\"2\",\"7\",\"6\",\"1\"],[\"8\",\"13\",\"12\",\"1\"]," +
                "[\"14\",\"21\",\"18\",\"1\"],[\"22\",\"28\",\"24\",\"1\"],[\"29\",\"35\",\"30\",\"1\"]], " +
                "\"mcv\" : [[\"27\",\"8\"],[\"19\",\"5\"],[\"20\",\"4\"]] }");

        Histogram histogram = Deencapsulation.invoke(columnHistogramStatsCacheLoader, "convert2Histogram", statisticData);
        Assert.assertEquals(5, histogram.getBuckets().size());
        Bucket bucket = histogram.getBuckets().get(0);
        Assert.assertEquals(2, bucket.getLower(), 0.1);
        Assert.assertEquals(7, bucket.getUpper(), 0.1);
        Assert.assertEquals(6, bucket.getCount(), 0.1);
        Assert.assertEquals(1, bucket.getUpperRepeats(), 0.1);

        Assert.assertEquals(3, histogram.getMCV().size());
        Assert.assertEquals("{27=8, 19=5, 20=4}", histogram.getMCV().toString());

        statisticData.setColumnName("v4");
        statisticData.setHistogram("{ \"buckets\" : [[\"2022-01-02\",\"2022-01-07\",\"6\",\"1\"]," +
                "[\"2022-01-08\",\"2022-01-13\",\"12\",\"1\"]," +
                "[\"2022-01-14\",\"2022-01-21\",\"18\",\"1\"]," +
                "[\"2022-01-22\",\"2022-01-28\",\"24\",\"1\"]," +
                "[\"2022-01-29\",\"2022-01-30\",\"30\",\"1\"]], " +
                "\"mcv\" : [[\"2022-01-27\",\"8\"],[\"2022-01-19\",\"5\"],[\"2022-01-20\",\"4\"]] }");

        histogram = Deencapsulation.invoke(columnHistogramStatsCacheLoader, "convert2Histogram", statisticData);
        bucket = histogram.getBuckets().get(0);
        Assert.assertEquals(1.6410528E9, bucket.getLower(), 0.1);
        Assert.assertEquals(1.6414848E9, bucket.getUpper(), 0.1);
        Assert.assertEquals(6, bucket.getCount(), 0.1);
        Assert.assertEquals(1, bucket.getUpperRepeats(), 0.1);
        Assert.assertEquals("{2022-01-19=5, 2022-01-27=8, 2022-01-20=4}", histogram.getMCV().toString());
    }

    @Test
    public void testCovertHistogramStatisticsDate() {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) db.getTable("t0");
        ColumnHistogramStatsCacheLoader columnHistogramStatsCacheLoader
                = Deencapsulation.newInstance(ColumnHistogramStatsCacheLoader.class);

        TStatisticData statisticData = new TStatisticData();
        statisticData.setDbId(db.getId());
        statisticData.setTableId(table.getId());
        statisticData.setColumnName("v4");

        statisticData.setHistogram("{ \"buckets\" : [[\"0000-01-01\",\"2022-01-01\",\"10\",\"1\"]," +
                "[\"2022-01-02\",\"9999-12-31\",\"10\",\"1\"]], " +
                "\"mcv\" : [[\"0000-01-01\",\"8\"],[\"9999-12-31\",\"5\"]] }");
        Histogram histogram = Deencapsulation.invoke(columnHistogramStatsCacheLoader, "convert2Histogram", statisticData);

        Bucket bucket = histogram.getBuckets().get(0);
        Assert.assertEquals(-6.2167248343E10, bucket.getLower(), 0.1);
        Assert.assertEquals(1.6409664E9, bucket.getUpper(), 0.1);

        bucket = histogram.getBuckets().get(1);
        Assert.assertEquals(1.6410528E9, bucket.getLower(), 0.1);
        Assert.assertEquals(2.534021856E11, bucket.getUpper(), 0.1);

        Assert.assertEquals("{0000-01-01=8, 9999-12-31=5}", histogram.getMCV().toString());
    }
}
