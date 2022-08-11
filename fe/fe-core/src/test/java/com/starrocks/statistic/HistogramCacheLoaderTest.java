// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.statistic;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.Bucket;
import com.starrocks.sql.optimizer.statistics.ColumnHistogramStatsCacheLoader;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HistogramCacheLoaderTest {
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        String DB_NAME = "test";
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

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
        Assert.assertEquals("{19.0=5, 20.0=4, 27.0=8}", histogram.getMCV().toString());

        statisticData.setColumnName("v4");
        statisticData.setHistogram("{ \"buckets\" : [[\"20220102\",\"20220107\",\"6\",\"1\"],[\"20220108\",\"20220113\",\"12\",\"1\"]," +
                "[\"20220114\",\"20220121\",\"18\",\"1\"],[\"20220122\",\"20220128\",\"24\",\"1\"],[\"20220129\",\"20220130\",\"30\",\"1\"]], " +
                "\"mcv\" : [[\"20220127\",\"8\"],[\"20220119\",\"5\"],[\"20220120\",\"4\"]] }");

        histogram = Deencapsulation.invoke(columnHistogramStatsCacheLoader, "convert2Histogram", statisticData);
        bucket = histogram.getBuckets().get(0);
        Assert.assertEquals(1.6410528E9, bucket.getLower(), 0.1);
        Assert.assertEquals(1.6414848E9, bucket.getUpper(), 0.1);
        Assert.assertEquals(6, bucket.getCount(), 0.1);
        Assert.assertEquals(1, bucket.getUpperRepeats(), 0.1);
        Assert.assertEquals("{1.6425216E9=5, 1.6432128E9=8, 1.642608E9=4}", histogram.getMCV().toString());
    }
}
