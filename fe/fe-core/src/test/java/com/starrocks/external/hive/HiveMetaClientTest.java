// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.external.Utils;
import com.starrocks.external.hive.text.TextFileFormatDesc;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HiveMetaClientTest {
    @Test
    public void testClientPool(@Mocked HiveMetaStoreThriftClient metaStoreClient) throws Exception {
        new Expectations() {
            {
                metaStoreClient.getTable(anyString, anyString);
                result = new Table();
                minTimes = 0;
            }
        };

        final int[] clientNum = {0};

        new MockUp<RetryingMetaStoreClient>() {
            @Mock
            public IMetaStoreClient getProxy(Configuration hiveConf, HiveMetaHookLoader hookLoader,
                                             ConcurrentHashMap<String, Long> metaCallTimeMap, String mscClassName,
                                             boolean allowEmbedded) throws MetaException {
                clientNum[0]++;
                return metaStoreClient;
            }
        };

        HiveMetaClient client = new HiveMetaClient("thrift://127.0.0.1:9030");
        // NOTE: this is HiveMetaClient.MAX_HMS_CONNECTION_POOL_SIZE
        int poolSize = 32;

        // call client method concurrently,
        // and make sure the number of hive clients will not exceed poolSize
        for (int i = 0; i < 10; i++) {
            ExecutorService es = Executors.newCachedThreadPool();
            for (int j = 0; j < poolSize; j++) {
                es.execute(() -> {
                    try {
                        client.getTable("db", "tbl");
                    } catch (Exception e) {
                        e.printStackTrace();
                        Assert.fail(e.getMessage());
                    }
                });
            }
            es.shutdown();
            es.awaitTermination(1, TimeUnit.HOURS);
        }

        System.out.println("called times is " + clientNum[0]);

        Assert.assertTrue(clientNum[0] >= 1 && clientNum[0] <= poolSize);
    }

    @Test
    public void testGetTableLevelColumnStatsForPartTable(@Mocked HiveMetaStoreThriftClient metaStoreClient)
            throws Exception {
        List<String> partColumnNames = Lists.newArrayList("pCol1", "pCol2");
        List<Column> partColumns = Lists.newArrayList(new Column("pCol1", Type.DATE),
                new Column("pCol2", ScalarType.createVarcharType(20)));
        List<List<String>> partValuesList = Lists.newArrayList(Lists.newArrayList("2021-07-01", "beijing"),
                Lists.newArrayList("2021-07-01", HiveMetaClient.PARTITION_NULL_VALUE),
                Lists.newArrayList(HiveMetaClient.PARTITION_NULL_VALUE, "beijing"));
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        List<Partition> partitions = Lists.newArrayList();
        // partitions row number: 0, 10, 20
        for (int i = 0; i < 3; i++) {
            List<String> partValues = partValuesList.get(i);
            partitionKeys.add(Utils.createPartitionKey(partValues, partColumns));
            Partition partition = new Partition();
            partition.setValues(partValues);
            Map<String, String> properties = Maps.newHashMap();
            properties.put(StatsSetupConst.ROW_COUNT, String.format("%d", i * 10));
            partition.setParameters(properties);
            partitions.add(partition);
        }

        Map<String, List<ColumnStatisticsObj>> partitionColumnStats = Maps.newHashMap();
        // col1: bool, col2: int, col3: float, col4: date, col5: string
        List<String> allColumnNames = Lists.newArrayList("pCol1", "pCol2", "col1", "col2", "col3", "col4", "col5");
        String partName = "pcol1=2021-07-01/pcol2=beijing";
        partitionColumnStats.put(partName, Lists.newArrayList(new ColumnStatisticsObj("col1", "BOOLEAN",
                        ColumnStatisticsData.booleanStats(new BooleanColumnStatsData())),
                new ColumnStatisticsObj("col2", "INT",
                        ColumnStatisticsData.longStats(new LongColumnStatsData())),
                new ColumnStatisticsObj("col3", "FLOAT",
                        ColumnStatisticsData.doubleStats(new DoubleColumnStatsData())),
                new ColumnStatisticsObj("col4", "DATE",
                        ColumnStatisticsData.dateStats(new DateColumnStatsData())),
                new ColumnStatisticsObj("col5", "STRING",
                        ColumnStatisticsData.stringStats(new StringColumnStatsData()))));

        partName = "pcol1=2021-07-01/pcol2=" + HiveMetaClient.PARTITION_NULL_VALUE;
        LongColumnStatsData longColumnStatsData = new LongColumnStatsData(4L, 5L);
        longColumnStatsData.setHighValue(100L);
        longColumnStatsData.setLowValue(-100L);
        DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData(1, 5);
        doubleColumnStatsData.setHighValue(1000);
        doubleColumnStatsData.setLowValue(-1000);
        DateColumnStatsData dateColumnStatsData = new DateColumnStatsData(2, 4);
        dateColumnStatsData.setHighValue(new Date(100000));
        dateColumnStatsData.setLowValue(new Date(10000));
        partitionColumnStats.put(partName, Lists.newArrayList(new ColumnStatisticsObj("col1", "BOOLEAN",
                        ColumnStatisticsData.booleanStats(new BooleanColumnStatsData(4, 5, 1))),
                new ColumnStatisticsObj("col2", "INT",
                        ColumnStatisticsData.longStats(longColumnStatsData)),
                new ColumnStatisticsObj("col3", "FLOAT",
                        ColumnStatisticsData.doubleStats(doubleColumnStatsData)),
                new ColumnStatisticsObj("col4", "DATE",
                        ColumnStatisticsData.dateStats(dateColumnStatsData)),
                new ColumnStatisticsObj("col5", "STRING",
                        ColumnStatisticsData.stringStats(new StringColumnStatsData(1000, 20, 1, 5)))));
        partName = "pcol1=" + HiveMetaClient.PARTITION_NULL_VALUE + "/pcol2=beijing";
        longColumnStatsData = new LongColumnStatsData(8L, 2L);
        longColumnStatsData.setHighValue(80L);
        longColumnStatsData.setLowValue(-50L);
        doubleColumnStatsData = new DoubleColumnStatsData(3, 5);
        doubleColumnStatsData.setHighValue(2000);
        doubleColumnStatsData.setLowValue(-1000);
        dateColumnStatsData = new DateColumnStatsData(4, 8);
        dateColumnStatsData.setHighValue(new Date(200000));
        dateColumnStatsData.setLowValue(new Date(1000));
        partitionColumnStats.put(partName, Lists.newArrayList(new ColumnStatisticsObj("col1", "BOOLEAN",
                        ColumnStatisticsData.booleanStats(new BooleanColumnStatsData(2, 15, 3))),
                new ColumnStatisticsObj("col2", "INT",
                        ColumnStatisticsData.longStats(longColumnStatsData)),
                new ColumnStatisticsObj("col3", "FLOAT",
                        ColumnStatisticsData.doubleStats(doubleColumnStatsData)),
                new ColumnStatisticsObj("col4", "DATE",
                        ColumnStatisticsData.dateStats(dateColumnStatsData)),
                new ColumnStatisticsObj("col5", "STRING",
                        ColumnStatisticsData.stringStats(new StringColumnStatsData(2000, 30, 3, 2)))));

        new Expectations() {
            {
                metaStoreClient.getCurrentNotificationEventId();
                result = new CurrentNotificationEventId(1L);
                minTimes = 0;

                metaStoreClient
                        .getPartitionColumnStatistics(anyString, anyString, (List<String>) any, (List<String>) any);
                result = partitionColumnStats;
                minTimes = 0;

                metaStoreClient.getPartitionsByNames(anyString, anyString, (List<String>) any);
                result = partitions;
                minTimes = 0;
            }
        };

        new MockUp<RetryingMetaStoreClient>() {
            @Mock
            public IMetaStoreClient getProxy(Configuration hiveConf, HiveMetaHookLoader hookLoader,
                                             ConcurrentHashMap<String, Long> metaCallTimeMap, String mscClassName,
                                             boolean allowEmbedded) throws MetaException {
                return metaStoreClient;
            }
        };

        HiveMetaClient client = new HiveMetaClient("thrift://127.0.0.1:9030");
        Map<String, HiveColumnStats> stats =
                client.getTableLevelColumnStatsForPartTable("db", "tbl", partitionKeys, partColumns, allColumnNames,
                        false);
        HiveColumnStats partitionStats = stats.get("col1");
        Assert.assertEquals(partitionStats.getNumNulls(), 4L);
        Assert.assertEquals(partitionStats.getNumDistinctValues(), 3L);

        partitionStats = stats.get("col2");
        Assert.assertEquals(partitionStats.getNumNulls(), 12L);
        Assert.assertEquals(partitionStats.getNumDistinctValues(), 5L);
        Assert.assertTrue(doubleEqual(partitionStats.getMaxValue(), 100f));
        Assert.assertTrue(doubleEqual(partitionStats.getMinValue(), -100f));

        partitionStats = stats.get("col3");
        Assert.assertEquals(partitionStats.getNumNulls(), 4L);
        Assert.assertEquals(partitionStats.getNumDistinctValues(), 5L);
        Assert.assertTrue(doubleEqual(partitionStats.getMaxValue(), 2000f));
        Assert.assertTrue(doubleEqual(partitionStats.getMinValue(), -1000f));

        partitionStats = stats.get("col4");
        Assert.assertEquals(partitionStats.getNumNulls(), 6L);
        Assert.assertEquals(partitionStats.getNumDistinctValues(), 8L);
        Assert.assertTrue(doubleEqual(partitionStats.getMaxValue(), 17280000000f));
        Assert.assertTrue(doubleEqual(partitionStats.getMinValue(), 86400000f));

        partitionStats = stats.get("col5");
        Assert.assertEquals(partitionStats.getNumNulls(), 4L);
        Assert.assertEquals(partitionStats.getNumDistinctValues(), 5L);
        Assert.assertTrue(doubleEqual(partitionStats.getAvgSize(), 26.53846153846154f));

        partitionStats = stats.get("pCol1");
        Assert.assertEquals(partitionStats.getNumNulls(), 20L);
        Assert.assertEquals(partitionStats.getNumDistinctValues(), 2L);
        Assert.assertTrue(doubleEqual(partitionStats.getMaxValue(), 1625068800f));
        Assert.assertTrue(doubleEqual(partitionStats.getMinValue(), 1625068800f));

        partitionStats = stats.get("pCol2");
        Assert.assertEquals(partitionStats.getNumNulls(), 10L);
        Assert.assertEquals(partitionStats.getNumDistinctValues(), 2L);
        Assert.assertTrue(doubleEqual(partitionStats.getAvgSize(), 7f));
    }

    @Test
    public void testGetTextFileFormatDesc() throws Exception {
        HiveMetaClient client = new HiveMetaClient("localhost");

        // Check is using default delimiter
        StorageDescriptor emptySd = new StorageDescriptor();
        emptySd.setSerdeInfo(new SerDeInfo("test", "test", new HashMap<>()));
        TextFileFormatDesc emptyDesc = client.getTextFileFormatDesc(emptySd);
        Assert.assertEquals("\001", emptyDesc.getFieldDelim());
        Assert.assertEquals("\n", emptyDesc.getLineDelim());
        Assert.assertEquals("\002", emptyDesc.getCollectionDelim());
        Assert.assertEquals("\003", emptyDesc.getMapkeyDelim());

        // Check is using custom delimiter
        StorageDescriptor customSd = new StorageDescriptor();
        Map<String, String> parameters = new HashMap<>();
        parameters.put("field.delim", ",");
        parameters.put("line.delim", "\004");
        parameters.put("collection.delim", "\006");
        parameters.put("mapkey.delim", ":");
        customSd.setSerdeInfo(new SerDeInfo("test", "test", parameters));
        TextFileFormatDesc customDesc = client.getTextFileFormatDesc(customSd);
        Assert.assertEquals(",", customDesc.getFieldDelim());
        Assert.assertEquals("\004", customDesc.getLineDelim());
        Assert.assertEquals("\006", customDesc.getCollectionDelim());
        Assert.assertEquals(":", customDesc.getMapkeyDelim());
    }

    private boolean doubleEqual(double v1, double v2) {
        return Math.abs(v1 - v2) < 1e-6;
    }
}

