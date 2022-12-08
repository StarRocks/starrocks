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


package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.TEST_FILES;

public class HiveStatisticsProviderTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private CachingHiveMetastore cachingHiveMetastore;
    private HiveMetastoreOperations hmsOps;
    private HiveRemoteFileIO hiveRemoteFileIO;
    private CachingRemoteFileIO cachingRemoteFileIO;
    private RemoteFileOperations fileOps;
    private ExecutorService executorForHmsRefresh;
    private ExecutorService executorForRemoteFileRefresh;
    private ExecutorService executorForPullFiles;
    private HiveStatisticsProvider statisticsProvider;

    private static ConnectContext connectContext;
    private static OptimizerContext optimizerContext;
    private static ColumnRefFactory columnRefFactory;

    @Before
    public void setUp() throws Exception {
        executorForHmsRefresh = Executors.newFixedThreadPool(5);
        executorForRemoteFileRefresh = Executors.newFixedThreadPool(5);
        executorForPullFiles = Executors.newFixedThreadPool(5);

        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "hive_catalog");
        cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executorForHmsRefresh, 100, 10, 1000, false);
        hmsOps = new HiveMetastoreOperations(cachingHiveMetastore, true);

        hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(TEST_FILES);
        hiveRemoteFileIO.setFileSystem(fs);
        cachingRemoteFileIO = CachingRemoteFileIO.createCatalogLevelInstance(
                hiveRemoteFileIO, executorForRemoteFileRefresh, 100, 10, 10);
        fileOps = new RemoteFileOperations(cachingRemoteFileIO, executorForPullFiles, false, true);
        statisticsProvider = new HiveStatisticsProvider(hmsOps, fileOps);

        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = new OptimizerContext(new Memo(), columnRefFactory, connectContext);
    }

    @After
    public void tearDown() {
        executorForHmsRefresh.shutdown();
        executorForRemoteFileRefresh.shutdown();
        executorForPullFiles.shutdown();
    }

    @Test
    public void testGetTableStatistics() throws AnalysisException {
        HiveTable hiveTable = (HiveTable) hmsOps.getTable("db1", "table1");
        ColumnRefOperator partColumnRefOperator = new ColumnRefOperator(0, Type.INT, "col1", true);
        ColumnRefOperator dataColumnRefOperator = new ColumnRefOperator(1, Type.INT, "col2", true);
        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());
        Statistics statistics = statisticsProvider.getTableStatistics(
                optimizerContext, hiveTable, Lists.newArrayList(partColumnRefOperator, dataColumnRefOperator),
                Lists.newArrayList(hivePartitionKey1, hivePartitionKey2));
        Assert.assertEquals(1,  statistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(0, statistics.getColumnStatistics().size());

        cachingHiveMetastore.getPartitionStatistics(hiveTable, Lists.newArrayList("col1=1", "col1=2"));
        statistics = statisticsProvider.getTableStatistics(
                optimizerContext, hiveTable, Lists.newArrayList(partColumnRefOperator, dataColumnRefOperator),
                Lists.newArrayList(hivePartitionKey1, hivePartitionKey2));
        Assert.assertEquals(100, statistics.getOutputRowCount(), 0.001);
        Map<ColumnRefOperator, ColumnStatistic> columnStatistics = statistics.getColumnStatistics();
        Assert.assertEquals(2, statistics.getColumnStatistics().size());
        ColumnStatistic partitionColumnStats = columnStatistics.get(partColumnRefOperator);
        Assert.assertEquals(1, partitionColumnStats.getMinValue(), 0.001);
        Assert.assertEquals(2, partitionColumnStats.getMaxValue(), 0.001);
        Assert.assertEquals(0, partitionColumnStats.getNullsFraction(), 0.001);
        Assert.assertEquals(4, partitionColumnStats.getAverageRowSize(), 0.001);
        Assert.assertEquals(2, partitionColumnStats.getDistinctValuesCount(), 0.001);

        ColumnStatistic dataColumnStats = columnStatistics.get(dataColumnRefOperator);
        Assert.assertEquals(0, dataColumnStats.getMinValue(), 0.001);
        Assert.assertEquals(0.03, dataColumnStats.getNullsFraction(), 0.001);
        Assert.assertEquals(4, dataColumnStats.getAverageRowSize(), 0.001);
        Assert.assertEquals(5, dataColumnStats.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testCreateUnpartitionedStats() {
        HiveTable hiveTable = (HiveTable) hmsOps.getTable("db1", "table1");
        ColumnRefOperator dataColumnRefOperator = new ColumnRefOperator(1, Type.INT, "col2", true);
        cachingHiveMetastore.getPartitionStatistics(hiveTable, Lists.newArrayList("col1=1"));
        Map<String, HivePartitionStats> statisticsMap = hmsOps.getPartitionStatistics(
                hiveTable, Lists.newArrayList("col1=1"));
        HivePartitionStats dataStats = statisticsMap.get("col1=1");
        Statistics.Builder builder = Statistics.builder();
        Statistics statistics = statisticsProvider.createUnpartitionedStats(
                dataStats, Lists.newArrayList(dataColumnRefOperator), builder, hiveTable);
        Map<ColumnRefOperator, ColumnStatistic> columnStatistics = statistics.getColumnStatistics();
        ColumnStatistic dataColumnStats = columnStatistics.get(dataColumnRefOperator);
        Assert.assertEquals(0, dataColumnStats.getMinValue(), 0.001);
        Assert.assertEquals(0.02, dataColumnStats.getNullsFraction(), 0.001);
        Assert.assertEquals(4, dataColumnStats.getAverageRowSize(), 0.001);
        Assert.assertEquals(2, dataColumnStats.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testCreateUnknownStatistics() throws AnalysisException {
        HiveTable hiveTable = (HiveTable) hmsOps.getTable("db1", "table1");
        ColumnRefOperator partColumnRefOperator = new ColumnRefOperator(0, Type.INT, "col1", true);
        ColumnRefOperator dataColumnRefOperator = new ColumnRefOperator(1, Type.INT, "col2", true);
        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());

        Statistics statistics = statisticsProvider.createUnknownStatistics(
                hiveTable, Lists.newArrayList(partColumnRefOperator, dataColumnRefOperator),
                Lists.newArrayList(hivePartitionKey1, hivePartitionKey2), 100);
        Assert.assertEquals(100, statistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(2, statistics.getColumnStatistics().size());
        Assert.assertTrue(statistics.getColumnStatistics().get(partColumnRefOperator).isUnknown());
        Assert.assertTrue(statistics.getColumnStatistics().get(dataColumnRefOperator).isUnknown());
    }

    @Test
    public void testEstimatedRowCount() throws AnalysisException {
        FeConstants.runningUnitTest = true;
        HiveTable hiveTable = (HiveTable) hmsOps.getTable("db1", "table1");

        List<String> partitionNames = Lists.newArrayList("col1=1", "col1=2");
        Map<String, Partition> partitions = metastore.getPartitionsByNames("db1", "table1", partitionNames);
        fileOps.getRemoteFiles(Lists.newArrayList(partitions.values()));
        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());
        long res = statisticsProvider.getEstimatedRowCount(hiveTable, Lists.newArrayList(hivePartitionKey1, hivePartitionKey2));
        Assert.assertEquals(10, res);
    }

    @Test
    public void testSamplePartitoins() {
        List<String> partitionNames = Lists.newArrayList("k=1", "k=2", "k=3", "k=4", "k=5");
        List<String> sampledPartitions = HiveStatisticsProvider.getPartitionsSample(partitionNames, 3);
        Assert.assertEquals(3, sampledPartitions.size());
        Assert.assertTrue(sampledPartitions.contains("k=1"));
        Assert.assertTrue(sampledPartitions.contains("k=5"));
    }

    @Test
    public void testHiveColumnInit() {
        HiveColumnStats stats = new HiveColumnStats();
        ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
        BooleanColumnStatsData booleanColumnStatsData = new BooleanColumnStatsData();
        booleanColumnStatsData.setNumTrues(10);
        columnStatisticsData.setBooleanStats(booleanColumnStatsData);
        stats.initialize(columnStatisticsData, 10);
        Assert.assertEquals(1, stats.getMax(), 0.000001);
        Assert.assertEquals(1, stats.getMin(), 0.000001);

        booleanColumnStatsData = new BooleanColumnStatsData();
        booleanColumnStatsData.setNumNulls(10);
        columnStatisticsData.setBooleanStats(booleanColumnStatsData);
        stats.initialize(columnStatisticsData, 10);
        Assert.assertEquals(0, stats.getMax(), 0.000001);
        Assert.assertEquals(0, stats.getMin(), 0.000001);


        columnStatisticsData = new ColumnStatisticsData();
        LongColumnStatsData longColumnStatsData = new LongColumnStatsData();
        longColumnStatsData.setNumNulls(1);
        columnStatisticsData.setLongStats(longColumnStatsData);
        stats.initialize(columnStatisticsData, 50);
        Assert.assertEquals(1, stats.getNumNulls());

        columnStatisticsData = new ColumnStatisticsData();
        DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData();
        doubleColumnStatsData.setNumNulls(2);
        columnStatisticsData.setDoubleStats(doubleColumnStatsData);
        stats.initialize(columnStatisticsData, 60);
        Assert.assertEquals(2, stats.getNumNulls());

        columnStatisticsData = new ColumnStatisticsData();
        DateColumnStatsData dateColumnStatsData = new DateColumnStatsData();
        dateColumnStatsData.setNumNulls(3);
        columnStatisticsData.setDateStats(dateColumnStatsData);
        stats.initialize(columnStatisticsData, 70);
        Assert.assertEquals(3, stats.getNumNulls());

        columnStatisticsData = new ColumnStatisticsData();
        DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData();
        decimalColumnStatsData.setNumNulls(4);
        columnStatisticsData.setDecimalStats(decimalColumnStatsData);
        stats.initialize(columnStatisticsData, 80);
        Assert.assertEquals(4, stats.getNumNulls());

        stats = new HiveColumnStats();
        columnStatisticsData = new ColumnStatisticsData();
        StringColumnStatsData stringColumnStatsData = new StringColumnStatsData();
        stringColumnStatsData.setNumNulls(5);
        columnStatisticsData.setStringStats(stringColumnStatsData);
        stats.initialize(columnStatisticsData, 90);
        Assert.assertEquals(5, stats.getNumNulls());
        Assert.assertEquals(Double.NEGATIVE_INFINITY, stats.getMin(), 0.000001);
        Assert.assertEquals(Double.POSITIVE_INFINITY, stats.getMax(), 0.000001);
    }
}
