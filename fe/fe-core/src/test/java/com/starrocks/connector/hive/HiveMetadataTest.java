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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.exception.StarRocksConnectorException;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.TEST_FILES;

public class HiveMetadataTest {
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
    private HiveMetadata hiveMetadata;

    private static ConnectContext connectContext;
    private static OptimizerContext optimizerContext;
    private static ColumnRefFactory columnRefFactory;

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        executorForHmsRefresh = Executors.newFixedThreadPool(5);
        executorForRemoteFileRefresh = Executors.newFixedThreadPool(5);
        executorForPullFiles = Executors.newFixedThreadPool(5);

        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "hive_catalog");
        cachingHiveMetastore = CachingHiveMetastore.createCatalogLevelInstance(
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
        hiveMetadata = new HiveMetadata("hive_catalog", hmsOps, fileOps, statisticsProvider, Optional.empty());
    }

    @After
    public void tearDown() {
        executorForHmsRefresh.shutdown();
        executorForRemoteFileRefresh.shutdown();
        executorForPullFiles.shutdown();
    }

    @Test
    public void testListDbNames() {
        List<String> databaseNames = hiveMetadata.listDbNames();
        Assert.assertEquals(Lists.newArrayList("db1", "db2", "information_schema"), databaseNames);
        CachingHiveMetastore queryLevelCache = CachingHiveMetastore.createQueryLevelInstance(cachingHiveMetastore, 100);
        Assert.assertEquals(Lists.newArrayList("db1", "db2", "information_schema"), queryLevelCache.getAllDatabaseNames());
    }

    @Test
    public void testListTableNames() {
        List<String> databaseNames = hiveMetadata.listTableNames("db1");
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), databaseNames);
    }

    @Test
    public void testGetPartitionKeys() {
        Assert.assertEquals(Lists.newArrayList("col1"), hiveMetadata.listPartitionNames("db1", "tbl1"));
    }

    @Test
    public void testGetDb() {
        Database database = hiveMetadata.getDb("db1");
        Assert.assertEquals("db1", database.getFullName());

    }

    @Test
    public void testGetTable() {
        com.starrocks.catalog.Table table = hiveMetadata.getTable("db1", "tbl1");
        HiveTable hiveTable = (HiveTable) table;
        Assert.assertEquals("db1", hiveTable.getDbName());
        Assert.assertEquals("tbl1", hiveTable.getTableName());
        Assert.assertEquals(Lists.newArrayList("col1"), hiveTable.getPartitionColumnNames());
        Assert.assertEquals(Lists.newArrayList("col2"), hiveTable.getDataColumnNames());
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive", hiveTable.getTableLocation());
        Assert.assertEquals(ScalarType.INT, hiveTable.getPartitionColumns().get(0).getType());
        Assert.assertEquals(ScalarType.INT, hiveTable.getBaseSchema().get(0).getType());
        Assert.assertEquals("hive_catalog", hiveTable.getCatalogName());
    }

    @Test
    public void testGetHiveRemoteFiles() throws AnalysisException {
        FeConstants.runningUnitTest = true;
        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/hive_tbl";
        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog");
        List<String> partitionNames = Lists.newArrayList("col1=1", "col1=2");
        Map<String, Partition> partitions = metastore.getPartitionsByNames("db1", "table1", partitionNames);
        HiveTable hiveTable = (HiveTable) hiveMetadata.getTable("db1", "table1");

        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());

        List<RemoteFileInfo> remoteFileInfos = hiveMetadata.getRemoteFileInfos(
                hiveTable, Lists.newArrayList(hivePartitionKey1, hivePartitionKey2), -1, null, null);
        Assert.assertEquals(2, remoteFileInfos.size());

        RemoteFileInfo fileInfo = remoteFileInfos.get(0);
        Assert.assertEquals(RemoteFileInputFormat.ORC, fileInfo.getFormat());
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=1", fileInfo.getFullPath());

        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(0).getFiles();
        Assert.assertNotNull(fileDescs);
        Assert.assertEquals(1, fileDescs.size());

        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assert.assertNotNull(fileDesc);
        Assert.assertNotNull(fileDesc.getTextFileFormatDesc());
        Assert.assertEquals("", fileDesc.getCompression());
        Assert.assertEquals(20, fileDesc.getLength());
        Assert.assertTrue(fileDesc.isSplittable());

        List<RemoteFileBlockDesc> blockDescs = fileDesc.getBlockDescs();
        Assert.assertEquals(1, blockDescs.size());
        RemoteFileBlockDesc blockDesc = blockDescs.get(0);
        Assert.assertEquals(0, blockDesc.getOffset());
        Assert.assertEquals(20, blockDesc.getLength());
        Assert.assertEquals(2, blockDesc.getReplicaHostIds().length);
    }

    @Test
    public void testGetFileWithSubdir() throws StarRocksConnectorException {
        RemotePathKey pathKey = new RemotePathKey("hdfs://127.0.0.1:10000/hive.db", true, Optional.empty());
        Map<RemotePathKey, List<RemoteFileDesc>> files = hiveRemoteFileIO.getRemoteFiles(pathKey);
        List<RemoteFileDesc> remoteFileDescs = files.get(pathKey);
        Assert.assertEquals(1, remoteFileDescs.size());
        Assert.assertEquals("hive_tbl/000000_0", remoteFileDescs.get(0).getFileName());
    }

    @Test
    public void testGetTableStatisticsWithUnknown() throws AnalysisException {
        optimizerContext.getSessionVariable().setEnableHiveColumnStats(false);
        HiveTable hiveTable = (HiveTable) hmsOps.getTable("db1", "table1");
        ColumnRefOperator partColumnRefOperator = new ColumnRefOperator(0, Type.INT, "col1", true);
        ColumnRefOperator dataColumnRefOperator = new ColumnRefOperator(1, Type.INT, "col2", true);
        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());

        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        columns.put(partColumnRefOperator, null);
        columns.put(dataColumnRefOperator, null);
        Statistics statistics = hiveMetadata.getTableStatistics(optimizerContext, hiveTable, columns,
                Lists.newArrayList(hivePartitionKey1, hivePartitionKey2), null);
        Assert.assertEquals(1, statistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(2, statistics.getColumnStatistics().size());
        Assert.assertTrue(statistics.getColumnStatistics().get(partColumnRefOperator).isUnknown());
        Assert.assertTrue(statistics.getColumnStatistics().get(dataColumnRefOperator).isUnknown());
    }

    @Test
    public void testGetTableStatisticsNormal() throws AnalysisException {
        HiveTable hiveTable = (HiveTable) hiveMetadata.getTable("db1", "table1");
        ColumnRefOperator partColumnRefOperator = new ColumnRefOperator(0, Type.INT, "col1", true);
        ColumnRefOperator dataColumnRefOperator = new ColumnRefOperator(1, Type.INT, "col2", true);
        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());
        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        columns.put(partColumnRefOperator, null);
        columns.put(dataColumnRefOperator, null);

        Statistics statistics = hiveMetadata.getTableStatistics(optimizerContext, hiveTable, columns,
                Lists.newArrayList(hivePartitionKey1, hivePartitionKey2), null);
        Assert.assertEquals(1,  statistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(2, statistics.getColumnStatistics().size());

        cachingHiveMetastore.getPartitionStatistics(hiveTable, Lists.newArrayList("col1=1", "col1=2"));
        statistics = hiveMetadata.getTableStatistics(optimizerContext, hiveTable, columns,
                Lists.newArrayList(hivePartitionKey1, hivePartitionKey2), null);

        Assert.assertEquals(100, statistics.getOutputRowCount(), 0.001);
        Map<ColumnRefOperator, ColumnStatistic> columnStatistics = statistics.getColumnStatistics();
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
}
