// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.CachingRemoteFileConf;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.TEST_FILES;

public class HiveConnectorTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private CachingHiveMetastore cachingHiveMetastore;
    private HiveRemoteFileIO hiveRemoteFileIO;
    private CachingRemoteFileIO cachingRemoteFileIO;
    private ExecutorService executorForHmsRefresh;
    private ExecutorService executorForRemoteFileRefresh;
    private ExecutorService executorForPullFiles;

    @Before
    public void setUp() throws Exception {
        executorForHmsRefresh = Executors.newFixedThreadPool(5);
        executorForRemoteFileRefresh = Executors.newFixedThreadPool(5);
        executorForPullFiles = Executors.newFixedThreadPool(5);

        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "hive_catalog");
        cachingHiveMetastore = CachingHiveMetastore.createCatalogLevelInstance(
                metastore, executorForHmsRefresh, 100, 10, 1000, false);
        hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(TEST_FILES);
        hiveRemoteFileIO.setFileSystem(fs);
        cachingRemoteFileIO = CachingRemoteFileIO.createCatalogLevelInstance(
                hiveRemoteFileIO, executorForRemoteFileRefresh, 100, 10, 10);
    }

    @After
    public void tearDown() {
        executorForHmsRefresh.shutdown();
        executorForRemoteFileRefresh.shutdown();
        executorForPullFiles.shutdown();
    }

    @Test
    public void testCreateHiveConnector(@Mocked HiveConnectorInternalMgr internalMgr) {
        FeConstants.runningUnitTest = true;
        Map<String, String> properties = ImmutableMap.of("hive.metastore.uris", "thrift://127.0.0.1:9083", "type", "hive");
        new Expectations() {
            {
                internalMgr.createHiveMetastore();
                result = cachingHiveMetastore;

                internalMgr.createRemoteFileIO();
                result = cachingRemoteFileIO;

                internalMgr.getHiveMetastoreConf();
                result = new CachingHiveMetastoreConf(properties, "hive");

                internalMgr.getRemoteFileConf();
                result = new CachingRemoteFileConf(properties);

                internalMgr.getPullRemoteFileExecutor();
                result = executorForPullFiles;

                internalMgr.isSearchRecursive();
                result = false;
            }
        };

        HiveConnector hiveConnector = new HiveConnector(new ConnectorContext("hive_catalog", "hive", properties));
        ConnectorMetadata metadata = hiveConnector.getMetadata();
        Assert.assertTrue(metadata instanceof HiveMetadata);
        HiveMetadata hiveMetadata = (HiveMetadata) metadata;
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
    public void testCreateHiveConnectorWithMetaStoreType(@Mocked HiveConnectorInternalMgr internalMgr) {
        FeConstants.runningUnitTest = true;
        Map<String, String> properties = ImmutableMap.of("hive.metastore.uris", "thrift://127.0.0.1:9083",
                "type", "hive", "hive.metastore.type", "hive");
        new Expectations() {
            {
                internalMgr.createHiveMetastore();
                result = cachingHiveMetastore;

                internalMgr.createRemoteFileIO();
                result = cachingRemoteFileIO;

                internalMgr.getHiveMetastoreConf();
                result = new CachingHiveMetastoreConf(properties, "hive");

                internalMgr.getRemoteFileConf();
                result = new CachingRemoteFileConf(properties);

                internalMgr.getPullRemoteFileExecutor();
                result = executorForPullFiles;

                internalMgr.isSearchRecursive();
                result = false;
            }
        };

        HiveConnector hiveConnector = new HiveConnector(new ConnectorContext("hive_catalog", "hive", properties));
        ConnectorMetadata metadata = hiveConnector.getMetadata();
        Assert.assertTrue(metadata instanceof HiveMetadata);
    }
}
