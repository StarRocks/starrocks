// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hudi;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.FeConstants;
import com.starrocks.external.CachingRemoteFileIO;
import com.starrocks.external.RemoteFileOperations;
import com.starrocks.external.hive.CachingHiveMetastore;
import com.starrocks.external.hive.HiveMetaClient;
import com.starrocks.external.hive.HiveMetastore;
import com.starrocks.external.hive.HiveMetastoreOperations;
import com.starrocks.external.hive.HiveMetastoreTest;
import com.starrocks.external.hive.HiveStatisticsProvider;
import com.starrocks.external.hudi.HudiRemoteFileIO;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HudiMetadataTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private CachingHiveMetastore cachingHiveMetastore;
    private HiveMetastoreOperations hmsOps;
    private HudiRemoteFileIO hudiRemoteFileIO;
    private CachingRemoteFileIO cachingRemoteFileIO;
    private RemoteFileOperations fileOps;
    private ExecutorService executorForHmsRefresh;
    private ExecutorService executorForRemoteFileRefresh;
    private ExecutorService executorForPullFiles;
    private HiveStatisticsProvider statisticsProvider;
    private HudiMetadata hudiMetadata;

    private static ConnectContext connectContext;
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

        hudiRemoteFileIO = new HudiRemoteFileIO(new Configuration());
        cachingRemoteFileIO = CachingRemoteFileIO.createCatalogLevelInstance(
                hudiRemoteFileIO, executorForRemoteFileRefresh, 100, 10, 10);
        fileOps = new RemoteFileOperations(cachingRemoteFileIO, executorForPullFiles, false, true);
        statisticsProvider = new HiveStatisticsProvider(hmsOps, fileOps);

        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        columnRefFactory = new ColumnRefFactory();
        hudiMetadata = new HudiMetadata("hive_catalog", hmsOps, fileOps, statisticsProvider, Optional.empty());
    }

    @After
    public void tearDown() {
        executorForHmsRefresh.shutdown();
        executorForRemoteFileRefresh.shutdown();
        executorForPullFiles.shutdown();
    }

    @Test
    public void testListDbNames() {
        List<String> databaseNames = hudiMetadata.listDbNames();
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
        CachingHiveMetastore queryLevelCache = CachingHiveMetastore.createQueryLevelInstance(cachingHiveMetastore, 100);
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), queryLevelCache.getAllDatabaseNames());
    }

    @Test
    public void testListTableNames() {
        List<String> databaseNames = hudiMetadata.listTableNames("db1");
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), databaseNames);
    }

    @Test
    public void testGetPartitionKeys() {
        Assert.assertEquals(Lists.newArrayList("col1"), hudiMetadata.listPartitionNames("db1", "tbl1"));
    }

    @Test
    public void testGetDb() {
        Database database = hudiMetadata.getDb("db1");
        Assert.assertEquals("db1", database.getFullName());

    }
}