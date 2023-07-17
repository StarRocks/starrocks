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


package com.starrocks.connector.hudi;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.hive.CachingHiveMetastore;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.HiveStatisticsProvider;
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
        Assert.assertEquals(Lists.newArrayList("db1", "db2", "information_schema"), databaseNames);
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