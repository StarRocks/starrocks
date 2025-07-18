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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.CachingRemoteFileConf;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.MetastoreType;
import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.HDFS_HIVE_TABLE;

public class HiveConnectorTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private CachingHiveMetastore cachingHiveMetastore;
    private HiveRemoteFileIO hiveRemoteFileIO;
    private CachingRemoteFileIO cachingRemoteFileIO;
    private ExecutorService executorForHmsRefresh;
    private ExecutorService executorForRemoteFileRefresh;
    private ExecutorService executorForPullFiles;

    @BeforeEach
    public void setUp() throws Exception {
        executorForHmsRefresh = Executors.newFixedThreadPool(5);
        executorForRemoteFileRefresh = Executors.newFixedThreadPool(5);
        executorForPullFiles = Executors.newFixedThreadPool(5);

        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        cachingHiveMetastore = CachingHiveMetastore.createCatalogLevelInstance(
                metastore, executorForHmsRefresh, executorForHmsRefresh,
                100, 10, 1000, false);
        hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        cachingRemoteFileIO = CachingRemoteFileIO.createCatalogLevelInstance(
                hiveRemoteFileIO, executorForRemoteFileRefresh, 100, 10, 10);
    }

    @AfterEach
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
        Assertions.assertTrue(metadata instanceof HiveMetadata);
        com.starrocks.catalog.Table table = metadata.getTable(new ConnectContext(), "db1", "tbl1");
        HiveTable hiveTable = (HiveTable) table;
        Assertions.assertEquals("db1", hiveTable.getCatalogDBName());
        Assertions.assertEquals("tbl1", hiveTable.getCatalogTableName());
        Assertions.assertEquals(Lists.newArrayList("col1"), hiveTable.getPartitionColumnNames());
        Assertions.assertEquals(Lists.newArrayList("col2"), hiveTable.getDataColumnNames());
        Assertions.assertEquals("hdfs://127.0.0.1:10000/hive", hiveTable.getTableLocation());
        Assertions.assertEquals(ScalarType.INT, hiveTable.getPartitionColumns().get(0).getType());
        Assertions.assertEquals(ScalarType.INT, hiveTable.getBaseSchema().get(0).getType());
        Assertions.assertEquals("hive_catalog", hiveTable.getCatalogName());
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
        Assertions.assertTrue(metadata instanceof HiveMetadata);
    }
}
