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

package com.starrocks.connector.delta;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DeltaLakeCacheUpdateProcessorTest {
    private HiveMetaClient client;
    private DeltaLakeMetastore metastore;
    private ExecutorService executor;
    private long expireAfterWriteSec = 30;
    private long refreshAfterWriteSec = -1;
    private CachingDeltaLakeMetastore cachingDeltaLakeMetastore;

    @Before
    public void setUp() throws Exception {
        client = new HiveMetastoreTest.MockedHiveMetaClient();
        IHiveMetastore hiveMetastore = new HiveMetastore(client, "delta0", MetastoreType.HMS);
        metastore = new HMSBackedDeltaMetastore("delta0", hiveMetastore, new Configuration(),
                new DeltaLakeCatalogProperties(Maps.newHashMap()));
        executor = Executors.newFixedThreadPool(5);
        cachingDeltaLakeMetastore =
                CachingDeltaLakeMetastore.createCatalogLevelInstance(metastore, executor, expireAfterWriteSec,
                        refreshAfterWriteSec, 100);
    }

    @Test
    public void testGetCachedTable(@Mocked ConnectContext connectContext) {
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                minTimes = 0;

                connectContext.getCommand();
                result = MysqlCommand.COM_QUERY;
                minTimes = 0;
            }
        };

        new MockUp<CachingDeltaLakeMetastore>() {
            @mockit.Mock
            public DeltaLakeSnapshot getCachedSnapshot(DatabaseTableName databaseTableName) {
                return new DeltaLakeSnapshot("db1", "table1", null, null,
                        123, "s3://bucket/path/to/table");
            }
        };

        new MockUp<DeltaUtils>() {
            @mockit.Mock
            public DeltaLakeTable convertDeltaSnapshotToSRTable(String catalog, DeltaLakeSnapshot snapshot) {
                return new DeltaLakeTable(1, "delta0", "db1", "table1",
                        Lists.newArrayList(), Lists.newArrayList("ts"), null,
                        "s3://bucket/path/to/table", null, 0);
            }
        };

        DeltaLakeCacheUpdateProcessor deltaLakeCacheUpdateProcessor =
                new DeltaLakeCacheUpdateProcessor(cachingDeltaLakeMetastore);
        Table table = cachingDeltaLakeMetastore.getTable("db1", "table1");

        Assert.assertEquals(1, deltaLakeCacheUpdateProcessor.getCachedTableNames().size());
        Assert.assertTrue(deltaLakeCacheUpdateProcessor.getCachedTableNames().
                contains(DatabaseTableName.of("db1", "table1")));
        Assert.assertTrue(table instanceof DeltaLakeTable);
    }

    @Test
    public void testRefreshTableBackground(@Mocked ConnectContext connectContext) throws InterruptedException {
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                minTimes = 0;

                connectContext.getCommand();
                result = MysqlCommand.COM_QUERY;
                minTimes = 0;
            }
        };

        new MockUp<CachingDeltaLakeMetastore>() {
            @mockit.Mock
            public DeltaLakeSnapshot getCachedSnapshot(DatabaseTableName databaseTableName) {
                return new DeltaLakeSnapshot("db1", "table1", null, null,
                        123, "s3://bucket/path/to/table");
            }
        };

        new MockUp<DeltaLakeMetastore>() {
            @mockit.Mock
            public DeltaLakeSnapshot getLatestSnapshot(String dbName, String tableName) {
                return new DeltaLakeSnapshot("db1", "table1", null, null,
                        123, "s3://bucket/path/to/table");
            }
        };

        new MockUp<DeltaUtils>() {
            @mockit.Mock
            public DeltaLakeTable convertDeltaSnapshotToSRTable(String catalog, DeltaLakeSnapshot snapshot) {
                return new DeltaLakeTable(1, "delta0", "db1", "table1",
                        Lists.newArrayList(), Lists.newArrayList("ts"), null,
                        "s3://bucket/path/to/table", null, 0);
            }
        };

        DeltaLakeCacheUpdateProcessor deltaLakeCacheUpdateProcessor =
                new DeltaLakeCacheUpdateProcessor(cachingDeltaLakeMetastore);
        Table table = cachingDeltaLakeMetastore.getTable("db1", "table1");

        deltaLakeCacheUpdateProcessor.refreshTableBackground(table, false, executor);
        Assert.assertEquals(1, deltaLakeCacheUpdateProcessor.getCachedTableNames().size());
        Assert.assertTrue(deltaLakeCacheUpdateProcessor.getCachedTableNames().
                contains(DatabaseTableName.of("db1", "table1")));
        Assert.assertTrue(cachingDeltaLakeMetastore.isTablePresent(DatabaseTableName.of("db1", "table1")));

        // sleep 1s, background refresh table will be skipped
        Thread.sleep(1000);
        long oldValue = Config.background_refresh_metadata_time_secs_since_last_access_secs;
        // not refresh table, just skip refresh table
        Config.background_refresh_metadata_time_secs_since_last_access_secs = 0;
        // refresh table again, test skip refresh
        try {
            deltaLakeCacheUpdateProcessor.refreshTableBackground(table, false, executor);
        } finally {
            Config.background_refresh_metadata_time_secs_since_last_access_secs = oldValue;
        }
        Assert.assertFalse(cachingDeltaLakeMetastore.isTablePresent(DatabaseTableName.of("db1", "table1")));
    }
}
