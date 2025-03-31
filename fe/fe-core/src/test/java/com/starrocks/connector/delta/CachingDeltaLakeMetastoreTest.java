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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.sql.analyzer.SemanticException;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableImpl;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CachingDeltaLakeMetastoreTest {
    private HiveMetaClient client;
    private DeltaLakeMetastore metastore;
    private ExecutorService executor;
    private long expireAfterWriteSec = 30;
    private long refreshAfterWriteSec = -1;
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        client = new HiveMetastoreTest.MockedHiveMetaClient();
        IHiveMetastore hiveMetastore = new HiveMetastore(client, "delta0", MetastoreType.HMS);
        metastore = new HMSBackedDeltaMetastore("delta0", hiveMetastore, new Configuration(),
                new DeltaLakeCatalogProperties(Maps.newHashMap()));
        executor = Executors.newFixedThreadPool(5);
    }

    @Test
    public void testGetAllDatabaseNames() {
        CachingDeltaLakeMetastore cachingDeltaLakeMetastore =
                CachingDeltaLakeMetastore.createCatalogLevelInstance(metastore, executor, expireAfterWriteSec,
                        refreshAfterWriteSec, 100);

        List<String> databaseNames = cachingDeltaLakeMetastore.getAllDatabaseNames();
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
        CachingDeltaLakeMetastore queryLevelCache = CachingDeltaLakeMetastore.
                createQueryLevelInstance(cachingDeltaLakeMetastore, 100);
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), queryLevelCache.getAllDatabaseNames());
    }

    @Test
    public void testGetAllTableNames() {
        CachingDeltaLakeMetastore cachingDeltaLakeMetastore =
                CachingDeltaLakeMetastore.createCatalogLevelInstance(metastore, executor, expireAfterWriteSec,
                        refreshAfterWriteSec, 100);

        List<String> tableNames = cachingDeltaLakeMetastore.getAllTableNames("db1");
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), tableNames);
        CachingDeltaLakeMetastore queryLevelCache = CachingDeltaLakeMetastore.
                createQueryLevelInstance(cachingDeltaLakeMetastore, 100);
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), queryLevelCache.getAllTableNames("db1"));
    }

    @Test
    public void testGetDb() {
        CachingDeltaLakeMetastore cachingDeltaLakeMetastore =
                CachingDeltaLakeMetastore.createCatalogLevelInstance(metastore, executor, expireAfterWriteSec,
                        refreshAfterWriteSec, 100);

        Database database = cachingDeltaLakeMetastore.getDb("db1");
        Assert.assertEquals("db1", database.getFullName());
    }

    @Test
    public void testGetTable() {
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

        CachingDeltaLakeMetastore cachingDeltaLakeMetastore =
                CachingDeltaLakeMetastore.createCatalogLevelInstance(metastore, executor, expireAfterWriteSec,
                        refreshAfterWriteSec, 100);

        Table table = cachingDeltaLakeMetastore.getTable("db1", "table1");
        Assert.assertTrue(table instanceof DeltaLakeTable);
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        Assert.assertEquals("db1", deltaLakeTable.getCatalogDBName());
        Assert.assertEquals("table1", deltaLakeTable.getCatalogTableName());
        Assert.assertEquals("s3://bucket/path/to/table", deltaLakeTable.getTableLocation());
    }

    @Test
    public void testGetLatestSnapshot1() {
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("Failed to find Delta table for delta0.db1.table1");
        new MockUp<HMSBackedDeltaMetastore>() {
            @mockit.Mock
            public MetastoreTable getMetastoreTable(String dbName, String tableName) {
                return new MetastoreTable("db1", "table1", "s3://bucket/path/to/table", 123);
            }
        };

        new MockUp<TableImpl>() {
            @mockit.Mock
            public io.delta.kernel.Table forPath(Engine engine, String path) {
                throw new TableNotFoundException("Table not found");
            }
        };

        metastore.getLatestSnapshot("db1", "table1");
    }

    @Test
    public void testGetLatestSnapshot2() {
        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("Failed to get latest snapshot");
        io.delta.kernel.Table table = new io.delta.kernel.Table() {
            public io.delta.kernel.Table forPath(Engine engine, String path) {
                return this;
            }

            @Override
            public String getPath(Engine engine) {
                return null;
            }

            @Override
            public SnapshotImpl getLatestSnapshot(Engine engine) {
                throw new RuntimeException("Failed to get latest snapshot");
            }

            @Override
            public Snapshot getSnapshotAsOfVersion(Engine engine, long versionId) throws TableNotFoundException {
                return null;
            }

            @Override
            public Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC)
                    throws TableNotFoundException {
                return null;
            }

            @Override
            public TransactionBuilder createTransactionBuilder(Engine engine, String engineInfo, Operation operation) {
                return null;
            }

            @Override
            public void checkpoint(Engine engine, long version)
                    throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {
            }
        };

        new MockUp<TableImpl>() {
            @Mock
            public io.delta.kernel.Table forPath(Engine engine, String path) {
                return table;
            }
        };
        metastore.getLatestSnapshot("db1", "table1");
    }

    @Test
    public void testTableExists() {
        CachingDeltaLakeMetastore cachingDeltaLakeMetastore =
                CachingDeltaLakeMetastore.createCatalogLevelInstance(metastore, executor, expireAfterWriteSec,
                        refreshAfterWriteSec, 100);

        Assert.assertTrue(cachingDeltaLakeMetastore.tableExists("db1", "table1"));
    }

    @Test
    public void testRefreshTable() {
        new Expectations(metastore) {
            {
                metastore.getTable(anyString, "notExistTbl");
                minTimes = 0;
                Throwable targetException = new NoSuchObjectException("no such obj");
                Throwable e = new InvocationTargetException(targetException);
                result = new StarRocksConnectorException("table not exist", e);
            }
        };
        CachingDeltaLakeMetastore cachingDeltaLakeMetastore = new CachingDeltaLakeMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000);
        try {
            cachingDeltaLakeMetastore.refreshTable("db1", "notExistTbl", true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof StarRocksConnectorException);
            Assert.assertTrue(e.getMessage().contains("invalidated cache"));
        }

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
                return new DeltaLakeTable(1, "delta0", "db1", "tbl1",
                        Lists.newArrayList(), Lists.newArrayList("ts"), null,
                        "s3://bucket/path/to/table", null, 0);
            }
        };

        try {
            cachingDeltaLakeMetastore.refreshTable("db1", "tbl1", true);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCacheMemoryUsage() {
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

        CachingDeltaLakeMetastore cachingDeltaLakeMetastore =
                CachingDeltaLakeMetastore.createCatalogLevelInstance(metastore, executor, expireAfterWriteSec,
                        refreshAfterWriteSec, 100);

        cachingDeltaLakeMetastore.getDb("db1");
        cachingDeltaLakeMetastore.getTable("db1", "table1");

        Assert.assertTrue(cachingDeltaLakeMetastore.estimateSize() > 0);
        Assert.assertFalse(cachingDeltaLakeMetastore.estimateCount().isEmpty());
        Assert.assertTrue(cachingDeltaLakeMetastore.estimateCount().containsKey("databaseCache"));
        Assert.assertTrue(cachingDeltaLakeMetastore.estimateCount().containsKey("tableCache"));
    }
}
