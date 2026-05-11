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

package com.starrocks.connector.iceberg;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.CachingIcebergCatalog.IcebergTableName;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;

public class CachingIcebergCatalogTest {
    private static final String CATALOG_NAME = "iceberg_catalog";
    public static final IcebergCatalogProperties DEFAULT_CATALOG_PROPERTIES;
    public static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();
    public static ConnectContext connectContext;

    static {
        DEFAULT_CONFIG.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732"); // non-exist ip, prevent to connect local service
        DEFAULT_CONFIG.put(ICEBERG_CATALOG_TYPE, "hive");
        DEFAULT_CATALOG_PROPERTIES = new IcebergCatalogProperties(DEFAULT_CONFIG);
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormalCreateAndDropDBTable(@Mocked IcebergCatalog icebergCatalog)
            throws MetaNotFoundException {
        new Expectations() {
            {
                icebergCatalog.createDB(connectContext, "test", (Map<String, String>) any);
                result = null;
                minTimes = 0;

                icebergCatalog.dropDB(connectContext, "test");
                result = null;
                minTimes = 0;

                icebergCatalog.dropTable(connectContext, "test", "table", anyBoolean);
                result = true;
                minTimes = 0;
            }
        };
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        cachingIcebergCatalog.createDB(connectContext, "test", new HashMap<>());
        cachingIcebergCatalog.dropDB(connectContext, "test");
        cachingIcebergCatalog.dropTable(connectContext, "test", "table", true);
        cachingIcebergCatalog.invalidateCache("test", "table");
        cachingIcebergCatalog.invalidatePartitionCache("test", "table");
    }

    @Test
    public void testListPartitionNames(@Mocked IcebergCatalog icebergCatalog) {
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Table nativeTable = createBaseTableWithManifests(1, 0, spec);
        new Expectations() {
            {
                icebergCatalog.getTable((ConnectContext) any, "db", "test");
                result = nativeTable;
                minTimes = 0;
            }
        };
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergTable table =
                IcebergTable.builder().setSrTableName("test")
                .setCatalogDBName("db").setCatalogTableName("test").setNativeTable(nativeTable).build();

        Assertions.assertFalse(nativeTable.spec().isUnpartitioned());
        {
            ConnectorMetadataRequestContext requestContext = new ConnectorMetadataRequestContext();
            SessionVariable sv = ConnectContext.getSessionVariableOrDefault();
            sv.setEnableConnectorAsyncListPartitions(true);
            requestContext.setQueryMVRewrite(true);
            List<String> res = cachingIcebergCatalog.listPartitionNames(table, requestContext, null);
            Assertions.assertNull(res);
        }
        {
            ConnectorMetadataRequestContext requestContext = new ConnectorMetadataRequestContext();
            SessionVariable sv = ConnectContext.getSessionVariableOrDefault();
            sv.setEnableConnectorAsyncListPartitions(false);
            requestContext.setQueryMVRewrite(true);
            List<String> res = cachingIcebergCatalog.listPartitionNames(table, requestContext, null);
            Assertions.assertEquals(res.size(), 0);
        }
    }

    @Test
    public void testGetPartitionsUsesCurrentSnapshotMicrosForUnpartitionedFallback() {
        IcebergCatalog catalog = new IcebergCatalog() {
            @Override
            public IcebergCatalogType getIcebergCatalogType() {
                return IcebergCatalogType.HIVE_CATALOG;
            }

            @Override
            public List<String> listAllDatabases(ConnectContext context) {
                return List.of();
            }

            @Override
            public Database getDB(ConnectContext context, String dbName) {
                return null;
            }

            @Override
            public List<String> listTables(ConnectContext context, String dbName) {
                return List.of();
            }

            @Override
            public void renameTable(ConnectContext context, String dbName, String tblName, String newTblName) {
            }

            @Override
            public Table getTable(ConnectContext context, String dbName, String tableName) {
                throw new UnsupportedOperationException();
            }
        };

        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        PartitionsTable partitionsTable = Mockito.mock(PartitionsTable.class);
        TableScan tableScan = Mockito.mock(TableScan.class);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(true);
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(nativeTable.name()).thenReturn("db.test");
        Mockito.when(snapshot.timestampMillis()).thenReturn(1234L);
        Mockito.when(snapshot.sequenceNumber()).thenReturn(9L);
        Mockito.when(partitionsTable.newScan()).thenReturn(tableScan);
        Mockito.when(tableScan.planFiles()).thenReturn(CloseableIterable.empty());

        try (MockedStatic<MetadataTableUtils> metadataTableUtils = Mockito.mockStatic(MetadataTableUtils.class)) {
            metadataTableUtils.when(() -> MetadataTableUtils.createMetadataTableInstance(
                    nativeTable, MetadataTableType.PARTITIONS)).thenReturn(partitionsTable);

            IcebergTable table = IcebergTable.builder()
                    .setSrTableName("test")
                    .setCatalogDBName("db")
                    .setCatalogTableName("test")
                    .setNativeTable(nativeTable)
                    .build();

            Map<String, Partition> partitions = catalog.getPartitions(table, -1, null);
            Partition partition = partitions.get(IcebergCatalog.EMPTY_PARTITION_NAME);

            Assertions.assertNotNull(partition);
            Assertions.assertEquals(TimeUnit.MICROSECONDS, partition.getModifiedTimeUnit());
            Assertions.assertEquals(TimeUnit.MILLISECONDS.toMicros(1234L), partition.getModifiedTime());
            Assertions.assertEquals(9L, partition.getVersion());
        }
    }

    @Test
    public void testGetDB(@Mocked IcebergCatalog icebergCatalog, @Mocked Database db) {
        new Expectations() {
            {
                icebergCatalog.getDB(connectContext, "test");
                result = db;
                minTimes = 1;
            }
        };
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        Assertions.assertEquals(db, cachingIcebergCatalog.getDB(connectContext, "test"));
        Assertions.assertEquals(db, cachingIcebergCatalog.getDB(connectContext, "test"));
    }


    @Test
    public void testGetTable(@Mocked IcebergCatalog icebergCatalog) {
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                icebergCatalog.getTable(connectContext, "test", "table");
                result = nativeTable;
                minTimes = 1;
            }
        };
        //test for cache
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        Assertions.assertEquals(nativeTable, cachingIcebergCatalog.getTable(connectContext, "test", "table"));
        Assertions.assertEquals(nativeTable, cachingIcebergCatalog.getTable(connectContext, "test", "table"));
        cachingIcebergCatalog.invalidateCache("test", "table");
    }

    @Test
    public void testGetTableIOError(@Mocked IcebergCatalog icebergCatalog) {
        new Expectations() {
            {
                icebergCatalog.getTable(connectContext, "test", "table");
                result = new RuntimeException(new java.io.IOException("io failure"));
            }
        };

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        StarRocksConnectorException ex = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> cachingIcebergCatalog.getTable(connectContext, "test", "table"));
        String expectedPrefix = "Failed to get iceberg table iceberg_catalog.test.table";
        Assertions.assertTrue(ex.getMessage().contains(expectedPrefix));
        Assertions.assertTrue(ex.getMessage().contains("io failure"));
    }

    private int getStaticIntField(String fieldName) {
        try {
            java.lang.reflect.Field f = CachingIcebergCatalog.class.getDeclaredField(fieldName);
            f.setAccessible(true);
            return f.getInt(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Table createBaseTableWithManifests(int snapshotCount, int manifestCount) {
        return createBaseTableWithManifests(snapshotCount, manifestCount, null);
    }

    private Table createBaseTableWithManifests(int snapshotCount, int manifestCount, PartitionSpec spec) {
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata meta = Mockito.mock(TableMetadata.class);
        Snapshot currentSnapshot = Mockito.mock(Snapshot.class);

        List<Snapshot> snapshots = new ArrayList<>();
        for (int i = 0; i < snapshotCount; i++) {
            snapshots.add(Mockito.mock(Snapshot.class));
        }
        List<ManifestFile> manifests = new ArrayList<>();
        for (int i = 0; i < manifestCount; i++) {
            manifests.add(Mockito.mock(ManifestFile.class));
        }
        String uuid = UUID.randomUUID().toString();
        Mockito.when(ops.current()).thenReturn(meta);
        Mockito.when(meta.snapshots()).thenReturn(snapshots);
        Mockito.when(meta.currentSnapshot()).thenReturn(currentSnapshot);
        Mockito.when(meta.metadataFileLocation()).thenReturn("metadata-" + uuid);
        Mockito.when(meta.uuid()).thenReturn(uuid);
        if (spec != null) {
            Mockito.when(meta.spec()).thenReturn(spec);
        }
        Mockito.when(currentSnapshot.allManifests(Mockito.any())).thenReturn(manifests);

        return new BaseTable(ops, "db.tbl");
    }

    @Test
    public void testInvalidateCache(@Mocked IcebergCatalog icebergCatalog) {
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                icebergCatalog.getTable(connectContext, "db1", "tbl1");
                result = nativeTable;
                times = 2; // Called twice: once for initial cache, once after invalidation
            }
        };

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        // First call - populates cache
        Table t1 = cachingIcebergCatalog.getTable(connectContext, "db1", "tbl1");
        Assertions.assertEquals(nativeTable, t1);

        // Invalidate cache
        cachingIcebergCatalog.invalidateCache("db1", "tbl1");

        // Second call - should hit delegate again because cache was invalidated
        Table t2 = cachingIcebergCatalog.getTable(connectContext, "db1", "tbl1");
        Assertions.assertEquals(nativeTable, t2);
    }

    @Test
    public void testTableCacheEnabled_hitsDelegateOnce(@Mocked IcebergCatalog delegate,
                                                       @Mocked IcebergCatalogProperties props,
                                                       @Mocked ConnectContext ctx) throws Exception {
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                props.isEnableIcebergMetadataCache(); 
                result = true;
                props.getIcebergMetaCacheTtlSec(); 
                result = 24L * 60 * 60;
                props.getIcebergDataFileCacheMemoryUsageRatio(); 
                result = 0.0;
                props.getIcebergDeleteFileCacheMemoryUsageRatio(); 
                result = 0.0;
                props.isEnableIcebergTableCache();
                result = true;
                props.getIcebergTableCacheMemoryUsageRatio();
                result = 1;

                delegate.getTable(ctx, "db1", "t1"); 
                result = nativeTable; 
                minTimes = 0;
            }
        };

        ExecutorService es = Executors.newFixedThreadPool(5);
        try {
            CachingIcebergCatalog catalog =
                    new CachingIcebergCatalog("iceberg0", delegate, props, es);

            org.apache.iceberg.Table r1 = catalog.getTable(ctx, "db1", "t1");
            org.apache.iceberg.Table r2 = catalog.getTable(ctx, "db1", "t1");

            org.junit.jupiter.api.Assertions.assertSame(r1, r2);

            new Verifications() {
                {
                    delegate.getTable(ctx, "db1", "t1"); 
                    times = 1;
                }
            };
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testTableCacheDisabled_hitsDelegateTwice(@Mocked IcebergCatalog delegate,
                                                         @Mocked IcebergCatalogProperties props,
                                                         @Mocked ConnectContext ctx) throws Exception {
        Table nativeTable1 = createBaseTableWithManifests(1, 1);
        Table nativeTable2 = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                props.isEnableIcebergMetadataCache(); 
                result = true;
                props.getIcebergTableCacheMemoryUsageRatio();
                result = 0.0;
                props.getIcebergMetaCacheTtlSec(); 
                result = 60;
                props.getIcebergDataFileCacheMemoryUsageRatio(); 
                result = 0.0;
                props.getIcebergDeleteFileCacheMemoryUsageRatio(); 
                result = 0.0;

                delegate.getTable(ctx, "db1", "t1"); 
                result = nativeTable1;
                minTimes = 0;
            }
        };

        ExecutorService es = Executors.newFixedThreadPool(5);
        try {
            CachingIcebergCatalog catalog =
                    new CachingIcebergCatalog("iceberg0", delegate, props, es);

            org.apache.iceberg.Table r1 = catalog.getTable(ctx, "db1", "t1");
            org.apache.iceberg.Table r2 = catalog.getTable(ctx, "db1", "t1");

            new Verifications() {
                {
                    delegate.getTable(ctx, "db1", "t1"); 
                    times = 2; //caffeine has a diff with guava here
                }
            };
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testEstimateCountReflectsTableCache(@Mocked IcebergCatalog icebergCatalog) {
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                icebergCatalog.getTable(connectContext, "db2", "tbl2");
                result = nativeTable;
                times = 1;
            }
        };
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        cachingIcebergCatalog.getTable(connectContext, "db2", "tbl2");
        Map<String, Long> counts = cachingIcebergCatalog.estimateCount();
        Assertions.assertEquals(1L, counts.get("Table"));
    }

    @Test
    public void testGetTableBypassCacheForRestCatalogWhenAuthToken(@Mocked IcebergRESTCatalog restCatalog) {
        ConnectContext ctx = new ConnectContext();
        ctx.setAuthToken("token");
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                restCatalog.getTable(ctx, "db3", "tbl3");
                result = nativeTable;
                times = 2;
            }
        };

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, restCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        Assertions.assertEquals(nativeTable, cachingIcebergCatalog.getTable(ctx, "db3", "tbl3"));
        Assertions.assertEquals(nativeTable, cachingIcebergCatalog.getTable(ctx, "db3", "tbl3"));
    }

    @Test
    public void testGetTableBypassCacheWhenVendedCredentialsEnabled(@Mocked IcebergRESTCatalog restCatalog) {
        // When vended credentials is enabled, caching should be bypassed to avoid
        // using expired credentials.
        ConnectContext ctx = new ConnectContext();
        Table nativeTable1 = createBaseTableWithManifests(1, 1);
        Table nativeTable2 = createBaseTableWithManifests(1, 1);

        new Expectations() {
            {
                restCatalog.isVendedCredentialsEnabled();
                result = true;
                minTimes = 0;

                restCatalog.getTable(ctx, "db4", "tbl4");
                result = nativeTable1;
                result = nativeTable2;
            }
        };

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, restCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        Table result1 = cachingIcebergCatalog.getTable(ctx, "db4", "tbl4");
        Table result2 = cachingIcebergCatalog.getTable(ctx, "db4", "tbl4");

        // Should return different instances (no caching)
        Assertions.assertSame(nativeTable1, result1);
        Assertions.assertSame(nativeTable2, result2);
    }

    @Test
    public void testGetTableWithCacheWhenVendedCredentialsDisabled(@Mocked IcebergRESTCatalog restCatalog) {
        // When vended credentials is disabled, normal caching should work.
        ConnectContext ctx = new ConnectContext();
        Table nativeTable = createBaseTableWithManifests(1, 1);

        new Expectations() {
            {
                restCatalog.isVendedCredentialsEnabled();
                result = false;
                minTimes = 0;

                restCatalog.getTable(ctx, "db5", "tbl5");
                result = nativeTable;
            }
        };

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, restCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        Table result1 = cachingIcebergCatalog.getTable(ctx, "db5", "tbl5");
        Table result2 = cachingIcebergCatalog.getTable(ctx, "db5", "tbl5");

        // Should return the same instance (cached)
        Assertions.assertSame(nativeTable, result1);
        Assertions.assertSame(nativeTable, result2);
        Assertions.assertSame(result1, result2);
    }

    @Test
    public void testGetCatalogPropertiesDelegatesToWrappedCatalog() {
        Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("s3.access-key-id", "test-key");
        expectedProperties.put("s3.secret-access-key", "test-secret");

        // Use Mockito for this test since JMockit doesn't properly handle default interface methods
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getCatalogProperties()).thenReturn(expectedProperties);

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, delegate,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        Map<String, String> actualProperties = cachingIcebergCatalog.getCatalogProperties();
        Assertions.assertEquals(expectedProperties, actualProperties);
        Assertions.assertEquals("test-key", actualProperties.get("s3.access-key-id"));
        Assertions.assertEquals("test-secret", actualProperties.get("s3.secret-access-key"));

        // Verify that getCatalogProperties was called on the delegate
        Mockito.verify(delegate).getCatalogProperties();
    }

    @Test
    public void testCacheFreshnessBug(@Mocked IcebergCatalog delegate, @Mocked PartitionSpec spec) {
        //this test will fail on 3.5.9
        System.out.println("===========Starting testCacheFreshnessBug==========");
        String dbName = "db";
        String tblName = "test_table";
        ConnectContext ctx = new ConnectContext();
        new Expectations() {
            {
                delegate.getPartitions((IcebergTable) any, anyLong, null);
                result = new HashMap<String, Partition>();

                delegate.getTable((ConnectContext) any, anyString, anyString);
                result = new Delegate<Table>() {
                    AtomicLong counter = new AtomicLong();

                    Table getTable(ConnectContext ctx, String db, String tbl) throws StarRocksConnectorException {
                        if (Thread.currentThread().getName().equals("main")) {
                            System.out.println("[loader] start Loading iceberg table " + 
                                    db + "." + tbl + " " + Thread.currentThread().getName());     
                        } else {
                            System.out.println("[async reloader] start ReLoading iceberg table " + 
                                    db + "." + tbl + " " + Thread.currentThread().getName());     
                        }

                        long n = counter.incrementAndGet();
                        Snapshot snapshot = Mockito.mock(Snapshot.class);
                        Mockito.when(snapshot.snapshotId()).thenReturn(n);
                        Mockito.when(snapshot.dataManifests(Mockito.any())).thenReturn(Lists.newArrayList());

                        TableMetadata meta = Mockito.mock(TableMetadata.class);
                        Mockito.when(meta.metadataFileLocation()).thenReturn("hdfs://path/to/table_" + n);
                        Mockito.when(meta.spec()).thenReturn(spec);
                        Mockito.when(meta.currentSnapshot()).thenReturn(snapshot);

                        TableOperations ops = Mockito.mock(TableOperations.class);
                        Mockito.when(ops.current()).thenReturn(meta);

                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }

                        if (Thread.currentThread().getName().equals("main")) {
                            System.out.println("[loader] finish Loading iceberg table " + 
                                    db + "." + tbl + " " + Thread.currentThread().getName());     
                        } else {
                            System.out.println("[async reloader] finish ReLoading iceberg table " + 
                                    db + "." + tbl + " " + Thread.currentThread().getName());     
                        }

                        return new BaseTable(ops, db + "." + tbl);
                    }
                };
            }
        };

        Map<String, String> config = new HashMap<>();
        config.put(IcebergCatalogProperties.ICEBERG_TABLE_CACHE_REFRESH_INVERVAL_SEC, "5");
        config.put(IcebergCatalogProperties.ICEBERG_META_CACHE_TTL, "30");
        config.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive");
        config.put(IcebergCatalogProperties.ICEBERG_TABLE_CACHE_MEMORY_SIZE_RATIO, "1");
        IcebergCatalogProperties icebergProperties = new IcebergCatalogProperties(config);
        ExecutorService exectorCatalog = Executors.newSingleThreadExecutor();
        ExecutorService exector = Executors.newSingleThreadExecutor();
        

        CachingIcebergCatalog catalog = new CachingIcebergCatalog("test_catalog", delegate, icebergProperties, exectorCatalog);
        //Guava cache will cause bug here, now we try the caffeine
        LoadingCache<IcebergTableName, Table> tables = Deencapsulation.getField(catalog, "tables");
        Table tmp1 = delegate.getTable(ctx, dbName, tblName);
        Table tmp2 = delegate.getTable(ctx, dbName, tblName);
        Table tmp3 = delegate.getTable(ctx, dbName, tblName);
        
        System.out.println("===== cache test =====");
        catalog.getTable(ctx, dbName, tblName);
        catalog.refreshTable(dbName, tblName, ctx, null);
        System.out.printf("[main] put key val: %s -> %d %n", "snap key", ((BaseTable) tmp1).currentSnapshot().snapshotId());
        catalog.refreshTable(dbName, tblName, ctx, null);
        System.out.printf("[main] put key val: %s -> %d %n", "snap key", ((BaseTable) tmp2).currentSnapshot().snapshotId());

        try {
            Thread.sleep(6000);
        } catch (InterruptedException ie) {
        }

        System.out.println("[main] first get key val begin");
        Table t1 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("[main] begin put key val begin snap 3");
        // try to mock the concurrency in async load and put here, usually between refresh table and get table.
        // here may be break the cache
        tables.put(new IcebergTableName(dbName, tblName, 3L), tmp3);
        System.out.println("[main] finish put key val begin snap 3");
        System.out.println("[main] first get key val res:" + ((BaseTable) t1).currentSnapshot().snapshotId());
        try {
            Thread.sleep(10100);
        } catch (InterruptedException ie) {
        }
        tables.invalidate(new IcebergTableName(dbName, tblName));
        tables.invalidate(new IcebergTableName(dbName, tblName));
        tables.invalidate(new IcebergTableName(dbName, tblName));
        tables.invalidate(new IcebergTableName(dbName, tblName));
        Table t2 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t2).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableName(dbName, tblName)));

        try {
            Thread.sleep(1100);
        } catch (InterruptedException ie) {
        }
        
        Table t3 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t3).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableName(dbName, tblName)));

        catalog.refreshTable(dbName, tblName, ctx, null);

        Table t4 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t4).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableName(dbName, tblName)));

        Assertions.assertTrue(t4.currentSnapshot().snapshotId() > t3.currentSnapshot().snapshotId());   
        Assertions.assertNotNull(tables.getIfPresent(new IcebergTableName(dbName, tblName)));
    }

    @Test
    public void testCacheFreshnessRandom(@Mocked IcebergCatalog delegate, @Mocked PartitionSpec spec) {
        System.out.println("===========Starting testCacheFreshnessRandom==========");
        String dbName = "db";
        String tblName = "test_table";
        ConnectContext ctx = new ConnectContext();
        new Expectations() {
            {
                delegate.getPartitions((IcebergTable) any, anyLong, null);
                result = new HashMap<String, Partition>();

                delegate.getTable((ConnectContext) any, anyString, anyString);
                result = new Delegate<Table>() {
                    AtomicLong counter = new AtomicLong();

                    Table getTable(ConnectContext ctx, String db, String tbl) throws StarRocksConnectorException {
                        if (Thread.currentThread().getName().equals("main")) {
                            System.out.println("[loader] start Loading iceberg table " + 
                                    db + "." + tbl + " " + Thread.currentThread().getName());     
                        } else {
                            System.out.println("[async reloader] start ReLoading iceberg table " + 
                                    db + "." + tbl + " " + Thread.currentThread().getName());     
                        }

                        long n = counter.incrementAndGet();
                        Snapshot snapshot = Mockito.mock(Snapshot.class);
                        Mockito.when(snapshot.snapshotId()).thenReturn(n);
                        Mockito.when(snapshot.dataManifests(Mockito.any())).thenReturn(Lists.newArrayList());

                        TableMetadata meta = Mockito.mock(TableMetadata.class);
                        Mockito.when(meta.metadataFileLocation()).thenReturn("hdfs://path/to/table_" + n);
                        Mockito.when(meta.spec()).thenReturn(spec);
                        Mockito.when(meta.currentSnapshot()).thenReturn(snapshot);

                        TableOperations ops = Mockito.mock(TableOperations.class);
                        Mockito.when(ops.current()).thenReturn(meta);

                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }

                        if (Thread.currentThread().getName().equals("main")) {
                            System.out.println("[loader] finish Loading iceberg table " + 
                                    db + "." + tbl + " " + Thread.currentThread().getName());     
                        } else {
                            System.out.println("[async reloader] finish ReLoading iceberg table " + 
                                    db + "." + tbl + " " + Thread.currentThread().getName());     
                        }

                        return new BaseTable(ops, db + "." + tbl);
                    }
                };
            }
        };

        Map<String, String> config = new HashMap<>();
        config.put(IcebergCatalogProperties.ICEBERG_TABLE_CACHE_REFRESH_INVERVAL_SEC, "2");
        config.put(IcebergCatalogProperties.ICEBERG_META_CACHE_TTL, "6");
        config.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive");
        config.put(IcebergCatalogProperties.ICEBERG_TABLE_CACHE_MEMORY_SIZE_RATIO, "1");
        IcebergCatalogProperties icebergProperties = new IcebergCatalogProperties(config);
        ExecutorService exectorCatalog = Executors.newSingleThreadExecutor();
        ExecutorService exector = Executors.newSingleThreadExecutor();
        

        CachingIcebergCatalog catalog = new CachingIcebergCatalog("test_catalog", delegate, icebergProperties, exectorCatalog);

        LoadingCache<IcebergTableName, Table> tables = Deencapsulation.getField(catalog, "tables");
        Table tmp1 = delegate.getTable(ctx, dbName, tblName);
        Table tmp2 = delegate.getTable(ctx, dbName, tblName);
        Table tmp3 = delegate.getTable(ctx, dbName, tblName);
        
        System.out.println("===== cache test =====");
        catalog.getTable(ctx, dbName, tblName);
        catalog.refreshTable(dbName, tblName, ctx, null);
        System.out.printf("[main] put key val: %s -> %d %n", "snap key", ((BaseTable) tmp1).currentSnapshot().snapshotId());
        catalog.refreshTable(dbName, tblName, ctx, null);
        System.out.printf("[main] put key val: %s -> %d %n", "snap key", ((BaseTable) tmp2).currentSnapshot().snapshotId());

        try {
            Thread.sleep(2100);
        } catch (InterruptedException ie) {
        }

        System.out.println("[main] first get key val begin");
        Table t1 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("[main] begin put key val begin snap 3");
        catalog.refreshTable(dbName, dbName, ctx, null);
        tables.invalidateAll();
        System.out.println("[main] finish put key val and invalidate all snap 3");
        System.out.println("[main] first get key val res:" + ((BaseTable) t1).currentSnapshot().snapshotId());

        try {
            Thread.sleep(2100);
        } catch (InterruptedException ie) {
        }
        System.out.println("[main] begin put key val begin snap 4, 5");
        catalog.refreshTable(dbName, dbName, ctx, null);
        catalog.getTable(ctx, dbName, tblName);
        catalog.refreshTable(dbName, dbName, ctx, exector);
        System.out.println("[main] begin put key val begin snap 4, 5");
        try {
            Thread.sleep(6100);
        } catch (InterruptedException ie) {
        }
        tables.invalidateAll();
        Table t2 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t2).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableName(dbName, tblName)));

        try {
            Thread.sleep(1100);
        } catch (InterruptedException ie) {
        }
        
        Table t3 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t3).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableName(dbName, tblName)));

        catalog.refreshTable(dbName, tblName, ctx, null);

        Table t4 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t4).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableName(dbName, tblName)));

        Assertions.assertTrue(t4.currentSnapshot().snapshotId() > t3.currentSnapshot().snapshotId());   
        Assertions.assertNotNull(tables.getIfPresent(new IcebergTableName(dbName, tblName)));
    }

    @Test
    public void testReloadIsAsync(@Mocked IcebergCatalog delegate,
                                  @Mocked IcebergCatalogProperties props,
                                  @Mocked ConnectContext ctx) throws Exception {
        System.out.println("===== test reload async =====");
        Table nativeTable1 = createBaseTableWithManifests(1, 1);
        Table nativeTable2 = createBaseTableWithManifests(2, 2);
        Mockito.when(((BaseTable) nativeTable1).operations().current().metadataFileLocation()).thenReturn("loc1");
        Mockito.when(((BaseTable) nativeTable2).operations().current().metadataFileLocation()).thenReturn("loc2");

        AtomicLong callCount = new AtomicLong(0);

        new Expectations() {
            {
                props.isEnableIcebergMetadataCache();
                result = true;
                props.getIcebergMetaCacheTtlSec();
                result = 60L;
                props.getIcebergTableCacheRefreshIntervalSec();
                result = 1L;
                props.getIcebergTableCacheMemoryUsageRatio();
                result = 1.0;
                props.isEnableIcebergTableCache();
                result = true;
                props.getIcebergDataFileCacheMemoryUsageRatio();
                result = 0.0;
                props.getIcebergDeleteFileCacheMemoryUsageRatio();
                result = 0.0;

                delegate.getTable((ConnectContext) any, "db1", "t1");
                result = new Delegate<Table>() {
                    Table capture(ConnectContext c, String db, String tbl) throws Exception {
                        if (Thread.currentThread().getName().equals("main")) {
                            System.out.println("[loader] start Loading iceberg table " + 
                                    db + "." + tbl + " " + Thread.currentThread().getName());     
                        } else {
                            System.out.println("[async reloader] start ReLoading iceberg table " + 
                                    db + "." + tbl + " " + Thread.currentThread().getName());     
                        }
                        long idx = callCount.incrementAndGet();
                        if (idx == 1) {
                            return nativeTable1;
                        }
                        return nativeTable2;
                    }
                };
            }
        };

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog =
                    new CachingIcebergCatalog("iceberg0", delegate, props, es);
            IcebergTableName key = new IcebergTableName("db1", "t1");

            Table cached = catalog.getTable(ctx, "db1", "t1");
            Assertions.assertSame(nativeTable1, cached);

            LoadingCache<IcebergTableName, Table> tableCache = Deencapsulation.getField(catalog, "tables");

            Table t1 = tableCache.get(key);
            Assertions.assertTrue(callCount.get() == 1);
            Thread.sleep(1100);
            Table t2 = tableCache.get(key);
            Assertions.assertSame(t1, nativeTable1, "table should be same yet");
            Assertions.assertSame(t1, cached, "table should be same yet");
            Assertions.assertSame(t1, t2, "table should be same yet");
            Thread.sleep(300);
            Assertions.assertTrue(callCount.get() == 2, "all count:" + String.valueOf(callCount.get()));
            Table t3 = tableCache.get(key);
            Assertions.assertSame(t3, nativeTable2, "table should be new after reload");
        } finally {
            es.shutdownNow();
            System.out.println("===== test reload async end =====");
        }
    }

    /**
     * Two concurrent refreshTable calls on the SAME table must be serialized: the second
     * waits for the first to finish before entering its critical section.
     */
    @Test
    public void testRefreshTableSameTableIsSerializedNotParallel() throws Exception {
        AtomicInteger concurrentRefreshes = new AtomicInteger(0);
        AtomicInteger maxConcurrentRefreshes = new AtomicInteger(0);
        CountDownLatch firstStarted = new CountDownLatch(1);

        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl")))
                .thenAnswer(inv -> {
                    int current = concurrentRefreshes.incrementAndGet();
                    maxConcurrentRefreshes.accumulateAndGet(current, Math::max);
                    firstStarted.countDown();
                    Thread.sleep(80);
                    concurrentRefreshes.decrementAndGet();

                    TableOperations ops = Mockito.mock(TableOperations.class);
                    TableMetadata meta = Mockito.mock(TableMetadata.class);
                    Mockito.when(ops.current()).thenReturn(meta);
                    Mockito.when(meta.metadataFileLocation()).thenReturn("loc-" + UUID.randomUUID());
                    Snapshot snap = Mockito.mock(Snapshot.class);
                    Mockito.when(snap.snapshotId()).thenReturn(1L);
                    Mockito.when(snap.dataManifests(Mockito.any())).thenReturn(List.of());
                    Mockito.when(meta.currentSnapshot()).thenReturn(snap);
                    return new BaseTable(ops, "db.tbl");
                });

        CachingIcebergCatalog catalog = new CachingIcebergCatalog(
                CATALOG_NAME, delegate, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        // Populate cache with an initial table so refreshTable enters the update branch.
        TableOperations initOps = Mockito.mock(TableOperations.class);
        TableMetadata initMeta = Mockito.mock(TableMetadata.class);
        Snapshot initSnap = Mockito.mock(Snapshot.class);
        Mockito.when(initOps.current()).thenReturn(initMeta);
        Mockito.when(initMeta.metadataFileLocation()).thenReturn("loc-initial");
        Mockito.when(initMeta.currentSnapshot()).thenReturn(initSnap);
        Mockito.when(initSnap.snapshotId()).thenReturn(0L);
        Mockito.when(initSnap.dataManifests(Mockito.any())).thenReturn(List.of());
        BaseTable initTable = new BaseTable(initOps, "db.tbl");
        LoadingCache<IcebergTableName, Table> tables1 = Deencapsulation.getField(catalog, "tables");
        tables1.put(new IcebergTableName("db", "tbl"), initTable);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        ConnectContext ctx = new ConnectContext();

        pool.submit(() -> catalog.refreshTable("db", "tbl", ctx, null));
        firstStarted.await(2, TimeUnit.SECONDS);
        pool.submit(() -> catalog.refreshTable("db", "tbl", ctx, null));

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        Assertions.assertEquals(1, maxConcurrentRefreshes.get(),
                "concurrent refreshes on same table must be serialized (max concurrent should be 1)");
    }

    /**
     * Two concurrent refreshTable calls on DIFFERENT tables must not block each other:
     * both should overlap in time.
     *
     * <p>A CyclicBarrier forces both threads to rendezvous inside the mock before either
     * proceeds, so the "max concurrent" reading is deterministic even under GC pressure.
     * No wall-clock assertion is used.
     */
    @Test
    public void testRefreshTableDifferentTablesRunInParallel() throws Exception {
        AtomicInteger maxConcurrent = new AtomicInteger(0);
        AtomicInteger concurrent = new AtomicInteger(0);
        // Barrier ensures both threads have incremented the counter before either continues.
        CyclicBarrier barrier = new CyclicBarrier(2);

        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.anyString()))
                .thenAnswer(inv -> {
                    int c = concurrent.incrementAndGet();
                    maxConcurrent.accumulateAndGet(c, Math::max);
                    // Wait until the other thread also reaches this point, guaranteeing overlap.
                    //
                    // Why 5 s: in the happy path both threads reach here in microseconds (the
                    // code path is a handful of ConcurrentHashMap ops + one synchronized block
                    // on different lock objects).  The timeout only fires when the lock is
                    // catalog-wide and permanently blocks the second thread — the regression we
                    // want to catch.
                    //
                    // Trade-off: a shorter timeout reduces false-negative latency when the
                    // regression is present, but risks a false-positive (flaky failure) under
                    // extreme GC pressure.  5 s is conservative enough to absorb even a full
                    // GC pause while still keeping test feedback fast.
                    try {
                        barrier.await(5, TimeUnit.SECONDS);
                    } catch (BrokenBarrierException | java.util.concurrent.TimeoutException e) {
                        throw new RuntimeException(
                                "Barrier timed out — second thread never reached the barrier. "
                                        + "This indicates catalog-wide locking is blocking concurrent "
                                        + "refreshes of different tables.", e);
                    }
                    concurrent.decrementAndGet();

                    TableOperations ops = Mockito.mock(TableOperations.class);
                    TableMetadata meta = Mockito.mock(TableMetadata.class);
                    Mockito.when(ops.current()).thenReturn(meta);
                    Mockito.when(meta.metadataFileLocation()).thenReturn("loc-" + UUID.randomUUID());
                    Snapshot snap = Mockito.mock(Snapshot.class);
                    Mockito.when(snap.snapshotId()).thenReturn(1L);
                    Mockito.when(snap.dataManifests(Mockito.any())).thenReturn(List.of());
                    Mockito.when(meta.currentSnapshot()).thenReturn(snap);
                    return new BaseTable(ops, "db." + inv.getArgument(2));
                });

        CachingIcebergCatalog catalog = new CachingIcebergCatalog(
                CATALOG_NAME, delegate, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        LoadingCache<IcebergTableName, Table> tables2 = Deencapsulation.getField(catalog, "tables");
        // Pre-populate cache for both tables so refreshTable enters the update branch.
        for (String tbl : List.of("tbl1", "tbl2")) {
            TableOperations initOps = Mockito.mock(TableOperations.class);
            TableMetadata initMeta = Mockito.mock(TableMetadata.class);
            Snapshot initSnap = Mockito.mock(Snapshot.class);
            Mockito.when(initOps.current()).thenReturn(initMeta);
            Mockito.when(initMeta.metadataFileLocation()).thenReturn("loc-initial-" + tbl);
            Mockito.when(initMeta.currentSnapshot()).thenReturn(initSnap);
            Mockito.when(initSnap.snapshotId()).thenReturn(0L);
            Mockito.when(initSnap.dataManifests(Mockito.any())).thenReturn(List.of());
            tables2.put(new IcebergTableName("db", tbl), new BaseTable(initOps, "db." + tbl));
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        ConnectContext ctx = new ConnectContext();

        pool.submit(() -> catalog.refreshTable("db", "tbl1", ctx, null));
        pool.submit(() -> catalog.refreshTable("db", "tbl2", ctx, null));

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        // The barrier above guarantees deterministic overlap: if the lock were catalog-wide,
        // the second thread would block on synchronized() and the barrier would time out,
        // causing the test to fail with BrokenBarrierException before reaching this line.
        Assertions.assertEquals(2, maxConcurrent.get(),
                "refreshes on different tables should overlap (max concurrent should be 2)");
    }

    @Test
    public void testLoadLargePartitionSetTriggersDiagnosticLog(@Mocked IcebergCatalog delegate,
                                                               @Mocked IcebergCatalogProperties props,
                                                               @Mocked ConnectContext ctx) throws Exception {
        // Build a partition map exceeding PARTITION_LOAD_LOG_THRESHOLD (10000) so the diagnostic
        // INFO branch in the partition cache loader is exercised.
        Map<String, Partition> bigPartitions = new HashMap<>();
        for (int i = 0; i <= 10000; i++) {
            bigPartitions.put("p" + i, new Partition(0L, 0L));
        }

        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Mockito.when(spec.fields()).thenReturn(java.util.Collections.emptyList());
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);

        BaseTable nativeTable = (BaseTable) createBaseTableWithManifests(1, 0, spec);
        TableMetadata meta = nativeTable.operations().current();
        Mockito.when(meta.specsById()).thenReturn(Map.of(0, spec));

        Snapshot currentSnap = meta.currentSnapshot();
        Map<String, String> summary = new HashMap<>();
        summary.put(SnapshotSummary.TOTAL_DATA_FILES_PROP, "5");
        summary.put(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0");
        Mockito.when(currentSnap.snapshotId()).thenReturn(42L);
        Mockito.when(currentSnap.summary()).thenReturn(summary);

        new Expectations() {
            {
                props.isEnableIcebergMetadataCache();
                result = true;
                props.getIcebergMetaCacheTtlSec();
                result = 60L;
                props.isEnableIcebergTableCache();
                result = true;
                props.getIcebergTableCacheMemoryUsageRatio();
                result = 1.0;
                props.getIcebergDataFileCacheMemoryUsageRatio();
                result = 0.0;
                props.getIcebergDeleteFileCacheMemoryUsageRatio();
                result = 0.0;

                delegate.getTable((ConnectContext) any, "db", "t");
                result = nativeTable;
                minTimes = 0;

                delegate.getPartitions((IcebergTable) any, -1L, null);
                result = bigPartitions;
                minTimes = 1;
            }
        };

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("c0", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setSrTableName("t")
                    .setCatalogDBName("db")
                    .setCatalogTableName("t")
                    .setNativeTable(nativeTable)
                    .build();

            // snapshotId == -1 forces the loader to fall back to nativeTable.currentSnapshot()
            // for the logged snapshot id and summary.
            Map<String, Partition> result = catalog.getPartitions(icebergTable, -1L, null);
            Assertions.assertEquals(10001, result.size());
        } finally {
            es.shutdownNow();
        }
    }
}
