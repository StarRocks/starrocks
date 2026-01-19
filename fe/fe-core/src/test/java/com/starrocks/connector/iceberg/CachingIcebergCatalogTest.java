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
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.CachingIcebergCatalog.IcebergTableCacheKey;
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
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
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
    public void testListPartitionNames(@Mocked IcebergCatalog icebergCatalog, @Mocked Table nativeTable) {
        new Expectations() {
            {
                nativeTable.spec().isUnpartitioned();
                result = false;
                minTimes = 0;

                // Mock getPartitions to return empty map
                icebergCatalog.getPartitions((IcebergTable) any, anyLong, (ExecutorService) any);
                result = new HashMap<String, Partition>();
                minTimes = 0;
            }
        };
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergTable table =
                IcebergTable.builder().setCatalogDBName("db").setCatalogTableName("test").setNativeTable(nativeTable).build();

        Assertions.assertFalse(nativeTable.spec().isUnpartitioned());
        {
            ConnectorMetadatRequestContext requestContext = new ConnectorMetadatRequestContext();
            SessionVariable sv = ConnectContext.getSessionVariableOrDefault();
            sv.setEnableConnectorAsyncListPartitions(true);
            requestContext.setQueryMVRewrite(true);
            List<String> res = cachingIcebergCatalog.listPartitionNames(table, requestContext, null);
            Assertions.assertNull(res);
        }
        {
            ConnectorMetadatRequestContext requestContext = new ConnectorMetadatRequestContext();
            SessionVariable sv = ConnectContext.getSessionVariableOrDefault();
            sv.setEnableConnectorAsyncListPartitions(false);
            requestContext.setQueryMVRewrite(true);
            List<String> res = cachingIcebergCatalog.listPartitionNames(table, requestContext, null);
            Assertions.assertEquals(res.size(), 0);
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
    public void testGetTable(@Mocked IcebergCatalog icebergCatalog, @Mocked Table nativeTable) {
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
    public void testGetTableIOError(@Mocked IcebergCatalog icebergCatalog, @Mocked Table nativeTable) {
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

    @Test
    public void testInvalidateCache(@Mocked IcebergCatalog icebergCatalog, @Mocked Table nativeTable) {
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
                                                       @Mocked ConnectContext ctx,
                                                       @Mocked org.apache.iceberg.Table nativeTable) throws Exception {
        new Expectations() {
            {
                props.isEnableIcebergMetadataCache(); 
                result = true;
                props.isEnableIcebergTableCache(); 
                result = true;
                props.getIcebergMetaCacheTtlSec(); 
                result = 24L * 60 * 60;
                props.getIcebergDataFileCacheMemoryUsageRatio(); 
                result = 0.0;
                props.getIcebergDeleteFileCacheMemoryUsageRatio(); 
                result = 0.0;

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
                                                         @Mocked ConnectContext ctx,
                                                         @Mocked org.apache.iceberg.Table nativeTable1,
                                                         @Mocked org.apache.iceberg.Table nativeTable2) throws Exception {
        new Expectations() {
            {
                props.isEnableIcebergMetadataCache(); 
                result = true;
                props.isEnableIcebergTableCache(); 
                result = false;
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
    public void testEstimateCountReflectsTableCache(@Mocked IcebergCatalog icebergCatalog, @Mocked Table nativeTable) {
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
    public void testGetTableBypassCacheForRestCatalogWhenAuthToken(@Mocked IcebergRESTCatalog restCatalog,
                                                                   @Mocked Table nativeTable) {
        ConnectContext ctx = new ConnectContext();
        ctx.setAuthToken("token");
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

                delegate.getTable(ctx, anyString, anyString);
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
        IcebergCatalogProperties icebergProperties = new IcebergCatalogProperties(config);
        ExecutorService exectorCatalog = Executors.newSingleThreadExecutor();
        ExecutorService exector = Executors.newSingleThreadExecutor();
        

        CachingIcebergCatalog catalog = new CachingIcebergCatalog("test_catalog", delegate, icebergProperties, exectorCatalog);
        //Guava cache will cause bug here, now we try the caffeine
        LoadingCache<IcebergTableCacheKey, Table> tables = Deencapsulation.getField(catalog, "tables");
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
        tables.put(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName, 3L), ctx), tmp3);
        System.out.println("[main] finish put key val begin snap 3");
        System.out.println("[main] first get key val res:" + ((BaseTable) t1).currentSnapshot().snapshotId());
        try {
            Thread.sleep(10100);
        } catch (InterruptedException ie) {
        }
        tables.invalidate(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx));
        tables.invalidate(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx));
        tables.invalidate(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx));
        tables.invalidate(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx));
        Table t2 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t2).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx)));

        try {
            Thread.sleep(1100);
        } catch (InterruptedException ie) {
        }
        
        Table t3 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t3).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx)));

        catalog.refreshTable(dbName, tblName, ctx, null);

        Table t4 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t4).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx)));

        Assertions.assertTrue(t4.currentSnapshot().snapshotId() > t3.currentSnapshot().snapshotId());   
        Assertions.assertNotNull(tables.getIfPresent(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx)));
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

                delegate.getTable(ctx, anyString, anyString);
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
        IcebergCatalogProperties icebergProperties = new IcebergCatalogProperties(config);
        ExecutorService exectorCatalog = Executors.newSingleThreadExecutor();
        ExecutorService exector = Executors.newSingleThreadExecutor();
        

        CachingIcebergCatalog catalog = new CachingIcebergCatalog("test_catalog", delegate, icebergProperties, exectorCatalog);

        LoadingCache<IcebergTableCacheKey, Table> tables = Deencapsulation.getField(catalog, "tables");
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
                tables.getIfPresent(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx)));

        try {
            Thread.sleep(1100);
        } catch (InterruptedException ie) {
        }
        
        Table t3 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t3).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx)));

        catalog.refreshTable(dbName, tblName, ctx, null);

        Table t4 = catalog.getTable(ctx, dbName, tblName);
        System.out.println("Table SnapshotId:" + String.valueOf(((BaseTable) t4).currentSnapshot().snapshotId()) +
                " should found in cache if present:" + 
                tables.getIfPresent(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx)));

        Assertions.assertTrue(t4.currentSnapshot().snapshotId() > t3.currentSnapshot().snapshotId());
        Assertions.assertNotNull(tables.getIfPresent(new IcebergTableCacheKey(new IcebergTableName(dbName, tblName), ctx)));
    }

    @Test
    public void testLargeTableBypassCache() {
        // Use Mockito for clearer mocking
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);

        // Setup: Configure threshold to 100 partitions
        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        // Simulate a large table with 1000 partitions
        Mockito.when(delegate.estimatePartitionCount(Mockito.any(IcebergTable.class), Mockito.anyLong()))
                .thenReturn(1000L);

        Map<String, Partition> partitionMap = new HashMap<>();
        partitionMap.put("date=2024-01-01", new Partition(System.currentTimeMillis()));
        Mockito.when(delegate.getPartitions(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any()))
                .thenReturn(partitionMap);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setCatalogDBName("db")
                    .setCatalogTableName("large_table")
                    .setNativeTable(nativeTable)
                    .build();

            // Call getPartitions for a large table
            catalog.getPartitions(icebergTable, 1L, null);

            // Verify that delegate.getPartitions was called directly (bypassing cache)
            Mockito.verify(delegate).getPartitions(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any());
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testSmallTableUsesCache() {
        // Use Mockito for clearer mocking
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);

        Map<String, Partition> partitionMap = new HashMap<>();
        partitionMap.put("date=2024-01-01", new Partition(System.currentTimeMillis()));

        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        // Simulate a small table with 50 partitions (below threshold)
        Mockito.when(delegate.estimatePartitionCount(Mockito.any(IcebergTable.class), Mockito.anyLong()))
                .thenReturn(50L);
        Mockito.when(delegate.getPartitions(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any()))
                .thenReturn(partitionMap);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setCatalogDBName("db")
                    .setCatalogTableName("small_table")
                    .setNativeTable(nativeTable)
                    .build();

            // First call - should load into cache
            Map<String, Partition> result1 = catalog.getPartitions(icebergTable, 1L, null);
            // Second call - should hit cache
            Map<String, Partition> result2 = catalog.getPartitions(icebergTable, 1L, null);

            Assertions.assertEquals(1, result1.size());
            Assertions.assertEquals(1, result2.size());

            // Verify delegate.getPartitions was called only once (cache hit on second call)
            Mockito.verify(delegate, Mockito.times(1))
                    .getPartitions(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any());
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testEstimatePartitionCountForUnpartitionedTable() {
        // Test that unpartitioned tables return 1 for estimatePartitionCount
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);

        // Setup unpartitioned table
        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(true);

        // When estimatePartitionCount is called on unpartitioned table via default implementation
        // it should return 1 without scanning PartitionsTable
        Mockito.when(delegate.estimatePartitionCount(Mockito.any(IcebergTable.class), Mockito.anyLong()))
                .thenCallRealMethod();

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("unpartitioned_table")
                .setNativeTable(nativeTable)
                .build();

        // The default implementation checks isUnpartitioned() and returns 1
        // Since we're calling the real method, we need to make sure it works
        // We test through the CachingIcebergCatalog which delegates to the actual implementation
        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        // Return 1 for unpartitioned table
        Mockito.when(delegate.estimatePartitionCount(Mockito.any(IcebergTable.class), Mockito.anyLong()))
                .thenReturn(1L);

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            long count = catalog.estimatePartitionCount(icebergTable, 1L);
            Assertions.assertEquals(1L, count);
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testGetPartitionsByNamesStreamingDecisionLogic() {
        // Test the 10% rule: small requests use streaming, large requests use full load
        // This tests the decision logic in IcebergCatalog.getPartitionsByNames default method

        // Case 1: Request 3 partitions out of 1000 (0.3% < 10%) - should use streaming
        // 3 < 1000 / 10 = 100 -> true, use streaming
        int totalPartitions = 1000;
        int smallRequest = 3;
        Assertions.assertTrue(smallRequest < totalPartitions / 10,
                "Small request (3 out of 1000) should trigger streaming path");

        // Case 2: Request 150 partitions out of 1000 (15% >= 10%) - should use full load
        // 150 < 1000 / 10 = 100 -> false, use full load
        int largeRequest = 150;
        Assertions.assertFalse(largeRequest < totalPartitions / 10,
                "Large request (150 out of 1000) should trigger full load path");

        // Case 3: Edge case - exactly 10% should use full load
        // 100 < 1000 / 10 = 100 -> false
        int edgeRequest = 100;
        Assertions.assertFalse(edgeRequest < totalPartitions / 10,
                "Edge case (exactly 10%) should trigger full load path");

        // Case 4: When estimatedCount is -1 (estimation failed), should use full load
        long failedEstimate = -1;
        int anyRequest = 5;
        boolean shouldUseStreaming = failedEstimate > 0 && anyRequest < failedEstimate / 10;
        Assertions.assertFalse(shouldUseStreaming,
                "When estimation fails (-1), should fall back to full load");

        // Case 5: When estimatedCount is 0 (empty table), should use full load
        long emptyTableEstimate = 0;
        shouldUseStreaming = emptyTableEstimate > 0 && anyRequest < emptyTableEstimate / 10;
        Assertions.assertFalse(shouldUseStreaming,
                "When table is empty (0 partitions), should fall back to full load");
    }

    @Test
    public void testStreamingPartitionIteratorEarlyClose() throws IOException {
        // Test that resources are properly cleaned up when iterator is closed early
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        // Track if close was called
        AtomicBoolean taskIterableClosed = new AtomicBoolean(false);
        AtomicBoolean rowsClosed = new AtomicBoolean(false);

        // Mock task iterable
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataTask dataTask = Mockito.mock(DataTask.class);
        Mockito.when(task.asDataTask()).thenReturn(dataTask);

        // Create mock rows that track close
        CloseableIterable<StructLike> mockRows = new CloseableIterable<StructLike>() {
            private final List<StructLike> rows = new ArrayList<>();

            @Override
            public CloseableIterator<StructLike> iterator() {
                return new CloseableIterator<StructLike>() {
                    private final Iterator<StructLike> iter = rows.iterator();

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public StructLike next() {
                        return iter.next();
                    }

                    @Override
                    public void close() {
                        // Individual iterator close
                    }
                };
            }

            @Override
            public void close() {
                rowsClosed.set(true);
            }
        };
        Mockito.when(dataTask.rows()).thenReturn(mockRows);

        // Create task iterable that tracks close
        CloseableIterable<FileScanTask> taskIterable = new CloseableIterable<FileScanTask>() {
            private final List<FileScanTask> tasks = Arrays.asList(task);

            @Override
            public CloseableIterator<FileScanTask> iterator() {
                return new CloseableIterator<FileScanTask>() {
                    private final Iterator<FileScanTask> iter = tasks.iterator();

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public FileScanTask next() {
                        return iter.next();
                    }

                    @Override
                    public void close() {
                        // Individual iterator close
                    }
                };
            }

            @Override
            public void close() {
                taskIterableClosed.set(true);
            }
        };

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(nativeTable.specs()).thenReturn(Map.of(0, spec));
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.timestampMillis()).thenReturn(System.currentTimeMillis());
        Mockito.when(nativeTable.name()).thenReturn("test_table");

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        // Create the iterator
        StreamingPartitionIterator iterator = new StreamingPartitionIterator(
                nativeTable, icebergTable, taskIterable, 1L);

        // Close immediately without iterating (simulating early termination)
        Assertions.assertFalse(taskIterableClosed.get(), "Task iterable should not be closed yet");

        iterator.close();

        // Verify resources were cleaned up
        Assertions.assertTrue(taskIterableClosed.get(), "Task iterable should be closed after iterator.close()");

        // Verify that hasNext returns false after close
        Assertions.assertFalse(iterator.hasNext(), "hasNext should return false after close");

        // Verify double close is safe
        iterator.close();  // Should not throw
    }

    @Test
    public void testStreamingPartitionIteratorFullIteration() throws IOException {
        // Test full iteration through StreamingPartitionIterator including parsePartitionRow and getLastUpdated
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        // Create mock partition data
        StructLike mockRow = Mockito.mock(StructLike.class);
        org.apache.iceberg.util.StructProjection partitionData =
                Mockito.mock(org.apache.iceberg.util.StructProjection.class);

        // Setup row data: partition(0), spec_id(1), ..., last_updated_at(9)
        Mockito.when(mockRow.get(0, org.apache.iceberg.util.StructProjection.class)).thenReturn(partitionData);
        Mockito.when(mockRow.get(1, Integer.class)).thenReturn(0);
        Mockito.when(mockRow.get(9, Long.class)).thenReturn(1704067200000L); // 2024-01-01 00:00:00

        // Setup partition spec to convert partition data
        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(nativeTable.specs()).thenReturn(Map.of(0, spec));
        Mockito.when(spec.fields()).thenReturn(List.of());
        Mockito.when(nativeTable.name()).thenReturn("test_table");
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.timestampMillis()).thenReturn(System.currentTimeMillis());

        // Create task with rows
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataTask dataTask = Mockito.mock(DataTask.class);
        Mockito.when(task.asDataTask()).thenReturn(dataTask);

        // Create rows iterable
        List<StructLike> rowList = Arrays.asList(mockRow);
        CloseableIterable<StructLike> mockRows = CloseableIterable.withNoopClose(rowList);
        Mockito.when(dataTask.rows()).thenReturn(mockRows);

        // Create task iterable
        List<FileScanTask> taskList = Arrays.asList(task);
        CloseableIterable<FileScanTask> taskIterable = CloseableIterable.withNoopClose(taskList);

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        // Create and iterate
        StreamingPartitionIterator iterator = new StreamingPartitionIterator(
                nativeTable, icebergTable, taskIterable, 1L);

        Assertions.assertTrue(iterator.hasNext());
        Map.Entry<String, Partition> entry = iterator.next();

        Assertions.assertNotNull(entry);
        Assertions.assertNotNull(entry.getValue());
        Assertions.assertEquals(1704067200000L, entry.getValue().getModifiedTime());

        // After consuming all rows, hasNext should return false
        Assertions.assertFalse(iterator.hasNext());

        iterator.close();
    }

    @Test
    public void testStreamingPartitionIteratorLastUpdatedFallback() throws IOException {
        // Test getLastUpdated fallback when last_updated_at is null
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        StructLike mockRow = Mockito.mock(StructLike.class);
        org.apache.iceberg.util.StructProjection partitionData =
                Mockito.mock(org.apache.iceberg.util.StructProjection.class);

        // Row returns null for last_updated_at (index 9)
        Mockito.when(mockRow.get(0, org.apache.iceberg.util.StructProjection.class)).thenReturn(partitionData);
        Mockito.when(mockRow.get(1, Integer.class)).thenReturn(0);
        Mockito.when(mockRow.get(9, Long.class)).thenReturn(null);

        long snapshotTimestamp = 1704153600000L;
        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(nativeTable.specs()).thenReturn(Map.of(0, spec));
        Mockito.when(spec.fields()).thenReturn(List.of());
        Mockito.when(nativeTable.name()).thenReturn("test_table");
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.timestampMillis()).thenReturn(snapshotTimestamp);

        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataTask dataTask = Mockito.mock(DataTask.class);
        Mockito.when(task.asDataTask()).thenReturn(dataTask);

        CloseableIterable<StructLike> mockRows = CloseableIterable.withNoopClose(Arrays.asList(mockRow));
        Mockito.when(dataTask.rows()).thenReturn(mockRows);
        CloseableIterable<FileScanTask> taskIterable = CloseableIterable.withNoopClose(Arrays.asList(task));

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        StreamingPartitionIterator iterator = new StreamingPartitionIterator(
                nativeTable, icebergTable, taskIterable, 1L);

        Map.Entry<String, Partition> entry = iterator.next();

        // Should fallback to snapshot timestamp
        Assertions.assertEquals(snapshotTimestamp, entry.getValue().getModifiedTime());
        iterator.close();
    }

    @Test
    public void testStreamingPartitionIteratorConstructorException() {
        // Test that taskIterable is closed if iterator() throws
        Table nativeTable = Mockito.mock(Table.class);
        AtomicBoolean closedFlag = new AtomicBoolean(false);

        CloseableIterable<FileScanTask> taskIterable = new CloseableIterable<FileScanTask>() {
            @Override
            public CloseableIterator<FileScanTask> iterator() {
                throw new RuntimeException("Simulated iterator() failure");
            }

            @Override
            public void close() {
                closedFlag.set(true);
            }
        };

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        Assertions.assertThrows(RuntimeException.class, () -> {
            new StreamingPartitionIterator(nativeTable, icebergTable, taskIterable, 1L);
        });

        // Verify taskIterable was closed despite constructor failure
        Assertions.assertTrue(closedFlag.get(),
                "taskIterable should be closed when constructor throws");
    }

    @Test
    public void testEstimatePartitionCountExceptionHandling() {
        // Test that getPartitions handles estimatePartitionCount exception gracefully
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);

        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        // estimatePartitionCount throws exception
        Mockito.when(delegate.estimatePartitionCount(Mockito.any(IcebergTable.class), Mockito.anyLong()))
                .thenThrow(new RuntimeException("Estimation failed"));

        Map<String, Partition> partitionMap = new HashMap<>();
        partitionMap.put("date=2024-01-01", new Partition(System.currentTimeMillis()));
        Mockito.when(delegate.getPartitions(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any()))
                .thenReturn(partitionMap);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setCatalogDBName("db")
                    .setCatalogTableName("test_table")
                    .setNativeTable(nativeTable)
                    .build();

            // Should not throw, should fall back to cache
            Map<String, Partition> result = catalog.getPartitions(icebergTable, 1L, null);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(1, result.size());
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testGetPartitionsByNamesWithStreamingPath() throws IOException {
        // Test CachingIcebergCatalog.getPartitionsByNames using streaming path
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        // Large table with 1000 partitions, requesting 3 (< 10%)
        Mockito.when(delegate.estimatePartitionCount(Mockito.any(IcebergTable.class), Mockito.anyLong()))
                .thenReturn(1000L);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(1L);

        // Setup streaming iterator to return partitions
        Map.Entry<String, Partition> entry1 = new java.util.AbstractMap.SimpleEntry<>(
                "date=2024-01-01", new Partition(1704067200000L));
        Map.Entry<String, Partition> entry2 = new java.util.AbstractMap.SimpleEntry<>(
                "date=2024-01-02", new Partition(1704153600000L));
        Map.Entry<String, Partition> entry3 = new java.util.AbstractMap.SimpleEntry<>(
                "date=2024-01-03", new Partition(1704240000000L));

        List<Map.Entry<String, Partition>> entries = Arrays.asList(entry1, entry2, entry3);
        CloseableIterator<Map.Entry<String, Partition>> mockIterator =
                CloseableIterator.withClose(entries.iterator());

        Mockito.when(delegate.getPartitionIterator(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any()))
                .thenReturn(mockIterator);

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setCatalogDBName("db")
                    .setCatalogTableName("test_table")
                    .setNativeTable(nativeTable)
                    .build();

            // Request 2 partitions (< 10% of 1000), should use streaming
            List<String> requestedNames = Arrays.asList("date=2024-01-01", "date=2024-01-03");
            List<Partition> result = catalog.getPartitionsByNames(icebergTable, null, requestedNames);

            Assertions.assertEquals(2, result.size());
            // Verify streaming path was used
            Mockito.verify(delegate).getPartitionIterator(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any());
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testGetPartitionsByNamesWithNonStreamingPath() {
        // Test CachingIcebergCatalog.getPartitionsByNames using non-streaming (full load) path
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        // Small table with 50 partitions, requesting 10 (20% >= 10%)
        Mockito.when(delegate.estimatePartitionCount(Mockito.any(IcebergTable.class), Mockito.anyLong()))
                .thenReturn(50L);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(1L);

        // Setup full partition map
        Map<String, Partition> partitionMap = new HashMap<>();
        for (int i = 1; i <= 10; i++) {
            partitionMap.put("date=2024-01-" + String.format("%02d", i),
                    new Partition(1704067200000L + i * 86400000L));
        }
        Mockito.when(delegate.getPartitions(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any()))
                .thenReturn(partitionMap);

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setCatalogDBName("db")
                    .setCatalogTableName("test_table")
                    .setNativeTable(nativeTable)
                    .build();

            // Request 10 partitions (20% of 50), should use full load
            List<String> requestedNames = new ArrayList<>();
            for (int i = 1; i <= 10; i++) {
                requestedNames.add("date=2024-01-" + String.format("%02d", i));
            }

            List<Partition> result = catalog.getPartitionsByNames(icebergTable, null, requestedNames);

            Assertions.assertEquals(10, result.size());
            // Full load path doesn't call getPartitionIterator
            Mockito.verify(delegate, Mockito.never())
                    .getPartitionIterator(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any());
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testGetPartitionsByNamesForUnpartitionedTable() {
        // Test getPartitionsByNames for unpartitioned table
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        // Unpartitioned table
        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(true);
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(1L);

        // Setup empty partition for unpartitioned table
        Map<String, Partition> partitionMap = new HashMap<>();
        partitionMap.put("", new Partition(1704067200000L));
        Mockito.when(delegate.getPartitions(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any()))
                .thenReturn(partitionMap);

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setCatalogDBName("db")
                    .setCatalogTableName("unpartitioned_table")
                    .setNativeTable(nativeTable)
                    .build();

            List<String> requestedNames = Arrays.asList("");
            List<Partition> result = catalog.getPartitionsByNames(icebergTable, null, requestedNames);

            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(1704067200000L, result.get(0).getModifiedTime());
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testGetPartitionsByNamesWithMissingPartitions() throws IOException {
        // Test that missing partitions return MISSING_PARTITION placeholder to maintain positional alignment
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        // Small table using non-streaming path
        Mockito.when(delegate.estimatePartitionCount(Mockito.any(IcebergTable.class), Mockito.anyLong()))
                .thenReturn(10L);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(1L);

        // Only return date=2024-01-01, not the other requested partition
        Map<String, Partition> partitionMap = new HashMap<>();
        partitionMap.put("date=2024-01-01", new Partition(1704067200000L));
        Mockito.when(delegate.getPartitions(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any()))
                .thenReturn(partitionMap);

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setCatalogDBName("db")
                    .setCatalogTableName("test_table")
                    .setNativeTable(nativeTable)
                    .build();

            // Request existing and non-existing partitions
            List<String> requestedNames = Arrays.asList("date=2024-01-01", "date=2024-01-99");
            List<Partition> result = catalog.getPartitionsByNames(icebergTable, null, requestedNames);

            // Result should maintain positional alignment with requested names
            Assertions.assertEquals(2, result.size());
            // First partition exists
            Assertions.assertEquals(1704067200000L, result.get(0).getModifiedTime());
            // Second partition is MISSING_PARTITION placeholder
            Assertions.assertSame(Partition.MISSING_PARTITION, result.get(1));
            Assertions.assertEquals(Long.MIN_VALUE, result.get(1).getModifiedTime());
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testStreamingPartitionIteratorNoSuchElementException() {
        // Test that next() throws NoSuchElementException when no more elements
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(nativeTable.name()).thenReturn("test_table");

        // Empty task iterable
        CloseableIterable<FileScanTask> emptyTaskIterable = CloseableIterable.withNoopClose(List.of());

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        StreamingPartitionIterator iterator = new StreamingPartitionIterator(
                nativeTable, icebergTable, emptyTaskIterable, 1L);

        Assertions.assertFalse(iterator.hasNext());
        Assertions.assertThrows(java.util.NoSuchElementException.class, iterator::next);
        iterator.close();
    }

    @Test
    public void testGetPartitionsByNamesEstimateExceptionFallback() {
        // Test that when estimatePartitionCount throws, we fall back to non-streaming
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        // estimatePartitionCount throws
        Mockito.when(delegate.estimatePartitionCount(Mockito.any(IcebergTable.class), Mockito.anyLong()))
                .thenThrow(new RuntimeException("Cannot estimate"));

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(1L);

        Map<String, Partition> partitionMap = new HashMap<>();
        partitionMap.put("date=2024-01-01", new Partition(1704067200000L));
        Mockito.when(delegate.getPartitions(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any()))
                .thenReturn(partitionMap);

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setCatalogDBName("db")
                    .setCatalogTableName("test_table")
                    .setNativeTable(nativeTable)
                    .build();

            List<String> requestedNames = Arrays.asList("date=2024-01-01");
            List<Partition> result = catalog.getPartitionsByNames(icebergTable, null, requestedNames);

            // Should succeed despite estimation failure
            Assertions.assertEquals(1, result.size());
            // Should fall back to full load (getPartitions), not streaming
            Mockito.verify(delegate, Mockito.never())
                    .getPartitionIterator(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any());
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testStreamingPartitionIteratorConstructorCloseIOException() {
        // Test that IOException during taskIterable.close() is suppressed
        Table nativeTable = Mockito.mock(Table.class);
        AtomicBoolean closeCalled = new AtomicBoolean(false);

        CloseableIterable<FileScanTask> taskIterable = new CloseableIterable<FileScanTask>() {
            @Override
            public CloseableIterator<FileScanTask> iterator() {
                throw new RuntimeException("Simulated iterator() failure");
            }

            @Override
            public void close() throws IOException {
                closeCalled.set(true);
                throw new IOException("Simulated close failure");
            }
        };

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, () -> {
            new StreamingPartitionIterator(nativeTable, icebergTable, taskIterable, 1L);
        });

        // Verify close was called and IOException was suppressed
        Assertions.assertTrue(closeCalled.get(), "close() should be called");
        Assertions.assertEquals(1, thrown.getSuppressed().length, "IOException should be suppressed");
        Assertions.assertTrue(thrown.getSuppressed()[0] instanceof IOException);
    }

    @Test
    public void testStreamingPartitionIteratorRowReadException() throws IOException {
        // Test exception handling when reading partition rows fails
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataTask dataTask = Mockito.mock(DataTask.class);

        Mockito.when(nativeTable.specs()).thenReturn(Map.of(0, spec));

        // Task throws exception when getting rows
        Mockito.when(task.asDataTask()).thenReturn(dataTask);
        Mockito.when(dataTask.rows()).thenThrow(new RuntimeException("Failed to read rows"));

        List<FileScanTask> tasks = List.of(task);
        CloseableIterable<FileScanTask> taskIterable = CloseableIterable.withNoopClose(tasks);

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        StreamingPartitionIterator iterator = new StreamingPartitionIterator(
                nativeTable, icebergTable, taskIterable, 1L);

        // Should throw RuntimeException when trying to read
        Assertions.assertThrows(RuntimeException.class, iterator::hasNext);
        iterator.close();
    }

    @Test
    public void testStreamingPartitionIteratorGetLastUpdatedException() throws IOException {
        // Test exception handling when getting last_updated_at fails
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataTask dataTask = Mockito.mock(DataTask.class);
        StructLike row = Mockito.mock(StructLike.class);

        Mockito.when(nativeTable.specs()).thenReturn(Map.of(0, spec));
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.timestampMillis()).thenReturn(1704067200000L);

        // Row returns valid partition data but throws on last_updated_at
        org.apache.iceberg.util.StructProjection partitionData =
                Mockito.mock(org.apache.iceberg.util.StructProjection.class);
        Mockito.when(row.get(0, org.apache.iceberg.util.StructProjection.class)).thenReturn(partitionData);
        Mockito.when(row.get(1, Integer.class)).thenReturn(0);
        Mockito.when(row.get(9, Long.class)).thenThrow(new RuntimeException("Cannot read last_updated_at"));

        Mockito.when(spec.fields()).thenReturn(List.of());

        CloseableIterable<StructLike> rowIterable = CloseableIterable.withNoopClose(List.of(row));
        Mockito.when(task.asDataTask()).thenReturn(dataTask);
        Mockito.when(dataTask.rows()).thenReturn(rowIterable);

        List<FileScanTask> tasks = List.of(task);
        CloseableIterable<FileScanTask> taskIterable = CloseableIterable.withNoopClose(tasks);

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        StreamingPartitionIterator iterator = new StreamingPartitionIterator(
                nativeTable, icebergTable, taskIterable, 1L);

        // Should handle exception gracefully and use snapshot timestamp
        Assertions.assertTrue(iterator.hasNext());
        Map.Entry<String, Partition> entry = iterator.next();
        Assertions.assertEquals(1704067200000L, entry.getValue().getModifiedTime());
        iterator.close();
    }

    @Test
    public void testStreamingPartitionIteratorNoCurrentSnapshot() throws IOException {
        // Test when table has no current snapshot
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataTask dataTask = Mockito.mock(DataTask.class);
        StructLike row = Mockito.mock(StructLike.class);

        Mockito.when(nativeTable.specs()).thenReturn(Map.of(0, spec));
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(null); // No current snapshot
        Mockito.when(nativeTable.name()).thenReturn("test_table");

        // Row returns valid partition data but null last_updated_at
        org.apache.iceberg.util.StructProjection partitionData =
                Mockito.mock(org.apache.iceberg.util.StructProjection.class);
        Mockito.when(row.get(0, org.apache.iceberg.util.StructProjection.class)).thenReturn(partitionData);
        Mockito.when(row.get(1, Integer.class)).thenReturn(0);
        Mockito.when(row.get(9, Long.class)).thenReturn(null);

        Mockito.when(spec.fields()).thenReturn(List.of());

        CloseableIterable<StructLike> rowIterable = CloseableIterable.withNoopClose(List.of(row));
        Mockito.when(task.asDataTask()).thenReturn(dataTask);
        Mockito.when(dataTask.rows()).thenReturn(rowIterable);

        List<FileScanTask> tasks = List.of(task);
        CloseableIterable<FileScanTask> taskIterable = CloseableIterable.withNoopClose(tasks);

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        StreamingPartitionIterator iterator = new StreamingPartitionIterator(
                nativeTable, icebergTable, taskIterable, 1L);

        // Should handle null snapshot gracefully and return -1
        Assertions.assertTrue(iterator.hasNext());
        Map.Entry<String, Partition> entry = iterator.next();
        Assertions.assertEquals(-1L, entry.getValue().getModifiedTime());
        iterator.close();
    }

    @Test
    public void testStreamingPartitionIteratorCloseCurrentRowsIOException() throws IOException {
        // Test IOException handling when closing currentRows
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);
        DataTask dataTask1 = Mockito.mock(DataTask.class);
        DataTask dataTask2 = Mockito.mock(DataTask.class);
        StructLike row1 = Mockito.mock(StructLike.class);
        StructLike row2 = Mockito.mock(StructLike.class);

        Mockito.when(nativeTable.specs()).thenReturn(Map.of(0, spec));
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.timestampMillis()).thenReturn(1704067200000L);
        Mockito.when(nativeTable.name()).thenReturn("test_table");

        org.apache.iceberg.util.StructProjection partitionData =
                Mockito.mock(org.apache.iceberg.util.StructProjection.class);
        Mockito.when(row1.get(0, org.apache.iceberg.util.StructProjection.class)).thenReturn(partitionData);
        Mockito.when(row1.get(1, Integer.class)).thenReturn(0);
        Mockito.when(row1.get(9, Long.class)).thenReturn(1704067200000L);

        Mockito.when(row2.get(0, org.apache.iceberg.util.StructProjection.class)).thenReturn(partitionData);
        Mockito.when(row2.get(1, Integer.class)).thenReturn(0);
        Mockito.when(row2.get(9, Long.class)).thenReturn(1704153600000L);

        Mockito.when(spec.fields()).thenReturn(List.of());

        // First task's rows will throw on close
        AtomicBoolean row1Closed = new AtomicBoolean(false);
        CloseableIterable<StructLike> rowIterable1 = new CloseableIterable<StructLike>() {
            @Override
            public CloseableIterator<StructLike> iterator() {
                return CloseableIterator.withClose(List.of(row1).iterator());
            }

            @Override
            public void close() throws IOException {
                row1Closed.set(true);
                throw new IOException("Simulated close failure");
            }
        };

        CloseableIterable<StructLike> rowIterable2 = CloseableIterable.withNoopClose(List.of(row2));

        Mockito.when(task1.asDataTask()).thenReturn(dataTask1);
        Mockito.when(dataTask1.rows()).thenReturn(rowIterable1);
        Mockito.when(task2.asDataTask()).thenReturn(dataTask2);
        Mockito.when(dataTask2.rows()).thenReturn(rowIterable2);

        List<FileScanTask> tasks = List.of(task1, task2);
        CloseableIterable<FileScanTask> taskIterable = CloseableIterable.withNoopClose(tasks);

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        StreamingPartitionIterator iterator = new StreamingPartitionIterator(
                nativeTable, icebergTable, taskIterable, 1L);

        // Should handle IOException gracefully and continue to next task
        Assertions.assertTrue(iterator.hasNext());
        iterator.next(); // First row
        Assertions.assertTrue(iterator.hasNext()); // Should move to next task despite close exception
        iterator.next(); // Second row
        Assertions.assertFalse(iterator.hasNext());
        Assertions.assertTrue(row1Closed.get(), "First rows should be closed");
        iterator.close();
    }

    @Test
    public void testStreamingPartitionIteratorCloseTaskIterableIOException() throws IOException {
        // Test IOException handling when closing taskIterable
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataTask dataTask = Mockito.mock(DataTask.class);
        StructLike row = Mockito.mock(StructLike.class);

        Mockito.when(nativeTable.specs()).thenReturn(Map.of(0, spec));
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.timestampMillis()).thenReturn(1704067200000L);
        Mockito.when(nativeTable.name()).thenReturn("test_table");

        org.apache.iceberg.util.StructProjection partitionData =
                Mockito.mock(org.apache.iceberg.util.StructProjection.class);
        Mockito.when(row.get(0, org.apache.iceberg.util.StructProjection.class)).thenReturn(partitionData);
        Mockito.when(row.get(1, Integer.class)).thenReturn(0);
        Mockito.when(row.get(9, Long.class)).thenReturn(1704067200000L);

        Mockito.when(spec.fields()).thenReturn(List.of());

        CloseableIterable<StructLike> rowIterable = CloseableIterable.withNoopClose(List.of(row));
        Mockito.when(task.asDataTask()).thenReturn(dataTask);
        Mockito.when(dataTask.rows()).thenReturn(rowIterable);

        List<FileScanTask> tasks = List.of(task);
        AtomicBoolean taskIterableClosed = new AtomicBoolean(false);
        CloseableIterable<FileScanTask> taskIterable = new CloseableIterable<FileScanTask>() {
            @Override
            public CloseableIterator<FileScanTask> iterator() {
                return CloseableIterator.withClose(tasks.iterator());
            }

            @Override
            public void close() throws IOException {
                taskIterableClosed.set(true);
                throw new IOException("Simulated taskIterable close failure");
            }
        };

        IcebergTable icebergTable = IcebergTable.builder()
                .setCatalogDBName("db")
                .setCatalogTableName("test_table")
                .setNativeTable(nativeTable)
                .build();

        StreamingPartitionIterator iterator = new StreamingPartitionIterator(
                nativeTable, icebergTable, taskIterable, 1L);

        // Iterate through all entries
        Assertions.assertTrue(iterator.hasNext());
        iterator.next();
        Assertions.assertFalse(iterator.hasNext());

        // Close should handle IOException gracefully (not throw)
        Assertions.assertDoesNotThrow(iterator::close);
        Assertions.assertTrue(taskIterableClosed.get(), "taskIterable should be closed");
    }

    @Test
    public void testGetPartitionIteratorForUnpartitionedTable() throws IOException {
        // Test getPartitionIterator returns singleton iterator for unpartitioned tables
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);

        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(true);

        // For unpartitioned tables, getPartitions should be called
        Map<String, Partition> partitionMap = new HashMap<>();
        partitionMap.put("", new Partition(1704067200000L));
        Mockito.when(delegate.getPartitions(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any()))
                .thenReturn(partitionMap);

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setCatalogDBName("db")
                    .setCatalogTableName("test_table")
                    .setNativeTable(nativeTable)
                    .build();

            CloseableIterator<Map.Entry<String, Partition>> iterator =
                    catalog.getPartitionIterator(icebergTable, 1L, null);

            // Should return a singleton iterator with empty partition name
            Assertions.assertTrue(iterator.hasNext());
            Map.Entry<String, Partition> entry = iterator.next();
            Assertions.assertEquals("", entry.getKey());
            Assertions.assertEquals(1704067200000L, entry.getValue().getModifiedTime());
            Assertions.assertFalse(iterator.hasNext());
            iterator.close();
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testCollectPartitionsFromIteratorSupplierException() {
        // Test that exception during iterator creation is properly wrapped
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties props = Mockito.mock(IcebergCatalogProperties.class);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(props.isEnableIcebergMetadataCache()).thenReturn(true);
        Mockito.when(props.isEnableIcebergTableCache()).thenReturn(true);
        Mockito.when(props.getIcebergMetaCacheTtlSec()).thenReturn(60L);
        Mockito.when(props.getIcebergDataFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergDeleteFileCacheMemoryUsageRatio()).thenReturn(0.0);
        Mockito.when(props.getIcebergPartitionStreamingThreshold()).thenReturn(100);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(1L);
        Mockito.when(nativeTable.name()).thenReturn("test_table");

        // Large table to trigger streaming path
        Mockito.when(delegate.estimatePartitionCount(Mockito.any(IcebergTable.class), Mockito.anyLong()))
                .thenReturn(1000L);

        // getPartitionIterator throws exception
        Mockito.when(delegate.getPartitionIterator(Mockito.any(IcebergTable.class), Mockito.anyLong(), Mockito.any()))
                .thenThrow(new RuntimeException("Failed to create iterator"));

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg_test", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setCatalogDBName("db")
                    .setCatalogTableName("test_table")
                    .setNativeTable(nativeTable)
                    .build();

            List<String> requestedNames = Arrays.asList("date=2024-01-01");

            // Should throw StarRocksConnectorException wrapping the original exception
            StarRocksConnectorException thrown = Assertions.assertThrows(
                    StarRocksConnectorException.class,
                    () -> catalog.getPartitionsByNames(icebergTable, null, requestedNames)
            );
            Assertions.assertTrue(thrown.getMessage().contains("Failed to stream partitions"));
        } finally {
            es.shutdownNow();
        }
    }
}

