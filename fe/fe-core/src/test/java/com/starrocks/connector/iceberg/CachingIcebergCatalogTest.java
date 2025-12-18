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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
}

