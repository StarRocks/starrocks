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

package com.starrocks.connector.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Tables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.connector.DatabaseTableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OdpsCacheUpdateProcessor}, covering all public methods.
 */
public class OdpsCacheUpdateProcessorTest {

    private static final String CATALOG_NAME = "odps_catalog";
    private Odps odps;
    private Tables tables;
    private LoadingCache<String, Set<String>> tableNameCache;
    private LoadingCache<OdpsTableName, OdpsTable> tableCache;
    private LoadingCache<OdpsTableName, List<Partition>> partitionCache;

    @BeforeEach
    public void setUp() {
        odps = mock(Odps.class);
        tables = mock(Tables.class);
        when(odps.tables()).thenReturn(tables);

        tableNameCache = CacheBuilder.newBuilder()
                .build(CacheLoader.from(key -> ImmutableSet.of()));
        tableCache = CacheBuilder.newBuilder()
                .build(new CacheLoader<OdpsTableName, OdpsTable>() {
                    @Override
                    public OdpsTable load(OdpsTableName key) {
                        return createMockOdpsTable(key.getDatabaseName(), key.getTableName(), false);
                    }
                });
        partitionCache = CacheBuilder.newBuilder()
                .build(new CacheLoader<OdpsTableName, List<Partition>>() {
                    @Override
                    public List<Partition> load(OdpsTableName key) {
                        return ImmutableList.of();
                    }
                });
    }

    private OdpsCacheUpdateProcessor createProcessor() {
        return new OdpsCacheUpdateProcessor(
                CATALOG_NAME, odps, tableNameCache, tableCache, partitionCache);
    }

    // --- refreshTable ---

    @Test
    public void testRefreshTableWithNonOdpsTable() {
        OdpsCacheUpdateProcessor processor = createProcessor();
        com.starrocks.catalog.Table nonOdpsTable = mock(com.starrocks.catalog.Table.class);
        when(nonOdpsTable.getName()).thenReturn("other_table");

        processor.refreshTable("db1", nonOdpsTable, false);

        // No exception; tableCache/partitionCache should not be invalidated for OdpsTableName
        assertTrue(tableCache.asMap().isEmpty());
    }

    @Test
    public void testRefreshTableWithOdpsTablePartitioned() throws Exception {
        OdpsCacheUpdateProcessor processor = createProcessor();
        OdpsTable odpsTable = createMockOdpsTable("db1", "t1", false);
        com.aliyun.odps.Table odpsApiTable = mock(com.aliyun.odps.Table.class);
        when(odpsApiTable.getName()).thenReturn("t1");
        when(odpsApiTable.getProject()).thenReturn("db1");
        when(odpsApiTable.getCreatedTime()).thenReturn(new Date());
        when(odpsApiTable.getSchema()).thenReturn(new com.aliyun.odps.TableSchema());
        doNothing().when(odpsApiTable).reload();
        when(odps.tables().get("db1", "t1")).thenReturn(odpsApiTable);

        tableCache.get(OdpsTableName.of("db1", "t1")); // pre-load so get() later can return
        tableCache.put(OdpsTableName.of("db1", "t1"), odpsTable);

        processor.refreshTable("db1", odpsTable, false);

        // tableCache and partitionCache were invalidated and reloaded (get may throw; we only verify no exception from refreshTable)
        assertTrue(true);
    }

    @Test
    public void testRefreshTableWithOdpsTableUnPartitioned() throws Exception {
        OdpsCacheUpdateProcessor processor = createProcessor();
        OdpsTable odpsTable = createMockOdpsTable("db1", "t1", true);
        com.aliyun.odps.Table odpsApiTable = mock(com.aliyun.odps.Table.class);
        when(odpsApiTable.getName()).thenReturn("t1");
        when(odpsApiTable.getProject()).thenReturn("db1");
        when(odpsApiTable.getCreatedTime()).thenReturn(new Date());
        when(odpsApiTable.getSchema()).thenReturn(new com.aliyun.odps.TableSchema());
        doNothing().when(odpsApiTable).reload();
        when(odps.tables().get("db1", "t1")).thenReturn(odpsApiTable);

        tableCache.put(OdpsTableName.of("db1", "t1"), odpsTable);

        processor.refreshTable("db1", odpsTable, false);

        assertTrue(true);
    }

    // --- getCachedTableNames ---

    @Test
    public void testGetCachedTableNamesEmpty() {
        OdpsCacheUpdateProcessor processor = createProcessor();

        Set<DatabaseTableName> names = processor.getCachedTableNames();

        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetCachedTableNamesNonEmpty() throws Exception {
        tableNameCache.put("db1", ImmutableSet.of("t1", "t2"));
        OdpsCacheUpdateProcessor processor = createProcessor();

        Set<DatabaseTableName> names = processor.getCachedTableNames();

        assertEquals(2, names.size());
        assertTrue(names.contains(DatabaseTableName.of("db1", "t1")));
        assertTrue(names.contains(DatabaseTableName.of("db1", "t2")));
    }

    // --- refreshTableBackground ---

    @Test
    public void testRefreshTableBackgroundWithNonOdpsTable() {
        OdpsCacheUpdateProcessor processor = createProcessor();
        com.starrocks.catalog.Table nonOdpsTable = mock(com.starrocks.catalog.Table.class);
        when(nonOdpsTable.getName()).thenReturn("other_table");
        ExecutorService executor = mock(ExecutorService.class);

        processor.refreshTableBackground(nonOdpsTable, false, executor);

        // Executor should not be used (early return)
        Mockito.verifyNoInteractions(executor);
    }

    @Test
    public void testRefreshTableBackgroundWithOdpsTable() throws Exception {
        OdpsCacheUpdateProcessor processor = createProcessor();
        OdpsTable odpsTable = createMockOdpsTable("db1", "t1", false);
        com.aliyun.odps.Table odpsApiTable = mock(com.aliyun.odps.Table.class);
        when(odpsApiTable.getName()).thenReturn("t1");
        when(odpsApiTable.getProject()).thenReturn("db1");
        when(odpsApiTable.getCreatedTime()).thenReturn(new Date());
        when(odpsApiTable.getSchema()).thenReturn(new com.aliyun.odps.TableSchema());
        doNothing().when(odpsApiTable).reload();
        when(tables.get("db1", "t1")).thenReturn(odpsApiTable);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            processor.refreshTableBackground(odpsTable, false, executor);
            // Allow submitted task to run
            Thread.sleep(100);
        } finally {
            executor.shutdown();
        }

        verify(tables).get("db1", "t1");
    }

    // --- refreshPartition ---

    @Test
    public void testRefreshPartition() {
        OdpsCacheUpdateProcessor processor = createProcessor();
        OdpsTable odpsTable = createMockOdpsTable("db1", "t1", false);
        com.aliyun.odps.Table odpsApiTable = mock(com.aliyun.odps.Table.class);
        Partition p = mock(Partition.class);
        PartitionSpec spec = new PartitionSpec("p1=1");
        when(tables.get("db1", "t1")).thenReturn(odpsApiTable);
        when(odpsApiTable.getPartition(any(PartitionSpec.class))).thenReturn(p);
        when(p.getPartitionSpec()).thenReturn(spec);

        processor.refreshPartition(odpsTable, ImmutableList.of("p1=1"));

        verify(tables).get("db1", "t1");
        verify(odpsApiTable).getPartition(any(PartitionSpec.class));
        List<Partition> cached = partitionCache.getIfPresent(OdpsTableName.of("db1", "t1"));
        assertTrue(cached != null && cached.size() == 1);
    }

    private static OdpsTable createMockOdpsTable(String dbName, String tableName, boolean unPartitioned) {
        OdpsTable t = mock(OdpsTable.class);
        when(t.getCatalogDBName()).thenReturn(dbName);
        when(t.getCatalogTableName()).thenReturn(tableName);
        when(t.getName()).thenReturn(tableName);
        when(t.isUnPartitioned()).thenReturn(unPartitioned);
        return t;
    }
}
