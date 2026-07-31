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

package com.starrocks.connector.starrocks;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.StarRocksExternalTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StarRocksMetadataCacheTest {
    @Test
    public void testMetadataListDbNamesUsesCacheAndFiltersInternalDatabases() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        when(feClient.listDbNames()).thenReturn(ImmutableList.of("db1", "_statistics_", "sys", "information_schema"));

        StarRocksMetadata metadata = new StarRocksMetadata("sr_catalog", feClient);

        Assertions.assertEquals(ImmutableList.of("db1"), metadata.listDbNames(null));
        Assertions.assertEquals(ImmutableList.of("db1"), metadata.listDbNames(null));
        verify(feClient, times(1)).listDbNames();
    }

    @Test
    public void testMetadataGetTableUsesCache() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        when(feClient.getTable("db1", "tbl1")).thenReturn(tableInfo("db1", "tbl1", 7, columnInfo("k1", "int", false)));
        StarRocksMetadata metadata = new StarRocksMetadata("sr_catalog", feClient);

        StarRocksExternalTable first = (StarRocksExternalTable) metadata.getTable(null, "db1", "tbl1");
        StarRocksExternalTable second = (StarRocksExternalTable) metadata.getTable(null, "db1", "tbl1");

        Assertions.assertNotNull(first);
        Assertions.assertNotNull(second);
        Assertions.assertEquals(7L, first.getSchemaVersion());
        Assertions.assertEquals(7L, second.getSchemaVersion());
        verify(feClient, times(1)).getTable("db1", "tbl1");
    }

    @Test
    public void testMetadataRefreshTableReloadsCachedSchema() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        when(feClient.getTable("db1", "tbl1"))
                .thenReturn(tableInfo("db1", "tbl1", 7, columnInfo("k1", "int", false)))
                .thenReturn(tableInfo("db1", "tbl1", 8,
                        columnInfo("k1", "int", false), columnInfo("k2", "varchar(10)", true)));
        StarRocksMetadata metadata = new StarRocksMetadata("sr_catalog", feClient);

        StarRocksExternalTable first = (StarRocksExternalTable) metadata.getTable(null, "db1", "tbl1");
        metadata.refreshTable("db1", first, ImmutableList.of(), true);
        StarRocksExternalTable refreshed = (StarRocksExternalTable) metadata.getTable(null, "db1", "tbl1");

        Assertions.assertEquals(7L, first.getSchemaVersion());
        Assertions.assertEquals(8L, refreshed.getSchemaVersion());
        Assertions.assertEquals(2, refreshed.getFullSchema().size());
        verify(feClient, times(2)).getTable("db1", "tbl1");
    }

    @Test
    public void testMetadataWithoutCacheGoesDirectEveryTime() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        when(feClient.listDbNames()).thenReturn(ImmutableList.of("db1", "sys"));
        when(feClient.listTableNames("db1")).thenReturn(ImmutableList.of("tbl1"));
        when(feClient.getTable("db1", "tbl1")).thenReturn(tableInfo("db1", "tbl1", 7, columnInfo("k1", "int", false)));

        StarRocksMetadata metadata = new StarRocksMetadata("sr_catalog", feClient, null);

        Assertions.assertEquals(ImmutableList.of("db1"), metadata.listDbNames(null));
        Assertions.assertEquals(ImmutableList.of("db1"), metadata.listDbNames(null));
        Assertions.assertEquals(ImmutableList.of("tbl1"), metadata.listTableNames(null, "db1"));
        StarRocksExternalTable table = (StarRocksExternalTable) metadata.getTable(null, "db1", "tbl1");
        Assertions.assertNotNull(table);
        Assertions.assertNotNull(metadata.getTable(null, "db1", "tbl1"));
        // Nothing is cached: every access hits the client.
        verify(feClient, times(2)).listDbNames();
        verify(feClient, times(2)).getTable("db1", "tbl1");

        // refreshTable is a no-op without a cache — no extra remote fetch.
        metadata.refreshTable("db1", table, ImmutableList.of(), true);
        verify(feClient, times(2)).getTable("db1", "tbl1");
    }

    @Test
    public void testTableCacheReturnsCachedTableAndAvoidsDuplicateGetTableRpc() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        when(feClient.getTable("db1", "tbl1")).thenReturn(tableInfo("db1", "tbl1", 7, columnInfo("k1", "int", false)));

        StarRocksMetadataCache cache = newCache(feClient);

        StarRocksRemoteScanWire.Table first = cache.getTable("db1", "tbl1");
        StarRocksRemoteScanWire.Table second = cache.getTable("db1", "tbl1");

        Assertions.assertSame(first, second);
        Assertions.assertEquals("k1", second.columns.get(0).name);
        Assertions.assertEquals(7L, second.schemaVersion);
        verify(feClient, times(1)).getTable("db1", "tbl1");
    }

    @Test
    public void testRefreshTableReloadsSchemaAndInvalidatesTableNames() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        when(feClient.getTable("db1", "tbl1"))
                .thenReturn(tableInfo("db1", "tbl1", 7, columnInfo("k1", "int", false)))
                .thenReturn(tableInfo("db1", "tbl1", 8,
                        columnInfo("k1", "int", false), columnInfo("k2", "varchar(10)", true)));
        when(feClient.listTableNames("db1"))
                .thenReturn(ImmutableList.of("tbl1"))
                .thenReturn(ImmutableList.of("tbl1", "tbl2"));

        StarRocksMetadataCache cache = newCache(feClient);

        Assertions.assertEquals(1, cache.getTable("db1", "tbl1").columns.size());
        Assertions.assertEquals(ImmutableList.of("tbl1"), cache.getTableNames("db1"));

        cache.refreshTable("db1", "tbl1");

        StarRocksRemoteScanWire.Table refreshed = cache.getTable("db1", "tbl1");
        Assertions.assertEquals(8L, refreshed.schemaVersion);
        Assertions.assertEquals(2, refreshed.columns.size());
        Assertions.assertEquals(ImmutableList.of("tbl1", "tbl2"), cache.getTableNames("db1"));
        verify(feClient, times(2)).getTable("db1", "tbl1");
        verify(feClient, times(2)).listTableNames("db1");
    }

    @Test
    public void testRefreshTableDoesNotHideDroppedRemoteTable() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        when(feClient.getTable("db1", "tbl1"))
                .thenReturn(tableInfo("db1", "tbl1", 7, columnInfo("k1", "int", false)))
                .thenReturn(null)
                .thenReturn(null);

        StarRocksMetadataCache cache = newCache(feClient);

        Assertions.assertNotNull(cache.getTable("db1", "tbl1"));
        cache.refreshTable("db1", "tbl1");
        Assertions.assertNull(cache.getTable("db1", "tbl1"));
        verify(feClient, times(3)).getTable("db1", "tbl1");
    }

    @Test
    public void testStatsSnapshotCacheLoadsOnceAndInvalidatesWithTable() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        AtomicInteger loads = new AtomicInteger();
        StarRocksMetadataCache cache = new StarRocksMetadataCache(feClient, defaultOptions(),
                (dbName, tableName, cachedEpochs) -> {
                    loads.incrementAndGet();
                    StarRocksRemoteTableStats.Snapshot snapshot = new StarRocksRemoteTableStats.Snapshot();
                    snapshot.status = 200;
                    snapshot.epochs = new StarRocksRemoteTableStats.Epochs("l" + loads.get(), "d", "a");
                    snapshot.tableRowCount = 42;
                    return snapshot;
                }, null);

        StarRocksRemoteTableStats.Snapshot first = cache.getTableStatsSnapshot("db1", "tbl1");
        StarRocksRemoteTableStats.Snapshot second = cache.getTableStatsSnapshot("db1", "tbl1");
        Assertions.assertSame(first, second);
        Assertions.assertEquals(42L, first.tableRowCount);
        Assertions.assertEquals(1, loads.get());

        cache.invalidateTable("db1", "tbl1");
        StarRocksRemoteTableStats.Snapshot reloaded = cache.getTableStatsSnapshot("db1", "tbl1");
        Assertions.assertEquals("l2", reloaded.epochs.list);
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    public void testStatsSnapshotUnavailableRemoteIsNegativeCachedUntilInvalidate() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        AtomicInteger loads = new AtomicInteger();
        StarRocksMetadataCache cache = new StarRocksMetadataCache(feClient, defaultOptions(),
                (dbName, tableName, cachedEpochs) -> {
                    loads.incrementAndGet();
                    return null;
                }, null);

        Assertions.assertNull(cache.getTableStatsSnapshot("db1", "tbl1"));
        Assertions.assertNull(cache.getTableStatsSnapshot("db1", "tbl1"));
        // Old remotes without the endpoint must not be hammered on every access.
        Assertions.assertEquals(1, loads.get());
    }

    @Test
    public void testPartitionColumnStatsBatchLoadAndCacheHit() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        AtomicInteger loads = new AtomicInteger();
        StarRocksMetadataCache cache = new StarRocksMetadataCache(feClient, defaultOptions(), null,
                (dbName, tableName, partitionIds, columns) -> {
                    loads.incrementAndGet();
                    StarRocksRemoteTableStats.PartitionStatsResponse response =
                            new StarRocksRemoteTableStats.PartitionStatsResponse();
                    response.status = 200;
                    response.partitionStats = new ArrayList<>();
                    for (Long partitionId : partitionIds) {
                        for (String column : columns) {
                            StarRocksRemoteTableStats.PartitionColumnStats stats =
                                    new StarRocksRemoteTableStats.PartitionColumnStats();
                            stats.partitionId = partitionId;
                            stats.column = column;
                            stats.ndv = 7;
                            stats.rowCount = 100;
                            response.partitionStats.add(stats);
                        }
                    }
                    return response;
                });

        Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> first =
                cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L, 2L), ImmutableList.of("c1"));
        Assertions.assertEquals(2, first.size());
        Assertions.assertEquals(7, first.get(1L).get("c1").ndv);
        Assertions.assertEquals(1, loads.get());

        // Second read for the same partitions/columns is served from cache.
        Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> second =
                cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L, 2L), ImmutableList.of("c1"));
        Assertions.assertEquals(2, second.size());
        Assertions.assertEquals(1, loads.get());

        // A superset of partitions only loads the missing one.
        cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L, 2L, 3L), ImmutableList.of("c1"));
        Assertions.assertEquals(2, loads.get());

        cache.invalidateTable("db1", "tbl1");
        cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L), ImmutableList.of("c1"));
        Assertions.assertEquals(3, loads.get());
    }

    @Test
    public void testPartitionColumnStatsHitIsCaseInsensitive() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        AtomicInteger loads = new AtomicInteger();
        // The remote reports its own column case ("C1"); local schema objects use "c1".
        StarRocksMetadataCache cache = new StarRocksMetadataCache(feClient, defaultOptions(), null,
                (dbName, tableName, partitionIds, columns) -> {
                    loads.incrementAndGet();
                    return partitionStatsResponse(partitionIds, "C1");
                });

        Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> first =
                cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L), ImmutableList.of("c1"));
        Assertions.assertEquals(1, loads.get());
        // Result maps are keyed by lowercase column name.
        Assertions.assertEquals(7, first.get(1L).get("c1").ndv);

        // Same partition again, any case: served from cache — the case difference must
        // not degrade every lookup into a remote batch call.
        cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L), ImmutableList.of("c1"));
        cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L), ImmutableList.of("C1"));
        Assertions.assertEquals(1, loads.get());
    }

    @Test
    public void testMergeSnapshotEpochGatedFieldMerge() {
        StarRocksRemoteTableStats.Snapshot old = snapshot("l1", "d1", "a1");
        old.tableRowCount = 10;
        old.partitions = ImmutableList.of(partitionMeta(1L));
        old.analyzeType = "FULL";
        old.columnStats = ImmutableList.of(columnStats("c1"));

        // No old snapshot: the fresh one is taken as-is.
        StarRocksRemoteTableStats.Snapshot freshOnly = snapshot("l1", "d1", "a1");
        Assertions.assertSame(freshOnly, StarRocksMetadataCache.mergeSnapshot(null, freshOnly));

        // list/data moved, analyze unchanged: partitions + row count re-shipped,
        // column stats kept from the previous generation.
        StarRocksRemoteTableStats.Snapshot listMoved = snapshot("l2", "d1", "a1");
        listMoved.tableRowCount = 20;
        listMoved.partitions = ImmutableList.of(partitionMeta(1L), partitionMeta(2L));
        StarRocksRemoteTableStats.Snapshot merged = StarRocksMetadataCache.mergeSnapshot(old, listMoved);
        Assertions.assertEquals(20, merged.tableRowCount);
        Assertions.assertEquals(2, merged.partitions.size());
        Assertions.assertEquals("FULL", merged.analyzeType);
        Assertions.assertSame(old.columnStats, merged.columnStats);
        Assertions.assertSame(listMoved.epochs, merged.epochs);

        // analyze moved, list/data unchanged: column stats re-shipped, partitions kept.
        StarRocksRemoteTableStats.Snapshot analyzeMoved = snapshot("l1", "d1", "a2");
        analyzeMoved.analyzeType = "SAMPLE";
        analyzeMoved.columnStats = ImmutableList.of(columnStats("c1"), columnStats("c2"));
        merged = StarRocksMetadataCache.mergeSnapshot(old, analyzeMoved);
        Assertions.assertSame(old.partitions, merged.partitions);
        Assertions.assertEquals(10, merged.tableRowCount);
        Assertions.assertEquals("SAMPLE", merged.analyzeType);
        Assertions.assertEquals(2, merged.columnStats.size());

        // Everything moved: the fresh snapshot is taken as-is.
        StarRocksRemoteTableStats.Snapshot allMoved = snapshot("l2", "d2", "a2");
        Assertions.assertSame(allMoved, StarRocksMetadataCache.mergeSnapshot(old, allMoved));
    }

    @Test
    public void testFirstSnapshotLoadCascadesExistingPartitionEntries() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        AtomicInteger partitionLoads = new AtomicInteger();
        List<List<String>> requestedColumns = new ArrayList<>();
        StarRocksMetadataCache cache = new StarRocksMetadataCache(feClient, defaultOptions(),
                (dbName, tableName, cachedEpochs) -> {
                    StarRocksRemoteTableStats.Snapshot snapshot = snapshot("l1", "d1", "a1");
                    snapshot.partitions = ImmutableList.of(partitionMeta(1L));
                    return snapshot;
                },
                (dbName, tableName, partitionIds, columns) -> {
                    partitionLoads.incrementAndGet();
                    requestedColumns.add(new ArrayList<>(columns));
                    return partitionStatsResponse(partitionIds, "C1");
                },
                Runnable::run);

        cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L), ImmutableList.of("c1"));
        Assertions.assertEquals(1, partitionLoads.get());

        // First snapshot load (nothing to compare against, e.g. after eviction):
        // the cascade re-pulls the surviving cached entries.
        Assertions.assertNotNull(cache.getTableStatsSnapshot("db1", "tbl1"));
        Assertions.assertEquals(2, partitionLoads.get());
        // The cascade re-requests under the remote's own column case.
        Assertions.assertEquals(ImmutableList.of("C1"), requestedColumns.get(1));

        // The refreshed entry still serves hits.
        cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L), ImmutableList.of("c1"));
        Assertions.assertEquals(2, partitionLoads.get());
    }

    @Test
    public void testSnapshotCascadeDropsDeadPartitionsWithoutReload() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        AtomicInteger partitionLoads = new AtomicInteger();
        StarRocksMetadataCache cache = new StarRocksMetadataCache(feClient, defaultOptions(),
                (dbName, tableName, cachedEpochs) -> {
                    StarRocksRemoteTableStats.Snapshot snapshot = snapshot("l1", "d1", "a1");
                    // Partition 1 no longer exists on the remote.
                    snapshot.partitions = ImmutableList.of(partitionMeta(2L));
                    return snapshot;
                },
                (dbName, tableName, partitionIds, columns) -> {
                    partitionLoads.incrementAndGet();
                    return partitionStatsResponse(partitionIds, "c1");
                },
                Runnable::run);

        cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L), ImmutableList.of("c1"));
        Assertions.assertEquals(1, partitionLoads.get());

        // The cascade invalidates the dead partition's entry and, with nothing left
        // to refresh, issues no batch call.
        Assertions.assertNotNull(cache.getTableStatsSnapshot("db1", "tbl1"));
        Assertions.assertEquals(1, partitionLoads.get());

        // The dropped entry is gone: the next query re-loads it.
        cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L), ImmutableList.of("c1"));
        Assertions.assertEquals(2, partitionLoads.get());
    }

    @Test
    public void testAnalyzeEpochChangeCascadesOnReload() throws Exception {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        AtomicInteger snapshotLoads = new AtomicInteger();
        AtomicInteger partitionLoads = new AtomicInteger();
        // refreshSec=1 plus a direct executor makes the refreshAfterWrite reload run
        // inline on the accessing thread — deterministic without a real pool.
        StarRocksMetadataCache cache = new StarRocksMetadataCache(feClient,
                new StarRocksMetadataCache.Options(3600, 1, 1000, 100000),
                (dbName, tableName, cachedEpochs) -> {
                    StarRocksRemoteTableStats.Snapshot snapshot =
                            snapshot("l1", "d1", "a" + snapshotLoads.incrementAndGet());
                    snapshot.partitions = ImmutableList.of(partitionMeta(1L));
                    return snapshot;
                },
                (dbName, tableName, partitionIds, columns) -> {
                    partitionLoads.incrementAndGet();
                    return partitionStatsResponse(partitionIds, "c1");
                },
                Runnable::run);

        Assertions.assertNotNull(cache.getTableStatsSnapshot("db1", "tbl1"));
        cache.getPartitionColumnStats("db1", "tbl1", ImmutableList.of(1L), ImmutableList.of("c1"));
        Assertions.assertEquals(1, partitionLoads.get());

        Thread.sleep(1100);
        // Access triggers the reload; the analyze epoch moved (a1 -> a2) so the
        // cascade re-pulls the cached partition entry.
        Assertions.assertNotNull(cache.getTableStatsSnapshot("db1", "tbl1"));
        Assertions.assertEquals(2, snapshotLoads.get());
        Assertions.assertEquals(2, partitionLoads.get());
    }

    private static StarRocksRemoteTableStats.Snapshot snapshot(String list, String data, String analyze) {
        StarRocksRemoteTableStats.Snapshot snapshot = new StarRocksRemoteTableStats.Snapshot();
        snapshot.status = 200;
        snapshot.epochs = new StarRocksRemoteTableStats.Epochs(list, data, analyze);
        return snapshot;
    }

    private static StarRocksRemoteTableStats.PartitionMeta partitionMeta(long id) {
        StarRocksRemoteTableStats.PartitionMeta partition = new StarRocksRemoteTableStats.PartitionMeta();
        partition.id = id;
        partition.name = "p" + id;
        return partition;
    }

    private static StarRocksRemoteTableStats.ColumnStats columnStats(String column) {
        StarRocksRemoteTableStats.ColumnStats stats = new StarRocksRemoteTableStats.ColumnStats();
        stats.column = column;
        stats.rowCount = 100;
        return stats;
    }

    private static StarRocksRemoteTableStats.PartitionStatsResponse partitionStatsResponse(
            List<Long> partitionIds, String column) {
        StarRocksRemoteTableStats.PartitionStatsResponse response =
                new StarRocksRemoteTableStats.PartitionStatsResponse();
        response.status = 200;
        response.partitionStats = new ArrayList<>();
        for (Long partitionId : partitionIds) {
            StarRocksRemoteTableStats.PartitionColumnStats stats =
                    new StarRocksRemoteTableStats.PartitionColumnStats();
            stats.partitionId = partitionId;
            stats.column = column;
            stats.ndv = 7;
            stats.rowCount = 100;
            response.partitionStats.add(stats);
        }
        return response;
    }

    private static StarRocksMetadataCache newCache(StarRocksFeClient feClient) {
        return new StarRocksMetadataCache(feClient, defaultOptions(), null, null);
    }

    private static StarRocksMetadataCache.Options defaultOptions() {
        return new StarRocksMetadataCache.Options(3600, 300, 1000, 100000);
    }

    private static StarRocksRemoteScanWire.Table tableInfo(String dbName, String tableName, long schemaVersion,
                                                           StarRocksRemoteScanWire.Column... columns) {
        StarRocksRemoteScanWire.Table table = new StarRocksRemoteScanWire.Table();
        table.db = dbName;
        table.table = tableName;
        table.schemaVersion = schemaVersion;
        table.columns = ImmutableList.copyOf(columns);
        return table;
    }

    private static StarRocksRemoteScanWire.Column columnInfo(String name, String type, boolean nullable) {
        StarRocksRemoteScanWire.Column column = new StarRocksRemoteScanWire.Column();
        column.name = name;
        column.type = type;
        column.nullable = nullable;
        return column;
    }
}
