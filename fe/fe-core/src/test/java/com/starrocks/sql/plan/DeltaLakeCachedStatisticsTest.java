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

package com.starrocks.sql.plan;

import com.google.common.cache.CacheBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.delta.DeltaLakeCatalogProperties;
import com.starrocks.connector.delta.DeltaLakeEngine;
import com.starrocks.connector.delta.DeltaLakeMetadata;
import com.starrocks.connector.delta.DeltaLakeSnapshotStatisticsCache;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import io.delta.kernel.internal.SnapshotImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeltaLakeCachedStatisticsTest {
    private static final String CATALOG = "delta";
    @TempDir
    public Path tablePath;
    private final DeltaLakeSnapshotStatisticsCache cache = new DeltaLakeSnapshotStatisticsCache();
    private ConnectContext context;
    private DeltaLakeTable table;
    private String metadataId = "limit-test";

    @BeforeEach
    public void setup() throws Exception {
        context = new ConnectContext();
        context.setThreadLocalInfo();
        context.getSessionVariable().setEnableDeltaLakeCachedStatistics(true);
        Tracers.register(context);
        Tracers.init(Tracers.Mode.VARS, Tracers.Module.EXTERNAL, true, false);
        table = createTable(20, false);
    }

    @AfterEach
    public void cleanup() {
        Tracers.close();
        ConnectContext.remove();
    }

    private DeltaLakeMetadata metadata() {
        return new DeltaLakeMetadata(new HdfsEnvironment(Map.of()), CATALOG, null, null,
                new ConnectorProperties(ConnectorType.DELTALAKE), cache);
    }

    private Statistics statistics(DeltaLakeMetadata metadata) {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        return metadata.getTableStatistics(OptimizerFactory.initContext(context, new ColumnRefFactory()), table,
                Map.of(column, table.getColumn("id")), List.of(), null, -1, TvrTableSnapshot.empty());
    }

    private RemoteFileInfoSource source(DeltaLakeMetadata metadata) {
        return metadata.getRemoteFilesAsync(table, GetRemoteFilesParams.newBuilder().build());
    }

    private DeltaLakeTable createTable(int fileCount, boolean deletionVectors) throws Exception {
        Path logDir = Files.createDirectories(tablePath.resolve("_delta_log"));
        String schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\","
                + "\"nullable\":true,\"metadata\":{}}]}";
        StringBuilder log = new StringBuilder();
        if (deletionVectors) {
            log.append("{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,"
                    + "\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"deletionVectors\"]}}\n");
        } else {
            log.append("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n");
        }
        log.append(GsonUtils.GSON.toJson(Map.of("metaData", Map.of(
                "id", metadataId, "format", Map.of("provider", "parquet", "options", Map.of()),
                "schemaString", schema, "partitionColumns", List.of(), "configuration", Map.of())))).append('\n');
        for (int i = 0; i < fileCount; i++) {
            // Metadata row counts must not cap execution: include empty files as well as non-empty ones.
            String stats = GsonUtils.GSON.toJson(Map.of("numRecords", i % 2 == 0 ? 0 : 100));
            Map<String, Object> add = new HashMap<>(Map.of(
                    "path", "part-" + i + ".parquet", "partitionValues", Map.of(), "size", 1000,
                    "modificationTime", 0, "dataChange", true, "stats", stats));
            if (deletionVectors && i == 1) {
                add.put("deletionVector", Map.of("storageType", "p", "pathOrInlineDv", "file:///test/dv.bin",
                        "offset", 0, "sizeInBytes", 10, "cardinality", 100));
            }
            log.append(GsonUtils.GSON.toJson(Map.of("add", add))).append('\n');
        }
        Files.writeString(logDir.resolve("00000000000000000000.json"), log);
        DeltaLakeEngine engine = DeltaLakeEngine.create(new Configuration(), new DeltaLakeCatalogProperties(Map.of()),
                CacheBuilder.newBuilder().build(), CacheBuilder.newBuilder().build());
        SnapshotImpl snapshot = (SnapshotImpl) io.delta.kernel.Table.forPath(engine, tablePath.toUri().toString())
                .getLatestSnapshot(engine);
        DeltaLakeTable table = new DeltaLakeTable(123456, CATALOG, "deltalake_db", "tbl",
                List.of(new Column("id", IntegerType.INT)), List.of(), snapshot, engine,
                new MetastoreTable("deltalake_db", "tbl", tablePath.toUri().toString(), 0));
        return table;
    }

    @Test
    public void testColdAndWarmQueriesDoNotPopulateFileTasks() throws Exception {
        DeltaLakeMetadata first = metadata();
        assertEquals(Config.default_statistics_output_row_count, statistics(first).getOutputRowCount());
        assertTrue(statistics(first).isTableRowCountMayInaccurate());
        assertNull(cache.getRowCount(table));
        assertTrue(Tracers.printVars().contains("default"));
        try (RemoteFileInfoSource source = source(first)) {
            assertEquals(20, source.getAllOutputs().size());
        }
        assertEquals(1000L, cache.getRowCount(table));
        // A fresh query metadata instance shares the catalog cache, but keeps its scan iterator independent.
        DeltaLakeMetadata next = metadata();
        assertEquals(1000, statistics(next).getOutputRowCount());
        assertTrue(Tracers.printVars().contains("snapshot_cache"));
        try (RemoteFileInfoSource source = source(next)) {
            assertEquals(20, source.getAllOutputs().size());
        }
    }

    @Test
    public void testPartialScanCannotPublish() throws Exception {
        RemoteFileInfoSource partial = source(metadata());
        assertTrue(partial.hasMoreOutput());
        partial.getOutput();
        partial.close();
        assertFalse(partial.hasMoreOutput());
        assertNull(cache.getRowCount(table));
        assertEquals(Config.default_statistics_output_row_count, statistics(metadata()).getOutputRowCount());
    }

    @Test
    public void testFilteredScanCannotPublishWholeTableCount() throws Exception {
        try (RemoteFileInfoSource source = metadata().getRemoteFilesAsync(table,
                GetRemoteFilesParams.newBuilder().setPredicate(ConstantOperator.createBoolean(true)).build())) {
            assertEquals(20, source.getAllOutputs().size());
        }
        assertNull(cache.getRowCount(table));
    }

    @Test
    public void testEmptyTablePublishesZero() throws Exception {
        table = createTable(0, false);
        try (RemoteFileInfoSource source = source(metadata())) {
            assertTrue(source.getAllOutputs().isEmpty());
        }
        assertEquals(0L, cache.getRowCount(table));
    }

    @Test
    public void testFullStatisticsStillPopulateSnapshotCache() {
        context.getSessionVariable().setEnableDeltaLakeColumnStatistics(true);
        assertEquals(1000, statistics(metadata()).getOutputRowCount());
        assertEquals(1000L, cache.getRowCount(table));
        assertTrue(Tracers.printVars().contains("files"));
    }

    @Test
    public void testDefaultRetainsExistingStatisticsPath() {
        context.getSessionVariable().setEnableDeltaLakeCachedStatistics(false);
        assertEquals(1000, statistics(metadata()).getOutputRowCount());
        assertTrue(Tracers.printVars().contains("files"));
    }

    @Test
    public void testAbortedCollectorNeverPublishesAndCacheCanBeInvalidated() {
        DeltaLakeSnapshotStatisticsCache.Collector collector = cache.newCollector(table);
        collector.add(7);
        collector.abort();
        collector.complete();
        assertNull(cache.getRowCount(table));
        collector = cache.newCollector(table);
        collector.add(10);
        collector.complete();
        assertEquals(10L, cache.getRowCount(table));
        cache.invalidateAll();
        assertNull(cache.getRowCount(table));
    }
    @Test
    public void testSnapshotVersionAndTableIdentityIsolateStatistics() throws Exception {
        try (RemoteFileInfoSource source = source(metadata())) {
            source.getAllOutputs();
        }
        DeltaLakeTable original = table;
        assertEquals(1000L, cache.getRowCount(original));
        Files.writeString(tablePath.resolve("_delta_log/00000000000000000001.json"),
                GsonUtils.GSON.toJson(Map.of("add", Map.of("path", "new.parquet", "partitionValues", Map.of(),
                        "size", 1000, "modificationTime", 1, "dataChange", true,
                        "stats", "{\"numRecords\":17}"))) + "\n");
        SnapshotImpl next = (SnapshotImpl) io.delta.kernel.Table.forPath(original.getDeltaEngine(),
                tablePath.toUri().toString()).getLatestSnapshot(original.getDeltaEngine());
        table = new DeltaLakeTable(123456, CATALOG, "deltalake_db", "tbl", original.getColumns(), List.of(),
                next, original.getDeltaEngine(),
                new MetastoreTable("deltalake_db", "tbl", tablePath.toUri().toString(), 0));
        assertNull(cache.getRowCount(table));
        try (RemoteFileInfoSource source = source(metadata())) {
            source.getAllOutputs();
        }
        assertEquals(1017L, cache.getRowCount(table));
        assertEquals(1000L, cache.getRowCount(original));
        Files.delete(tablePath.resolve("_delta_log/00000000000000000001.json"));
        metadataId = "recreated-table";
        table = createTable(2, false);
        assertNull(cache.getRowCount(table));
        assertEquals(1000L, cache.getRowCount(original));
    }

    @Test
    public void testOverflowAndInvalidCounts() {
        DeltaLakeSnapshotStatisticsCache.Collector collector = cache.newCollector(table);
        collector.add(-1);
        collector.complete();
        assertNull(cache.getRowCount(table));
        collector = cache.newCollector(table);
        collector.add(Long.MAX_VALUE);
        collector.add(1);
        collector.complete();
        assertEquals(Long.MAX_VALUE, cache.getRowCount(table));
    }

}
