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

import com.google.common.collect.Lists;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;

import static com.starrocks.connector.iceberg.IcebergMetadataTest.DEFAULT_CONFIG;

// The manifest cache keeps file-level min/max of columns outside the partition/sort keep-set, so a warm cache prunes
// the same files as a cold one.
public class IcebergManifestCacheStatsTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    private static final String DB_NAME = "db";
    private static final String TABLE_NAME = "ta";
    private static final int FILES = 8;

    private CachingIcebergCatalog newCatalog(Map<String, String> extraProperties) {
        Map<String, String> properties = new HashMap<>(DEFAULT_CONFIG);
        properties.putAll(extraProperties);
        IcebergHiveCatalog hiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        return new CachingIcebergCatalog(CATALOG_NAME, hiveCatalog, new IcebergCatalogProperties(properties),
                Executors.newSingleThreadExecutor());
    }

    private static ByteBuffer id(int value) {
        return Conversions.toByteBuffer(Types.IntegerType.get(), value);
    }

    // file i holds ids [10 * i, 10 * i + 9]; `id` is neither a partition source (bucket(data)) nor a sort column
    private void appendFiles(Table table) {
        AppendFiles append = table.newAppend();
        for (int i = 0; i < FILES; i++) {
            append.appendFile(DataFiles.builder(SPEC_A)
                    .withPath("/path/to/data-id-" + i + ".parquet")
                    .withFileSizeInBytes(10)
                    .withPartitionPath("data_bucket=0")
                    .withRecordCount(10)
                    .withMetrics(new Metrics(10L, Map.of(1, 40L + i), Map.of(1, 10L), Map.of(1, 0L), null,
                            Map.of(1, id(10 * i)), Map.of(1, id(10 * i + 9))))
                    .build());
        }
        append.commit();
    }

    private static Set<String> planFiles(CachingIcebergCatalog catalog, Table table, Expression filter)
            throws IOException {
        Set<String> files = new TreeSet<>();
        for (FileScanTask task : planTasks(catalog, table, filter)) {
            files.add(task.file().location());
        }
        return files;
    }

    private static List<FileScanTask> planTasks(CachingIcebergCatalog catalog, Table table, Expression filter)
            throws IOException {
        StarRocksIcebergTableScanContext scanContext =
                new StarRocksIcebergTableScanContext(CATALOG_NAME, DB_NAME, TABLE_NAME, PlanMode.LOCAL);
        try (CloseableIterable<FileScanTask> tasks = catalog.getTableScan(table, scanContext)
                .useSnapshot(table.currentSnapshot().snapshotId())
                .filter(filter)
                .planFiles()) {
            return Lists.newArrayList(tasks);
        }
    }

    @Test
    public void testWarmCachePrunesOnColumnOutsideKeepSet() throws IOException {
        appendFiles(mockedNativeTableA);
        CachingIcebergCatalog catalog = newCatalog(Map.of());
        Expression filter = Expressions.equal("id", 25);
        Set<String> expected = Set.of("/path/to/data-id-2.parquet");

        // cold: reads the manifest (and fills the cache); warm: served from the cache
        Assertions.assertEquals(expected, planFiles(catalog, mockedNativeTableA, filter));
        Assertions.assertEquals(expected, planFiles(catalog, mockedNativeTableA, filter));
        Assertions.assertEquals(expected, planFiles(catalog, mockedNativeTableA, filter));
        Assertions.assertEquals(3, planFiles(catalog, mockedNativeTableA,
                Expressions.and(Expressions.greaterThanOrEqual("id", 15), Expressions.lessThan("id", 40))).size());
        Assertions.assertEquals(FILES, planFiles(catalog, mockedNativeTableA, Expressions.alwaysTrue()).size());

        // the cached files carry the statistics through the shared per-manifest store
        ManifestFile manifest = mockedNativeTableA.currentSnapshot().dataManifests(mockedNativeTableA.io()).get(0);
        Map<String, DataFile> cached = catalog.findCachedDataFiles(manifest, Set.of("/path/to/data-id-3.parquet"));
        DataFile file = cached.get("/path/to/data-id-3.parquet");
        Assertions.assertInstanceOf(DataFileWrapper.class, file);
        Assertions.assertNotNull(((DataFileWrapper) file).columnStats());
        Assertions.assertEquals(id(30), file.lowerBounds().get(1));
        Assertions.assertEquals(id(39), file.upperBounds().get(1));
        Assertions.assertEquals(0L, file.nullValueCounts().get(1));
        Assertions.assertEquals(Map.of(1, 43L), file.columnSizes());

        // warm scan tasks do not reference the shared store, so an evicted entry's statistics are not kept alive
        List<FileScanTask> tasks = planTasks(catalog, mockedNativeTableA,
                Expressions.and(Expressions.greaterThanOrEqual("id", 15), Expressions.lessThan("id", 40)));
        Assertions.assertEquals(3, tasks.size());
        for (FileScanTask task : tasks) {
            Assertions.assertFalse(task.file() instanceof DataFileWrapper, task.file().location());
            Assertions.assertNull(task.file().lowerBounds());
        }
    }

    @Test
    public void testNegativeColumnLimitIsTreatedAsZero() throws IOException {
        Map<String, String> negative = Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "-1");
        Map<String, String> properties = new HashMap<>(DEFAULT_CONFIG);
        properties.putAll(negative);
        Assertions.assertEquals(0, new IcebergCatalogProperties(properties).getIcebergManifestCacheStatisticsMaxColumns());

        appendFiles(mockedNativeTableA);
        CachingIcebergCatalog catalog = newCatalog(negative);
        Expression filter = Expressions.equal("id", 25);

        // like 0: only the keep-set (the partition source column) is cached, so the warm cache cannot prune on id
        Assertions.assertEquals(1, planFiles(catalog, mockedNativeTableA, filter).size());
        Assertions.assertEquals(FILES, planFiles(catalog, mockedNativeTableA, filter).size());
    }

    // Without cached column statistics, equality deletes still need the identifier column's min/max to skip data
    // files whose key range cannot match: those columns alone are cached, column by column.
    @Test
    public void testIdentifierColumnsCachedWithoutColumnStatistics() throws IOException {
        Schema schema = new Schema(List.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "v", Types.IntegerType.get())), Set.of(1));
        Table table = create(schema, PartitionSpec.unpartitioned(), "t_identifier", 2);
        AppendFiles append = table.newAppend();
        for (int i = 0; i < FILES; i++) {
            append.appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath("/path/to/identifier-" + i + ".parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(10)
                    .withMetrics(new Metrics(10L, null, Map.of(1, 10L, 2, 10L), Map.of(1, 0L, 2, 0L), null,
                            Map.of(1, id(10 * i), 2, id(0)), Map.of(1, id(10 * i + 9), 2, id(1000))))
                    .build());
        }
        append.commit();
        // deletes id = 25, which only data file 2 can contain
        DeleteFile deletes = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(1)
                .withPath("/path/to/identifier-deletes.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withMetrics(new Metrics(1L, null, Map.of(1, 1L), Map.of(1, 0L), null,
                        Map.of(1, id(25)), Map.of(1, id(25))))
                .build();
        RowDelta delta = table.newRowDelta();
        delta.addDeletes(deletes);
        delta.commit();

        CachingIcebergCatalog catalog = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_WITH_COLUMN_STATISTICS, "false"));
        // cold, then warm: the delete file is attached to data file 2 only
        for (int round = 0; round < 3; round++) {
            List<String> withDeletes = new ArrayList<>();
            for (FileScanTask task : planTasks(catalog, table, Expressions.alwaysTrue())) {
                if (!task.deletes().isEmpty()) {
                    withDeletes.add(task.file().location());
                }
            }
            Assertions.assertEquals(List.of("/path/to/identifier-2.parquet"), withDeletes, "round " + round);
        }

        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        DataFile cached = catalog.findCachedDataFiles(manifest, Set.of("/path/to/identifier-3.parquet"))
                .get("/path/to/identifier-3.parquet");
        Assertions.assertEquals(Map.of(1, id(30)), cached.lowerBounds(), "only the identifier column is cached");
        Assertions.assertEquals(Map.of(1, id(39)), cached.upperBounds());
    }

    @Test
    public void testColumnLimitZeroKeepsTheKeepSetOnly() throws IOException {
        appendFiles(mockedNativeTableA);
        CachingIcebergCatalog catalog = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "0"));

        Assertions.assertEquals(1, planFiles(catalog, mockedNativeTableA, Expressions.equal("id", 25)).size());
        Assertions.assertEquals(FILES, planFiles(catalog, mockedNativeTableA, Expressions.equal("id", 25)).size());
    }
}
