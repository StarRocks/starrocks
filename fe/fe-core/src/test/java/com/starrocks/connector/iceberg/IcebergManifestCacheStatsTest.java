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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.AnyStructType;
import com.starrocks.type.Type;
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
import org.apache.iceberg.TableScan;
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;

import static com.starrocks.connector.iceberg.IcebergMetadataTest.DEFAULT_CATALOG_PROPERTIES;
import static com.starrocks.connector.iceberg.IcebergMetadataTest.DEFAULT_CONFIG;
import static com.starrocks.connector.iceberg.IcebergMetadataTest.HDFS_ENVIRONMENT;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarcharType.VARCHAR;

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
    // Plans with the statistics of `statsColumns` (all when null), as the min/max optimization does.
    private static List<FileScanTask> planTasksWithStats(CachingIcebergCatalog catalog, Table table,
                                                         List<String> statsColumns) throws IOException {
        StarRocksIcebergTableScanContext scanContext =
                new StarRocksIcebergTableScanContext(CATALOG_NAME, DB_NAME, TABLE_NAME, PlanMode.LOCAL);
        TableScan scan = catalog.getTableScan(table, scanContext).useSnapshot(table.currentSnapshot().snapshotId());
        scan = statsColumns == null ? scan.includeColumnStats() : scan.includeColumnStats(statsColumns);
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            return Lists.newArrayList(tasks);
        }
    }

    private static Map<String, List<Object>> statsByFile(List<FileScanTask> tasks) {
        Map<String, List<Object>> stats = new HashMap<>();
        for (FileScanTask task : tasks) {
            DataFile file = task.file();
            stats.put(file.location(), List.of(orEmpty(file.lowerBounds()), orEmpty(file.upperBounds()),
                    orEmpty(file.nullValueCounts()), orEmpty(file.valueCounts()), orEmpty(file.columnSizes())));
        }
        return stats;
    }

    // A file read fresh has null for a statistic the manifest lacks, a cached one an empty map: the same to min/max.
    private static <V> Map<Integer, V> orEmpty(Map<Integer, V> values) {
        return values == null ? Map.of() : values;
    }

    // Whether the tasks were served from the manifest cache: cached files reach the tasks as wrappers that carry
    // copied statistics, not the shared store.
    private static boolean fromCache(List<FileScanTask> tasks) {
        boolean fromCache = tasks.stream().allMatch(task -> task.file() instanceof DataFileWrapper);
        if (fromCache) {
            tasks.forEach(task -> Assertions.assertNull(((DataFileWrapper) task.file()).columnStats()));
        }
        return fromCache;
    }

    @Test
    public void testMinMaxStatsServedFromWarmCache() throws IOException {
        appendFiles(mockedNativeTableA);
        CachingIcebergCatalog catalog = newCatalog(Map.of());

        List<FileScanTask> cold = planTasksWithStats(catalog, mockedNativeTableA, List.of("id"));
        Assertions.assertFalse(fromCache(cold));
        Map<String, List<Object>> expected = statsByFile(cold);
        Assertions.assertEquals(FILES, expected.size());
        Assertions.assertEquals(List.of(Map.of(1, id(30)), Map.of(1, id(39)), Map.of(1, 0L), Map.of(1, 10L),
                Map.of(1, 43L)), expected.get("/path/to/data-id-3.parquet"));

        for (int round = 0; round < 2; round++) {
            List<FileScanTask> warm = planTasksWithStats(catalog, mockedNativeTableA, List.of("id"));
            Assertions.assertTrue(fromCache(warm), "round " + round);
            Assertions.assertEquals(expected, statsByFile(warm), "round " + round);
        }
        // the keep-set column (the bucket source) is served from the cache too; it has no statistics here
        Assertions.assertTrue(fromCache(planTasksWithStats(catalog, mockedNativeTableA, List.of("data"))));
    }

    @Test
    public void testStatsNotKeptByCacheBypassIt() throws IOException {
        appendFiles(mockedNativeTableA);
        // all columns' statistics: never covered by the cache
        CachingIcebergCatalog catalog = newCatalog(Map.of());
        planFiles(catalog, mockedNativeTableA, Expressions.alwaysTrue());
        for (int round = 0; round < 2; round++) {
            List<FileScanTask> tasks = planTasksWithStats(catalog, mockedNativeTableA, null);
            Assertions.assertFalse(fromCache(tasks));
            Assertions.assertEquals(Map.of(1, id(30)), statsByFile(tasks).get("/path/to/data-id-3.parquet").get(0));
        }

        // column limit 0: only the keep-set is cached, so id is read fresh while data is served from the cache
        CachingIcebergCatalog keepSetOnly = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "0"));
        planFiles(keepSetOnly, mockedNativeTableA, Expressions.alwaysTrue());
        List<FileScanTask> tasks = planTasksWithStats(keepSetOnly, mockedNativeTableA, List.of("id"));
        Assertions.assertFalse(fromCache(tasks));
        Assertions.assertEquals(Map.of(1, id(30)), statsByFile(tasks).get("/path/to/data-id-3.parquet").get(0));
        Assertions.assertTrue(fromCache(planTasksWithStats(keepSetOnly, mockedNativeTableA, List.of("data"))));

        // without cached column statistics nothing is served from the cache, not even the keep-set
        CachingIcebergCatalog withoutStats = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_WITH_COLUMN_STATISTICS, "false"));
        planFiles(withoutStats, mockedNativeTableA, Expressions.alwaysTrue());
        Assertions.assertFalse(fromCache(planTasksWithStats(withoutStats, mockedNativeTableA, List.of("data"))));
        tasks = planTasksWithStats(withoutStats, mockedNativeTableA, List.of("id"));
        Assertions.assertFalse(fromCache(tasks));
        Assertions.assertEquals(Map.of(1, id(30)), statsByFile(tasks).get("/path/to/data-id-3.parquet").get(0));
    }

    // Only the columns ranked within the column limit are kept in every manifest: a column ranked past it may be
    // crowded out, so a scan asking for it bypasses the cache.
    @Test
    public void testOnlyColumnsRankedWithinLimitAreServedFromCache() throws IOException {
        Schema schema = new Schema(List.of(
                Types.NestedField.optional(1, "a", Types.IntegerType.get()),
                Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                Types.NestedField.optional(3, "c", Types.IntegerType.get())));
        Table table = create(schema, PartitionSpec.unpartitioned(), "t_ranked", 2);
        AppendFiles append = table.newAppend();
        for (int i = 0; i < FILES; i++) {
            append.appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath("/path/to/ranked-" + i + ".parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(10)
                    .withMetrics(new Metrics(10L, null, Map.of(1, 10L, 2, 10L, 3, 10L), Map.of(1, 0L, 2, 0L, 3, 0L),
                            null, Map.of(1, id(i), 2, id(10 + i), 3, id(20 + i)),
                            Map.of(1, id(i), 2, id(10 + i), 3, id(20 + i))))
                    .build());
        }
        append.commit();

        CachingIcebergCatalog catalog = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "2"));
        Map<String, List<Object>> cold = statsByFile(planTasksWithStats(catalog, table, List.of("a", "b")));
        List<FileScanTask> warm = planTasksWithStats(catalog, table, List.of("a", "b"));
        Assertions.assertTrue(fromCache(warm));
        Assertions.assertEquals(cold, statsByFile(warm));
        Assertions.assertEquals(Map.of(1, id(3), 2, id(13)), statsByFile(warm).get("/path/to/ranked-3.parquet").get(0));

        for (List<String> columns : List.of(List.of("c"), List.of("a", "c"))) {
            List<FileScanTask> tasks = planTasksWithStats(catalog, table, columns);
            Assertions.assertFalse(fromCache(tasks), columns.toString());
            Assertions.assertEquals(id(23), tasks.stream()
                    .filter(t -> t.file().location().equals("/path/to/ranked-3.parquet"))
                    .findFirst().orElseThrow().file().lowerBounds().get(3), columns.toString());
        }
    }

    // The keep-set columns count against the column limit, so only the budget left after them is sure to be cached.
    @Test
    public void testKeepSetColumnsUseTheColumnLimit() throws IOException {
        Schema schema = new Schema(List.of(
                Types.NestedField.optional(1, "p", Types.IntegerType.get()),
                Types.NestedField.optional(2, "c", Types.IntegerType.get())));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        Table table = create(schema, spec, "t_keep_budget", 2);
        AppendFiles append = table.newAppend();
        for (int i = 0; i < FILES; i++) {
            append.appendFile(DataFiles.builder(spec)
                    .withPath("/path/to/keep-budget-" + i + ".parquet")
                    .withFileSizeInBytes(10)
                    .withPartitionPath("p=0")
                    .withRecordCount(10)
                    .withMetrics(new Metrics(10L, null, Map.of(1, 10L, 2, 10L), Map.of(1, 0L, 2, 0L), null,
                            Map.of(1, id(0), 2, id(i)), Map.of(1, id(0), 2, id(i))))
                    .build());
        }
        append.commit();

        // limit 1: the partition source p takes it, so c is not cached and is read fresh
        CachingIcebergCatalog one = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "1"));
        planFiles(one, table, Expressions.alwaysTrue());
        List<FileScanTask> tasks = planTasksWithStats(one, table, List.of("c"));
        Assertions.assertFalse(fromCache(tasks));
        Assertions.assertEquals(Map.of(2, id(3)), statsByFile(tasks).get("/path/to/keep-budget-3.parquet").get(0));
        Assertions.assertTrue(fromCache(planTasksWithStats(one, table, List.of("p"))));

        // limit 2: room for c too
        CachingIcebergCatalog two = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "2"));
        Map<String, List<Object>> cold = statsByFile(planTasksWithStats(two, table, List.of("c")));
        tasks = planTasksWithStats(two, table, List.of("c"));
        Assertions.assertTrue(fromCache(tasks));
        Assertions.assertEquals(cold, statsByFile(tasks));
        Assertions.assertEquals(Map.of(2, id(3)), statsByFile(tasks).get("/path/to/keep-budget-3.parquet").get(0));
    }

    // The runtime filter probe columns ask only for statistics the cache keeps: the keep-set plus the columns ranked
    // within the budget left after it, the same columns a scan serves from the cache.
    @Test
    public void testStatsColumnsKeptByManifestCache() throws IOException {
        Schema schema = new Schema(List.of(
                Types.NestedField.optional(1, "p", Types.IntegerType.get()),
                Types.NestedField.optional(2, "a", Types.IntegerType.get()),
                Types.NestedField.optional(3, "b", Types.IntegerType.get())));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        Table table = create(schema, spec, "t_kept_columns", 2);
        Set<String> all = Set.of("p", "a", "b", "no_such_column");

        CachingIcebergCatalog two = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "2"));
        Assertions.assertEquals(Set.of("p", "a"), two.statsColumnsKeptByManifestCache(table, schema, all));
        Assertions.assertEquals(Set.of(), two.statsColumnsKeptByManifestCache(table, schema, Set.of("b")));
        Assertions.assertEquals(Set.of(), two.statsColumnsKeptByManifestCache(table, schema, Set.of()));

        CachingIcebergCatalog zero = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "0"));
        Assertions.assertEquals(Set.of("p"), zero.statsColumnsKeptByManifestCache(table, schema, all));

        CachingIcebergCatalog withoutStats = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_WITH_COLUMN_STATISTICS, "false"));
        Assertions.assertEquals(Set.of(), withoutStats.statsColumnsKeptByManifestCache(table, schema, all));
    }

    // The min/max optimization through IcebergMetadata: each query plans afresh (a new metadata), the first one
    // fills the manifest cache and the later ones are served from it with the same file statistics.
    @Test
    public void testMinMaxOptPlanningWarmCacheMatchesCold() {
        appendFiles(mockedNativeTableA);
        CachingIcebergCatalog catalog = newCatalog(Map.of());
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", DB_NAME,
                TABLE_NAME, "", Lists.newArrayList(new Column("id", INT)), mockedNativeTableA, Maps.newHashMap());

        List<Map<String, List<Object>>> rounds = new ArrayList<>();
        for (int round = 0; round < 3; round++) {
            IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, catalog,
                    Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
            IcebergGetRemoteFilesParams params = (IcebergGetRemoteFilesParams) IcebergGetRemoteFilesParams.newBuilder()
                    .setAllParams(IcebergTableMORParams.EMPTY)
                    .setParams(IcebergMORParams.EMPTY)
                    .setTableVersionRange(TvrTableSnapshot.of(
                            Optional.of(mockedNativeTableA.currentSnapshot().snapshotId())))
                    .setPredicate(ConstantOperator.TRUE)
                    .setEnableColumnStats(true)
                    .setFieldNames(List.of("id"))
                    .setStatsColumnNames(Set.of("id"))
                    .build();
            RemoteFileInfoSource source = metadata.getRemoteFilesAsync(icebergTable, params);
            List<FileScanTask> tasks = new ArrayList<>();
            while (source.hasMoreOutput()) {
                tasks.add(((IcebergRemoteFileInfo) source.getOutput()).getFileScanTask());
            }
            Assertions.assertEquals(round > 0, fromCache(tasks), "round " + round);
            rounds.add(statsByFile(tasks));
        }
        Assertions.assertEquals(FILES, rounds.get(0).size());
        Assertions.assertEquals(List.of(Map.of(1, id(70)), Map.of(1, id(79))),
                rounds.get(0).get("/path/to/data-id-7.parquet").subList(0, 2));
        Assertions.assertEquals(rounds.get(0), rounds.get(1));
        Assertions.assertEquals(rounds.get(0), rounds.get(2));
    }

    // ---- the column-statistics path (enable_iceberg_column_statistics=true) ----

    private static ByteBuffer str(String value) {
        return Conversions.toByteBuffer(Types.StringType.get(), value);
    }

    // a, b, c INT and s STRING; file i: a in [i, i + 5], b in [100 + i, 200] with 2 nulls, c = 20 + i, s in
    // ["s<i>", "s<i>z"]. The last file lacks b's bounds without being all null (corrupted). A column d is added after
    // the files, which have no statistics for it.
    private Table columnStatsTable(String name) {
        Schema schema = new Schema(List.of(
                Types.NestedField.optional(1, "a", Types.IntegerType.get()),
                Types.NestedField.optional(2, "b", Types.IntegerType.get()),
                Types.NestedField.optional(3, "c", Types.IntegerType.get()),
                Types.NestedField.optional(4, "s", Types.StringType.get())));
        Table table = create(schema, PartitionSpec.unpartitioned(), name, 2);
        AppendFiles append = table.newAppend();
        for (int i = 0; i < FILES; i++) {
            Map<Integer, ByteBuffer> lower = new HashMap<>(Map.of(1, id(i), 2, id(100 + i), 3, id(20 + i),
                    4, str("s" + i)));
            Map<Integer, ByteBuffer> upper = new HashMap<>(Map.of(1, id(i + 5), 2, id(200), 3, id(20 + i),
                    4, str("s" + i + "z")));
            if (i == FILES - 1) {
                lower.remove(2);
                upper.remove(2);
            }
            append.appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath("/path/to/" + name + "-" + i + ".parquet")
                    .withFileSizeInBytes(100)
                    .withRecordCount(10)
                    .withMetrics(new Metrics(10L, Map.of(1, 40L, 2, 30L + i, 3, 20L, 4, 60L),
                            Map.of(1, 10L, 2, 10L, 3, 10L, 4, 10L), Map.of(1, 0L, 2, 2L, 3, 0L, 4, 0L), null,
                            lower, upper))
                    .build());
        }
        append.commit();
        table.updateSchema().addColumn("d", Types.IntegerType.get()).commit();
        return table;
    }

    private static IcebergTable srTable(Table table, String name, List<Column> columns) {
        return new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", DB_NAME, name, "", columns, table,
                Maps.newHashMap());
    }

    private static IcebergMetadata statsMetadata(IcebergCatalog catalog) {
        return new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, catalog, Executors.newSingleThreadExecutor(),
                DEFAULT_CATALOG_PROPERTIES, new ConnectorProperties(ConnectorType.ICEBERG,
                Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")), null);
    }

    // The statistics the optimizer sees, in comparable form: the row count and, per column, every estimate.
    private static List<Object> columnStatistics(IcebergMetadata metadata, IcebergTable table,
                                                 Map<ColumnRefOperator, Column> columns, ScalarOperator predicate,
                                                 boolean enableColumnStats) {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setEnableIcebergColumnStatistics(enableColumnStats);
        context.setThreadLocalInfo();
        try {
            OptimizerContext optimizerContext = OptimizerFactory.mockContext(context, new ColumnRefFactory());
            Statistics statistics = metadata.getTableStatistics(optimizerContext, table, columns, null, predicate, -1,
                    TvrTableSnapshot.of(Optional.of(table.getNativeTable().currentSnapshot().snapshotId())));
            List<Object> result = new ArrayList<>();
            result.add(statistics.getOutputRowCount());
            columns.keySet().stream().sorted(Comparator.comparing(ColumnRefOperator::getName)).forEach(ref -> {
                ColumnStatistic stat = statistics.getColumnStatistic(ref);
                result.add(List.of(ref.getName(), stat.getMinValue(), stat.getMaxValue(),
                        String.valueOf(stat.getMinString()), String.valueOf(stat.getMaxString()),
                        stat.getNullsFraction(), stat.getAverageRowSize(), stat.getDistinctValuesCount(),
                        stat.getType()));
            });
            return result;
        } finally {
            ConnectContext.remove();
        }
    }

    private static Map<ColumnRefOperator, Column> columnRefs(String... names) {
        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        int id = 1;
        for (String name : names) {
            Type type = name.equals("s") || name.equals("data") ? VARCHAR : INT;
            columns.put(new ColumnRefOperator(id++, type, name, true), new Column(name, type));
        }
        return columns;
    }

    private static ScalarOperator greaterEqual(Map<ColumnRefOperator, Column> columns, String name, int value) {
        ColumnRefOperator ref = columns.keySet().stream().filter(c -> c.getName().equals(name)).findFirst()
                .orElseThrow();
        return new BinaryPredicateOperator(BinaryType.GE, ref, ConstantOperator.createInt(value));
    }

    // The column-statistics path asks for the estimated columns only and is served from the manifest cache, with the
    // same statistics as the path that reads every manifest; repeated queries hit the warm cache.
    @Test
    public void testColumnStatisticsFromCacheMatchNonCachePath() {
        String name = "t_colstats";
        Table table = columnStatsTable(name);
        Map<ColumnRefOperator, Column> columns = columnRefs("a", "b", "s", "d", "not_in_table");
        IcebergTable icebergTable = srTable(table, name, new ArrayList<>(columns.values()));
        CachingIcebergCatalog catalog = newCatalog(Map.of());
        IcebergHiveCatalog nonCaching = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        TvrTableSnapshot version = TvrTableSnapshot.of(Optional.of(table.currentSnapshot().snapshotId()));
        Assertions.assertEquals(Set.of("a", "b", "s", "d"), statsMetadata(catalog).statsColumnNamesServedFromCache(
                icebergTable, columns.values(), null, version, null));
        Assertions.assertNull(statsMetadata(nonCaching).statsColumnNamesServedFromCache(
                icebergTable, columns.values(), null, version, null));

        List<ScalarOperator> predicates = Arrays.asList(null, greaterEqual(columns, "a", 10));
        List<List<Object>> expected = new ArrayList<>();
        for (ScalarOperator predicate : predicates) {
            expected.add(columnStatistics(statsMetadata(nonCaching), icebergTable, columns, predicate, true));
        }
        // a >= 10 keeps files 5..7 only
        Assertions.assertEquals(30.0, expected.get(1).get(0));
        for (int i = 0; i < predicates.size(); i++) {
            Assertions.assertEquals(expected.get(i),
                    columnStatistics(statsMetadata(catalog), icebergTable, columns, predicates.get(i), true));
        }
        // With the manifests gone, only the cache can serve the statistics.
        for (ManifestFile manifest : table.currentSnapshot().allManifests(table.io())) {
            table.io().deleteFile(manifest.path());
        }
        for (int round = 0; round < 2; round++) {
            for (int i = 0; i < predicates.size(); i++) {
                Assertions.assertEquals(expected.get(i),
                        columnStatistics(statsMetadata(catalog), icebergTable, columns, predicates.get(i), true),
                        "predicate " + predicates.get(i) + " round " + round);
            }
        }
        List<Object> all = expected.get(0);
        Assertions.assertEquals(80.0, all.get(0));
        // a: [0, 12]; b: neither min nor max once a file lacks both its bounds (the max of the other files is not an
        // upper bound of that file); s: string bounds; d: no statistics in any file
        Assertions.assertEquals(List.of("a", 0.0, 12.0), ((List<?>) all.get(1)).subList(0, 3));
        Assertions.assertEquals(List.of("b", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.2),
                List.of(((List<?>) all.get(2)).get(0), ((List<?>) all.get(2)).get(1), ((List<?>) all.get(2)).get(2),
                        ((List<?>) all.get(2)).get(5)));
        Assertions.assertEquals(List.of("s0", "s7z"), ((List<?>) all.get(5)).subList(3, 5));
    }

    // A column the cache does not keep in every manifest sends the path past the cache, requesting all columns as
    // before; the statistics do not change.
    @Test
    public void testColumnStatisticsNotKeptByCacheUseNonCachePath() {
        String name = "t_colstats_limit";
        Table table = columnStatsTable(name);
        Map<ColumnRefOperator, Column> columns = columnRefs("a", "c");
        IcebergTable icebergTable = srTable(table, name, new ArrayList<>(columns.values()));
        CachingIcebergCatalog catalog = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "2"));
        IcebergHiveCatalog nonCaching = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        TvrTableSnapshot version = TvrTableSnapshot.of(Optional.of(table.currentSnapshot().snapshotId()));

        Assertions.assertEquals(Set.of("a"), statsMetadata(catalog).statsColumnNamesServedFromCache(
                icebergTable, columnRefs("a").values(), null, version, null));
        Assertions.assertNull(statsMetadata(catalog).statsColumnNamesServedFromCache(
                icebergTable, columns.values(), null, version, null));
        Assertions.assertEquals(columnStatistics(statsMetadata(nonCaching), icebergTable, columns, null, true),
                columnStatistics(statsMetadata(catalog), icebergTable, columns, null, true));

        // it reads the manifests even with a warm cache, so it fails without them, while a covered column set is
        // still served from the cache
        List<Object> onlyA = columnStatistics(statsMetadata(catalog), icebergTable, columnRefs("a"), null, true);
        for (ManifestFile manifest : table.currentSnapshot().allManifests(table.io())) {
            table.io().deleteFile(manifest.path());
        }
        Assertions.assertThrows(Exception.class,
                () -> columnStatistics(statsMetadata(catalog), icebergTable, columns, null, true));
        Assertions.assertEquals(onlyA,
                columnStatistics(statsMetadata(catalog), icebergTable, columnRefs("a"), null, true));
    }

    // A partitioned table: the bucket source is in the cache keep-set, id ranks within the limit.
    @Test
    public void testPartitionedColumnStatisticsFromCacheMatchNonCachePath() {
        appendFiles(mockedNativeTableA);
        Map<ColumnRefOperator, Column> columns = columnRefs("id", "data");
        IcebergTable icebergTable = srTable(mockedNativeTableA, TABLE_NAME, new ArrayList<>(columns.values()));
        CachingIcebergCatalog catalog = newCatalog(Map.of());
        IcebergHiveCatalog nonCaching = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        Assertions.assertEquals(Set.of("id", "data"), statsMetadata(catalog).statsColumnNamesServedFromCache(
                icebergTable, columns.values(), null,
                TvrTableSnapshot.of(Optional.of(mockedNativeTableA.currentSnapshot().snapshotId())), null));
        List<Object> expected = columnStatistics(statsMetadata(nonCaching), icebergTable, columns, null, true);
        for (int round = 0; round < 2; round++) {
            Assertions.assertEquals(expected, columnStatistics(statsMetadata(catalog), icebergTable, columns, null, true));
        }
        Assertions.assertEquals(List.of("id", 0.0, 79.0), ((List<?>) expected.get(2)).subList(0, 3));
    }

    // With the switch off (the default) the columns stay UNKNOWN and nothing is planned for statistics.
    @Test
    public void testColumnStatisticsOffKeepsColumnsUnknown() {
        String name = "t_colstats_off";
        Table table = columnStatsTable(name);
        Map<ColumnRefOperator, Column> columns = columnRefs("a");
        IcebergTable icebergTable = srTable(table, name, new ArrayList<>(columns.values()));
        List<Object> stats = columnStatistics(statsMetadata(newCatalog(Map.of())), icebergTable, columns, null, false);
        Assertions.assertEquals(80.0, stats.get(0));
        Assertions.assertEquals(ColumnStatistic.StatisticType.UNKNOWN, ((List<?>) stats.get(1)).get(8));
    }

    // The fields a pushed-down predicate prunes files on count toward the cache coverage: a struct subfield that the
    // cache does not keep sends the path past the cache, so it prunes the same files as before.
    @Test
    public void testPredicateFieldsCountTowardCacheCoverage() {
        String name = "t_nested";
        Schema schema = new Schema(List.of(
                Types.NestedField.optional(1, "a", Types.IntegerType.get()),
                Types.NestedField.optional(2, "st", Types.StructType.of(
                        Types.NestedField.optional(3, "x", Types.IntegerType.get())))));
        Table table = create(schema, PartitionSpec.unpartitioned(), name, 2);
        AppendFiles append = table.newAppend();
        for (int i = 0; i < FILES; i++) {
            append.appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath("/path/to/" + name + "-" + i + ".parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(10)
                    .withMetrics(new Metrics(10L, null, Map.of(1, 10L, 3, 10L), Map.of(1, 0L, 3, 0L), null,
                            Map.of(1, id(i), 3, id(10 * i)), Map.of(1, id(i), 3, id(10 * i + 9))))
                    .build());
        }
        append.commit();
        Map<ColumnRefOperator, Column> columns = columnRefs("a");
        IcebergTable icebergTable = srTable(table, name, new ArrayList<>(columns.values()));
        // the cache keeps the statistics of one column: a
        CachingIcebergCatalog catalog = newCatalog(
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "1"));
        IcebergHiveCatalog nonCaching = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        TvrTableSnapshot version = TvrTableSnapshot.of(Optional.of(table.currentSnapshot().snapshotId()));
        ColumnRefOperator st = new ColumnRefOperator(10, AnyStructType.ANY_STRUCT, "st", true);
        ScalarOperator onSubfield = new BinaryPredicateOperator(BinaryType.EQ,
                new SubfieldOperator(st, INT, ImmutableList.of("x")), ConstantOperator.createInt(25));

        Assertions.assertEquals(Set.of("a"), statsMetadata(catalog).statsColumnNamesServedFromCache(
                icebergTable, columns.values(), greaterEqual(columns, "a", 2), version, null));
        Assertions.assertNull(statsMetadata(catalog).statsColumnNamesServedFromCache(
                icebergTable, columns.values(), onSubfield, version, null));
        List<Object> expected = columnStatistics(statsMetadata(nonCaching), icebergTable, columns, onSubfield, true);
        // st.x = 25 keeps file 2 only
        Assertions.assertEquals(10.0, expected.get(0));
        for (int round = 0; round < 2; round++) {
            Assertions.assertEquals(expected,
                    columnStatistics(statsMetadata(catalog), icebergTable, columns, onSubfield, true));
        }
    }

    // The cache truncates STRING bounds to 16 characters, as Iceberg's default metrics mode does. Longer bounds (a
    // `full` metrics mode) are served truncated: still a valid lower / upper bound, a looser estimate; the rest of
    // the statistics are the same.
    @Test
    public void testLongStringBoundsServedTruncated() {
        String name = "t_long_string";
        Schema schema = new Schema(List.of(Types.NestedField.optional(1, "s", Types.StringType.get())));
        Table table = create(schema, PartitionSpec.unpartitioned(), name, 2);
        String longMin = "aaaaaaaaaaaaaaaaaaaa-min";
        String longMax = "zzzzzzzzzzzzzzzzyyyy-max";
        table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/" + name + ".parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(10)
                .withMetrics(new Metrics(10L, Map.of(1, 50L), Map.of(1, 10L), Map.of(1, 0L), null,
                        Map.of(1, str(longMin)), Map.of(1, str(longMax))))
                .build()).commit();
        Map<ColumnRefOperator, Column> columns = columnRefs("s");
        IcebergTable icebergTable = srTable(table, name, new ArrayList<>(columns.values()));
        CachingIcebergCatalog catalog = newCatalog(Map.of());
        IcebergHiveCatalog nonCaching = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        List<?> exact = (List<?>) columnStatistics(statsMetadata(nonCaching), icebergTable, columns, null, true).get(1);
        Assertions.assertEquals(List.of(longMin, longMax), exact.subList(3, 5));
        // the first query fills the cache, the second one is served from it
        columnStatistics(statsMetadata(catalog), icebergTable, columns, null, true);
        List<Object> cached = columnStatistics(statsMetadata(catalog), icebergTable, columns, null, true);
        List<?> truncated = (List<?>) cached.get(1);
        Assertions.assertEquals(longMin.substring(0, 16), truncated.get(3));
        Assertions.assertTrue(((String) truncated.get(4)).compareTo(longMax) >= 0, truncated.get(4).toString());
        Assertions.assertEquals(10.0, cached.get(0));
        Assertions.assertEquals(exact.subList(5, 9), truncated.subList(5, 9));
    }

    // The CBO statistics request and the min/max scan of `SELECT max(id)` share a planning key; when the statistics
    // request plans first, the min/max scan reuses its tasks, which must keep the file statistics it needs.
    @Test
    public void testMinMaxScanReusingStatisticsTasksKeepsFileStats() {
        appendFiles(mockedNativeTableA);
        Map<ColumnRefOperator, Column> columns = columnRefs("id");
        IcebergTable icebergTable = srTable(mockedNativeTableA, TABLE_NAME, new ArrayList<>(columns.values()));
        IcebergMetadata metadata = statsMetadata(newCatalog(Map.of()));
        columnStatistics(metadata, icebergTable, columns, ConstantOperator.TRUE, true);

        IcebergGetRemoteFilesParams params = (IcebergGetRemoteFilesParams) IcebergGetRemoteFilesParams.newBuilder()
                .setAllParams(IcebergTableMORParams.EMPTY)
                .setParams(IcebergMORParams.EMPTY)
                .setTableVersionRange(TvrTableSnapshot.of(
                        Optional.of(mockedNativeTableA.currentSnapshot().snapshotId())))
                .setPredicate(ConstantOperator.TRUE)
                .setEnableColumnStats(true)
                .setFieldNames(List.of("id"))
                .setStatsColumnNames(Set.of("id"))
                .build();
        RemoteFileInfoSource source = metadata.getRemoteFilesAsync(icebergTable, params);
        List<FileScanTask> tasks = new ArrayList<>();
        while (source.hasMoreOutput()) {
            tasks.add(((IcebergRemoteFileInfo) source.getOutput()).getFileScanTask());
        }
        Assertions.assertEquals(FILES, tasks.size());
        // reused from the statistics request
        tasks.forEach(task -> Assertions.assertInstanceOf(IcebergSplitScanTask.class, task));
        Assertions.assertEquals(List.of(Map.of(1, id(70)), Map.of(1, id(79))),
                statsByFile(tasks).get("/path/to/data-id-7.parquet").subList(0, 2));
    }

    // Optional statistics (the runtime filter probe columns) reach the scan ranges on the planning path that plans
    // every file up front too: the tasks keep them, and no table statistics are collected from them.
    @Test
    public void testOptionalStatsKeptOnSynchronouslyPlannedTasks() {
        appendFiles(mockedNativeTableA);
        CachingIcebergCatalog catalog = newCatalog(Map.of());
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", DB_NAME,
                TABLE_NAME, "", Lists.newArrayList(new Column("id", INT)), mockedNativeTableA, Maps.newHashMap());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, catalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergGetRemoteFilesParams params = (IcebergGetRemoteFilesParams) IcebergGetRemoteFilesParams.newBuilder()
                .setAllParams(IcebergTableMORParams.EMPTY)
                .setParams(IcebergMORParams.EMPTY)
                .setTableVersionRange(TvrTableSnapshot.of(
                        Optional.of(mockedNativeTableA.currentSnapshot().snapshotId())))
                .setPredicate(ConstantOperator.TRUE)
                .setEnableColumnStats(true)
                .setOptionalColumnStats(true)
                .setFieldNames(List.of("id"))
                .setStatsColumnNames(Set.of("id"))
                .build();
        List<FileScanTask> tasks = metadata.getRemoteFiles(icebergTable, params).stream()
                .map(info -> ((IcebergRemoteFileInfo) info).getFileScanTask()).toList();
        Assertions.assertEquals(FILES, tasks.size());
        Assertions.assertEquals(List.of(Map.of(1, id(70)), Map.of(1, id(79))),
                statsByFile(tasks).get("/path/to/data-id-7.parquet").subList(0, 2));
    }
}
