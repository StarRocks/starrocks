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

import com.github.benmanes.caffeine.cache.Cache;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.iceberg.CachingIcebergCatalog.IcebergTableName;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.GraphLayout;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;

import static com.starrocks.connector.iceberg.IcebergMetadataTest.DEFAULT_CONFIG;

/**
 * Checks the memory accounting of every weighed cache of {@link CachingIcebergCatalog} against the heap the
 * cached objects actually retain, as measured by JOL. Two numbers are checked per cache:
 * <ul>
 *   <li>the ledger -- the Caffeine weigher sum that eviction is bounded by;</li>
 *   <li>the report -- {@link CachingIcebergCatalog#estimateSize()}, which feeds {@code sys.fe_memory_usage}.</li>
 * </ul>
 * Both drifted in both directions over time (containers and strings missed; catalog-shared objects and
 * manifest-shared objects charged once per entry; a sampled walk missing the one snapshot that holds the
 * manifest list). Each case prints a {@code MEMACC} line with the ratios; the assertions keep them in a band
 * so a later Estimator or weigher change cannot silently move them out of it.
 */
public class IcebergCacheMemoryAccuracyTest extends TableTestBase {
    static {
        // The table graph reaches the catalog's Configuration and its class loader, whose CodeSource keys are
        // records when classes come from jars; JOL needs this to lay out record classes. Each test class runs in
        // its own JVM (reuseForks=false), and JOL reads the property when it first initializes.
        System.setProperty("jol.magicFieldOffset", "true");
    }

    private static final String CATALOG_NAME = "iceberg_catalog";
    private static final double LOW = 0.90;
    private static final double HIGH = 1.15;
    // A table's schema reaches the same column names from its fields and from two name/id maps, in different
    // orders. The sampled walk of each map lands on names the field walk has not visited and extrapolates them
    // again, about 80 B per column. That is a fixed few KB per table: ~1.30x on a single-snapshot table with
    // 110 columns, ~1.05x once the table has history. Sampling cannot see that overlap; only a full walk could.
    private static final double HIGH_TABLE = 1.35;

    // Every ratio of a test is printed before any is asserted, so one run shows the whole picture.
    private final List<String> outOfBand = new ArrayList<>();

    private void check(String label, String what, long estimated, long actual) {
        check(label, what, estimated, actual, HIGH);
    }

    private void check(String label, String what, long estimated, long actual, double high) {
        double ratio = estimated * 1.0 / actual;
        System.out.printf("MEMACC %s | %s estimated=%d actual=%d ratio=%.3f%n", label, what, estimated, actual, ratio);
        if (ratio < LOW || ratio > high) {
            outOfBand.add(String.format("%s %s: estimated %d vs actual %d (%.3f) is outside [%.2f, %.2f]",
                    label, what, estimated, actual, ratio, LOW, high));
        }
    }

    @AfterEach
    public void assertInBand() {
        Assertions.assertTrue(outOfBand.isEmpty(), String.join("\n", outOfBand));
    }

    // Column names carry `tag` so schemas of different tables are distinct objects: Iceberg's SchemaParser
    // caches parsed schemas by their JSON, which would let same-shaped test tables share one Schema.
    private static Schema wideSchema(int n, String tag) {
        List<Types.NestedField> fields = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Type t;
            switch (i % 4) {
                case 0:
                    t = Types.StringType.get();
                    break;
                case 1:
                    t = Types.LongType.get();
                    break;
                case 2:
                    t = Types.IntegerType.get();
                    break;
                default:
                    t = Types.TimestampType.withZone();
            }
            fields.add(Types.NestedField.optional(i + 1, tag + "_column_" + i, t));
        }
        return new Schema(fields);
    }

    private static CachingIcebergCatalog newCatalog(Map<String, String> extraProperties) {
        IcebergHiveCatalog hiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        Map<String, String> properties = new HashMap<>(DEFAULT_CONFIG);
        properties.putAll(extraProperties);
        return new CachingIcebergCatalog(CATALOG_NAME, hiveCatalog, new IcebergCatalogProperties(properties),
                Executors.newSingleThreadExecutor());
    }

    @SuppressWarnings("unchecked")
    private static <T> T field(Object o, String name) throws Exception {
        Field f = o.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(o);
    }

    private static long ledger(Cache<?, ?> cache) {
        cache.cleanUp();
        return cache.policy().eviction().orElseThrow().weightedSize().orElseThrow();
    }

    private static PartitionData day(PartitionSpec spec, int day) {
        PartitionData pd = new PartitionData(spec.partitionType());
        pd.set(0, day);
        return pd;
    }

    private static String path(Random r, String kind) {
        return "s3://bucket/warehouse/db.db/tbl/" + kind + "/ts_day=2025-01-01/00000-" + r.nextInt(100000) + "-" +
                UUID.randomUUID() + "-00001.parquet";
    }

    private static String randomString(Random r, int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('a' + r.nextInt(26)));
        }
        return sb.toString();
    }

    // Per-column statistics the way the default truncate(16) metrics mode writes them.
    private static Metrics columnMetrics(Schema schema, Random r, long rows) {
        Map<Integer, Long> sizes = new HashMap<>();
        Map<Integer, Long> values = new HashMap<>();
        Map<Integer, Long> nulls = new HashMap<>();
        Map<Integer, ByteBuffer> lower = new HashMap<>();
        Map<Integer, ByteBuffer> upper = new HashMap<>();
        for (Types.NestedField f : schema.columns()) {
            int id = f.fieldId();
            sizes.put(id, (long) r.nextInt(50_000_000));
            values.put(id, rows);
            nulls.put(id, (long) r.nextInt(100));
            Object lo;
            Object hi;
            switch (f.type().typeId()) {
                case STRING:
                    lo = "a" + randomString(r, 6 + r.nextInt(10));
                    hi = "z" + randomString(r, 6 + r.nextInt(10));
                    break;
                case INTEGER:
                    lo = r.nextInt(1 << 20);
                    hi = (1 << 21) + r.nextInt(1 << 20);
                    break;
                default:
                    lo = (long) r.nextInt(1 << 30);
                    hi = (1L << 31) + r.nextInt(1 << 30);
            }
            lower.put(id, Conversions.toByteBuffer(f.type(), lo));
            upper.put(id, Conversions.toByteBuffer(f.type(), hi));
        }
        return new Metrics(rows, sizes, values, nulls, null, lower, upper);
    }

    // ---------------------------------------------------------------- table cache

    @Test
    public void testTableCache() throws Exception {
        measureTables(1, 4, false);
        measureTables(40, 3, false);
    }

    // The usual order: the cache loads (and weighs) the table, then planning loads the current snapshot's
    // manifest list into the cached object. The ledger cannot see that growth -- nothing re-weighs an entry
    // whose value changed in place -- so only the reported number is checked; the ledger is printed.
    @Test
    public void testTableCachePlannedAfterCaching() throws Exception {
        measureTables(40, 3, true);
    }

    private void measureTables(int snapshots, int tableCount, boolean planAfterCaching) throws Exception {
        String label = "table_cache snapshots=" + snapshots + (planAfterCaching ? " planned_after_caching" : "");
        File warehouse = Files.createTempDirectory("iceberg_cache_memory").toFile();
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehouse.getAbsolutePath());
        Random r = new Random(7);
        List<Table> loaded = new ArrayList<>();
        for (int t = 0; t < tableCount; t++) {
            Schema schema = wideSchema(110, "t" + t);
            PartitionSpec spec = PartitionSpec.builderFor(schema).day("t" + t + "_column_3").build();
            TableIdentifier id = TableIdentifier.of("db", "t" + t);
            Table created = hadoopCatalog.createTable(id, schema, spec,
                    Map.of("format-version", "2", "commit.manifest-merge.enabled", "false"));
            for (int s = 0; s < snapshots; s++) {
                created.newAppend().appendFile(DataFiles.builder(created.spec()).withPath(path(r, "data"))
                        .withFormat(FileFormat.PARQUET).withFileSizeInBytes(1000).withRecordCount(10)
                        .withPartition(day(created.spec(), 20000 + s % 30)).build()).commit();
            }
            loaded.add(hadoopCatalog.loadTable(id));
        }

        CachingIcebergCatalog catalog = newCatalog(Map.of());
        com.github.benmanes.caffeine.cache.LoadingCache<IcebergTableName, Table> tables = field(catalog, "tables");
        if (!planAfterCaching) {
            loaded.forEach(IcebergCacheMemoryAccuracyTest::plan);
        }
        for (int t = 0; t < tableCount; t++) {
            tables.put(new IcebergTableName("db", "t" + t), loaded.get(t));
        }
        if (planAfterCaching) {
            loaded.forEach(IcebergCacheMemoryAccuracyTest::plan);
        }
        // What an entry retains on its own: the part of its graph no other table reaches. The tables share the
        // catalog's Configuration and FileIO, plus whatever gets lazily attached to only some of them, so
        // subtracting a single table's graph is not stable; subtracting all the others is.
        long actual = 0;
        for (int t = 0; t < tableCount; t++) {
            List<Table> others = new ArrayList<>(loaded);
            others.remove(t);
            actual += GraphLayout.parseInstance(loaded.get(t))
                    .subtract(GraphLayout.parseInstance(others.toArray())).totalSize();
        }
        if (planAfterCaching) {
            System.out.printf("MEMACC %s | ledger (not checked) estimated=%d actual=%d ratio=%.3f%n", label,
                    ledger(tables), actual, ledger(tables) * 1.0 / actual);
        } else {
            check(label, "ledger", ledger(tables), actual, HIGH_TABLE);
        }
        check(label, "estimateSize()", catalog.estimateSize(), actual, HIGH_TABLE);
    }

    // What planning leaves in the table: the current snapshot caches its manifest list.
    private static void plan(Table table) {
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            tasks.forEach(task -> { });
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    // ---------------------------------------------------------------- partition cache

    @Test
    public void testPartitionCache() throws Exception {
        String label = "partition_cache partitions=2000";
        Schema schema = wideSchema(8, "p");
        Table table = create(schema, PartitionSpec.builderFor(schema).day("p_column_3").build(), "part", 2);
        Random r = new Random(11);
        AppendFiles append = table.newAppend();
        for (int p = 0; p < 2000; p++) {
            append.appendFile(DataFiles.builder(table.spec()).withPath(path(r, "data")).withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(1000).withRecordCount(10).withPartition(day(table.spec(), 10000 + p)).build());
        }
        append.commit();
        IcebergHiveCatalog delegate = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergTable icebergTable = IcebergTable.builder().setCatalogDBName("db").setSrTableName("part")
                .setCatalogTableName("part").setNativeTable(table).build();
        Map<String, Partition> partitions = delegate.getPartitions(icebergTable, -1, null);

        CachingIcebergCatalog catalog = newCatalog(Map.of());
        com.github.benmanes.caffeine.cache.LoadingCache<IcebergTableName, Map<String, Partition>> cache =
                field(catalog, "partitionCache");
        cache.put(new IcebergTableName("db", "part", table.currentSnapshot().snapshotId()), partitions);
        long actual = GraphLayout.parseInstance(partitions).totalSize();
        check(label, "ledger", ledger(cache), actual);
        check(label, "estimateSize()", catalog.estimateSize(), actual);
    }

    // ---------------------------------------------------------------- data file + delete file caches

    @Test
    public void testDataFileCacheWithColumnStats() throws Exception {
        measureContentFiles("data_files column_stats", Map.of(), 40, true, false, false);
    }

    @Test
    public void testDataFileCacheWithoutColumnStats() throws Exception {
        measureContentFiles("data_files no_column_stats",
                Map.of(IcebergCatalogProperties.ICEBERG_MANIFEST_CACHE_STATISTICS_MAX_COLUMNS, "0"), 40, true, false,
                false);
    }

    @Test
    public void testPositionDeleteFileCache() throws Exception {
        measureContentFiles("position_deletes", Map.of(), 8, false, true, false);
    }

    @Test
    public void testEqualityDeleteFileCache() throws Exception {
        measureContentFiles("equality_deletes", Map.of(), 8, false, false, true);
    }

    private static Metrics positionDeleteMetrics(String dataPath, long rows) {
        int pathId = MetadataColumns.DELETE_FILE_PATH.fieldId();
        int posId = MetadataColumns.DELETE_FILE_POS.fieldId();
        Map<Integer, ByteBuffer> lower = new HashMap<>();
        Map<Integer, ByteBuffer> upper = new HashMap<>();
        lower.put(pathId, Conversions.toByteBuffer(Types.StringType.get(), dataPath));
        upper.put(pathId, Conversions.toByteBuffer(Types.StringType.get(), dataPath));
        lower.put(posId, Conversions.toByteBuffer(Types.LongType.get(), 0L));
        upper.put(posId, Conversions.toByteBuffer(Types.LongType.get(), rows));
        return new Metrics(rows, new HashMap<>(Map.of(pathId, 1000L, posId, 800L)),
                new HashMap<>(Map.of(pathId, rows, posId, rows)), new HashMap<>(Map.of(pathId, 0L, posId, 0L)),
                null, lower, upper);
    }

    private static Metrics equalityDeleteMetrics(Random r, long rows) {
        return new Metrics(rows, new HashMap<>(Map.of(2, 1000L)), new HashMap<>(Map.of(2, rows)),
                new HashMap<>(Map.of(2, 0L)), null,
                new HashMap<>(Map.of(2, Conversions.toByteBuffer(Types.LongType.get(), (long) r.nextInt(1000)))),
                new HashMap<>(Map.of(2, Conversions.toByteBuffer(Types.LongType.get(), 1000L + r.nextInt(1000)))));
    }

    @SuppressWarnings("unchecked")
    private void measureContentFiles(String label, Map<String, String> catalogProperties, int columns,
                                     boolean dataStats, boolean positionDeletes, boolean equalityDeletes)
            throws Exception {
        Schema schema = wideSchema(columns, "c");
        String name = "content_" + Math.abs(label.hashCode());
        Table table = create(schema, PartitionSpec.builderFor(schema).day("c_column_3").build(), name, 2);
        table.updateProperties().set("commit.manifest-merge.enabled", "false").commit();
        Random r = new Random(13);
        for (int c = 0; c < 4; c++) {
            RowDelta delta = table.newRowDelta();
            for (int i = 0; i < 300; i++) {
                String dataPath = path(r, "data");
                PartitionData partition = day(table.spec(), 20000 + c);
                DataFiles.Builder data = DataFiles.builder(table.spec()).withPath(dataPath)
                        .withFormat(FileFormat.PARQUET).withFileSizeInBytes(1000).withRecordCount(1000)
                        .withPartition(partition);
                if (dataStats) {
                    data.withMetrics(columnMetrics(table.schema(), r, 1000));
                }
                delta.addRows(data.build());
                if (positionDeletes) {
                    delta.addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                            .withPath(path(r, "pos")).withFormat(FileFormat.PARQUET).withFileSizeInBytes(500)
                            .withRecordCount(10).withPartition(partition)
                            .withMetrics(positionDeleteMetrics(dataPath, 10)).build());
                }
                if (equalityDeletes) {
                    delta.addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofEqualityDeletes(2)
                            .withPath(path(r, "eq")).withFormat(FileFormat.PARQUET).withFileSizeInBytes(500)
                            .withRecordCount(100).withPartition(partition)
                            .withMetrics(equalityDeleteMetrics(r, 100)).build());
                }
            }
            delta.commit();
        }

        CachingIcebergCatalog catalog = newCatalog(catalogProperties);
        StarRocksIcebergTableScanContext scanContext =
                new StarRocksIcebergTableScanContext(CATALOG_NAME, "db", name, PlanMode.LOCAL);
        try (CloseableIterable<FileScanTask> tasks = catalog.getTableScan(table, scanContext)
                .useSnapshot(table.currentSnapshot().snapshotId())
                .filter(Expressions.alwaysTrue())
                .planFiles()) {
            tasks.forEach(task -> { });
        }
        // The cached files reference the table's schema and specs, which the table cache pays for.
        GraphLayout tableGraph = GraphLayout.parseInstance(table, ((BaseTable) table).operations().current());
        List<Object> allRoots = new ArrayList<>();
        String cacheName = dataStats || (!positionDeletes && !equalityDeletes) ? "dataFileCache" : "deleteFileCache";
        for (String each : new String[] {"dataFileCache", "deleteFileCache"}) {
            Cache<String, Set<?>> cache = field(catalog, each);
            List<Object> roots = new ArrayList<>();
            for (Map.Entry<String, Set<?>> e : cache.asMap().entrySet()) {
                roots.add(e.getKey());
                roots.add(e.getValue());
            }
            allRoots.addAll(roots);
            if (each.equals(cacheName)) {
                Assertions.assertFalse(roots.isEmpty(), label + ": " + each + " is empty");
                long actual = GraphLayout.parseInstance(roots.toArray()).subtract(tableGraph).totalSize();
                check(label, each + " ledger", ledger(cache), actual);
            }
        }
        long actual = GraphLayout.parseInstance(allRoots.toArray()).subtract(tableGraph).totalSize();
        check(label, "estimateSize()", catalog.estimateSize(), actual);
    }
}
