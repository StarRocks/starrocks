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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.cost.IcebergFileStats;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergGeoColumnStatisticsTest {
    private static final String CATALOG = "iceberg_geo_stats";

    @TempDir
    Path tableDir;

    @AfterEach
    void clearTables() {
        TestTables.clearTables();
    }

    static Stream<Arguments> geoTables() {
        return Stream.of(Types.GeographyType.crs84(), Types.GeometryType.crs84())
                .flatMap(type -> Stream.of(Arguments.of(type, 3, 3), Arguments.of(type, 4, 4), Arguments.of(type, 3, 4)));
    }

    @ParameterizedTest(name = "{0}, v{1} to v{2}")
    @MethodSource("geoTables")
    void testGeoBounds(Type.PrimitiveType geoType, int firstVersion, int secondVersion) throws Exception {
        BaseTable table = writeTable(geoType, firstVersion, secondVersion);
        List<Column> columns = IcebergApiConverter.toFullSchemas(table.schema());
        IcebergTable srTable = new IcebergTable(1, table.name(), CATALOG, "resource_name", "test_db",
                table.name(), "", columns, table, new HashMap<>());
        ColumnRefOperator id = new ColumnRefOperator(1, columns.get(0).getType(), "id", false);
        ColumnRefOperator shape = new ColumnRefOperator(2, columns.get(1).getType(), "shape", true);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            // Each projection gets a fresh metadata instance, so a prior scan cannot supply its cached statistics.
            assertAll(
                    () -> {
                        IcebergMetadata metadata = newMetadata(executor);
                        Statistics result = planStatistics(metadata, srTable,
                                Map.of(id, columns.get(0), shape, columns.get(1)));
                        assertIdBounds(result.getColumnStatistic(id));
                        ColumnStatistic geo = result.getColumnStatistic(shape);
                        assertEquals(Double.NEGATIVE_INFINITY, geo.getMinValue());
                        assertEquals(Double.POSITIVE_INFINITY, geo.getMaxValue());
                        if (geoType.typeId() == Type.TypeID.GEOGRAPHY) {
                            assertEquals(0.5, geo.getNullsFraction());
                        } else {
                            assertTrue(geo.isUnknown());
                        }
                        assertFileStatistics(metadata, table.schema().findField("shape").fieldId());
                    },
                    // The stats requests for SELECT id and SELECT count(*) omit the geo column.
                    () -> assertIdBounds(planStatistics(newMetadata(executor), srTable, Map.of(id, columns.get(0)))
                            .getColumnStatistic(id)),
                    () -> planStatistics(newMetadata(executor), srTable, Map.of()));
        } finally {
            executor.shutdownNow();
        }
    }

    private static void assertIdBounds(ColumnStatistic id) {
        assertFalse(id.isUnknown());
        assertEquals(1.0, id.getMinValue());
        assertEquals(4.0, id.getMaxValue());
        assertEquals(0.0, id.getNullsFraction());
    }

    private static void assertFileStatistics(IcebergMetadata metadata, int shapeId) throws IllegalAccessException {
        // GEOMETRY is still UNKNOWN in the SQL schema, so verify its retained counts in the file aggregate.
        Object provider = FieldUtils.readField(metadata, "statisticProvider", true);
        Map<?, ?> aggregates = (Map<?, ?>) FieldUtils.readField(provider, "icebergFileStatistics", true);
        assertEquals(1, aggregates.size());
        IcebergFileStats stats = (IcebergFileStats) aggregates.values().iterator().next();
        assertFalse(stats.getMinValues().containsKey(shapeId));
        assertFalse(stats.getMaxValues().containsKey(shapeId));
        assertEquals(4, stats.getRecordCount());
        assertEquals(2L, stats.getNullCounts().get(shapeId));
        assertEquals(4, stats.getColumnSizeRecordCount(shapeId));
        assertTrue(stats.getColumnSizes().get(shapeId) > 0);
    }

    private static Statistics planStatistics(IcebergMetadata metadata, IcebergTable table,
                                             Map<ColumnRefOperator, Column> columns) {
        OptimizerContext context = OptimizerFactory.mockContext(new ColumnRefFactory());
        context.getSessionVariable().setEnableIcebergColumnStatistics(true);
        TvrTableSnapshot snapshot = TvrTableSnapshot.of(Optional.of(table.getNativeTable().currentSnapshot().snapshotId()));
        Statistics statistics = metadata.getTableStatistics(context, table, columns, null, null, -1, snapshot);
        assertEquals(4.0, statistics.getOutputRowCount());
        return statistics;
    }

    private static IcebergMetadata newMetadata(ExecutorService executor) {
        IcebergHiveCatalog catalog = new IcebergHiveCatalog((HiveCatalog) null, new Configuration());
        return new IcebergMetadata(CATALOG, new HdfsEnvironment(), catalog, executor,
                new IcebergCatalogProperties(Map.of("iceberg.catalog.type", "hive")),
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")), null);
    }

    private BaseTable writeTable(Type.PrimitiveType geoType, int firstVersion, int secondVersion) throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "shape", geoType));
        var ops = new TestTables.TestTableOperations(CATALOG, tableDir.toFile());
        ops.commit(null, TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), SortOrder.unsorted(),
                tableDir.toUri().toString(), Map.of(TableProperties.FORMAT_VERSION, Integer.toString(firstVersion))));
        BaseTable table = new BaseTable(ops, CATALOG);
        Path dataDir = Files.createDirectory(tableDir.resolve("data"));
        int shapeId = table.schema().findField("shape").fieldId();
        table.newFastAppend().appendFile(writeFile(table, dataDir, geoType, shapeId, 1)).commit();
        if (firstVersion != secondVersion) {
            table.updateProperties().set(TableProperties.FORMAT_VERSION, Integer.toString(secondVersion)).commit();
        }
        table.newFastAppend().appendFile(writeFile(table, dataDir, geoType, shapeId, 3)).commit();

        Set<FileFormat> formats = table.currentSnapshot().dataManifests(table.io()).stream()
                .map(manifest -> FileFormat.fromFileName(manifest.path())).collect(Collectors.toSet());
        Set<FileFormat> expectedFormats = firstVersion == 3
                ? (secondVersion == 3 ? Set.of(FileFormat.AVRO) : Set.of(FileFormat.AVRO, FileFormat.PARQUET))
                : Set.of(FileFormat.PARQUET);
        assertEquals(expectedFormats, formats);
        try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
            int files = 0;
            for (FileScanTask task : tasks) {
                assertTrue(task.file().lowerBounds().containsKey(shapeId));
                assertTrue(task.file().upperBounds().containsKey(shapeId));
                assertEquals(1L, task.file().nullValueCounts().get(shapeId));
                files++;
            }
            assertEquals(2, files);
        }
        return table;
    }

    private static DataFile writeFile(BaseTable table, Path dataDir, Type.PrimitiveType geoType,
                                      int shapeId, int firstId) throws Exception {
        Path path = dataDir.resolve("part-" + firstId + ".parquet");
        DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(path.toString()))
                .schema(table.schema()).withSpec(table.spec()).createWriterFunc(GenericParquetWriter::create).build();
        try (writer) {
            // A WKB POINT has a byte-order flag, a type ID, and two doubles.
            ByteBuffer point = ByteBuffer.allocate(1 + Integer.BYTES + 2 * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            point.put((byte) 1).putInt(1).putDouble(firstId).putDouble(10).flip();
            GenericRecord row = GenericRecord.create(table.schema());
            writer.write(row.copy("id", firstId, "shape", point));
            writer.write(row.copy("id", firstId + 1));
        }
        DataFile file = writer.toDataFile();
        if (geoType.typeId() == Type.TypeID.GEOGRAPHY) {
            // The generic GEOGRAPHY writer emits counts. Supply the singleton point's bounds in the manifest.
            ByteBuffer bound = Conversions.toByteBuffer(geoType, GeospatialBound.createXY(firstId, 10));
            Map<Integer, ByteBuffer> lower = new HashMap<>(file.lowerBounds());
            Map<Integer, ByteBuffer> upper = new HashMap<>(file.upperBounds());
            lower.put(shapeId, bound);
            upper.put(shapeId, bound.duplicate());
            return DataFiles.builder(table.spec()).copy(file)
                    .withMetrics(new Metrics(file.recordCount(), file.columnSizes(), file.valueCounts(),
                            file.nullValueCounts(), file.nanValueCounts(), lower, upper)).build();
        }
        return file;
    }
}
