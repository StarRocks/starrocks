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
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarcharType.VARCHAR;

// Reading Iceberg format-version 4 tables. Iceberg 1.12 writes v4 manifests as Parquet (the manifest list stays Avro),
// so every planning path that reads manifests -- file planning, delete-file matching, row-count and column statistics
// -- must handle Parquet manifests, and tables upgraded from v3 mix Avro and Parquet manifests.
public class IcebergFormatV4ReadTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    private static final PartitionSpec SPEC_K2 = PartitionSpec.builderFor(SCHEMA_J).identity("k2").build();
    private static final ColumnRefOperator K1 = new ColumnRefOperator(2, INT, "k1", true);
    private static final ColumnRefOperator K2 = new ColumnRefOperator(3, VARCHAR, "k2", true);

    private static DataFile dataFile(String name, String partition, long rows, int k1Min, int k1Max) {
        ByteBuffer lower = Conversions.toByteBuffer(Types.IntegerType.get(), k1Min);
        ByteBuffer upper = Conversions.toByteBuffer(Types.IntegerType.get(), k1Max);
        return DataFiles.builder(SPEC_K2).withPath("/path/to/" + name + ".parquet").withFileSizeInBytes(20)
                .withPartitionPath(partition).withRecordCount(rows)
                .withMetrics(new Metrics(rows, null, Map.of(2, rows), Map.of(2, 0L), null,
                        Map.of(2, lower), Map.of(2, upper)))
                .build();
    }

    private static DeleteFile dvOn(DataFile file, String partition, long deletedRows) {
        return FileMetadata.deleteFileBuilder(SPEC_K2).ofPositionDeletes()
                .withPath(file.location() + "-dv.puffin").withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(100).withRecordCount(deletedRows)
                .withReferencedDataFile(file.location())
                .withContentOffset(4).withContentSizeInBytes(40)
                .withPartitionPath(partition).build();
    }

    // k2=2020-06-14: 10 rows, k1 in [1, 10]; k2=2020-06-15: 6 rows, k1 in [20, 30]; a DV deletes 4 rows of the first.
    private TestTables.TestTable tableWithDv(String name, int formatVersion) {
        TestTables.TestTable table = create(SCHEMA_J, SPEC_K2, name, formatVersion);
        DataFile f0614 = dataFile(name + "-0614", "k2=2020-06-14", 10, 1, 10);
        table.newFastAppend().appendFile(f0614).commit();
        table.newFastAppend().appendFile(dataFile(name + "-0615", "k2=2020-06-15", 6, 20, 30)).commit();
        table.newRowDelta().addDeletes(dvOn(f0614, "k2=2020-06-14", 4)).commit();
        table.refresh();
        return table;
    }

    private static IcebergTable icebergTable(TestTables.TestTable table) {
        List<Column> columns = Lists.newArrayList(
                new Column("id", INT), new Column("k1", INT), new Column("k2", VARCHAR));
        return new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                table.name(), "", columns, table, Maps.newHashMap());
    }

    private static IcebergMetadata newMetadata() {
        IcebergHiveCatalog catalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(),
                IcebergMetadataTest.DEFAULT_CONFIG);
        return new IcebergMetadata(CATALOG_NAME, IcebergMetadataTest.HDFS_ENVIRONMENT, catalog,
                Executors.newSingleThreadExecutor(), IcebergMetadataTest.DEFAULT_CATALOG_PROPERTIES,
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")), null);
    }

    // Data file location -> locations of its delete files, as planned by StarRocks.
    private static Map<String, List<String>> plannedFiles(IcebergMetadata metadata, TestTables.TestTable table,
                                                          ScalarOperator predicate) {
        GetRemoteFilesParams.Builder params = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(TvrTableSnapshot.of(Optional.of(table.currentSnapshot().snapshotId())))
                .setFieldNames(Lists.newArrayList());
        if (predicate != null) {
            params.setPredicate(predicate);
        }
        List<RemoteFileInfo> infos = metadata.getRemoteFiles(icebergTable(table), params.build());
        return infos.stream().map(info -> ((IcebergRemoteFileInfo) info).getFileScanTask())
                .collect(Collectors.toMap(task -> task.file().location(),
                        task -> task.deletes().stream().map(DeleteFile::location).sorted().collect(Collectors.toList())));
    }

    private static Statistics tableStatistics(IcebergMetadata metadata, TestTables.TestTable table,
                                              boolean columnStatistics) {
        OptimizerContext context = OptimizerFactory.mockContext(new ColumnRefFactory());
        context.getSessionVariable().setEnableIcebergColumnStatistics(columnStatistics);
        return metadata.getTableStatistics(context, icebergTable(table),
                Map.of(K1, new Column("k1", INT), K2, new Column("k2", VARCHAR)), null, null, -1,
                TvrTableSnapshot.of(Optional.of(table.currentSnapshot().snapshotId())));
    }

    private static boolean allManifestsHaveFormat(TestTables.TestTable table, FileFormat format) {
        List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
        return !manifests.isEmpty() && manifests.stream()
                .allMatch(m -> FileFormat.fromFileName(m.path()) == format);
    }

    @Test
    public void testV4TableWritesParquetManifests() {
        TestTables.TestTable table = tableWithDv("tv4_manifests", 4);
        Assertions.assertEquals(4, table.ops().current().formatVersion());
        Assertions.assertTrue(allManifestsHaveFormat(table, FileFormat.PARQUET),
                "the test relies on Iceberg writing Parquet manifests for v4 tables");
    }

    @Test
    public void testPlanFilesWithDeletionVector() {
        TestTables.TestTable table = tableWithDv("tv4_plan", 4);
        IcebergMetadata metadata = newMetadata();
        String f0614 = "/path/to/tv4_plan-0614.parquet";
        String f0615 = "/path/to/tv4_plan-0615.parquet";

        Assertions.assertEquals(Map.of(f0614, List.of(f0614 + "-dv.puffin"), f0615, List.of()),
                plannedFiles(metadata, table, null));

        // Partition pruning through the Parquet manifests' partition summaries and entries.
        ScalarOperator k2Eq0615 = new BinaryPredicateOperator(BinaryType.EQ, K2,
                ConstantOperator.createVarchar("2020-06-15"));
        Assertions.assertEquals(Map.of(f0615, List.of()), plannedFiles(metadata, table, k2Eq0615));

        // Pruning by the k1 bounds stored in the Parquet manifest entries.
        ScalarOperator k1Gt15 = new BinaryPredicateOperator(BinaryType.GT, K1, ConstantOperator.createInt(15));
        Assertions.assertEquals(Map.of(f0615, List.of()), plannedFiles(metadata, table, k1Gt15));
    }

    @Test
    public void testRowCountFromParquetManifests() {
        TestTables.TestTable table = tableWithDv("tv4_rows", 4);
        IcebergMetadata metadata = newMetadata();
        // 10 + 6 rows on both statistics paths; the row-count estimate sums data file records and ignores deletes.
        Assertions.assertEquals(16.0, tableStatistics(metadata, table, false).getOutputRowCount(), 0.001);
        Assertions.assertEquals(16.0, tableStatistics(metadata, table, true).getOutputRowCount(), 0.001);
    }

    @Test
    public void testColumnStatisticsFromParquetManifests() {
        TestTables.TestTable table = tableWithDv("tv4_colstats", 4);
        ColumnStatistic k1 = tableStatistics(newMetadata(), table, true).getColumnStatistic(K1);
        Assertions.assertFalse(k1.isUnknown());
        Assertions.assertEquals(1.0, k1.getMinValue(), 0.001);
        Assertions.assertEquals(30.0, k1.getMaxValue(), 0.001);
    }

    @Test
    public void testUpgradedTableMixesAvroAndParquetManifests() {
        TestTables.TestTable table = tableWithDv("tv3_to_v4", 3);
        Assertions.assertTrue(allManifestsHaveFormat(table, FileFormat.AVRO));
        // Iceberg refuses to upgrade a table whose location has no URI scheme to v4.
        table.updateLocation().setLocation("file:" + table.location()).commit();
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
        DataFile f0616 = dataFile("tv3_to_v4-0616", "k2=2020-06-16", 5, 40, 50);
        table.newFastAppend().appendFile(f0616).commit();
        table.newRowDelta().addDeletes(dvOn(f0616, "k2=2020-06-16", 1)).commit();
        table.refresh();
        List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
        Assertions.assertTrue(manifests.stream().anyMatch(m -> FileFormat.fromFileName(m.path()) == FileFormat.AVRO));
        Assertions.assertTrue(manifests.stream().anyMatch(m -> FileFormat.fromFileName(m.path()) == FileFormat.PARQUET));

        IcebergMetadata metadata = newMetadata();
        String f0614 = "/path/to/tv3_to_v4-0614.parquet";
        Assertions.assertEquals(Map.of(
                f0614, List.of(f0614 + "-dv.puffin"),
                "/path/to/tv3_to_v4-0615.parquet", List.of(),
                f0616.location(), List.of(f0616.location() + "-dv.puffin")), plannedFiles(metadata, table, null));
        // 10 + 6 + 5 rows; the row-count estimate ignores deletes.
        Assertions.assertEquals(21.0, tableStatistics(metadata, table, false).getOutputRowCount(), 0.001);
        Assertions.assertEquals(21.0, tableStatistics(metadata, table, true).getOutputRowCount(), 0.001);
    }

    @Test
    public void testIcebergScanAgreesWithStarRocksPlanning() {
        TestTables.TestTable table = tableWithDv("tv4_oracle", 4);
        Map<String, List<String>> expected = Lists.newArrayList(table.newScan().planFiles()).stream()
                .collect(Collectors.toMap((FileScanTask task) -> task.file().location(),
                        task -> task.deletes().stream().map(DeleteFile::location).sorted().collect(Collectors.toList())));
        Assertions.assertEquals(expected, plannedFiles(newMetadata(), table, null));
    }
}
