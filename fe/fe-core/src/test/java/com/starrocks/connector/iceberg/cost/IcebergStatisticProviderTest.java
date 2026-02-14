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

package com.starrocks.connector.iceberg.cost;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.cost.IcebergFileStats.convertObjectToOptionalDouble;

public class IcebergStatisticProviderTest extends TableTestBase {
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\"type\"=\"iceberg\", " +
                "\"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);
    }

    @Test
    public void testUnknownTableStatistics() {
        IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, StringType.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", IntegerType.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", StringType.STRING));

        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableA.currentSnapshot().snapshotId()));
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(version)
                .build();
        Statistics statistics = statisticProvider.getTableStatistics(icebergTable, colRefToColumnMetaMap, null, params);
        Assertions.assertEquals(1.0, statistics.getOutputRowCount(), 0.001);
    }

    @Test
    public void testMakeTableStatisticsWithStructField() {
        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.of(1, false, "col1", new Types.LongType()));
        fields.add(Types.NestedField.of(2, false, "col2", new Types.DateType()));

        List<Types.NestedField> structFields = new ArrayList<>();
        structFields.add(Types.NestedField.of(4, false, "col4", new Types.LongType()));
        structFields.add(Types.NestedField.of(5, false, "col5", new Types.DoubleType()));
        fields.add(Types.NestedField.of(3, false, "col3", Types.StructType.of(structFields)));

        Map<Integer, org.apache.iceberg.types.Type.PrimitiveType> idToTypeMapping = fields.stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, column -> column.type().asPrimitiveType()));

        Map<Integer, ByteBuffer> bounds = Maps.newHashMap();
        bounds.put(1, ByteBuffer.allocate(8));
        bounds.put(2, ByteBuffer.allocate(8));
        bounds.put(4, ByteBuffer.allocate(8));
        bounds.put(5, ByteBuffer.allocate(8));

        Map<Integer, Object> result = IcebergFileStats.toMap(idToTypeMapping, bounds);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGetEmptyTableStatistics() {
        IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, StringType.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", IntegerType.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", StringType.STRING));

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().
                setTableVersionRange(TvrTableSnapshot.empty()).build();
        Statistics statistics = statisticProvider.getTableStatistics(icebergTable, colRefToColumnMetaMap,
                null, params);
        Assertions.assertEquals(1.0, statistics.getOutputRowCount(), 0.001);
    }

    @Test
    public void testDoubleValue() {
        Assertions.assertEquals(1.0, convertObjectToOptionalDouble(Types.BooleanType.get(), true).get(), 0.001);
        Assertions.assertEquals(1.0, convertObjectToOptionalDouble(Types.IntegerType.get(), 1).get(), 0.001);
        Assertions.assertEquals(1.0, convertObjectToOptionalDouble(Types.LongType.get(), 1L).get(), 0.001);
        Assertions.assertEquals(1.0, convertObjectToOptionalDouble(Types.FloatType.get(), Float.valueOf("1")).get(), 0.001);
        Assertions.assertEquals(1.0, convertObjectToOptionalDouble(Types.DoubleType.get(), 1.0).get(), 0.001);
        Assertions.assertEquals(121.0, convertObjectToOptionalDouble(Types.DecimalType.of(5, 2), new BigDecimal(121)).get(),
                0.001);
        Assertions.assertFalse(convertObjectToOptionalDouble(Types.BinaryType.get(), "11").isPresent());
    }

    /**
     * Helper to build an IcebergFileStats directly (bypassing DataFiles.builder which may not
     * preserve column-level metrics in newer Iceberg versions).
     */
    private IcebergFileStats buildFileStats(
            Map<Integer, org.apache.iceberg.types.Type.PrimitiveType> idToTypeMapping,
            List<Types.NestedField> nonPartitionPrimitiveColumns,
            long recordCount, long size,
            Map<Integer, Object> minValues,
            Map<Integer, Object> maxValues,
            Map<Integer, Long> nullCounts,
            Map<Integer, Long> columnSizes) {
        return new IcebergFileStats(idToTypeMapping, nonPartitionPrimitiveColumns,
                recordCount, size, minValues, maxValues, nullCounts, columnSizes);
    }

    private Map<Integer, org.apache.iceberg.types.Type.PrimitiveType> getIdToTypeMappingB() {
        return mockedNativeTableB.schema().columns().stream()
                .filter(c -> c.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, c -> c.type().asPrimitiveType()));
    }

    private List<Types.NestedField> getNonPartitionColumnsB() {
        return mockedNativeTableB.schema().columns().stream()
                .filter(c -> c.fieldId() != 2 && c.type().isPrimitiveType())
                .collect(Collectors.toList());
    }

    @Test
    public void testAverageRowSizeWithColumnSizes() {
        Map<Integer, org.apache.iceberg.types.Type.PrimitiveType> idToTypeMapping = getIdToTypeMappingB();
        List<Types.NestedField> nonPartitionCols = getNonPartitionColumnsB();

        IcebergFileStats fileStats = buildFileStats(idToTypeMapping, nonPartitionCols,
                2, 20,
                ImmutableMap.of(1, 1, 2, 2),
                ImmutableMap.of(1, 2, 2, 2),
                ImmutableMap.of(1, 0L, 2, 0L),
                ImmutableMap.of(1, 50L, 2, 50L));

        IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_1).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableB.currentSnapshot().snapshotId()));
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(version)
                .build();

        PredicateSearchKey key = PredicateSearchKey.of(icebergTable.getCatalogDBName(),
                icebergTable.getCatalogTableName(), params);
        statisticProvider.putIcebergFileStats(key, fileStats);

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        ColumnRefOperator k1Ref = new ColumnRefOperator(3, IntegerType.INT, "k1", true);
        ColumnRefOperator k2Ref = new ColumnRefOperator(4, IntegerType.INT, "k2", true);
        colRefToColumnMetaMap.put(k1Ref, new Column("k1", IntegerType.INT));
        colRefToColumnMetaMap.put(k2Ref, new Column("k2", IntegerType.INT));

        Statistics statistics = statisticProvider.getTableStatistics(icebergTable, colRefToColumnMetaMap, null, params);

        // k1 (fieldId=1): averageRowSize = max(50.0/2, 4) = 25.0
        ColumnStatistic k1Stat = statistics.getColumnStatistic(k1Ref);
        Assertions.assertEquals(25.0, k1Stat.getAverageRowSize(), 0.001);

        // k2 (fieldId=2): also has columnSizes
        ColumnStatistic k2Stat = statistics.getColumnStatistic(k2Ref);
        Assertions.assertEquals(25.0, k2Stat.getAverageRowSize(), 0.001);
    }

    @Test
    public void testAverageRowSizeWithPartialColumnSizes() {
        Map<Integer, org.apache.iceberg.types.Type.PrimitiveType> idToTypeMapping = getIdToTypeMappingB();
        List<Types.NestedField> nonPartitionCols = getNonPartitionColumnsB();

        // File 1: 1000 rows with columnSizes
        IcebergFileStats fileStats = buildFileStats(idToTypeMapping, nonPartitionCols,
                1000, 5000,
                ImmutableMap.of(1, 1, 2, 2),
                ImmutableMap.of(1, 100, 2, 200),
                ImmutableMap.of(1, 0L, 2, 0L),
                ImmutableMap.of(1, 25000L, 2, 25000L));

        // File 2: 2000 rows without columnSizes (simulates older Iceberg writer)
        fileStats.incrementRecordCount(2000);
        fileStats.incrementSize(10000);
        // updateColumnSizes with null should NOT increment columnSizeRecordCounts
        IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();
        statisticProvider.updateColumnSizes(fileStats, null, 2000);

        // File 3: 1500 rows with columnSizes
        fileStats.incrementRecordCount(1500);
        fileStats.incrementSize(7500);
        statisticProvider.updateColumnSizes(fileStats, ImmutableMap.of(1, 37500L, 2, 37500L), 1500);

        // Total: recordCount=4500, columnSizeRecordCount=2500 (File1+File3)
        // columnSizes[1] = 25000 + 37500 = 62500
        // averageRowSize = max(62500/2500, 4) = max(25.0, 4) = 25.0
        // Without fix: 62500/4500 = 13.89 (underestimated)

        mockedNativeTableB.newFastAppend().appendFile(FILE_B_1).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableB.currentSnapshot().snapshotId()));
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(version)
                .build();

        PredicateSearchKey key = PredicateSearchKey.of(icebergTable.getCatalogDBName(),
                icebergTable.getCatalogTableName(), params);
        statisticProvider.putIcebergFileStats(key, fileStats);

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        ColumnRefOperator k1Ref = new ColumnRefOperator(3, IntegerType.INT, "k1", true);
        colRefToColumnMetaMap.put(k1Ref, new Column("k1", IntegerType.INT));

        Statistics statistics = statisticProvider.getTableStatistics(icebergTable, colRefToColumnMetaMap, null, params);

        ColumnStatistic k1Stat = statistics.getColumnStatistic(k1Ref);
        Assertions.assertEquals(25.0, k1Stat.getAverageRowSize(), 0.001);
    }

    @Test
    public void testAverageRowSizeWithoutColumnSizes() {
        IcebergFileStats fileStats = new IcebergFileStats(4);

        IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_5).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableB.currentSnapshot().snapshotId()));
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(version)
                .build();

        PredicateSearchKey key = PredicateSearchKey.of(icebergTable.getCatalogDBName(),
                icebergTable.getCatalogTableName(), params);
        statisticProvider.putIcebergFileStats(key, fileStats);

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        ColumnRefOperator k1Ref = new ColumnRefOperator(3, IntegerType.INT, "k1", true);
        colRefToColumnMetaMap.put(k1Ref, new Column("k1", IntegerType.INT));

        Statistics statistics = statisticProvider.getTableStatistics(icebergTable, colRefToColumnMetaMap, null, params);

        // No columnSizes → fallback to type size (INT = 4 bytes)
        ColumnStatistic k1Stat = statistics.getColumnStatistic(k1Ref);
        Assertions.assertEquals(4.0, k1Stat.getAverageRowSize(), 0.001);
    }

    @Test
    public void testPerFieldColumnSizeRecordCounts() {
        // Use unpartitioned table (mockedNativeTableH) to include all columns
        List<Types.NestedField> allColumns = Lists.newArrayList(
                Types.NestedField.required(1, "k1", Types.IntegerType.get()),
                Types.NestedField.required(2, "k2", Types.IntegerType.get())
        );
        Map<Integer, org.apache.iceberg.types.Type.PrimitiveType> idToTypeMapping = allColumns.stream()
                .collect(Collectors.toMap(Types.NestedField::fieldId, c -> c.type().asPrimitiveType()));

        // File 1: 100 rows, both fields have columnSizes
        IcebergFileStats fileStats = buildFileStats(idToTypeMapping, allColumns,
                100, 1000,
                ImmutableMap.of(1, 1, 2, 1),
                ImmutableMap.of(1, 100, 2, 100),
                ImmutableMap.of(1, 0L, 2, 0L),
                ImmutableMap.of(1, 2500L, 2, 5000L));  // k1: 2500 bytes, k2: 5000 bytes

        IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();

        // File 2: 200 rows, only field 1 has columnSize
        fileStats.incrementRecordCount(200);
        fileStats.incrementSize(2000);
        statisticProvider.updateColumnSizes(fileStats, ImmutableMap.of(1, 5000L), 200);  // only k1

        // File 3: 300 rows, only field 2 has columnSize
        fileStats.incrementRecordCount(300);
        fileStats.incrementSize(3000);
        statisticProvider.updateColumnSizes(fileStats, ImmutableMap.of(2, 15000L), 300);  // only k2

        // Expected per-field record counts:
        // field 1: 100 (File1) + 200 (File2) = 300
        // field 2: 100 (File1) + 300 (File3) = 400

        // Expected columnSizes:
        // field 1: 2500 + 5000 = 7500
        // field 2: 5000 + 15000 = 20000

        // Expected averageRowSize:
        // field 1: max(7500/300, 4) = max(25.0, 4) = 25.0
        // field 2: max(20000/400, 4) = max(50.0, 4) = 50.0

        Assertions.assertEquals(300, fileStats.getColumnSizeRecordCount(1));
        Assertions.assertEquals(400, fileStats.getColumnSizeRecordCount(2));

        mockedNativeTableG.newFastAppend().appendFile(FILE_B_5).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableG, Maps.newHashMap());

        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableG.currentSnapshot().snapshotId()));
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(version)
                .build();

        PredicateSearchKey key = PredicateSearchKey.of(icebergTable.getCatalogDBName(),
                icebergTable.getCatalogTableName(), params);
        statisticProvider.putIcebergFileStats(key, fileStats);

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        ColumnRefOperator k1Ref = new ColumnRefOperator(3, IntegerType.INT, "k1", true);
        ColumnRefOperator k2Ref = new ColumnRefOperator(4, IntegerType.INT, "k2", true);
        colRefToColumnMetaMap.put(k1Ref, new Column("k1", IntegerType.INT));
        colRefToColumnMetaMap.put(k2Ref, new Column("k2", IntegerType.INT));

        Statistics statistics = statisticProvider.getTableStatistics(icebergTable, colRefToColumnMetaMap, null, params);

        ColumnStatistic k1Stat = statistics.getColumnStatistic(k1Ref);
        ColumnStatistic k2Stat = statistics.getColumnStatistic(k2Ref);

        // Per-field averageRowSize calculation
        Assertions.assertEquals(25.0, k1Stat.getAverageRowSize(), 0.001);
        Assertions.assertEquals(50.0, k2Stat.getAverageRowSize(), 0.001);
    }

    @Test
    public void testColumnSizeRecordCountNotInflatedWhenStatsInvalid() {
        // Use all columns (no partition filtering)
        List<Types.NestedField> allColumns = Lists.newArrayList(
                Types.NestedField.required(1, "k1", Types.IntegerType.get()),
                Types.NestedField.required(2, "k2", Types.IntegerType.get())
        );
        Map<Integer, org.apache.iceberg.types.Type.PrimitiveType> idToTypeMapping = allColumns.stream()
                .collect(Collectors.toMap(Types.NestedField::fieldId, c -> c.type().asPrimitiveType()));

        // File 1: 100 rows with valid stats and columnSizes
        IcebergFileStats fileStats = buildFileStats(idToTypeMapping, allColumns,
                100, 1000,
                ImmutableMap.of(1, 1, 2, 1),
                ImmutableMap.of(1, 100, 2, 100),
                ImmutableMap.of(1, 0L, 2, 0L),
                ImmutableMap.of(1, 2500L, 2, 2500L));

        Assertions.assertTrue(fileStats.hasValidColumnMetrics());
        Assertions.assertEquals(100, fileStats.getColumnSizeRecordCount(1));

        IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();

        // File 2: 200 rows with null min/max stats (invalidates hasValidColumnMetrics)
        // but still has columnSizes
        fileStats.incrementRecordCount(200);
        fileStats.incrementSize(2000);
        // Simulate updateStats with null bounds which sets hasValidColumnMetrics = false
        fileStats.updateStats(fileStats.getMinValues(), null, null, 200, i -> i < 0);

        Assertions.assertFalse(fileStats.hasValidColumnMetrics());

        // Now updateColumnSizes should NOT add to columnSizeRecordCounts because hasValidColumnMetrics is false
        statisticProvider.updateColumnSizes(fileStats, ImmutableMap.of(1, 5000L, 2, 5000L), 200);

        // columnSizeRecordCount should still be 100 (only from File 1)
        Assertions.assertEquals(100, fileStats.getColumnSizeRecordCount(1));
        Assertions.assertEquals(100, fileStats.getColumnSizeRecordCount(2));

        // columnSizes should also not be updated (still 2500)
        Assertions.assertEquals(2500L, fileStats.getColumnSizes().get(1).longValue());
    }

}
