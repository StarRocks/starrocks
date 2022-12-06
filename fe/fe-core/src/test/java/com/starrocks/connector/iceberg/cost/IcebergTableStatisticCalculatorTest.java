// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg.cost;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.connector.iceberg.cost.IcebergFileStats;
import com.starrocks.connector.iceberg.cost.IcebergTableStatisticCalculator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergTableStatisticCalculatorTest {

    @Test
    public void testMakeTableStatistics(@Mocked Table iTable) {
        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.of(1, false, "col1", new Types.LongType()));
        fields.add(Types.NestedField.of(2, false, "col2", new Types.DateType()));
        Schema schema = new Schema(fields);

        new Expectations() {
            {
                iTable.schema();
                result = schema;
            }
            {
                // empty iceberg's snapshot is null or snapshot is not null but no datafile.
                // so here mock iceberg table with null snapshot
                iTable.currentSnapshot();
                result = null;
            }
        };

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1000, Type.BIGINT, "col1", true);
        colRefToColumnMetaMap.put(columnRefOperator, new Column("col1", Type.BIGINT));
        Statistics statistics = IcebergTableStatisticCalculator.getTableStatistics(null, iTable, colRefToColumnMetaMap);
        Assert.assertNotNull(statistics);
        statistics.getColumnStatistic(columnRefOperator);
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
        Assert.assertNotNull(result);
    }

    @Test
    public void testMakeTableStatisticsWithArrayField(@Mocked Table iTable) {
        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.of(1, false, "col1", new Types.LongType()));
        fields.add(Types.NestedField.of(2, false, "col2", new Types.DateType()));
        fields.add(Types.NestedField.of(3, false, "colArray",
                Types.ListType.ofOptional(4, new Types.IntegerType())));
        Schema schema = new Schema(fields);

        new Expectations() {
            {
                iTable.schema();
                result = schema;
            }
            {
                // empty iceberg's snapshot is null or snapshot is not null but no datafile.
                // so here mock iceberg table with null snapshot
                iTable.currentSnapshot();
                result = null;
            }
        };

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(1000, Type.BIGINT, "col1", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(1001, Type.ARRAY_INT, "colArray", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("col1", Type.BIGINT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("colArray", Type.ARRAY_INT));
        Statistics statistics = IcebergTableStatisticCalculator.getTableStatistics(null, iTable, colRefToColumnMetaMap);
        Assert.assertNotNull(statistics);
        ColumnStatistic arrayStatistic = statistics.getColumnStatistic(columnRefOperator2);
        Assert.assertNotNull(arrayStatistic);
    }
}
