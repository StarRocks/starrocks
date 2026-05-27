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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.Column;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.thrift.TExprMinMaxValue;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PaimonMinMaxUtilTest {

    private SlotDescriptor createSlotDescriptor(int slotId, String name, com.starrocks.type.Type type) {
        SlotDescriptor sd = new SlotDescriptor(new SlotId(slotId), name, type, true);
        sd.setColumn(new Column(name, type));
        return sd;
    }

    private BinaryArray createNullCountArray(Long... values) {
        return BinaryArray.fromLongArray(values);
    }

    @Test
    public void testIntegerMinMax() {
        // Schema: id INT, value INT
        RowType rowType = RowType.of(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "value", DataTypes.INT()));

        BinaryRow minRow = new BinaryRow(2);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.writeInt(0, 10);
        minWriter.writeInt(1, 100);
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(2);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.writeInt(0, 50);
        maxWriter.writeInt(1, 500);
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L, 5L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "id", IntegerType.INT),
                createSlotDescriptor(2, "value", IntegerType.INT)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertEquals(2, result.size());

        // Check id column
        TExprMinMaxValue idValue = result.get(1);
        Assertions.assertNotNull(idValue);
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, idValue.getType());
        Assertions.assertEquals(10, idValue.getMin_int_value());
        Assertions.assertEquals(50, idValue.getMax_int_value());
        Assertions.assertFalse(idValue.isHas_null());
        Assertions.assertFalse(idValue.isAll_null());

        // Check value column
        TExprMinMaxValue valueValue = result.get(2);
        Assertions.assertNotNull(valueValue);
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, valueValue.getType());
        Assertions.assertEquals(100, valueValue.getMin_int_value());
        Assertions.assertEquals(500, valueValue.getMax_int_value());
        Assertions.assertTrue(valueValue.isHas_null());
        Assertions.assertFalse(valueValue.isAll_null());
    }

    @Test
    public void testLongMinMax() {
        RowType rowType = RowType.of(
                new DataField(0, "big_id", DataTypes.BIGINT()));

        BinaryRow minRow = new BinaryRow(1);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.writeLong(0, Long.MIN_VALUE);
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(1);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.writeLong(0, Long.MAX_VALUE);
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "big_id", IntegerType.BIGINT)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertEquals(1, result.size());
        TExprMinMaxValue value = result.get(1);
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, value.getType());
        Assertions.assertEquals(Long.MIN_VALUE, value.getMin_int_value());
        Assertions.assertEquals(Long.MAX_VALUE, value.getMax_int_value());
    }

    @Test
    public void testDoubleMinMax() {
        RowType rowType = RowType.of(
                new DataField(0, "price", DataTypes.DOUBLE()));

        BinaryRow minRow = new BinaryRow(1);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.writeDouble(0, 1.5);
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(1);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.writeDouble(0, 99.9);
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "price", FloatType.DOUBLE)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertEquals(1, result.size());
        TExprMinMaxValue value = result.get(1);
        Assertions.assertEquals(TExprNodeType.FLOAT_LITERAL, value.getType());
        Assertions.assertEquals(1.5, value.getMin_float_value(), 0.001);
        Assertions.assertEquals(99.9, value.getMax_float_value(), 0.001);
    }

    @Test
    public void testFloatMinMax() {
        RowType rowType = RowType.of(
                new DataField(0, "score", DataTypes.FLOAT()));

        BinaryRow minRow = new BinaryRow(1);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.writeFloat(0, 0.5f);
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(1);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.writeFloat(0, 10.0f);
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "score", FloatType.FLOAT)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertEquals(1, result.size());
        TExprMinMaxValue value = result.get(1);
        Assertions.assertEquals(TExprNodeType.FLOAT_LITERAL, value.getType());
        Assertions.assertEquals(0.5f, (float) value.getMin_float_value(), 0.001);
        Assertions.assertEquals(10.0f, (float) value.getMax_float_value(), 0.001);
    }

    @Test
    public void testBooleanMinMax() {
        RowType rowType = RowType.of(
                new DataField(0, "flag", DataTypes.BOOLEAN()));

        BinaryRow minRow = new BinaryRow(1);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.writeBoolean(0, false);
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(1);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.writeBoolean(0, true);
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "flag", BooleanType.BOOLEAN)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertEquals(1, result.size());
        TExprMinMaxValue value = result.get(1);
        Assertions.assertEquals(TExprNodeType.BOOL_LITERAL, value.getType());
        Assertions.assertEquals(0, value.getMin_int_value());
        Assertions.assertEquals(1, value.getMax_int_value());
    }

    @Test
    public void testAllNullColumn() {
        RowType rowType = RowType.of(
                new DataField(0, "id", DataTypes.INT()));

        BinaryRow minRow = new BinaryRow(1);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.setNullAt(0);
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(1);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.setNullAt(0);
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(100L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "id", IntegerType.INT)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertEquals(1, result.size());
        TExprMinMaxValue value = result.get(1);
        Assertions.assertEquals(TExprNodeType.NULL_LITERAL, value.getType());
        Assertions.assertTrue(value.isAll_null());
        Assertions.assertTrue(value.isHas_null());
    }

    @Test
    public void testEmptyStats() {
        RowType rowType = RowType.of(
                new DataField(0, "id", DataTypes.INT()));

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "id", IntegerType.INT)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, SimpleStats.EMPTY_STATS, null, 100L, slots);

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testNullStats() {
        RowType rowType = RowType.of(
                new DataField(0, "id", DataTypes.INT()));

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "id", IntegerType.INT)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, null, null, 100L, slots);

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testValueStatsColsSubset() {
        // Schema: id INT, name STRING, value INT
        // Stats only available for "id" and "value"
        RowType rowType = RowType.of(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "name", DataTypes.STRING()),
                new DataField(2, "value", DataTypes.INT()));

        // Build min/max with 2 fields (matching valueStatsCols size)
        BinaryRow minRow = new BinaryRow(2);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.writeInt(0, 1);    // id min
        minWriter.writeInt(1, 10);   // value min
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(2);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.writeInt(0, 100);  // id max
        maxWriter.writeInt(1, 200);  // value max
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L, 0L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<String> valueStatsCols = Arrays.asList("id", "value");

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "id", IntegerType.INT),
                createSlotDescriptor(2, "name", TypeFactory.createDefaultCatalogString()),
                createSlotDescriptor(3, "value", IntegerType.INT)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, valueStatsCols, 100L, slots);

        // "name" column is STRING (unsupported) and also not in valueStatsCols
        Assertions.assertEquals(2, result.size());
        Assertions.assertNull(result.get(2)); // name not present

        TExprMinMaxValue idValue = result.get(1);
        Assertions.assertNotNull(idValue);
        Assertions.assertEquals(1, idValue.getMin_int_value());
        Assertions.assertEquals(100, idValue.getMax_int_value());

        TExprMinMaxValue valValue = result.get(3);
        Assertions.assertNotNull(valValue);
        Assertions.assertEquals(10, valValue.getMin_int_value());
        Assertions.assertEquals(200, valValue.getMax_int_value());
    }

    @Test
    public void testUnsupportedTypeSkipped() {
        // STRING and DECIMAL types should be skipped
        RowType rowType = RowType.of(
                new DataField(0, "name", DataTypes.STRING()),
                new DataField(1, "amount", DataTypes.DECIMAL(10, 2)));

        BinaryRow minRow = new BinaryRow(2);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.setNullAt(0);
        minWriter.setNullAt(1);
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(2);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.setNullAt(0);
        maxWriter.setNullAt(1);
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L, 0L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "name", TypeFactory.createDefaultCatalogString()),
                createSlotDescriptor(2, "amount", TypeFactory.createDecimalV3NarrowestType(10, 2))
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testDateMinMax() {
        RowType rowType = RowType.of(
                new DataField(0, "dt", DataTypes.DATE()));

        BinaryRow minRow = new BinaryRow(1);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.writeInt(0, 18628); // 2021-01-01 days since epoch
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(1);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.writeInt(0, 18993); // 2022-01-01 days since epoch
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "dt", DateType.DATE)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertEquals(1, result.size());
        TExprMinMaxValue value = result.get(1);
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, value.getType());
        Assertions.assertEquals(18628, value.getMin_int_value());
        Assertions.assertEquals(18993, value.getMax_int_value());
    }

    @Test
    public void testMixedTypes() {
        // Schema with multiple types
        RowType rowType = RowType.of(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "score", DataTypes.DOUBLE()),
                new DataField(2, "active", DataTypes.BOOLEAN()));

        BinaryRow minRow = new BinaryRow(3);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.writeInt(0, 1);
        minWriter.writeDouble(1, 0.5);
        minWriter.writeBoolean(2, false);
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(3);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.writeInt(0, 100);
        maxWriter.writeDouble(1, 99.9);
        maxWriter.writeBoolean(2, true);
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L, 2L, 0L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "id", IntegerType.INT),
                createSlotDescriptor(2, "score", FloatType.DOUBLE),
                createSlotDescriptor(3, "active", BooleanType.BOOLEAN)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertEquals(3, result.size());

        // INT
        TExprMinMaxValue idVal = result.get(1);
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, idVal.getType());
        Assertions.assertEquals(1, idVal.getMin_int_value());
        Assertions.assertEquals(100, idVal.getMax_int_value());

        // DOUBLE
        TExprMinMaxValue scoreVal = result.get(2);
        Assertions.assertEquals(TExprNodeType.FLOAT_LITERAL, scoreVal.getType());
        Assertions.assertTrue(scoreVal.isHas_null());

        // BOOLEAN
        TExprMinMaxValue activeVal = result.get(3);
        Assertions.assertEquals(TExprNodeType.BOOL_LITERAL, activeVal.getType());
    }

    @Test
    public void testColumnNotInSchema() {
        RowType rowType = RowType.of(
                new DataField(0, "id", DataTypes.INT()));

        BinaryRow minRow = new BinaryRow(1);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.writeInt(0, 1);
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(1);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.writeInt(0, 100);
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        // Request a column that doesn't exist in the schema
        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "nonexistent", IntegerType.INT)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testSmallintAndTinyint() {
        RowType rowType = RowType.of(
                new DataField(0, "small_val", DataTypes.SMALLINT()),
                new DataField(1, "tiny_val", DataTypes.TINYINT()));

        BinaryRow minRow = new BinaryRow(2);
        BinaryRowWriter minWriter = new BinaryRowWriter(minRow);
        minWriter.writeShort(0, (short) -100);
        minWriter.writeByte(1, (byte) 0);
        minWriter.complete();

        BinaryRow maxRow = new BinaryRow(2);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxRow);
        maxWriter.writeShort(0, (short) 100);
        maxWriter.writeByte(1, (byte) 127);
        maxWriter.complete();

        BinaryArray nullCounts = createNullCountArray(0L, 0L);
        SimpleStats stats = new SimpleStats(minRow, maxRow, nullCounts);

        List<SlotDescriptor> slots = Arrays.asList(
                createSlotDescriptor(1, "small_val", IntegerType.SMALLINT),
                createSlotDescriptor(2, "tiny_val", IntegerType.TINYINT)
        );

        Map<Integer, TExprMinMaxValue> result = PaimonMinMaxUtil.toThriftMinMaxValueBySlots(
                rowType, stats, null, 100L, slots);

        Assertions.assertEquals(2, result.size());

        TExprMinMaxValue smallVal = result.get(1);
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, smallVal.getType());
        Assertions.assertEquals(-100, smallVal.getMin_int_value());
        Assertions.assertEquals(100, smallVal.getMax_int_value());

        TExprMinMaxValue tinyVal = result.get(2);
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, tinyVal.getType());
        Assertions.assertEquals(0, tinyVal.getMin_int_value());
        Assertions.assertEquals(127, tinyVal.getMax_int_value());
    }
}
