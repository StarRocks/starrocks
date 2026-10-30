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

package com.starrocks.fluss.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlussColumnValueTest {
    private static final String UTC = "UTC";

    @Test
    public void testPrimitiveValues() {
        Assertions.assertTrue(value(true, DataTypes.BOOLEAN()).getBoolean());
        Assertions.assertEquals((byte) 1, value((byte) 1, DataTypes.TINYINT()).getByte());
        Assertions.assertEquals((short) 2, value((short) 2, DataTypes.SMALLINT()).getShort());
        Assertions.assertEquals(3, value(3, DataTypes.INT()).getInt());
        Assertions.assertEquals(4L, value(4L, DataTypes.BIGINT()).getLong());
        Assertions.assertEquals(1.25F, value(1.25F, DataTypes.FLOAT()).getFloat());
        Assertions.assertEquals(2.5D, value(2.5D, DataTypes.DOUBLE()).getDouble());

        BinaryString string = BinaryString.fromString("fluss");
        Assertions.assertEquals(
                "fluss",
                value(string, DataTypes.STRING()).getString(ColumnType.TypeValue.STRING));

        byte[] bytes = new byte[] {1, 2, 3};
        Assertions.assertArrayEquals(bytes, value(bytes, DataTypes.BYTES()).getBytes());
    }

    @Test
    public void testDecimalDateAndTimestamps() {
        BigDecimal decimal = new BigDecimal("123.45");
        Decimal flussDecimal = Decimal.fromBigDecimal(decimal, 5, 2);
        Assertions.assertEquals(decimal, value(flussDecimal, DataTypes.DECIMAL(5, 2)).getDecimal());

        LocalDate date = LocalDate.of(2026, 7, 15);
        Assertions.assertEquals(date, value((int) date.toEpochDay(), DataTypes.DATE()).getDate());

        LocalDateTime localDateTime = LocalDateTime.of(2026, 7, 15, 9, 30, 15, 123_000_000);
        TimestampNtz timestampNtz = TimestampNtz.fromLocalDateTime(localDateTime);
        Assertions.assertEquals(
                localDateTime,
                value(timestampNtz, DataTypes.TIMESTAMP()).getDateTime(ColumnType.TypeValue.DATETIME_MILLIS));

        Instant instant = Instant.parse("2026-07-15T01:30:15.123Z");
        String timeZone = "Asia/Shanghai";
        FlussColumnValue timestampLtzValue = new FlussColumnValue(
                TimestampLtz.fromInstant(instant), DataTypes.TIMESTAMP_LTZ(), timeZone);
        Assertions.assertEquals(
                LocalDateTime.ofInstant(instant, ZoneId.of(timeZone)),
                timestampLtzValue.getDateTime(ColumnType.TypeValue.DATETIME_MILLIS));

        UnsupportedOperationException exception = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> value(1, DataTypes.INT()).getDateTime(ColumnType.TypeValue.DATETIME));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported type"));
    }

    @Test
    public void testUnpackArrayWithNullElement() {
        GenericArray array = GenericArray.of(10, null, 30);
        FlussColumnValue arrayValue = value(array, DataTypes.ARRAY(DataTypes.INT()));
        List<ColumnValue> values = new ArrayList<>();

        arrayValue.unpackArray(values);

        Assertions.assertEquals(3, values.size());
        Assertions.assertEquals(10, values.get(0).getInt());
        Assertions.assertNull(values.get(1));
        Assertions.assertEquals(30, values.get(2).getInt());
    }

    @Test
    public void testUnpackMapWithNullValue() {
        Map<BinaryString, Integer> map = new LinkedHashMap<>();
        map.put(BinaryString.fromString("first"), 1);
        map.put(BinaryString.fromString("second"), null);
        map.put(null, 3);
        FlussColumnValue mapValue = value(
                new GenericMap(map),
                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        List<ColumnValue> keys = new ArrayList<>();
        List<ColumnValue> values = new ArrayList<>();

        mapValue.unpackMap(keys, values);

        Assertions.assertEquals(3, keys.size());
        Assertions.assertEquals("first", keys.get(0).getString(ColumnType.TypeValue.STRING));
        Assertions.assertEquals("second", keys.get(1).getString(ColumnType.TypeValue.STRING));
        Assertions.assertNull(keys.get(2));
        Assertions.assertEquals(1, values.get(0).getInt());
        Assertions.assertNull(values.get(1));
        Assertions.assertEquals(3, values.get(2).getInt());
    }

    @Test
    public void testUnpackStructAllAndSelectedFields() {
        GenericRow row = GenericRow.of(7, BinaryString.fromString("alice"), null);
        FlussColumnValue rowValue = value(
                row,
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.BIGINT())));

        List<ColumnValue> allFields = new ArrayList<>();
        rowValue.unpackStruct(null, allFields);
        Assertions.assertEquals(3, allFields.size());
        Assertions.assertEquals(7, allFields.get(0).getInt());
        Assertions.assertEquals("alice", allFields.get(1).getString(ColumnType.TypeValue.STRING));
        Assertions.assertNull(allFields.get(2));

        List<ColumnValue> selectedFields = new ArrayList<>();
        rowValue.unpackStruct(Arrays.asList(1, null, 0), selectedFields);
        Assertions.assertEquals(3, selectedFields.size());
        Assertions.assertEquals("alice", selectedFields.get(0).getString(ColumnType.TypeValue.STRING));
        Assertions.assertNull(selectedFields.get(1));
        Assertions.assertEquals(7, selectedFields.get(2).getInt());
    }

    private static FlussColumnValue value(Object fieldData, org.apache.fluss.types.DataType dataType) {
        return new FlussColumnValue(fieldData, dataType, UTC);
    }
}
