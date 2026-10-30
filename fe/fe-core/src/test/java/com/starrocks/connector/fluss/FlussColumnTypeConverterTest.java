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

package com.starrocks.connector.fluss;

import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarbinaryType;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlussColumnTypeConverterTest {
    @Test
    public void testConvertBinary() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.BINARY(16));
        Assertions.assertEquals(VarbinaryType.VARBINARY, result);
    }

    @Test
    public void testConvertBytes() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.BYTES());
        Assertions.assertEquals(VarbinaryType.VARBINARY, result);
    }

    @Test
    public void testConvertChar() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.CHAR(8));
        Assertions.assertEquals(TypeFactory.createCharType(8), result);
    }

    @Test
    public void testConvertString() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.STRING());
        Assertions.assertEquals(TypeFactory.createDefaultCatalogString(), result);
    }

    @Test
    public void testConvertBool() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.BOOLEAN());
        Assertions.assertEquals(com.starrocks.type.BooleanType.BOOLEAN, result);
    }

    @Test
    public void testConvertDecimal() {
        int precision = 10;
        int scale = 2;
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.DECIMAL(precision, scale));
        Assertions.assertEquals(TypeFactory.createUnifiedDecimalType(precision, scale), result);
    }

    @Test
    public void testConvertTinyInt() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.TINYINT());
        Assertions.assertEquals(IntegerType.TINYINT, result);
    }

    @Test
    public void testConvertSmallint() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.SMALLINT());
        Assertions.assertEquals(IntegerType.SMALLINT, result);
    }

    @Test
    public void testConvertInt() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.INT());
        Assertions.assertEquals(IntegerType.INT, result);
    }

    @Test
    public void testConvertBigint() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.BIGINT());
        Assertions.assertEquals(IntegerType.BIGINT, result);
    }

    @Test
    public void testConvertFloat() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.FLOAT());
        Assertions.assertEquals(FloatType.FLOAT, result);
    }

    @Test
    public void testConvertDouble() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.DOUBLE());
        Assertions.assertEquals(FloatType.DOUBLE, result);
    }

    @Test
    public void testConvertDate() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.DATE());
        Assertions.assertEquals(DateType.DATE, result);
    }

    @Test
    public void testConvertTime() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.TIME());
        Assertions.assertEquals(DateType.TIME, result);
    }

    @Test
    public void testConvertDatetime() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.TIMESTAMP());
        Assertions.assertEquals(DateType.DATETIME, result);
    }

    @Test
    public void testConvertLocalZonedDatetime() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.TIMESTAMP_LTZ());
        Assertions.assertEquals(DateType.DATETIME, result);
    }

    @Test
    public void testConvertArray() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.ARRAY(DataTypes.SMALLINT()));
        Assertions.assertTrue(result instanceof ArrayType);
        Assertions.assertEquals(IntegerType.SMALLINT, ((ArrayType) result).getItemType());
    }

    @Test
    public void testConvertMap() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.MAP(DataTypes.STRING(), DataTypes.TIMESTAMP_LTZ()));
        Assertions.assertTrue(result instanceof MapType);
        MapType mapType = (MapType) result;
        Assertions.assertEquals(TypeFactory.createDefaultCatalogString(), mapType.getKeyType());
        Assertions.assertEquals(DateType.DATETIME, mapType.getValueType());
    }

    @Test
    public void testConvertStruct() {
        Type result = ColumnTypeConverter.fromFlussType(DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
                DataTypes.FIELD("amount", DataTypes.DECIMAL(12, 4))));
        Assertions.assertTrue(result instanceof StructType);
        StructType structType = (StructType) result;
        Assertions.assertEquals(3, structType.getFields().size());
        Assertions.assertEquals(IntegerType.INT, structType.getField("id").getType());
        Assertions.assertTrue(structType.getField("tags").getType() instanceof ArrayType);
        Assertions.assertEquals(TypeFactory.createDefaultCatalogString(),
                ((ArrayType) structType.getField("tags").getType()).getItemType());
        Assertions.assertEquals(TypeFactory.createUnifiedDecimalType(12, 4),
                structType.getField("amount").getType());
    }
}
