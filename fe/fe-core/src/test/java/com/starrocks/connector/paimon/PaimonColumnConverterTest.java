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

import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.AnyElementType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.wildfly.common.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PaimonColumnConverterTest {

    @Test
    public void testConvertBinary() {
        org.apache.paimon.types.BinaryType paimonType = new org.apache.paimon.types.BinaryType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, VarbinaryType.VARBINARY);
    }

    @Test
    public void testConvertVarBinary() {
        org.apache.paimon.types.VarBinaryType paimonType = new org.apache.paimon.types.VarBinaryType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, VarbinaryType.VARBINARY);
    }

    @Test
    public void testConvertChar() {
        org.apache.paimon.types.CharType paimonType = new org.apache.paimon.types.CharType(10);
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Type srType = TypeFactory.createCharType(10);
        Assertions.assertEquals(result, srType);
    }

    @Test
    public void testConvertVarchar() {
        org.apache.paimon.types.VarCharType paimonType = new org.apache.paimon.types.VarCharType(20);
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Type srType = TypeFactory.createVarcharType(20);
        Assertions.assertEquals(result, srType);
    }

    @Test
    public void testConvertBool() {
        org.apache.paimon.types.BooleanType paimonType = new org.apache.paimon.types.BooleanType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, com.starrocks.type.BooleanType.BOOLEAN);
    }

    @Test
    public void testConvertDecimal() {
        int precision = 9;
        int scale = 5;
        org.apache.paimon.types.DecimalType paimonType = new org.apache.paimon.types.DecimalType(precision, scale);
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Type srType = TypeFactory.createUnifiedDecimalType(precision, scale);
        Assertions.assertEquals(result, srType);
    }

    @Test
    public void testConvertTinyInt() {
        org.apache.paimon.types.TinyIntType paimonType = new org.apache.paimon.types.TinyIntType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, IntegerType.TINYINT);
    }

    @Test
    public void testConvertSmallint() {
        org.apache.paimon.types.SmallIntType paimonType = new org.apache.paimon.types.SmallIntType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, IntegerType.SMALLINT);
    }

    @Test
    public void testConvertInt() {
        org.apache.paimon.types.IntType paimonType = new org.apache.paimon.types.IntType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, IntegerType.INT);
    }

    @Test
    public void testConvertBigint() {
        org.apache.paimon.types.BigIntType paimonType = new org.apache.paimon.types.BigIntType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, IntegerType.BIGINT);
    }

    @Test
    public void testConvertFlout() {
        org.apache.paimon.types.FloatType paimonType = new org.apache.paimon.types.FloatType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, com.starrocks.type.FloatType.FLOAT);
    }

    @Test
    public void testConvertDouble() {
        org.apache.paimon.types.DoubleType paimonType = new org.apache.paimon.types.DoubleType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, com.starrocks.type.FloatType.DOUBLE);
    }

    @Test
    public void testConvertDate() {
        org.apache.paimon.types.DateType paimonType = new org.apache.paimon.types.DateType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, com.starrocks.type.DateType.DATE);
    }

    @Test
    public void testConvertDatetime() {
        org.apache.paimon.types.TimestampType paimonType = new org.apache.paimon.types.TimestampType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, com.starrocks.type.DateType.DATETIME);
    }

    @Test
    public void testConvertLocalZonedDatetime() {
        org.apache.paimon.types.LocalZonedTimestampType paimonType = new org.apache.paimon.types.LocalZonedTimestampType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertEquals(result, com.starrocks.type.DateType.DATETIME);
    }

    @Test
    public void testConvertArray() {
        org.apache.paimon.types.ArrayType paimonType =
                new org.apache.paimon.types.ArrayType(new org.apache.paimon.types.SmallIntType());
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertTrue(result instanceof com.starrocks.type.ArrayType);
        com.starrocks.type.ArrayType srType = (com.starrocks.type.ArrayType) result;
        Assertions.assertEquals(IntegerType.SMALLINT, srType.getItemType());
    }

    @Test
    public void testConvertMap() {
        org.apache.paimon.types.MapType paimonType = new org.apache.paimon.types.MapType(
                new org.apache.paimon.types.VarCharType(20),
                new org.apache.paimon.types.TimestampType());
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertTrue(result instanceof com.starrocks.type.MapType);
        com.starrocks.type.MapType srType = (com.starrocks.type.MapType) result;
        Assertions.assertEquals(TypeFactory.createVarcharType(20), srType.getKeyType());
        Assertions.assertEquals(com.starrocks.type.DateType.DATETIME, srType.getValueType());
    }

    @Test
    public void testConvertStruct() {
        List<org.apache.paimon.types.DataField> fields =
                Arrays.asList(
                        new org.apache.paimon.types.DataField(0, "f0", new org.apache.paimon.types.BinaryType()),
                        new org.apache.paimon.types.DataField(1, "f1", new org.apache.paimon.types.BigIntType()),
                        new org.apache.paimon.types.DataField(2, "f2", new org.apache.paimon.types.FloatType()));
        org.apache.paimon.types.RowType paimonType = new org.apache.paimon.types.RowType(fields);
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assertions.assertTrue(result instanceof StructType);
        StructType srType = (StructType) result;
        Assertions.assertEquals(3, srType.getFields().size());
        Assertions.assertEquals(VarbinaryType.VARBINARY, srType.getField("f0").getType());
        Assertions.assertEquals(IntegerType.BIGINT, srType.getField("f1").getType());
        Assertions.assertEquals(com.starrocks.type.FloatType.FLOAT, srType.getField("f2").getType());
    }

    @Test
    public void testConvertFromVarBinary() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(VarbinaryType.VARBINARY);
        Assertions.assertEquals(paimonDataType,
                org.apache.paimon.types.DataTypes.VARBINARY(org.apache.paimon.types.VarBinaryType.MAX_LENGTH));
    }

    @Test
    public void testConvertFromChar() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(CharType.CHAR);
        Assertions.assertEquals(paimonDataType,
                org.apache.paimon.types.DataTypes.CHAR(org.apache.paimon.types.CharType.MAX_LENGTH));
    }

    @Test
    public void testConvertFromVarchar() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(VarcharType.VARCHAR);
        Assertions.assertEquals(paimonDataType,
                org.apache.paimon.types.DataTypes.VARCHAR(org.apache.paimon.types.VarCharType.MAX_LENGTH));
    }

    @Test
    public void testConvertFromBool() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(BooleanType.BOOLEAN);
        Assertions.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.BOOLEAN());
    }

    @Test
    public void testConvertFromDecimal() {
        int precision = 9;
        int scale = 5;
        Type srType = TypeFactory.createUnifiedDecimalType(precision, scale);
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(srType);
        Assertions.assertEquals(new org.apache.paimon.types.DecimalType(precision, scale), paimonDataType);
    }

    @Test
    public void testConvertFromTinyInt() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(IntegerType.TINYINT);
        Assertions.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.TINYINT());
    }

    @Test
    public void testConvertFromSmallint() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(IntegerType.SMALLINT);
        Assertions.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.SMALLINT());
    }

    @Test
    public void testConvertFromInt() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(IntegerType.INT);
        Assertions.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.INT());
    }

    @Test
    public void testConvertFromBigint() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(IntegerType.BIGINT);
        Assertions.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.BIGINT());
    }

    @Test
    public void testConvertFromFlout() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(FloatType.FLOAT);
        Assertions.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.FLOAT());
    }

    @Test
    public void testConvertFromDouble() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(FloatType.DOUBLE);
        Assertions.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.DOUBLE());
    }

    @Test
    public void testConvertFromDate() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(DateType.DATE);
        Assertions.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.DATE());
    }

    @Test
    public void testConvertFromDatetime() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(DateType.DATETIME);
        Assertions.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }

    @Test
    public void testConvertFromArray() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(ArrayType.ARRAY_SMALLINT);
        Assert.assertTrue(paimonDataType instanceof org.apache.paimon.types.ArrayType);
        Assertions.assertEquals(org.apache.paimon.types.DataTypes.SMALLINT(),
                ((org.apache.paimon.types.ArrayType) paimonDataType).getElementType());
    }

    @Test
    public void testConvertFromMap() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(MapType.MAP_VARCHAR_VARCHAR);
        Assert.assertTrue(paimonDataType instanceof org.apache.paimon.types.MapType);
        org.apache.paimon.types.MapType paimonType = (org.apache.paimon.types.MapType) paimonDataType;
        Assertions.assertEquals(org.apache.paimon.types.DataTypes.VARCHAR(org.apache.paimon.types.VarCharType.MAX_LENGTH),
                paimonType.getKeyType());
        Assertions.assertEquals(org.apache.paimon.types.DataTypes.VARCHAR(org.apache.paimon.types.VarCharType.MAX_LENGTH),
                paimonType.getValueType());
    }

    @Test
    public void testConvertFromStruct() {
        ArrayList<StructField> structFields = new ArrayList<>(3);
        structFields.add(new StructField("f0", VarbinaryType.VARBINARY));
        structFields.add(new StructField("f1", IntegerType.BIGINT));
        structFields.add(new StructField("f2", FloatType.FLOAT));
        StructType structType = new StructType(structFields);
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(structType);
        Assert.assertTrue(paimonDataType instanceof org.apache.paimon.types.RowType);
        org.apache.paimon.types.RowType paimonType = (org.apache.paimon.types.RowType) paimonDataType;
        List<org.apache.paimon.types.DataField> fields = paimonType.getFields();
        Assertions.assertEquals(3, fields.size());
        Assertions.assertEquals(org.apache.paimon.types.DataTypes.VARBINARY(org.apache.paimon.types.VarBinaryType.MAX_LENGTH),
                paimonType.getField("f0").type());
        Assertions.assertEquals(org.apache.paimon.types.DataTypes.BIGINT(), paimonType.getField("f1").type());
        Assertions.assertEquals(org.apache.paimon.types.DataTypes.FLOAT(), paimonType.getField("f2").type());
    }

    @Test
    public void testConvertFromUnsupportedType() {
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> ColumnTypeConverter.toPaimonDataType(JsonType.JSON));
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> ColumnTypeConverter.toPaimonDataType(AnyElementType.ANY_ELEMENT));

    }
}
