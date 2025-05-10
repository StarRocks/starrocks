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

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ColumnTypeConverter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PaimonColumnConverterTest {

    @Test
    public void testConvertBinary() {
        BinaryType paimonType = new BinaryType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.VARBINARY);
    }

    @Test
    public void testConvertVarBinary() {
        VarBinaryType paimonType = new VarBinaryType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.VARBINARY);
    }

    @Test
    public void testConvertChar() {
        CharType paimonType = new CharType(10);
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Type srType = ScalarType.createCharType(10);
        Assert.assertEquals(result, srType);
    }

    @Test
    public void testConvertVarchar() {
        VarCharType paimonType = new VarCharType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Type srType = ScalarType.createDefaultCatalogString();
        Assert.assertEquals(result, srType);
    }

    @Test
    public void testConvertBool() {
        BooleanType paimonType = new BooleanType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.BOOLEAN);
    }

    @Test
    public void testConvertDecimal() {
        int precision = 9;
        int scale = 5;
        DecimalType paimonType = new DecimalType(precision, scale);
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Type srType = ScalarType.createUnifiedDecimalType(precision, scale);
        Assert.assertEquals(result, srType);
    }

    @Test
    public void testConvertTinyInt() {
        TinyIntType paimonType = new TinyIntType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.TINYINT);
    }

    @Test
    public void testConvertSmallint() {
        SmallIntType paimonType = new SmallIntType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.SMALLINT);
    }

    @Test
    public void testConvertInt() {
        IntType paimonType = new IntType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.INT);
    }

    @Test
    public void testConvertBigint() {
        BigIntType paimonType = new BigIntType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.BIGINT);
    }

    @Test
    public void testConvertFlout() {
        FloatType paimonType = new FloatType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.FLOAT);
    }

    @Test
    public void testConvertDouble() {
        DoubleType paimonType = new DoubleType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.DOUBLE);
    }

    @Test
    public void testConvertDate() {
        DateType paimonType = new DateType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.DATE);
    }

    @Test
    public void testConvertDatetime() {
        TimestampType paimonType = new TimestampType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.DATETIME);
    }

    @Test
    public void testConvertLocalZonedDatetime() {
        LocalZonedTimestampType paimonType = new LocalZonedTimestampType();
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertEquals(result, Type.DATETIME);
    }

    @Test
    public void testConvertArray() {
        ArrayType paimonType = new ArrayType(new SmallIntType());
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertTrue(result instanceof com.starrocks.catalog.ArrayType);
        com.starrocks.catalog.ArrayType srType = (com.starrocks.catalog.ArrayType) result;
        Assert.assertEquals(Type.SMALLINT, srType.getItemType());
    }

    @Test
    public void testConvertMap() {
        MapType paimonType = new MapType(new VarCharType(20), new TimestampType());
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertTrue(result instanceof com.starrocks.catalog.MapType);
        com.starrocks.catalog.MapType srType = (com.starrocks.catalog.MapType) result;
        Assert.assertEquals(ScalarType.createDefaultCatalogString(), srType.getKeyType());
        Assert.assertEquals(Type.DATETIME, srType.getValueType());
    }

    @Test
    public void testConvertStruct() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new BinaryType()),
                        new DataField(1, "f1", new BigIntType()),
                        new DataField(2, "f2", new FloatType()));
        RowType paimonType = new RowType(fields);
        Type result = ColumnTypeConverter.fromPaimonType(paimonType);
        Assert.assertTrue(result instanceof StructType);
        StructType srType = (StructType) result;
        Assert.assertEquals(3, srType.getFields().size());
        Assert.assertEquals(Type.VARBINARY, srType.getField("f0").getType());
        Assert.assertEquals(Type.BIGINT, srType.getField("f1").getType());
        Assert.assertEquals(Type.FLOAT, srType.getField("f2").getType());
    }

    @Test
    public void testConvertFromVarBinary() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.VARBINARY);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.VARBINARY(VarBinaryType.MAX_LENGTH));
    }

    @Test
    public void testConvertFromChar() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.CHAR);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.CHAR(CharType.MAX_LENGTH));
    }

    @Test
    public void testConvertFromVarchar() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.VARCHAR);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.VARCHAR(VarCharType.MAX_LENGTH));
    }

    @Test
    public void testConvertFromBool() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.BOOLEAN);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.BOOLEAN());
    }

    @Test
    public void testConvertFromDecimal() {
        int precision = 9;
        int scale = 5;
        Type srType = ScalarType.createUnifiedDecimalType(precision, scale);
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(srType);
        Assert.assertEquals(new DecimalType(precision, scale), paimonDataType);
    }

    @Test
    public void testConvertFromTinyInt() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.TINYINT);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.INT());
    }

    @Test
    public void testConvertFromSmallint() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.SMALLINT);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.INT());
    }

    @Test
    public void testConvertFromInt() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.INT);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.INT());
    }

    @Test
    public void testConvertFromBigint() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.BIGINT);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.BIGINT());
    }

    @Test
    public void testConvertFromFlout() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.FLOAT);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.FLOAT());
    }

    @Test
    public void testConvertFromDouble() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.DOUBLE);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.DOUBLE());
    }

    @Test
    public void testConvertFromDate() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.DATE);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.DATE());
    }

    @Test
    public void testConvertFromDatetime() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.DATETIME);
        Assert.assertEquals(paimonDataType, org.apache.paimon.types.DataTypes.TIMESTAMP());
    }

    @Test
    public void testConvertFromArray() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.ARRAY_SMALLINT);
        Assert.assertTrue(paimonDataType instanceof ArrayType);
        Assert.assertEquals(new SmallIntType(), new ArrayType(new SmallIntType()).getElementType());
    }

    @Test
    public void testConvertFromMap() {
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(Type.MAP_VARCHAR_VARCHAR);
        Assert.assertTrue(paimonDataType instanceof MapType);
        MapType paimonType = (MapType) paimonDataType;
        Assert.assertEquals(DataTypes.VARCHAR(VarCharType.MAX_LENGTH), paimonType.getKeyType());
        Assert.assertEquals(DataTypes.VARCHAR(VarCharType.MAX_LENGTH), paimonType.getValueType());
    }

    @Test
    public void testConvertFromStruct() {
        ArrayList<StructField> structFields = new ArrayList<>(3);
        structFields.add(new StructField("f0", Type.VARBINARY));
        structFields.add(new StructField("f1", Type.BIGINT));
        structFields.add(new StructField("f2", Type.FLOAT));
        StructType structType = new StructType(structFields);
        org.apache.paimon.types.DataType paimonDataType = ColumnTypeConverter.toPaimonDataType(structType);
        Assert.assertTrue(paimonDataType instanceof RowType);
        RowType paimonType = (RowType) paimonDataType;
        List<DataField> fields = paimonType.getFields();
        Assert.assertEquals(3, fields.size());
        Assert.assertEquals(DataTypes.VARBINARY(VarBinaryType.MAX_LENGTH), paimonType.getField("f0").type());
        Assert.assertEquals(DataTypes.BIGINT(), paimonType.getField("f1").type());
        Assert.assertEquals(DataTypes.FLOAT(), paimonType.getField("f2").type());
    }
}
