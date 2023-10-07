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

package com.starrocks.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.starrocks.catalog.ScalarType.MAX_VARCHAR_LENGTH;
import static com.starrocks.catalog.Type.UNKNOWN_TYPE;
import static com.starrocks.connector.ColumnTypeConverter.columnEquals;
import static com.starrocks.connector.ColumnTypeConverter.fromHiveTypeToArrayType;
import static com.starrocks.connector.ColumnTypeConverter.fromHiveTypeToMapType;
import static com.starrocks.connector.ColumnTypeConverter.fromHudiType;
import static com.starrocks.connector.ColumnTypeConverter.getPrecisionAndScale;
import static com.starrocks.connector.ColumnTypeConverter.toHiveType;

public class ColumnTypeConverterTest {

    @Test
    public void testDecimalString() {
        String t1 = "decimal(3,2)";
        int[] res = getPrecisionAndScale(t1);
        Assert.assertEquals(3, res[0]);
        Assert.assertEquals(2, res[1]);

        t1 = "decimal(222233,4442)";
        res = getPrecisionAndScale(t1);
        Assert.assertEquals(222233, res[0]);
        Assert.assertEquals(4442, res[1]);

        t1 = "decimal(3, 2)";
        res = getPrecisionAndScale(t1);
        Assert.assertEquals(3, res[0]);
        Assert.assertEquals(2, res[1]);

        try {
            t1 = "decimal(3.222,2)";
            getPrecisionAndScale(t1);
            Assert.fail();
        } catch (StarRocksConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal(a,2)";
            getPrecisionAndScale(t1);
            Assert.fail();
        } catch (StarRocksConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal(-1,2)";
            getPrecisionAndScale(t1);
            Assert.fail();
        } catch (StarRocksConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal()";
            getPrecisionAndScale(t1);
            Assert.fail();
        } catch (StarRocksConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal(1)";
            getPrecisionAndScale(t1);
            Assert.fail();
        } catch (StarRocksConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }
    }

    @Test
    public void testArrayString() {
        ScalarType itemType = ScalarType.createType(PrimitiveType.DATE);
        ArrayType arrayType = new ArrayType(new ArrayType(itemType));
        String typeStr = "Array<Array<date>>";
        Type resType = fromHiveTypeToArrayType(typeStr);
        Assert.assertEquals(arrayType, resType);

        itemType = ScalarType.createDefaultExternalTableString();
        arrayType = new ArrayType(itemType);
        typeStr = "Array<string>";
        resType = fromHiveTypeToArrayType(typeStr);
        Assert.assertEquals(arrayType, resType);

        itemType = ScalarType.createType(PrimitiveType.INT);
        arrayType = new ArrayType(new ArrayType(new ArrayType(itemType)));
        typeStr = "array<Array<Array<int>>>";
        resType = fromHiveTypeToArrayType(typeStr);
        Assert.assertEquals(arrayType, resType);

        itemType = ScalarType.createType(PrimitiveType.BIGINT);
        arrayType = new ArrayType(new ArrayType(new ArrayType(itemType)));
        typeStr = "array<Array<Array<bigint>>>";
        resType = fromHiveTypeToArrayType(typeStr);
        Assert.assertEquals(arrayType, resType);

        itemType = ScalarType.createUnifiedDecimalType(4, 2);
        Assert.assertEquals(new ArrayType(new ArrayType(itemType)),
                fromHiveTypeToArrayType("array<Array<decimal(4, 2)>>"));
    }

    @Test
    public void testMapString() {
        ScalarType keyType = ScalarType.createType(PrimitiveType.TINYINT);
        ScalarType valueType = ScalarType.createType(PrimitiveType.SMALLINT);
        MapType mapType = new MapType(keyType, valueType);
        String typeStr = "map<tinyint,smallint>";
        Type resType = fromHiveTypeToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createType(PrimitiveType.INT);
        valueType = ScalarType.createType(PrimitiveType.INT);
        mapType = new MapType(keyType, valueType);
        typeStr = "Map<INT,INTEGER>";
        resType = fromHiveTypeToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createType(PrimitiveType.FLOAT);
        valueType = ScalarType.createType(PrimitiveType.DOUBLE);
        mapType = new MapType(keyType, valueType);
        typeStr = "map<float,double>";
        resType = fromHiveTypeToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createUnifiedDecimalType(10, 7);
        valueType = ScalarType.createType(PrimitiveType.DATETIME);
        mapType = new MapType(keyType, valueType);
        typeStr = "map<decimal(10,7),timestamp>";
        resType = fromHiveTypeToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createType(PrimitiveType.DATE);
        valueType = ScalarType.createDefaultExternalTableString();
        mapType = new MapType(keyType, valueType);
        typeStr = "map<date,string>";
        resType = fromHiveTypeToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createVarcharType(10);
        valueType = ScalarType.createCharType(5);
        mapType = new MapType(keyType, valueType);
        typeStr = "map<varchar(10),char(5)>";
        resType = fromHiveTypeToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createType(PrimitiveType.BOOLEAN);
        valueType = ScalarType.createVarcharType(10);
        mapType = new MapType(keyType, valueType);
        typeStr = "map<boolean,varchar(10)>";
        resType = fromHiveTypeToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createCharType(10);
        ScalarType itemType = ScalarType.createType(PrimitiveType.INT);
        ArrayType vType = new ArrayType(itemType);
        mapType = new MapType(keyType, vType);
        typeStr = "map<char(10),array<int>>";
        resType = fromHiveTypeToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createCharType(10);
        ScalarType inKeyType = ScalarType.createType(PrimitiveType.INT);
        itemType = ScalarType.createType(PrimitiveType.DATETIME);
        ArrayType inValueType = new ArrayType(itemType);
        MapType mValueType = new MapType(inKeyType, inValueType);
        mapType = new MapType(keyType, mValueType);
        typeStr = "map<char(10),map<int,array<timestamp>>>";
        resType = fromHiveTypeToMapType(typeStr);
        Assert.assertEquals(mapType, resType);
    }

    @Test
    public void testStructString() {
        {
            String typeStr = "struct<a:struct<aa:date>,b:int>";
            StructField aa = new StructField("aa", ScalarType.createType(PrimitiveType.DATE));

            StructType innerStruct = new StructType(Lists.newArrayList(aa));
            StructField a = new StructField("a", innerStruct);
            StructField b = new StructField("b", ScalarType.createType(PrimitiveType.INT));
            StructType outerStruct = new StructType(Lists.newArrayList(a, b));

            Type resType = ColumnTypeConverter.fromHiveType(typeStr);
            Assert.assertEquals(outerStruct, resType);
        }

        {
            String typeStr = "array<struct<a:int,b:map<int,int>>>";
            MapType map =
                    new MapType(ScalarType.createType(PrimitiveType.INT), ScalarType.createType(PrimitiveType.INT));
            StructField a = new StructField("a", ScalarType.createType(PrimitiveType.INT));
            StructField b = new StructField("b", map);
            StructType structType = new StructType(Lists.newArrayList(a, b));
            ArrayType arrayType = new ArrayType(structType);

            Type resType = ColumnTypeConverter.fromHiveType(typeStr);
            Assert.assertEquals(arrayType, resType);
        }

        {
            String typeStr = "struct<struct_test:int,c1:struct<c1:int,cc1:string>>";
            StructType c1 = new StructType(Lists.newArrayList(
                    new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                    new StructField("cc1", ScalarType.createDefaultExternalTableString())
            ));
            StructType root = new StructType(Lists.newArrayList(
                    new StructField("struct_test", ScalarType.createType(PrimitiveType.INT)),
                    new StructField("c1", c1)
            ));

            Type resType = ColumnTypeConverter.fromHiveType(typeStr);
            Assert.assertEquals(root, resType);
        }
    }

    @Test
    public void testSplitByFirstLevel() {
        // Test for struct
        String str = "a: int, b: struct<a: int, b: double>";
        String[] result = ColumnTypeConverter.splitByFirstLevel(str, ',');
        String[] expected = new String[] {"a: int", "b: struct<a: int, b: double>"};
        Assert.assertArrayEquals(result, expected);

        // Test for map
        str = "int, struct<a:int,b:double>";
        result = ColumnTypeConverter.splitByFirstLevel(str, ',');
        expected = new String[] {"int", "struct<a:int,b:double>"};
        Assert.assertArrayEquals(result, expected);

        str = "b: struct<a: int, b: double>";
        result = ColumnTypeConverter.splitByFirstLevel(str, ':');
        expected = new String[] {"b", "struct<a: int, b: double>"};
        Assert.assertArrayEquals(result, expected);
    }

    @Test
    public void testCharString() {
        Type charType = ScalarType.createCharType(100);
        String typeStr = "char(100)";
        Type resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assert.assertEquals(resType, charType);

        typeStr = "char(50)";
        resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assert.assertNotEquals(resType, charType);
    }

    @Test
    public void testVarcharString() {
        Type varcharType = ScalarType.createVarcharType(100);
        String typeStr = "varchar(100)";
        Type resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assert.assertEquals(resType, varcharType);

        typeStr = "varchar(50)";
        resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assert.assertNotEquals(resType, varcharType);

        varcharType = ScalarType.createVarcharType();
        typeStr = "varchar(-1)";
        resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assert.assertEquals(resType, varcharType);

        Type stringType = ScalarType.createDefaultExternalTableString();
        typeStr = "string";
        resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assert.assertEquals(resType, stringType);
    }

    @Test
    public void testArrayHudiSchema() {
        Schema unionSchema;
        Schema arraySchema;

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.INT));
        Assert.assertEquals(fromHudiType(unionSchema), ScalarType.createType(PrimitiveType.INT));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.INT));
        arraySchema = Schema.createArray(unionSchema);
        Schema.createArray(unionSchema);
        Assert.assertEquals(fromHudiType(arraySchema), new ArrayType(ScalarType.createType(PrimitiveType.INT)));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.BOOLEAN));
        arraySchema = Schema.createArray(unionSchema);
        Assert.assertEquals(fromHudiType(arraySchema), new ArrayType(ScalarType.createType(PrimitiveType.BOOLEAN)));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.STRING));
        arraySchema = Schema.createArray(unionSchema);
        Assert.assertEquals(fromHudiType(arraySchema), new ArrayType(ScalarType.createDefaultExternalTableString()));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.BYTES));
        arraySchema = Schema.createArray(unionSchema);
        Assert.assertEquals(fromHudiType(arraySchema), new ArrayType(ScalarType.createType(PrimitiveType.VARCHAR)));
    }

    @Test
    public void testStructHudiSchema() {
        Schema.Field field1 = new Schema.Field("field1", Schema.create(Schema.Type.INT), null, null);
        Schema.Field field2 = new Schema.Field("field2", Schema.create(Schema.Type.STRING), null, null);
        List<Schema.Field> fields = new LinkedList<>();
        fields.add(field1);
        fields.add(field2);
        Schema structSchema = Schema.createRecord(fields);

        StructField structField1 = new StructField("field1", ScalarType.createType(PrimitiveType.INT));
        StructField structField2 = new StructField("field2", ScalarType.createDefaultExternalTableString());
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(structField1);
        structFields.add(structField2);
        StructType structType = new StructType(structFields);
        Assert.assertEquals(structType, fromHudiType(structSchema));

        structSchema = Schema.createRecord(
                ImmutableList.of(new Schema.Field("enum", Schema.create(Schema.Type.NULL))));
        Assert.assertEquals(UNKNOWN_TYPE, fromHudiType(structSchema));
    }

    @Test
    public void testMapHudiSchema() {
        Schema.Field field1 = new Schema.Field("field1", Schema.create(Schema.Type.INT), null, null);
        Schema.Field field2 = new Schema.Field("field2", Schema.create(Schema.Type.STRING), null, null);
        List<Schema.Field> fields = new LinkedList<>();
        fields.add(field1);
        fields.add(field2);
        Schema structSchema = Schema.createRecord(fields);

        Schema mapSchema = Schema.createMap(structSchema);

        StructField structField1 = new StructField("field1", ScalarType.createType(PrimitiveType.INT));
        StructField structField2 = new StructField("field2", ScalarType.createDefaultExternalTableString());
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(structField1);
        structFields.add(structField2);
        StructType structType = new StructType(structFields);

        MapType mapType = new MapType(ScalarType.createDefaultExternalTableString(), structType);

        Assert.assertEquals(mapType, fromHudiType(mapSchema));

        mapSchema = Schema.createMap(Schema.create(Schema.Type.NULL));
        Assert.assertEquals(UNKNOWN_TYPE, fromHudiType(mapSchema));
    }

    @Test
    public void testColumnEquals() {
        Column base = new Column("k1", Type.INT, false);
        Column other = new Column("k1", Type.INT, false);

        Assert.assertTrue(columnEquals(base, base));
        Assert.assertTrue(columnEquals(base, other));

        other = new Column("k2", Type.INT, false);
        Assert.assertFalse(columnEquals(base, other));

        other = new Column("k1", Type.STRING, false);
        Assert.assertFalse(columnEquals(base, other));

        base = new Column("k1", ScalarType.createCharType(5), false);
        other = new Column("k1", ScalarType.createCharType(10), false);
        Assert.assertFalse(columnEquals(base, other));

        base = new Column("k1", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 5, 5), false);
        other = new Column("k1", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 6, 5), false);
        Assert.assertFalse(columnEquals(base, other));

        base = new Column("k1", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 5, 5), false);
        other = new Column("k1", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 5, 4), false);
        Assert.assertFalse(columnEquals(base, other));
    }

    @Test
    public void testSRTypeToHiveType() {
        Assert.assertEquals("tinyint", toHiveType(Type.TINYINT));
        Assert.assertEquals("smallint", toHiveType(Type.SMALLINT));
        Assert.assertEquals("int", toHiveType(Type.INT));
        Assert.assertEquals("bigint", toHiveType(Type.BIGINT));
        Assert.assertEquals("float", toHiveType(Type.FLOAT));
        Assert.assertEquals("double", toHiveType(Type.DOUBLE));
        Assert.assertEquals("boolean", toHiveType(Type.BOOLEAN));
        Assert.assertEquals("binary", toHiveType(Type.VARBINARY));
        Assert.assertEquals("date", toHiveType(Type.DATE));
        Assert.assertEquals("timestamp", toHiveType(Type.DATETIME));

        Assert.assertEquals("char(10)", toHiveType(ScalarType.createCharType(10)));
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Unsupported Hive type: CHAR(10000). Supported CHAR types: CHAR(<=255)",
                () -> toHiveType(ScalarType.createCharType(10000)));

        Assert.assertEquals("varchar(100)", toHiveType(ScalarType.createVarchar(100)));
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Unsupported Hive type: VARCHAR(200000). Supported VARCHAR types: VARCHAR(<=65535)",
                () -> toHiveType(ScalarType.createVarcharType(200000)));

        Assert.assertEquals("string", toHiveType(ScalarType.createVarchar(MAX_VARCHAR_LENGTH)));

        ScalarType itemType = ScalarType.createType(PrimitiveType.DATE);
        ArrayType arrayType = new ArrayType(new ArrayType(itemType));
        Assert.assertEquals("array<array<date>>", toHiveType(arrayType));

        ScalarType keyType = ScalarType.createType(PrimitiveType.TINYINT);
        ScalarType valueType = ScalarType.createType(PrimitiveType.SMALLINT);
        MapType mapType = new MapType(keyType, valueType);
        String typeStr = "map<tinyint,smallint>";
        Assert.assertEquals(typeStr, toHiveType(mapType));

        typeStr = "struct<a:struct<aa:date>,b:int>";
        StructField aa = new StructField("aa", ScalarType.createType(PrimitiveType.DATE));

        StructType innerStruct = new StructType(Lists.newArrayList(aa));
        StructField a = new StructField("a", innerStruct);
        StructField b = new StructField("b", ScalarType.createType(PrimitiveType.INT));
        StructType outerStruct = new StructType(Lists.newArrayList(a, b));
        Assert.assertEquals(typeStr, toHiveType(outerStruct));
    }
}
