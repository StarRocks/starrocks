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
import com.starrocks.catalog.Column;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StringType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.starrocks.connector.ColumnTypeConverter.columnEquals;
import static com.starrocks.connector.ColumnTypeConverter.fromHiveTypeToArrayType;
import static com.starrocks.connector.ColumnTypeConverter.fromHiveTypeToMapType;
import static com.starrocks.connector.ColumnTypeConverter.fromHudiType;
import static com.starrocks.connector.ColumnTypeConverter.fromPaimonType;
import static com.starrocks.connector.ColumnTypeConverter.getPrecisionAndScale;
import static com.starrocks.connector.ColumnTypeConverter.toHiveType;
import static com.starrocks.type.TypeFactory.CATALOG_MAX_VARCHAR_LENGTH;
import static com.starrocks.type.TypeFactory.getOlapMaxVarcharLength;
import static com.starrocks.type.UnknownType.UNKNOWN_TYPE;


public class ColumnTypeConverterTest {

    @Test
    public void testDecimalString() {
        String t1 = "decimal(3,2)";
        int[] res = getPrecisionAndScale(t1);
        Assertions.assertEquals(3, res[0]);
        Assertions.assertEquals(2, res[1]);

        t1 = "decimal(222233,4442)";
        res = getPrecisionAndScale(t1);
        Assertions.assertEquals(222233, res[0]);
        Assertions.assertEquals(4442, res[1]);

        t1 = "decimal(3, 2)";
        res = getPrecisionAndScale(t1);
        Assertions.assertEquals(3, res[0]);
        Assertions.assertEquals(2, res[1]);

        try {
            t1 = "decimal(3.222,2)";
            getPrecisionAndScale(t1);
            Assertions.fail();
        } catch (StarRocksConnectorException e) {
            Assertions.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal(a,2)";
            getPrecisionAndScale(t1);
            Assertions.fail();
        } catch (StarRocksConnectorException e) {
            Assertions.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal(-1,2)";
            getPrecisionAndScale(t1);
            Assertions.fail();
        } catch (StarRocksConnectorException e) {
            Assertions.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal()";
            getPrecisionAndScale(t1);
            Assertions.fail();
        } catch (StarRocksConnectorException e) {
            Assertions.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal(1)";
            getPrecisionAndScale(t1);
            Assertions.fail();
        } catch (StarRocksConnectorException e) {
            Assertions.assertTrue(e.getMessage().contains("Failed to get"));
        }
    }

    @Test
    public void testArrayString() {
        ScalarType itemType = DateType.DATE;
        ArrayType arrayType = new ArrayType(new ArrayType(itemType));
        String typeStr = "Array<Array<date>>";
        Type resType = fromHiveTypeToArrayType(typeStr);
        Assertions.assertEquals(arrayType, resType);

        itemType = TypeFactory.createDefaultCatalogString();
        arrayType = new ArrayType(itemType);
        typeStr = "Array<string>";
        resType = fromHiveTypeToArrayType(typeStr);
        Assertions.assertEquals(arrayType, resType);

        itemType = IntegerType.INT;
        arrayType = new ArrayType(new ArrayType(new ArrayType(itemType)));
        typeStr = "array<Array<Array<int>>>";
        resType = fromHiveTypeToArrayType(typeStr);
        Assertions.assertEquals(arrayType, resType);

        itemType = IntegerType.BIGINT;
        arrayType = new ArrayType(new ArrayType(new ArrayType(itemType)));
        typeStr = "array<Array<Array<bigint>>>";
        resType = fromHiveTypeToArrayType(typeStr);
        Assertions.assertEquals(arrayType, resType);

        itemType = TypeFactory.createUnifiedDecimalType(4, 2);
        Assertions.assertEquals(new ArrayType(new ArrayType(itemType)),
                fromHiveTypeToArrayType("array<Array<decimal(4, 2)>>"));

        itemType = VarbinaryType.VARBINARY;
        Assertions.assertEquals(new ArrayType(itemType),
                fromHiveTypeToArrayType("array<BINARY>"));
    }

    @Test
    public void testMapString() {
        ScalarType keyType = IntegerType.TINYINT;
        ScalarType valueType = IntegerType.SMALLINT;
        MapType mapType = new MapType(keyType, valueType);
        String typeStr = "map<tinyint,smallint>";
        Type resType = fromHiveTypeToMapType(typeStr);
        Assertions.assertEquals(mapType, resType);

        keyType = IntegerType.INT;
        valueType = IntegerType.INT;
        mapType = new MapType(keyType, valueType);
        typeStr = "Map<INT,INTEGER>";
        resType = fromHiveTypeToMapType(typeStr);
        Assertions.assertEquals(mapType, resType);

        keyType = FloatType.FLOAT;
        valueType = FloatType.DOUBLE;
        mapType = new MapType(keyType, valueType);
        typeStr = "map<float,double>";
        resType = fromHiveTypeToMapType(typeStr);
        Assertions.assertEquals(mapType, resType);

        keyType = TypeFactory.createUnifiedDecimalType(10, 7);
        valueType = DateType.DATETIME;
        mapType = new MapType(keyType, valueType);
        typeStr = "map<decimal(10,7),timestamp>";
        resType = fromHiveTypeToMapType(typeStr);
        Assertions.assertEquals(mapType, resType);

        keyType = DateType.DATE;
        valueType = TypeFactory.createDefaultCatalogString();
        mapType = new MapType(keyType, valueType);
        typeStr = "map<date,string>";
        resType = fromHiveTypeToMapType(typeStr);
        Assertions.assertEquals(mapType, resType);

        keyType = TypeFactory.createVarcharType(10);
        valueType = TypeFactory.createCharType(5);
        mapType = new MapType(keyType, valueType);
        typeStr = "map<varchar(10),char(5)>";
        resType = fromHiveTypeToMapType(typeStr);
        Assertions.assertEquals(mapType, resType);

        keyType = BooleanType.BOOLEAN;
        valueType = TypeFactory.createVarcharType(10);
        mapType = new MapType(keyType, valueType);
        typeStr = "map<boolean,varchar(10)>";
        resType = fromHiveTypeToMapType(typeStr);
        Assertions.assertEquals(mapType, resType);

        keyType = TypeFactory.createCharType(10);
        ScalarType itemType = IntegerType.INT;
        ArrayType vType = new ArrayType(itemType);
        mapType = new MapType(keyType, vType);
        typeStr = "map<char(10),array<int>>";
        resType = fromHiveTypeToMapType(typeStr);
        Assertions.assertEquals(mapType, resType);

        keyType = TypeFactory.createCharType(10);
        ScalarType inKeyType = IntegerType.INT;
        itemType = DateType.DATETIME;
        ArrayType inValueType = new ArrayType(itemType);
        MapType mValueType = new MapType(inKeyType, inValueType);
        mapType = new MapType(keyType, mValueType);
        typeStr = "map<char(10),map<int,array<timestamp>>>";
        resType = fromHiveTypeToMapType(typeStr);
        Assertions.assertEquals(mapType, resType);
    }

    @Test
    public void testStructString() {
        {
            String typeStr = "struct<a:struct<aa:date>,b:int>";
            StructField aa = new StructField("aa", DateType.DATE);

            StructType innerStruct = new StructType(Lists.newArrayList(aa));
            StructField a = new StructField("a", innerStruct);
            StructField b = new StructField("b", IntegerType.INT);
            StructType outerStruct = new StructType(Lists.newArrayList(a, b));

            Type resType = ColumnTypeConverter.fromHiveType(typeStr);
            Assertions.assertEquals(outerStruct, resType);
        }

        {
            String typeStr = "array<struct<a:int,b:map<int,int>>>";
            MapType map =
                    new MapType(IntegerType.INT,
                            IntegerType.INT);
            StructField a = new StructField("a", IntegerType.INT);
            StructField b = new StructField("b", map);
            StructType structType = new StructType(Lists.newArrayList(a, b));
            ArrayType arrayType = new ArrayType(structType);

            Type resType = ColumnTypeConverter.fromHiveType(typeStr);
            Assertions.assertEquals(arrayType, resType);
        }

        {
            String typeStr = "struct<struct_test:int,c1:struct<c1:int,cc1:string>>";
            StructType c1 = new StructType(Lists.newArrayList(
                    new StructField("c1", IntegerType.INT),
                    new StructField("cc1", TypeFactory.createDefaultCatalogString())
            ));
            StructType root = new StructType(Lists.newArrayList(
                    new StructField("struct_test", IntegerType.INT),
                    new StructField("c1", c1)
            ));

            Type resType = ColumnTypeConverter.fromHiveType(typeStr);
            Assertions.assertEquals(root, resType);
        }
    }

    @Test
    public void testSplitByFirstLevel() {
        // Test for struct
        String str = "a: int, b: struct<a: int, b: double>";
        String[] result = ColumnTypeConverter.splitByFirstLevel(str, ',');
        String[] expected = new String[] {"a: int", "b: struct<a: int, b: double>"};
        Assertions.assertArrayEquals(result, expected);

        // Test for map
        str = "int, struct<a:int,b:double>";
        result = ColumnTypeConverter.splitByFirstLevel(str, ',');
        expected = new String[] {"int", "struct<a:int,b:double>"};
        Assertions.assertArrayEquals(result, expected);

        str = "b: struct<a: int, b: double>";
        result = ColumnTypeConverter.splitByFirstLevel(str, ':');
        expected = new String[] {"b", "struct<a: int, b: double>"};
        Assertions.assertArrayEquals(result, expected);
    }

    @Test
    public void testCharString() {
        Type charType = TypeFactory.createCharType(100);
        String typeStr = "char(100)";
        Type resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assertions.assertEquals(resType, charType);

        typeStr = "char(50)";
        resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assertions.assertNotEquals(resType, charType);
    }

    @Test
    public void testVarcharString() {
        Type varcharType = TypeFactory.createVarcharType(100);
        String typeStr = "varchar(100)";
        Type resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assertions.assertEquals(resType, varcharType);

        typeStr = "varchar(50)";
        resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assertions.assertNotEquals(resType, varcharType);

        varcharType = VarcharType.VARCHAR;
        typeStr = "varchar(-1)";
        resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assertions.assertEquals(resType, varcharType);

        Type stringType = TypeFactory.createDefaultCatalogString();
        typeStr = "string";
        resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assertions.assertEquals(resType, stringType);

        Assertions.assertEquals("varchar(65535)", toHiveType(TypeFactory.createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH)));
        Assertions.assertEquals("varchar(65534)", toHiveType(TypeFactory.createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH - 1)));
        Assertions.assertEquals("string", toHiveType(TypeFactory.createVarcharType(getOlapMaxVarcharLength())));
        Assertions.assertEquals("string", toHiveType(TypeFactory.createVarcharType(CATALOG_MAX_VARCHAR_LENGTH)));
    }

    @Test
    public void testArrayHudiSchema() {
        Schema unionSchema;
        Schema arraySchema;

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.INT));
        Assertions.assertEquals(fromHudiType(unionSchema), IntegerType.INT);

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.INT));
        arraySchema = Schema.createArray(unionSchema);
        Schema.createArray(unionSchema);
        Assertions.assertEquals(fromHudiType(arraySchema), new ArrayType(IntegerType.INT));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.BOOLEAN));
        arraySchema = Schema.createArray(unionSchema);
        Assertions.assertEquals(fromHudiType(arraySchema), new ArrayType(BooleanType.BOOLEAN));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.STRING));
        arraySchema = Schema.createArray(unionSchema);
        Assertions.assertEquals(fromHudiType(arraySchema), new ArrayType(TypeFactory.createDefaultCatalogString()));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.BYTES));
        arraySchema = Schema.createArray(unionSchema);
        Assertions.assertEquals(fromHudiType(arraySchema), new ArrayType(VarcharType.VARCHAR));
    }

    @Test
    public void testPaimonSchema() {
        org.apache.paimon.types.TimeType type = new org.apache.paimon.types.TimeType(3);
        Assertions.assertEquals(DateType.TIME, fromPaimonType(type));
    }

    @Test
    public void testStructHudiSchema() {
        Schema.Field field1 = new Schema.Field("field1", Schema.create(Schema.Type.INT), null, null);
        Schema.Field field2 = new Schema.Field("field2", Schema.create(Schema.Type.STRING), null, null);
        List<Schema.Field> fields = new LinkedList<>();
        fields.add(field1);
        fields.add(field2);
        Schema structSchema = Schema.createRecord(fields);

        StructField structField1 = new StructField("field1", IntegerType.INT);
        StructField structField2 = new StructField("field2", TypeFactory.createDefaultCatalogString());
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(structField1);
        structFields.add(structField2);
        StructType structType = new StructType(structFields);
        Assertions.assertEquals(structType, fromHudiType(structSchema));

        structSchema = Schema.createRecord(
                ImmutableList.of(new Schema.Field("enum", Schema.create(Schema.Type.NULL))));
        Assertions.assertEquals(UNKNOWN_TYPE, fromHudiType(structSchema));
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

        StructField structField1 = new StructField("field1", IntegerType.INT);
        StructField structField2 = new StructField("field2", TypeFactory.createDefaultCatalogString());
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(structField1);
        structFields.add(structField2);
        StructType structType = new StructType(structFields);

        MapType mapType = new MapType(TypeFactory.createDefaultCatalogString(), structType);

        Assertions.assertEquals(mapType, fromHudiType(mapSchema));

        mapSchema = Schema.createMap(Schema.create(Schema.Type.NULL));
        Assertions.assertEquals(UNKNOWN_TYPE, fromHudiType(mapSchema));
    }

    @Test
    public void testColumnEquals() {
        Column base = new Column("k1", IntegerType.INT, false);
        Column other = new Column("k1", IntegerType.INT, false);

        Assertions.assertTrue(columnEquals(base, base));
        Assertions.assertTrue(columnEquals(base, other));

        other = new Column("k2", IntegerType.INT, false);
        Assertions.assertFalse(columnEquals(base, other));

        other = new Column("k1", StringType.STRING, false);
        Assertions.assertFalse(columnEquals(base, other));

        base = new Column("k1", TypeFactory.createCharType(5), false);
        other = new Column("k1", TypeFactory.createCharType(10), false);
        Assertions.assertFalse(columnEquals(base, other));

        base = new Column("k1", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 5, 5), false);
        other = new Column("k1", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 6, 5), false);
        Assertions.assertFalse(columnEquals(base, other));

        base = new Column("k1", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 5, 5), false);
        other = new Column("k1", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 5, 4), false);
        Assertions.assertFalse(columnEquals(base, other));
    }

    @Test
    public void testSRTypeToHiveType() {
        Assertions.assertEquals("tinyint", toHiveType(IntegerType.TINYINT));
        Assertions.assertEquals("smallint", toHiveType(IntegerType.SMALLINT));
        Assertions.assertEquals("int", toHiveType(IntegerType.INT));
        Assertions.assertEquals("bigint", toHiveType(IntegerType.BIGINT));
        Assertions.assertEquals("float", toHiveType(FloatType.FLOAT));
        Assertions.assertEquals("double", toHiveType(FloatType.DOUBLE));
        Assertions.assertEquals("boolean", toHiveType(BooleanType.BOOLEAN));
        Assertions.assertEquals("binary", toHiveType(VarbinaryType.VARBINARY));
        Assertions.assertEquals("date", toHiveType(DateType.DATE));
        Assertions.assertEquals("timestamp", toHiveType(DateType.DATETIME));

        Assertions.assertEquals("char(10)", toHiveType(TypeFactory.createCharType(10)));
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Unsupported Hive type: CHAR(10000). Supported CHAR types: CHAR(<=255)",
                () -> toHiveType(TypeFactory.createCharType(10000)));

        Assertions.assertEquals("varchar(100)", toHiveType(TypeFactory.createVarcharType(100)));
        Assertions.assertEquals("string", toHiveType(TypeFactory.createVarcharType(200000)));

        Assertions.assertEquals("string", toHiveType(TypeFactory.createVarcharType(getOlapMaxVarcharLength())));

        ScalarType itemType = DateType.DATE;
        ArrayType arrayType = new ArrayType(new ArrayType(itemType));
        Assertions.assertEquals("array<array<date>>", toHiveType(arrayType));

        ScalarType keyType = IntegerType.TINYINT;
        ScalarType valueType = IntegerType.SMALLINT;
        MapType mapType = new MapType(keyType, valueType);
        String typeStr = "map<tinyint,smallint>";
        Assertions.assertEquals(typeStr, toHiveType(mapType));

        typeStr = "struct<a:struct<aa:date>,b:int>";
        StructField aa = new StructField("aa", DateType.DATE);

        StructType innerStruct = new StructType(Lists.newArrayList(aa));
        StructField a = new StructField("a", innerStruct);
        StructField b = new StructField("b", IntegerType.INT);
        StructType outerStruct = new StructType(Lists.newArrayList(a, b));
        Assertions.assertEquals(typeStr, toHiveType(outerStruct));
    }
}
