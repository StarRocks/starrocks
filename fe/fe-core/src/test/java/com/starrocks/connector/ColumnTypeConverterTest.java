// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

import static com.starrocks.connector.ColumnTypeConverter.fromHiveTypeToArrayType;
import static com.starrocks.connector.ColumnTypeConverter.fromHiveTypeToMapType;
import static com.starrocks.connector.ColumnTypeConverter.fromHudiType;
import static com.starrocks.connector.ColumnTypeConverter.getPrecisionAndScale;

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
            t1 = "decimal(3, 2)";
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

        itemType = ScalarType.createDefaultString();
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
        try {
            new ArrayType(new ArrayType(itemType));
            Assert.fail();
        } catch (InternalError e) {
            Assert.assertTrue(e.getMessage().contains("Decimal32/64/128"));
        }
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
        valueType = ScalarType.createDefaultString();
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

        Type stringType = ScalarType.createDefaultString();
        typeStr = "string";
        resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assert.assertEquals(resType, stringType);
    }

    @Test
    public void testArraySchema() {
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
        Assert.assertEquals(fromHudiType(arraySchema), new ArrayType(ScalarType.createDefaultString()));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.BYTES));
        arraySchema = Schema.createArray(unionSchema);
        Assert.assertEquals(fromHudiType(arraySchema), new ArrayType(ScalarType.createType(PrimitiveType.VARCHAR)));
    }
}
