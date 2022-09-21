// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.external.ColumnTypeConverter;
import com.starrocks.external.HiveMetaStoreTableUtils;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.external.ColumnTypeConverter.convertToArrayType;
import static com.starrocks.external.ColumnTypeConverter.convertToMapType;
import static com.starrocks.external.ColumnTypeConverter.getPrecisionAndScale;
import static com.starrocks.external.ColumnTypeConverter.getSuffixName;
import static com.starrocks.external.Utils.createPartitionKey;
import static com.starrocks.external.Utils.getPartitionValues;
import static com.starrocks.external.Utils.getRowCount;
import static com.starrocks.external.Utils.getTotalSize;

public class UtilsTest {
    private List<Column> partColumns = Lists.newArrayList(new Column("k1", Type.INT),
            new Column("k2", ScalarType.createVarcharType(10)),
            new Column("k3", Type.DOUBLE),
            new Column("k4", Type.INT));

    @Test
    public void testCreatePartitionKey() throws Exception {
        PartitionKey partitionKey =
                createPartitionKey(Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE),
                        partColumns);
        Assert.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testGetPartitionValues() throws Exception {
        List<String> values = Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE);
        PartitionKey partitionKey = createPartitionKey(values, partColumns);
        Assert.assertEquals(values, getPartitionValues(partitionKey, false));

        List<Column> partColumns1 = Lists.newArrayList(new Column("k1", Type.DATE), new Column("k2", Type.BOOLEAN));
        PartitionKey partitionKey1 = createPartitionKey(Lists.newArrayList("2021-01-01", "false"), partColumns1);
        List<String> partValues1 = getPartitionValues(partitionKey1, false);
        Assert.assertEquals("2021-01-01", partValues1.get(0));
        Assert.assertEquals("false", partValues1.get(1));
    }

    @Test
    public void testGetRowCount() {
        Map<String, String> params = Maps.newHashMap();
        Assert.assertEquals(-1L, getRowCount(params));

        params.put(StatsSetupConst.ROW_COUNT, "10");
        Assert.assertEquals(10L, getRowCount(params));
    }

    @Test
    public void testGetTotalSize() {
        Map<String, String> params = Maps.newHashMap();
        Assert.assertEquals(-1L, getTotalSize(params));

        params.put(StatsSetupConst.TOTAL_SIZE, "10");
        Assert.assertEquals(10L, getTotalSize(params));
    }

    @Test
    public void testGetSuffixName() {
        Assert.assertEquals("file", getSuffixName("/path/", "/path/file"));
        Assert.assertEquals("file", getSuffixName("/path", "/path/file"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSuffixNameIllegal() {
        getSuffixName("/path//", "/path/file");
    }

    @Test
    public void testGetPartitionValuesFromPath() {
        String path = "hdfs://127.0.0.1:10000/path/a=1/b=2/c=3";
        Assert.assertEquals(Lists.newArrayList("1", "2", "3"),
                getPartitionValues(path, Lists.newArrayList("a", "b", "c")));
    }

    @Test(expected = StarRocksConnectorException.class)
    public void testGetPartitionValuesFromIllegalPath() {
        String path = "hdfs://127.0.0.1:10000/path/1/2/3";
        Assert.assertEquals(Lists.newArrayList("1", "2", "3"),
                getPartitionValues(path, Lists.newArrayList("a", "b", "c")));
    }

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
    public void testArrayString() throws DdlException {
        ScalarType itemType = ScalarType.createType(PrimitiveType.DATE);
        ArrayType arrayType = new ArrayType(new ArrayType(itemType));
        String typeStr = "Array<Array<date>>";
        Type resType = convertToArrayType(typeStr);
        Assert.assertEquals(arrayType, resType);

        itemType = ScalarType.createDefaultString();
        arrayType = new ArrayType(itemType);
        typeStr = "Array<string>";
        resType = convertToArrayType(typeStr);
        Assert.assertEquals(arrayType, resType);

        itemType = ScalarType.createType(PrimitiveType.INT);
        arrayType = new ArrayType(new ArrayType(new ArrayType(itemType)));
        typeStr = "array<Array<Array<int>>>";
        resType = convertToArrayType(typeStr);
        Assert.assertEquals(arrayType, resType);

        itemType = ScalarType.createType(PrimitiveType.BIGINT);
        arrayType = new ArrayType(new ArrayType(new ArrayType(itemType)));
        typeStr = "array<Array<Array<bigint>>>";
        resType = convertToArrayType(typeStr);
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
    public void testMapString() throws DdlException {
        ScalarType keyType = ScalarType.createType(PrimitiveType.TINYINT);
        ScalarType valueType = ScalarType.createType(PrimitiveType.SMALLINT);
        MapType mapType = new MapType(keyType, valueType);
        String typeStr = "map<tinyint,smallint>";
        Type resType = convertToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createType(PrimitiveType.INT);
        valueType = ScalarType.createType(PrimitiveType.INT);
        mapType = new MapType(keyType, valueType);
        typeStr = "Map<INT,INTEGER>";
        resType = convertToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createType(PrimitiveType.FLOAT);
        valueType = ScalarType.createType(PrimitiveType.DOUBLE);
        mapType = new MapType(keyType, valueType);
        typeStr = "map<float,double>";
        resType = convertToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createUnifiedDecimalType(10, 7);
        valueType = ScalarType.createType(PrimitiveType.DATETIME);
        mapType = new MapType(keyType, valueType);
        typeStr = "map<decimal(10,7),timestamp>";
        resType = convertToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createType(PrimitiveType.DATE);
        valueType = ScalarType.createDefaultString();
        mapType = new MapType(keyType, valueType);
        typeStr = "map<date,string>";
        resType = convertToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createVarcharType(10);
        valueType = ScalarType.createCharType(5);
        mapType = new MapType(keyType, valueType);
        typeStr = "map<varchar(10),char(5)>";
        resType = convertToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createType(PrimitiveType.BOOLEAN);
        valueType = ScalarType.createVarcharType(10);
        mapType = new MapType(keyType, valueType);
        typeStr = "map<boolean,varchar(10)>";
        resType = convertToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createCharType(10);
        ScalarType itemType = ScalarType.createType(PrimitiveType.INT);
        ArrayType vType = new ArrayType(itemType);
        mapType = new MapType(keyType, vType);
        typeStr = "map<char(10),array<int>>";
        resType = convertToMapType(typeStr);
        Assert.assertEquals(mapType, resType);

        keyType = ScalarType.createCharType(10);
        ScalarType inKeyType = ScalarType.createType(PrimitiveType.INT);
        itemType = ScalarType.createType(PrimitiveType.DATETIME);
        ArrayType inValueType = new ArrayType(itemType);
        MapType mValueType = new MapType(inKeyType, inValueType);
        mapType = new MapType(keyType, mValueType);
        typeStr = "map<char(10),map<int,array<timestamp>>>";
        resType = convertToMapType(typeStr);
        Assert.assertEquals(mapType, resType);
    }

    @Test
    public void testCharString() throws DdlException {
        Type charType = ScalarType.createCharType(100);
        String typeStr = "char(100)";
        Type resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assert.assertEquals(resType, charType);

        typeStr = "char(50)";
        resType = ColumnTypeConverter.fromHiveType(typeStr);
        Assert.assertNotEquals(resType, charType);
    }

    @Test
    public void testVarcharString() throws DdlException {
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
    public void testArraySchema() throws DdlException {
        Schema unionSchema = null;
        Schema arraySchema = null;

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.INT));
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(unionSchema),
                ScalarType.createType(PrimitiveType.INT));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.INT));
        arraySchema = Schema.createArray(unionSchema);
        Schema.createArray(unionSchema);
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(arraySchema),
                new ArrayType(ScalarType.createType(PrimitiveType.INT)));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.BOOLEAN));
        arraySchema = Schema.createArray(unionSchema);
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(arraySchema),
                new ArrayType(ScalarType.createType(PrimitiveType.BOOLEAN)));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.STRING));
        arraySchema = Schema.createArray(unionSchema);
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(arraySchema),
                new ArrayType(ScalarType.createDefaultString()));

        unionSchema = Schema.createUnion(Schema.create(Schema.Type.BYTES));
        arraySchema = Schema.createArray(unionSchema);
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(arraySchema),
                new ArrayType(ScalarType.createType(PrimitiveType.VARCHAR)));
    }
}
