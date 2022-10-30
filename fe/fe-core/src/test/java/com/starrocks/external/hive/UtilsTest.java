// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.external.HiveMetaStoreTableUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class UtilsTest {
    private List<Column> partColumns = Lists.newArrayList(new Column("k1", Type.INT),
            new Column("k2", ScalarType.createVarcharType(10)),
            new Column("k3", Type.DOUBLE),
            new Column("k4", Type.INT));

    @Test
    public void testCreatePartitionKey() throws Exception {
        PartitionKey partitionKey =
                Utils.createPartitionKey(Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE),
                        partColumns);
        Assert.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testGetPartitionValues() throws Exception {
        List<String> values = Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE);
        PartitionKey partitionKey = Utils.createPartitionKey(values, partColumns);
        Assert.assertEquals(values, Utils.getPartitionValues(partitionKey, false));

        List<Column> partColumns1 = Lists.newArrayList(new Column("k1", Type.DATE), new Column("k2", Type.BOOLEAN));
        PartitionKey partitionKey1 = Utils.createPartitionKey(Lists.newArrayList("2021-01-01", "false"), partColumns1);
        List<String> partValues1 = Utils.getPartitionValues(partitionKey1, false);
        Assert.assertEquals("2021-01-01", partValues1.get(0));
        Assert.assertEquals("false", partValues1.get(1));
    }

    @Test
    public void testGetRowCount() {
        Map<String, String> params = Maps.newHashMap();
        Assert.assertEquals(-1L, Utils.getRowCount(params));

        params.put(StatsSetupConst.ROW_COUNT, "10");
        Assert.assertEquals(10L, Utils.getRowCount(params));
    }

    @Test
    public void testGetTotalSize() {
        Map<String, String> params = Maps.newHashMap();
        Assert.assertEquals(-1L, Utils.getTotalSize(params));

        params.put(StatsSetupConst.TOTAL_SIZE, "10");
        Assert.assertEquals(10L, Utils.getTotalSize(params));
    }

    @Test
    public void testGetSuffixName() {
        Assert.assertEquals("file", Utils.getSuffixName("/path/", "/path/file"));
        Assert.assertEquals("file", Utils.getSuffixName("/path", "/path/file"));
        Assert.assertEquals("file", Utils.getSuffixName("/dt=(a)/", "/dt=(a)/file"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSuffixNameIllegal() {
        Utils.getSuffixName("/path//", "/path/file");
    }

    @Test
    public void testGetPartitionValuesFromPath() throws Exception {
        String path = "hdfs://127.0.0.1:10000/path/a=1/b=2/c=3";
        Assert.assertEquals(Lists.newArrayList("1", "2", "3"),
                Utils.getPartitionValues(path, Lists.newArrayList("a", "b", "c")));
    }

    @Test(expected = DdlException.class)
    public void testGetPartitionValuesFromIllegalPath() throws DdlException {
        String path = "hdfs://127.0.0.1:10000/path/1/2/3";
        Assert.assertEquals(Lists.newArrayList("1", "2", "3"),
                Utils.getPartitionValues(path, Lists.newArrayList("a", "b", "c")));
    }

    @Test
    public void testDecimalString() throws DdlException {
        String t1 = "decimal(3,2)";
        int[] res = Utils.getPrecisionAndScale(t1);
        Assert.assertEquals(3, res[0]);
        Assert.assertEquals(2, res[1]);

        t1 = "decimal(222233,4442)";
        res = Utils.getPrecisionAndScale(t1);
        Assert.assertEquals(222233, res[0]);
        Assert.assertEquals(4442, res[1]);

        try {
            t1 = "decimal(3.222,2)";
            Utils.getPrecisionAndScale(t1);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal(a,2)";
            Utils.getPrecisionAndScale(t1);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal(3, 2)";
            Utils.getPrecisionAndScale(t1);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal(-1,2)";
            Utils.getPrecisionAndScale(t1);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal()";
            Utils.getPrecisionAndScale(t1);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }

        try {
            t1 = "decimal(1)";
            Utils.getPrecisionAndScale(t1);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get"));
        }
    }

    @Test
    public void testArrayString() throws DdlException {
        ScalarType itemType = ScalarType.createType(PrimitiveType.DATE);
        ArrayType arrayType = new ArrayType(new ArrayType(itemType));
        String typeStr = "Array<Array<date>>";
        Type resType = Utils.convertToArrayType(typeStr);
        Assert.assertEquals(arrayType, resType);

        itemType = ScalarType.createDefaultString();
        arrayType = new ArrayType(itemType);
        typeStr = "Array<string>";
        resType = Utils.convertToArrayType(typeStr);
        Assert.assertEquals(arrayType, resType);

        itemType = ScalarType.createType(PrimitiveType.INT);
        arrayType = new ArrayType(new ArrayType(new ArrayType(itemType)));
        typeStr = "array<Array<Array<int>>>";
        resType = Utils.convertToArrayType(typeStr);
        Assert.assertEquals(arrayType, resType);

        itemType = ScalarType.createType(PrimitiveType.BIGINT);
        arrayType = new ArrayType(new ArrayType(new ArrayType(itemType)));
        typeStr = "array<Array<Array<bigint>>>";
        resType = Utils.convertToArrayType(typeStr);
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
    public void testCharString() throws DdlException {
        Type charType = ScalarType.createCharType(100);
        String typeStr = "char(100)";
        Type resType = HiveMetaStoreTableUtils.convertColumnType(typeStr);
        Assert.assertEquals(resType, charType);

        typeStr = "char(50)";
        resType = HiveMetaStoreTableUtils.convertColumnType(typeStr);
        Assert.assertNotEquals(resType, charType);
    }

    @Test
    public void testVarcharString() throws DdlException {
        Type varcharType = ScalarType.createVarcharType(100);
        String typeStr = "varchar(100)";
        Type resType = HiveMetaStoreTableUtils.convertColumnType(typeStr);
        Assert.assertEquals(resType, varcharType);

        typeStr = "varchar(50)";
        resType = HiveMetaStoreTableUtils.convertColumnType(typeStr);
        Assert.assertNotEquals(resType, varcharType);

        varcharType = ScalarType.createVarcharType();
        typeStr = "varchar(-1)";
        resType = HiveMetaStoreTableUtils.convertColumnType(typeStr);
        Assert.assertEquals(resType, varcharType);

        Type stringType = ScalarType.createDefaultString();
        typeStr = "string";
        resType = HiveMetaStoreTableUtils.convertColumnType(typeStr);
        Assert.assertEquals(resType, stringType);
    }
}
