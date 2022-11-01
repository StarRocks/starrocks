// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HivePartitionName;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static com.starrocks.connector.PartitionUtil.createPartitionKey;
import static com.starrocks.connector.PartitionUtil.fromPartitionKey;
import static com.starrocks.connector.PartitionUtil.getSuffixName;
import static com.starrocks.connector.PartitionUtil.toPartitionValues;

public class PartitionUtilTest {
    private final List<Column> partColumns = Lists.newArrayList(new Column("k1", Type.INT),
            new Column("k2", ScalarType.createVarcharType(10)),
            new Column("k3", Type.DOUBLE),
            new Column("k4", Type.INT));

    @Test
    public void testCreatePartitionKey() throws Exception {
        PartitionKey partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE), partColumns);
        Assert.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testCreateHudiPartitionKey() throws AnalysisException {
        PartitionKey partitionKey = createPartitionKey(
                Lists.newArrayList("1", "a", "3.0", HiveMetaClient.HUDI_PARTITION_NULL_VALUE), partColumns, Table.TableType.HUDI);
        Assert.assertEquals("(\"1\", \"a\", \"3.0\", \"NULL\")", partitionKey.toSql());
    }

    @Test
    public void testGetPartitionValues() throws Exception {
        List<String> values = Lists.newArrayList("1", "a", "3.0", HiveMetaClient.PARTITION_NULL_VALUE);
        PartitionKey partitionKey = createPartitionKey(values, partColumns);
        Assert.assertEquals(values, fromPartitionKey(partitionKey));
    }

    @Test
    public void testGetSuffixName() {
        Assert.assertEquals("file", getSuffixName("/path/", "/path/file"));
        Assert.assertEquals("file", getSuffixName("/path", "/path/file"));
        Assert.assertEquals("file", getSuffixName("/dt=(a)/", "/dt=(a)/file"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSuffixNameIllegal() {
        getSuffixName("/path//", "/path/file");
    }

    @Test
    public void testToPartitionValues() {
        String  partitionNames = "a=1/b=2/c=3";
        Assert.assertEquals(Lists.newArrayList("1", "2", "3"), toPartitionValues(partitionNames));
    }

    @Test
    public void testFromPartitionKey() {
        PartitionKey partitionKey = new PartitionKey();
        LiteralExpr boolTrue1 = new BoolLiteral(true);
        partitionKey.pushColumn(boolTrue1, PrimitiveType.BOOLEAN);
        Assert.assertEquals(Lists.newArrayList("true"), fromPartitionKey(partitionKey));
    }

    @Test
    public void testHivePartitionNames() {
        List<String> partitionValues = Lists.newArrayList("1", "2", "3");
        String partitionNames = "a=1/b=2/c=3";
        HivePartitionName hivePartitionName = new HivePartitionName("db", "table",
                partitionValues, Optional.of(partitionNames));
        Assert.assertEquals("HivePartitionName{databaseName='db', tableName='table'," +
                " partitionValues=[1, 2, 3], partitionNames=Optional[a=1/b=2/c=3]}", hivePartitionName.toString());
    }
}
