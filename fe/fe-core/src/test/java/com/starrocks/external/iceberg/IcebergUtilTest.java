// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.iceberg;

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.external.hive.RemoteFileInputFormat;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.external.iceberg.IcebergUtil.convertColumnType;


public class IcebergUtilTest {

    @Test
    public void testGetIcebergTableIdentifier() {
        TableIdentifier identifier = IcebergUtil.getIcebergTableIdentifier("database", "table");
        Assert.assertTrue(identifier.toString().equals("database.table"));
    }

    @Test
    public void testGetHdfsFileFormat() {
        RemoteFileInputFormat fileFormat = IcebergUtil.getHdfsFileFormat(FileFormat.PARQUET);
        Assert.assertTrue(fileFormat.equals(RemoteFileInputFormat.PARQUET));
        Assert.assertThrows("Unexpected file format: %s", StarRocksIcebergException.class, () -> {
            IcebergUtil.getHdfsFileFormat(FileFormat.AVRO);
        });
    }

    @Test
    public void testGetIcebergHiveCatalog() {
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergCatalog catalog = IcebergUtil.getIcebergHiveCatalog("thrift://test:9030", icebergProperties);
        Assert.assertTrue(catalog instanceof IcebergHiveCatalog);
    }

    @Test
    public void testDecimal() {
        int precision = 9;
        int scale = 5;
        Type decimalType = ScalarType.createUnifiedDecimalType(precision, scale);
        org.apache.iceberg.types.Type icebergType = Types.DecimalType.of(precision, scale);
        Type resType = convertColumnType(icebergType);
        Assert.assertEquals(resType, decimalType);
    }

    @Test
    public void testString() {
        Type stringType = ScalarType.createDefaultString();
        org.apache.iceberg.types.Type icebergType = Types.StringType.get();
        Type resType = convertColumnType(icebergType);
        Assert.assertEquals(resType, stringType);
    }

    @Test
    public void testArray() {
        Type stringType = new ArrayType(ScalarType.createType(PrimitiveType.INT));
        org.apache.iceberg.types.Type icebergType = Types.ListType.ofRequired(136, Types.IntegerType.get());
        Type resType = convertColumnType(icebergType);
        Assert.assertEquals(resType, stringType);
    }

    @Test
    public void testUnsupported() {
        org.apache.iceberg.types.Type icebergType = Types.MapType.ofRequired(1, 2,
                Types.StringType.get(), Types.StringType.get());
        Type resType = convertColumnType(icebergType);
        Assert.assertEquals(resType, ScalarType.createType(PrimitiveType.UNKNOWN_TYPE));
    }
}
