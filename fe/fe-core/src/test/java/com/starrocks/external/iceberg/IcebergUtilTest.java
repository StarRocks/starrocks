// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg;

import com.starrocks.external.hive.HdfsFileFormat;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Assert;
import org.junit.Test;

public class IcebergUtilTest {

    @Test
    public void testGetIcebergTableIdentifier() {
        TableIdentifier identifier = IcebergUtil.getIcebergTableIdentifier("database", "table");
        Assert.assertTrue(identifier.toString().equals("database.table"));
    }

    @Test
    public void testGetHdfsFileFormat() {
        HdfsFileFormat fileFormat = IcebergUtil.getHdfsFileFormat(FileFormat.PARQUET);
        Assert.assertTrue(fileFormat.equals(HdfsFileFormat.PARQUET));
        Assert.assertThrows("Unexpected file format: %s", StarRocksIcebergException.class, () -> {
            IcebergUtil.getHdfsFileFormat(FileFormat.AVRO);
        });
    }

    @Test
    public void testGetIcebergHiveCatalog() {
        IcebergCatalog catalog = IcebergUtil.getIcebergHiveCatalog("thrift://test:9030");
        Assert.assertTrue(catalog instanceof IcebergHiveCatalog);
    }
}
