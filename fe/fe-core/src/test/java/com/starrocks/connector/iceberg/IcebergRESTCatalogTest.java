// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergRESTCatalogTest {
    @Test
    public void testCatalogType() {
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog();
        Assert.assertEquals(IcebergCatalogType.REST_CATALOG, icebergRESTCatalog.getIcebergCatalogType());
    }

    @Test
    public void testLoadTable(@Mocked IcebergRESTCatalog restCatalog) {
        TableIdentifier identifier = IcebergUtil.getIcebergTableIdentifier("db", "table");
        new Expectations() {
            {
                restCatalog.loadTable(identifier);
                result = new BaseTable(null, "test");
                minTimes = 0;
            }
        };

        new MockUp<CatalogUtil>() {
            @Mock
            public Catalog loadCatalog(String catalogImpl, String catalogName,
                                       Map<String, String> properties,
                                       Configuration hadoopConf) {
                return restCatalog;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        IcebergRESTCatalog icebergRESTCatalog = IcebergRESTCatalog.getInstance(icebergProperties);
        Table table = icebergRESTCatalog.loadTable(identifier);
        Assert.assertEquals("test", table.name());
    }

    @Test
    public void testListAllDatabases(@Mocked IcebergRESTCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.listAllDatabases();
                result = Arrays.asList("db1", "db2");
                minTimes = 0;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        IcebergRESTCatalog icebergRESTCatalog = IcebergRESTCatalog.getInstance(icebergProperties);
        List<String> dbs = icebergRESTCatalog.listAllDatabases();
        Assert.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }
}
