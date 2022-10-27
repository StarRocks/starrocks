// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.iceberg;

import com.starrocks.external.iceberg.glue.IcebergGlueCatalog;
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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergGlueCatalogTest {
    @Test
    public void testCatalogType() {
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = IcebergGlueCatalog.getInstance("glue_iceberg", icebergProperties);
        Assert.assertEquals(IcebergCatalogType.GLUE_CATALOG, icebergGlueCatalog.getIcebergCatalogType());
    }

    @Test
    public void testLoadTable(@Mocked IcebergGlueCatalog glueCatalog) {
        TableIdentifier identifier = IcebergUtil.getIcebergTableIdentifier("db", "table");
        new Expectations() {
            {
                glueCatalog.loadTable(identifier);
                result = new BaseTable(null, "test");
                minTimes = 0;
            }
        };

        new MockUp<CatalogUtil>() {
            @Mock
            public Catalog loadCatalog(String catalogImpl, String catalogName,
                                       Map<String, String> properties,
                                       Configuration hadoopConf) {
                return glueCatalog;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = IcebergGlueCatalog.getInstance("glue_iceberg", icebergProperties);
        Table table = icebergGlueCatalog.loadTable(identifier);
        Assert.assertEquals("test", table.name());
    }

    @Test
    public void testInitialize() {
        try {
            IcebergGlueCatalog catalog = new IcebergGlueCatalog();
            catalog.initialize("glue_iceberg", Maps.newHashMap());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testListAllDatabases(@Mocked IcebergGlueCatalog glueCatalog) {
        new Expectations() {
            {
                glueCatalog.listAllDatabases();
                result = Arrays.asList("db1", "db2");
                minTimes = 0;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = IcebergGlueCatalog.getInstance("glue_iceberg", icebergProperties);
        List<String> dbs = icebergGlueCatalog.listAllDatabases();
        Assert.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }
}
