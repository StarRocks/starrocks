// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg;

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

import java.util.HashMap;
import java.util.Map;

public class IcebergHiveCatalogTest {

    @Test
    public void testCatalogType() {
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergHiveCatalog icebergHiveCatalog = IcebergHiveCatalog.getInstance("thrift://test:9030", icebergProperties);
        Assert.assertEquals(IcebergCatalogType.HIVE_CATALOG, icebergHiveCatalog.getIcebergCatalogType());
    }

    @Test
    public void testLoadTable(@Mocked IcebergHiveCatalog hiveCatalog) {
        TableIdentifier identifier = IcebergUtil.getIcebergTableIdentifier("db", "table");
        new Expectations() {
            {
                hiveCatalog.loadTable(identifier);
                result = new BaseTable(null, "test");
                minTimes = 0;
            }
        };

        new MockUp<CatalogUtil>() {
            @Mock
            public Catalog loadCatalog(String catalogImpl, String catalogName,
                                       Map<String, String> properties,
                                       Configuration hadoopConf) {
                return hiveCatalog;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        IcebergHiveCatalog icebergHiveCatalog = IcebergHiveCatalog.getInstance("thrift://test:9030", icebergProperties);
        Table table = icebergHiveCatalog.loadTable(identifier);
        Assert.assertTrue(table.name().equals("test"));
    }

    @Test
    public void testInitialize() {
        try {
            IcebergHiveCatalog catalog = new IcebergHiveCatalog();
            catalog.initialize("hive", Maps.newHashMap());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
