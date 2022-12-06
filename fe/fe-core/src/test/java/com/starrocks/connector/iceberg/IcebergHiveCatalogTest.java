// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.IcebergHiveCatalog;
import com.starrocks.connector.iceberg.IcebergUtil;
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
        Assert.assertEquals("test", table.name());
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

    @Test
    public void testListAllDatabases(@Mocked IcebergHiveCatalog hiveCatalog) {
        new Expectations() {
            {
                hiveCatalog.listAllDatabases();
                result = Arrays.asList("db1", "db2");
                minTimes = 0;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        IcebergHiveCatalog icebergHiveCatalog = IcebergHiveCatalog.getInstance("thrift://test:9030", icebergProperties);
        List<String> dbs = icebergHiveCatalog.listAllDatabases();
        Assert.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }
}
