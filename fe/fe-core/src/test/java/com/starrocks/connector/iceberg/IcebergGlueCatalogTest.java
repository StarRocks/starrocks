// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.connector.iceberg;

import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.glue.IcebergGlueCatalog;
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
        IcebergGlueCatalog icebergGlueCatalog =
                (IcebergGlueCatalog) CatalogLoader.glue("glue_native_catalog", new Configuration(), icebergProperties)
                        .loadCatalog();
        Assert.assertEquals(IcebergCatalogType.GLUE_CATALOG, icebergGlueCatalog.getIcebergCatalogType());
    }

    @Test
    public void testLoadTable(@Mocked IcebergGlueCatalog glueCatalog) {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
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
        IcebergGlueCatalog icebergGlueCatalog =
                (IcebergGlueCatalog) CatalogLoader.glue("glue_native_catalog", new Configuration(), icebergProperties)
                        .loadCatalog();
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
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        IcebergGlueCatalog icebergGlueCatalog =
                (IcebergGlueCatalog) CatalogLoader.glue("glue_native_catalog", new Configuration(), icebergProperties)
                        .loadCatalog();
        List<String> dbs = icebergGlueCatalog.listAllDatabases();
        Assert.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }
}
