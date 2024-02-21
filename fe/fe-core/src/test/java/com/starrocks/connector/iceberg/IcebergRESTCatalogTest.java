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

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergRESTCatalogTest {
    @Test
    public void testListAllDatabases(@Mocked RESTCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.listNamespaces();
                result = ImmutableList.of(Namespace.of("db1"), Namespace.of("db2"));
                times = 1;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), icebergProperties);
        List<String> dbs = icebergRESTCatalog.listAllDatabases();
        Assert.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }

    @Test
    public void testTableExists(@Mocked RESTCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), new HashMap<>());
        boolean exists = icebergRESTCatalog.tableExists("db1", "tbl1");
        Assert.assertTrue(exists);
    }

    @Test
    public void testRenameTable(@Mocked RESTCatalog restCatalog) {
        new Expectations() {
            {
                restCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        IcebergRESTCatalog icebergRESTCatalog = new IcebergRESTCatalog(
                "rest_native_catalog", new Configuration(), new HashMap<>());
        icebergRESTCatalog.renameTable("db", "tb1", "tb2");
        boolean exists = icebergRESTCatalog.tableExists("db", "tbl2");
        Assert.assertTrue(exists);
    }
}
