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
package com.starrocks.authorization.ranger;

import com.starrocks.authorization.ranger.starrocks.RangerStarRocksResource;
import com.starrocks.catalog.InternalCatalog;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RangerResourceTest {
    @Test
    public void testBasic() {
        RangerAccessResourceImpl starRocksResource = RangerStarRocksResource.builder().setSystem().build();
        Assertions.assertEquals("[system]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("*", starRocksResource.getValue("system"));

        starRocksResource = RangerStarRocksResource.builder().setUser("u1").build();
        Assertions.assertEquals("[user]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("u1", starRocksResource.getValue("user"));

        starRocksResource = RangerStarRocksResource.builder().setCatalog("c1").build();
        Assertions.assertEquals("[catalog]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("c1", starRocksResource.getValue("catalog"));

        starRocksResource = RangerStarRocksResource.builder().setCatalog("c1").setDatabase("d1").build();
        Assertions.assertEquals("[database, catalog]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("c1", starRocksResource.getValue("catalog"));
        Assertions.assertEquals("d1", starRocksResource.getValue("database"));

        starRocksResource = RangerStarRocksResource.builder().setCatalog("c1").setDatabase("d1").setTable("t1").build();
        Assertions.assertEquals("[database, catalog, table]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("c1", starRocksResource.getValue("catalog"));
        Assertions.assertEquals("d1", starRocksResource.getValue("database"));
        Assertions.assertEquals("t1", starRocksResource.getValue("table"));

        starRocksResource = RangerStarRocksResource.builder().setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .setDatabase("d1").setView("v1").build();
        Assertions.assertEquals("[database, view, catalog]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("default_catalog", starRocksResource.getValue("catalog"));
        Assertions.assertEquals("d1", starRocksResource.getValue("database"));
        Assertions.assertEquals("v1", starRocksResource.getValue("view"));

        starRocksResource = RangerStarRocksResource.builder().setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .setDatabase("d1").setMaterializedView("mv1").build();
        Assertions.assertEquals("[database, catalog, materialized_view]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("default_catalog", starRocksResource.getValue("catalog"));
        Assertions.assertEquals("d1", starRocksResource.getValue("database"));
        Assertions.assertEquals("mv1", starRocksResource.getValue("materialized_view"));

        starRocksResource = RangerStarRocksResource.builder().setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .setDatabase("d1").setFunction("f1").build();
        Assertions.assertEquals("[database, catalog, function]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("default_catalog", starRocksResource.getValue("catalog"));
        Assertions.assertEquals("d1", starRocksResource.getValue("database"));
        Assertions.assertEquals("f1", starRocksResource.getValue("function"));

        starRocksResource = RangerStarRocksResource.builder().setGlobalFunction("gf1").build();
        Assertions.assertEquals("[global_function]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("gf1", starRocksResource.getValue("global_function"));

        starRocksResource = RangerStarRocksResource.builder().setResource("r1").build();
        Assertions.assertEquals("[resource]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("r1", starRocksResource.getValue("resource"));

        starRocksResource = RangerStarRocksResource.builder().setResourceGroup("rg1").build();
        Assertions.assertEquals("[resource_group]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("rg1", starRocksResource.getValue("resource_group"));

        starRocksResource = RangerStarRocksResource.builder().setStorageVolume("sv1").build();
        Assertions.assertEquals("[storage_volume]", starRocksResource.getKeys().toString());
        Assertions.assertEquals("sv1", starRocksResource.getValue("storage_volume"));
    }
}
