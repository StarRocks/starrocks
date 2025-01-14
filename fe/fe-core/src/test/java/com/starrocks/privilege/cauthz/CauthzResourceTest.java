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
package com.starrocks.privilege.cauthz;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.privilege.cauthz.CauthzAccessResourceImpl;
import com.starrocks.privilege.cauthz.starrocks.CauthzStarRocksResource;
import org.junit.Assert;
import org.junit.Test;

public class CauthzResourceTest {
    @Test
    public void testBasic() {
        CauthzAccessResourceImpl starRocksResource = CauthzStarRocksResource.builder().setSystem().build();
        Assert.assertEquals("[system]", starRocksResource.getKeys().toString());
        Assert.assertEquals("*", starRocksResource.getValue("system"));

        starRocksResource = CauthzStarRocksResource.builder().setUser("u1").build();
        Assert.assertEquals("[user]", starRocksResource.getKeys().toString());
        Assert.assertEquals("u1", starRocksResource.getValue("user"));

        starRocksResource = CauthzStarRocksResource.builder().setCatalog("c1").build();
        Assert.assertEquals("[catalog]", starRocksResource.getKeys().toString());
        Assert.assertEquals("c1", starRocksResource.getValue("catalog"));

        starRocksResource = CauthzStarRocksResource.builder().setCatalog("c1").setDatabase("d1").build();
        Assert.assertEquals("[database, catalog]", starRocksResource.getKeys().toString());
        Assert.assertEquals("c1", starRocksResource.getValue("catalog"));
        Assert.assertEquals("d1", starRocksResource.getValue("database"));

        starRocksResource = CauthzStarRocksResource.builder().setCatalog("c1").setDatabase("d1").setTable("t1").build();
        Assert.assertEquals("[database, catalog, table]", starRocksResource.getKeys().toString());
        Assert.assertEquals("c1", starRocksResource.getValue("catalog"));
        Assert.assertEquals("d1", starRocksResource.getValue("database"));
        Assert.assertEquals("t1", starRocksResource.getValue("table"));

        starRocksResource = CauthzStarRocksResource.builder().setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .setDatabase("d1").setView("v1").build();
        Assert.assertEquals("[database, view, catalog]", starRocksResource.getKeys().toString());
        Assert.assertEquals("default_catalog", starRocksResource.getValue("catalog"));
        Assert.assertEquals("d1", starRocksResource.getValue("database"));
        Assert.assertEquals("v1", starRocksResource.getValue("view"));

        starRocksResource = CauthzStarRocksResource.builder().setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .setDatabase("d1").setMaterializedView("mv1").build();
        Assert.assertEquals("[database, catalog, materialized_view]", starRocksResource.getKeys().toString());
        Assert.assertEquals("default_catalog", starRocksResource.getValue("catalog"));
        Assert.assertEquals("d1", starRocksResource.getValue("database"));
        Assert.assertEquals("mv1", starRocksResource.getValue("materialized_view"));

        starRocksResource = CauthzStarRocksResource.builder().setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .setDatabase("d1").setFunction("f1").build();
        Assert.assertEquals("[database, catalog, function]", starRocksResource.getKeys().toString());
        Assert.assertEquals("default_catalog", starRocksResource.getValue("catalog"));
        Assert.assertEquals("d1", starRocksResource.getValue("database"));
        Assert.assertEquals("f1", starRocksResource.getValue("function"));

        starRocksResource = CauthzStarRocksResource.builder().setGlobalFunction("gf1").build();
        Assert.assertEquals("[global_function]", starRocksResource.getKeys().toString());
        Assert.assertEquals("gf1", starRocksResource.getValue("global_function"));

        starRocksResource = CauthzStarRocksResource.builder().setResource("r1").build();
        Assert.assertEquals("[resource]", starRocksResource.getKeys().toString());
        Assert.assertEquals("r1", starRocksResource.getValue("resource"));

        starRocksResource = CauthzStarRocksResource.builder().setResourceGroup("rg1").build();
        Assert.assertEquals("[resource_group]", starRocksResource.getKeys().toString());
        Assert.assertEquals("rg1", starRocksResource.getValue("resource_group"));

        starRocksResource = CauthzStarRocksResource.builder().setStorageVolume("sv1").build();
        Assert.assertEquals("[storage_volume]", starRocksResource.getKeys().toString());
        Assert.assertEquals("sv1", starRocksResource.getValue("storage_volume"));
    }
}