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

package com.starrocks.authorization.opa;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class OpaResourceTest {
    private static final Gson GSON = new Gson();

    @Test
    public void testBasic() {
        JsonObject resource = resourceJson(OpaResource.system());
        assertOnlyKeys(resource, "system");
        Assertions.assertEquals("*", resource.get("system").getAsString());

        resource = resourceJson(OpaResource.user("u1"));
        assertOnlyKeys(resource, "user");
        Assertions.assertEquals("u1", resource.get("user").getAsString());

        resource = resourceJson(OpaResource.catalog("c1"));
        assertOnlyKeys(resource, "catalog");
        Assertions.assertEquals("c1", resource.get("catalog").getAsString());

        resource = resourceJson(OpaResource.database("c1", "d1"));
        assertOnlyKeys(resource, "catalog", "database");
        Assertions.assertEquals("c1", resource.get("catalog").getAsString());
        Assertions.assertEquals("d1", resource.get("database").getAsString());

        resource = resourceJson(OpaResource.table(new TableName("c1", "d1", "t1")));
        assertOnlyKeys(resource, "catalog", "database", "table");
        Assertions.assertEquals("c1", resource.get("catalog").getAsString());
        Assertions.assertEquals("d1", resource.get("database").getAsString());
        Assertions.assertEquals("t1", resource.get("table").getAsString());

        resource = resourceJson(OpaResource.view(new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "d1", "v1")));
        assertOnlyKeys(resource, "catalog", "database", "view");
        Assertions.assertEquals("default_catalog", resource.get("catalog").getAsString());
        Assertions.assertEquals("d1", resource.get("database").getAsString());
        Assertions.assertEquals("v1", resource.get("view").getAsString());

        resource = resourceJson(OpaResource.materializedView(
                new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "d1", "mv1")));
        assertOnlyKeys(resource, "catalog", "database", "materialized_view");
        Assertions.assertEquals("default_catalog", resource.get("catalog").getAsString());
        Assertions.assertEquals("d1", resource.get("database").getAsString());
        Assertions.assertEquals("mv1", resource.get("materialized_view").getAsString());

        resource = resourceJson(OpaResource.function(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "d1", "f1"));
        assertOnlyKeys(resource, "catalog", "database", "function");
        Assertions.assertEquals("default_catalog", resource.get("catalog").getAsString());
        Assertions.assertEquals("d1", resource.get("database").getAsString());
        Assertions.assertEquals("f1", resource.get("function").getAsString());

        resource = resourceJson(OpaResource.globalFunction("gf1"));
        assertOnlyKeys(resource, "global_function");
        Assertions.assertEquals("gf1", resource.get("global_function").getAsString());

        resource = resourceJson(OpaResource.resource("r1"));
        assertOnlyKeys(resource, "resource");
        Assertions.assertEquals("r1", resource.get("resource").getAsString());

        resource = resourceJson(OpaResource.resourceGroup("rg1"));
        assertOnlyKeys(resource, "resource_group");
        Assertions.assertEquals("rg1", resource.get("resource_group").getAsString());

        resource = resourceJson(OpaResource.storageVolume("sv1"));
        assertOnlyKeys(resource, "storage_volume");
        Assertions.assertEquals("sv1", resource.get("storage_volume").getAsString());

        resource = resourceJson(OpaResource.pipe("d1", "p1"));
        assertOnlyKeys(resource, "catalog", "database", "pipe");
        Assertions.assertEquals("default_catalog", resource.get("catalog").getAsString());
        Assertions.assertEquals("d1", resource.get("database").getAsString());
        Assertions.assertEquals("p1", resource.get("pipe").getAsString());

        resource = resourceJson(OpaResource.warehouse("w1"));
        assertOnlyKeys(resource, "warehouse");
        Assertions.assertEquals("w1", resource.get("warehouse").getAsString());
    }

    private static JsonObject resourceJson(OpaResource resource) {
        return GSON.toJsonTree(resource).getAsJsonObject();
    }

    private static void assertOnlyKeys(JsonObject resource, String... keys) {
        Assertions.assertEquals(Set.of(keys), resource.keySet());
    }
}
