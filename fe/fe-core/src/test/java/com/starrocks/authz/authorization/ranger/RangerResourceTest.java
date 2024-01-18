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
package com.starrocks.authz.authorization.ranger;

import com.starrocks.authz.authorization.ObjectType;
import com.starrocks.authz.authorization.ranger.starrocks.RangerStarRocksResource;
import org.apache.hadoop.util.Lists;
import org.junit.Assert;
import org.junit.Test;

public class RangerResourceTest {
    @Test
    public void testBasic() {
        RangerStarRocksResource starRocksResource = new RangerStarRocksResource(ObjectType.SYSTEM, null);
        Assert.assertEquals("[system]", starRocksResource.getKeys().toString());
        Assert.assertEquals("*", starRocksResource.getValue("system"));

        starRocksResource = new RangerStarRocksResource(ObjectType.USER, Lists.newArrayList("u1"));
        Assert.assertEquals("[user]", starRocksResource.getKeys().toString());
        Assert.assertEquals("u1", starRocksResource.getValue("user"));

        starRocksResource = new RangerStarRocksResource(ObjectType.CATALOG, Lists.newArrayList("c1"));
        Assert.assertEquals("[catalog]", starRocksResource.getKeys().toString());
        Assert.assertEquals("c1", starRocksResource.getValue("catalog"));

        starRocksResource = new RangerStarRocksResource(ObjectType.DATABASE, Lists.newArrayList("c1", "d1"));
        Assert.assertEquals("[database, catalog]", starRocksResource.getKeys().toString());
        Assert.assertEquals("c1", starRocksResource.getValue("catalog"));
        Assert.assertEquals("d1", starRocksResource.getValue("database"));

        starRocksResource = new RangerStarRocksResource(ObjectType.TABLE, Lists.newArrayList("c1", "d1", "t1"));
        Assert.assertEquals("[database, catalog, table]", starRocksResource.getKeys().toString());
        Assert.assertEquals("c1", starRocksResource.getValue("catalog"));
        Assert.assertEquals("d1", starRocksResource.getValue("database"));
        Assert.assertEquals("t1", starRocksResource.getValue("table"));

        starRocksResource = new RangerStarRocksResource(ObjectType.VIEW, Lists.newArrayList("d1", "v1"));
        Assert.assertEquals("[database, view, catalog]", starRocksResource.getKeys().toString());
        Assert.assertEquals("default_catalog", starRocksResource.getValue("catalog"));
        Assert.assertEquals("d1", starRocksResource.getValue("database"));
        Assert.assertEquals("v1", starRocksResource.getValue("view"));

        starRocksResource = new RangerStarRocksResource(ObjectType.MATERIALIZED_VIEW, Lists.newArrayList("d1", "mv1"));
        Assert.assertEquals("[database, catalog, materialized_view]", starRocksResource.getKeys().toString());
        Assert.assertEquals("default_catalog", starRocksResource.getValue("catalog"));
        Assert.assertEquals("d1", starRocksResource.getValue("database"));
        Assert.assertEquals("mv1", starRocksResource.getValue("materialized_view"));

        starRocksResource = new RangerStarRocksResource(ObjectType.FUNCTION, Lists.newArrayList("d1", "f1"));
        Assert.assertEquals("[database, catalog, function]", starRocksResource.getKeys().toString());
        Assert.assertEquals("default_catalog", starRocksResource.getValue("catalog"));
        Assert.assertEquals("d1", starRocksResource.getValue("database"));
        Assert.assertEquals("f1", starRocksResource.getValue("function"));

        starRocksResource = new RangerStarRocksResource(ObjectType.GLOBAL_FUNCTION, Lists.newArrayList("gf1"));
        Assert.assertEquals("[global_function]", starRocksResource.getKeys().toString());
        Assert.assertEquals("gf1", starRocksResource.getValue("global_function"));

        starRocksResource = new RangerStarRocksResource(ObjectType.RESOURCE, Lists.newArrayList("r1"));
        Assert.assertEquals("[resource]", starRocksResource.getKeys().toString());
        Assert.assertEquals("r1", starRocksResource.getValue("resource"));

        starRocksResource = new RangerStarRocksResource(ObjectType.RESOURCE_GROUP, Lists.newArrayList("rg1"));
        Assert.assertEquals("[resource_group]", starRocksResource.getKeys().toString());
        Assert.assertEquals("rg1", starRocksResource.getValue("resource_group"));

        starRocksResource = new RangerStarRocksResource(ObjectType.STORAGE_VOLUME, Lists.newArrayList("sv1"));
        Assert.assertEquals("[storage_volume]", starRocksResource.getKeys().toString());
        Assert.assertEquals("sv1", starRocksResource.getValue("storage_volume"));
    }
}
