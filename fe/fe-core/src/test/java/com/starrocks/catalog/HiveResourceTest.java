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


package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.starrocks.common.UserException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class HiveResourceTest {
    private static ConnectContext connectContext;

    @Before
    public void setUp() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testFromStmt(@Mocked GlobalStateMgr globalStateMgr) throws UserException {
        String name = "hive0";
        String type = "hive";
        String metastoreURIs = "thrift://127.0.0.1:9380";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("hive.metastore.uris", metastoreURIs);
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        HiveResource resource = (HiveResource) Resource.fromStmt(stmt);
        Assert.assertEquals("hive0", resource.getName());
        Assert.assertEquals(type, resource.getType().name().toLowerCase());
        Assert.assertEquals(metastoreURIs, resource.getHiveMetastoreURIs());
    }

    @Test
    public void testSerialization() throws Exception {
        Resource resource = new HiveResource("hive0");
        String metastoreURIs = "thrift://127.0.0.1:9380";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("hive.metastore.uris", metastoreURIs);
        resource.setProperties(properties);

        String json = GsonUtils.GSON.toJson(resource);
        Resource resource2 = GsonUtils.GSON.fromJson(json, Resource.class);
        Assert.assertTrue(resource2 instanceof HiveResource);
        Assert.assertEquals(metastoreURIs, ((HiveResource) resource2).getHiveMetastoreURIs());
    }
}
