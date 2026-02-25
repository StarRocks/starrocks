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
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.persist.AlterResourceInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class IcebergResourceTest {
    private static ConnectContext connectContext;

    @BeforeEach
    public void setUp() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testFromStmt(@Mocked GlobalStateMgr globalStateMgr) throws StarRocksException {
        String name = "iceberg0";
        String type = "iceberg";
        String catalogType = "HIVE";
        String metastoreURIs = "thrift://127.0.0.1:9380";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("iceberg.catalog.type", catalogType);
        properties.put("iceberg.catalog.hive.metastore.uris", metastoreURIs);
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        IcebergResource resource = (IcebergResource) Resource.fromStmt(stmt);
        Assertions.assertEquals("iceberg0", resource.getName());
        Assertions.assertEquals(type, resource.getType().name().toLowerCase());
        Assertions.assertEquals(IcebergCatalogType.fromString(catalogType), resource.getCatalogType());
        Assertions.assertEquals(metastoreURIs, resource.getHiveMetastoreURIs());
        resource.alterProperties(new AlterResourceInfo(resource.name, "thrift://127.0.0.2:9380"));
        Assertions.assertEquals("thrift://127.0.0.2:9380", resource.getHiveMetastoreURIs());
    }

    @Test
    public void testCustomStmt(@Mocked GlobalStateMgr globalStateMgr) throws StarRocksException {
        String name = "iceberg1";
        String type = "iceberg";
        String catalogType = "CUSTOM";
        String catalogImpl = "com.starrocks.connector.iceberg.hive.IcebergHiveCatalog";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("iceberg.catalog.type", catalogType);
        properties.put("iceberg.catalog-impl", catalogImpl);
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);

        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        IcebergResource resource = (IcebergResource) Resource.fromStmt(stmt);
        Assertions.assertEquals("iceberg1", resource.getName());
        Assertions.assertEquals(type, resource.getType().name().toLowerCase());
        Assertions.assertEquals(IcebergCatalogType.fromString(catalogType), resource.getCatalogType());
        Assertions.assertEquals(catalogImpl, resource.getIcebergImpl());
    }

    @Test
    public void testSerialization() throws Exception {
        Resource resource = new IcebergResource("iceberg0");
        String metastoreURIs = "thrift://127.0.0.1:9380";
        String catalogType = "HIVE";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("iceberg.catalog.hive.metastore.uris", metastoreURIs);
        properties.put("iceberg.catalog.type", catalogType);
        resource.setProperties(properties);

        String json = GsonUtils.GSON.toJson(resource);
        Resource resource2 = GsonUtils.GSON.fromJson(json, Resource.class);
        Assertions.assertTrue(resource2 instanceof IcebergResource);
        Assertions.assertEquals(metastoreURIs, ((IcebergResource) resource2).getHiveMetastoreURIs());
    }
}
