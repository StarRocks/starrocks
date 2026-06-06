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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/ResourceMgrTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ResourceMgrTest {
    private static ConnectContext connectContext;
    private String name;
    private String type;
    private String master;
    private String workingDir;
    private String broker;
    private String hiveMetastoreUris;
    private Map<String, String> properties;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr.getCurrentState().getBrokerMgr()
            .addBrokers("broker0", Collections.singletonList(Pair.create("127.0.0.1", 9000)));

        connectContext = UtFrameUtils.createDefaultCtx();
        name = "spark0";
        type = "spark";
        master = "spark://127.0.0.1:7077";
        workingDir = "hdfs://127.0.0.1/tmp/starrocks";
        broker = "broker0";
        properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("spark.master", master);
        properties.put("spark.submit.deployMode", "cluster");
        properties.put("working_dir", workingDir);
        properties.put("broker", broker);
        hiveMetastoreUris = "thrift://10.10.44.98:9083";
    }

    @AfterEach
    public void tearDown() throws DdlException {
        try {
            GlobalStateMgr.getCurrentState().getBrokerMgr().dropAllBroker("broker0");
        } catch (Exception e) {
            // Ignore if broker doesn't exist
        }
        
        // Only try to drop catalog if it exists
        if (name != null && type != null) {
            String catalogName = CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(name, type);
            CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
            if (catalogMgr.getCatalogByName(catalogName) != null) {
                try {
                    DropCatalogStmt dropCatalogStmt = new DropCatalogStmt(catalogName);
                    catalogMgr.dropCatalog(dropCatalogStmt);
                } catch (Exception e) {
                    // Ignore if catalog doesn't exist or already dropped
                }
            }
        }
        
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAddDropResource() throws DdlException {
        ResourceMgr mgr = new ResourceMgr();

        // add
        addSparkResource(mgr);

        // drop
        mgr.dropResource(new DropResourceStmt(name));
        Assertions.assertEquals(0, mgr.getResourceNum());
    }

    @Test
    public void testAddResourceExist() throws DdlException {
        assertThrows(DdlException.class, () -> {
            ResourceMgr mgr = new ResourceMgr();

            // add
            CreateResourceStmt stmt = addSparkResource(mgr);

            // add again
            mgr.createResource(stmt);
        });
    }

    @Test
    public void testDropResourceNotExist() {
        assertThrows(DdlException.class, () -> {
            // drop
            ResourceMgr mgr = new ResourceMgr();
            Assertions.assertEquals(0, mgr.getResourceNum());
            DropResourceStmt stmt = new DropResourceStmt(name);
            mgr.dropResource(stmt);
        });
    }

    @Test
    public void testAlterResource() throws DdlException {
        ResourceMgr mgr = new ResourceMgr();

        // add hive resource
        name = "hive0";
        type = "hive";
        addHiveResource(mgr);

        // alter hive resource
        String newThriftPath = "thrift://10.10.44.xxx:9083";
        Map<String, String> properties = new HashMap<>();
        properties.put("hive.metastore.uris", newThriftPath);
        AlterResourceStmt stmt = new AlterResourceStmt(name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        mgr.alterResource(stmt);

        // assert
        Resource resource = mgr.getResource(name);
        Assertions.assertTrue(resource instanceof HiveResource);

        String metastoreURIs = ((HiveResource) resource).getHiveMetastoreURIs();
        Assertions.assertEquals(newThriftPath, metastoreURIs);
    }

    @Test
    public void testAllowAlterHiveResourceOnly() {
        assertThrows(DdlException.class, () -> {
            ResourceMgr mgr = new ResourceMgr();

            // add spark resource
            addSparkResource(mgr);

            // alter spark resource
            Map<String, String> properties = new HashMap<>();
            properties.put("broker", "broker2");
            AlterResourceStmt stmt = new AlterResourceStmt(name, properties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
            mgr.alterResource(stmt);
        });
    }

    @Test
    public void testAlterResourceNotExist() {
        assertThrows(DdlException.class, () -> {
            ResourceMgr mgr = new ResourceMgr();

            // add hive resource
            name = "hive0";
            type = "hive";
            addHiveResource(mgr);

            // alter hive resource
            Map<String, String> properties = new HashMap<>();
            properties.put("hive.metastore.uris", "thrift://10.10.44.xxx:9083");
            String noExistName = "hive1";
            AlterResourceStmt stmt = new AlterResourceStmt(noExistName, properties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
            mgr.alterResource(stmt);
        });
    }

    @Test
    public void testAlterResourcePropertyNotExist() {
        assertThrows(DdlException.class, () -> {
            ResourceMgr mgr = new ResourceMgr();

            // add hive resource
            name = "hive0";
            type = "hive";
            addHiveResource(mgr);

            // alter hive resource
            Map<String, String> properties = new HashMap<>();
            properties.put("hive.metastore.uris.xxx", "thrift://10.10.44.xxx:9083");
            AlterResourceStmt stmt = new AlterResourceStmt(name, properties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
            mgr.alterResource(stmt);
        });
    }

    @Test
    public void testReplayCreateResource() throws DdlException {
        ResourceMgr mgr = new ResourceMgr();
        type = "hive";
        name = "hive0";

        addHiveResource(mgr);
        Resource hiveRes = new HiveResource(name);
        Map<String, String> properties = new HashMap<>();
        String newUris = "thrift://10.10.44.xxx:9083";
        properties.put("hive.metastore.uris", newUris);
        hiveRes.setProperties(properties);
        mgr.replayCreateResource(hiveRes);
        Assertions.assertNotNull(mgr.getResource(name));
    }

    private CreateResourceStmt addHiveResource(ResourceMgr mgr) throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("hive.metastore.uris", hiveMetastoreUris);
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        Assertions.assertEquals(0, mgr.getResourceNum());
        mgr.createResource(stmt);
        Assertions.assertEquals(1, mgr.getResourceNum());

        Resource resource = mgr.getResource(name);
        Assertions.assertTrue(resource instanceof HiveResource);
        return stmt;
    }

    private CreateResourceStmt addSparkResource(ResourceMgr mgr) throws DdlException {
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        Assertions.assertEquals(0, mgr.getResourceNum());
        mgr.createResource(stmt);
        Assertions.assertEquals(1, mgr.getResourceNum());
        Assertions.assertTrue(mgr.containsResource(name));
        SparkResource resource = (SparkResource) mgr.getResource(name);
        Assertions.assertNotNull(resource);
        Assertions.assertEquals(broker, resource.getBroker());

        return stmt;
    }

}
