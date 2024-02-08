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
import com.starrocks.common.UserException;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ResourceMgrTest {
    private static ConnectContext connectContext;
    private String name;
    private String type;
    private String master;
    private String workingDir;
    private String broker;
    private String hiveMetastoreUris;
    private Map<String, String> properties;

    @Before
    public void setUp() throws Exception {
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

    @Test
    public void testAddDropResource(@Injectable BrokerMgr brokerMgr, @Injectable EditLog editLog,
                                    @Mocked GlobalStateMgr globalStateMgr) throws UserException {
        ResourceMgr mgr = new ResourceMgr();

        // add
        addSparkResource(mgr, brokerMgr, editLog, globalStateMgr);

        // drop
        DropResourceStmt dropStmt = new DropResourceStmt(name);
        mgr.dropResource(dropStmt);
        Assert.assertEquals(0, mgr.getResourceNum());
    }

    @Test(expected = DdlException.class)
    public void testAddResourceExist(@Injectable BrokerMgr brokerMgr, @Injectable EditLog editLog,
                                     @Mocked GlobalStateMgr globalStateMgr)
            throws UserException {
        ResourceMgr mgr = new ResourceMgr();

        // add
        CreateResourceStmt stmt = addSparkResource(mgr, brokerMgr, editLog, globalStateMgr);

        // add again
        mgr.createResource(stmt);
    }

    @Test(expected = DdlException.class)
    public void testDropResourceNotExist() throws UserException {
        // drop
        ResourceMgr mgr = new ResourceMgr();
        Assert.assertEquals(0, mgr.getResourceNum());
        DropResourceStmt stmt = new DropResourceStmt(name);
        mgr.dropResource(stmt);
    }

    @Test
    public void testAlterResource(@Injectable EditLog editLog, @Mocked GlobalStateMgr globalStateMgr) throws UserException {
        ResourceMgr mgr = new ResourceMgr();

        // add hive resource
        name = "hive0";
        type = "hive";
        addHiveResource(mgr, editLog, globalStateMgr);

        // alter hive resource
        String newThriftPath = "thrift://10.10.44.xxx:9083";
        Map<String, String> properties = new HashMap<>();
        properties.put("hive.metastore.uris", newThriftPath);
        AlterResourceStmt stmt = new AlterResourceStmt(name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        mgr.alterResource(stmt);

        // assert
        Resource resource = mgr.getResource(name);
        Assert.assertTrue(resource instanceof HiveResource);

        String metastoreURIs = ((HiveResource) resource).getHiveMetastoreURIs();
        Assert.assertEquals(newThriftPath, metastoreURIs);
    }

    @Test(expected = DdlException.class)
    public void testAllowAlterHiveResourceOnly(@Injectable BrokerMgr brokerMgr, @Injectable EditLog editLog,
                                               @Mocked GlobalStateMgr globalStateMgr)
            throws UserException {
        ResourceMgr mgr = new ResourceMgr();

        // add spark resource
        addSparkResource(mgr, brokerMgr, editLog, globalStateMgr);

        // alter spark resource
        Map<String, String> properties = new HashMap<>();
        properties.put("broker", "broker2");
        AlterResourceStmt stmt = new AlterResourceStmt(name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        mgr.alterResource(stmt);
    }

    @Test(expected = DdlException.class)
    public void testAlterResourceNotExist(@Injectable EditLog editLog, @Mocked GlobalStateMgr globalStateMgr)
            throws UserException {
        ResourceMgr mgr = new ResourceMgr();

        // add hive resource
        name = "hive0";
        type = "hive";
        addHiveResource(mgr, editLog, globalStateMgr);

        // alter hive resource
        Map<String, String> properties = new HashMap<>();
        properties.put("hive.metastore.uris", "thrift://10.10.44.xxx:9083");
        String noExistName = "hive1";
        AlterResourceStmt stmt = new AlterResourceStmt(noExistName, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        mgr.alterResource(stmt);
    }

    @Test(expected = DdlException.class)
    public void testAlterResourcePropertyNotExist(@Injectable EditLog editLog, @Mocked GlobalStateMgr globalStateMgr)
            throws UserException {
        ResourceMgr mgr = new ResourceMgr();

        // add hive resource
        name = "hive0";
        type = "hive";
        addHiveResource(mgr, editLog, globalStateMgr);

        // alter hive resource
        Map<String, String> properties = new HashMap<>();
        properties.put("hive.metastore.uris.xxx", "thrift://10.10.44.xxx:9083");
        AlterResourceStmt stmt = new AlterResourceStmt(name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        mgr.alterResource(stmt);
    }

    @Test
    public void testReplayCreateResource(@Injectable EditLog editLog, @Mocked GlobalStateMgr globalStateMgr)
            throws UserException {
        ResourceMgr mgr = new ResourceMgr();
        type = "hive";
        name = "hive0";

        addHiveResource(mgr, editLog, globalStateMgr);
        Resource hiveRes = new HiveResource(name);
        Map<String, String> properties = new HashMap<>();
        String newUris = "thrift://10.10.44.xxx:9083";
        properties.put("hive.metastore.uris", newUris);
        hiveRes.setProperties(properties);
        mgr.replayCreateResource(hiveRes);
        Assert.assertNotNull(mgr.getResource(name));
    }

    private CreateResourceStmt addHiveResource(ResourceMgr mgr, EditLog editLog,
                                               GlobalStateMgr globalStateMgr) throws UserException {
        new Expectations() {
            {
                globalStateMgr.getEditLog();
                result = editLog;
            }
        };

        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("hive.metastore.uris", hiveMetastoreUris);
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        Assert.assertEquals(0, mgr.getResourceNum());
        mgr.createResource(stmt);
        Assert.assertEquals(1, mgr.getResourceNum());

        Resource resource = mgr.getResource(name);
        Assert.assertTrue(resource instanceof HiveResource);
        return stmt;
    }

    private CreateResourceStmt addSparkResource(ResourceMgr mgr, BrokerMgr brokerMgr, EditLog editLog,
                                                GlobalStateMgr globalStateMgr) throws UserException {
        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
                globalStateMgr.getEditLog();
                result = editLog;
            }
        };

        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        Assert.assertEquals(0, mgr.getResourceNum());
        mgr.createResource(stmt);
        Assert.assertEquals(1, mgr.getResourceNum());
        Assert.assertTrue(mgr.containsResource(name));
        SparkResource resource = (SparkResource) mgr.getResource(name);
        Assert.assertNotNull(resource);
        Assert.assertEquals(broker, resource.getBroker());

        return stmt;
    }

}
