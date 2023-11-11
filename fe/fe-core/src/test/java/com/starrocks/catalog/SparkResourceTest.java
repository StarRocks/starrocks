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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/SparkResourceTest.java

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
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.ResourceDesc;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class SparkResourceTest {
    private static ConnectContext connectContext;
    private String name;
    private String type;
    private String master;
    private String workingDir;
    private String broker;
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
    }

    @Test
    public void testFromStmt(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr,
                             @Injectable Auth auth)
            throws UserException {
        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
                PrivilegeActions.checkSystemAction(connectContext, PrivilegeType.CREATE_RESOURCE);
                result = true;
            }
        };

        // master: spark, deploy_mode: cluster
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        PrivilegeChecker.check(stmt, connectContext);
        SparkResource resource = (SparkResource) Resource.fromStmt(stmt);
        Assert.assertEquals(name, resource.getName());
        Assert.assertEquals(type, resource.getType().name().toLowerCase());
        Assert.assertEquals(master, resource.getMaster());
        Assert.assertEquals("cluster", resource.getDeployMode().name().toLowerCase());
        Assert.assertEquals(workingDir, resource.getWorkingDir());
        Assert.assertEquals(broker, resource.getBroker());
        Assert.assertEquals(2, resource.getSparkConfigs().size());
        Assert.assertFalse(resource.isYarnMaster());

        // master: spark, deploy_mode: client
        properties.put("spark.submit.deployMode", "client");
        stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        PrivilegeChecker.check(stmt, connectContext);
        resource = (SparkResource) Resource.fromStmt(stmt);
        Assert.assertEquals("client", resource.getDeployMode().name().toLowerCase());

        // master: yarn, deploy_mode: cluster
        properties.put("spark.master", "yarn");
        properties.put("spark.submit.deployMode", "cluster");
        properties.put("spark.jars", "xxx.jar,yyy.jar");
        properties.put("spark.files", "/tmp/aaa,/tmp/bbb");
        properties.put("spark.driver.memory", "1g");
        properties.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        properties.put("spark.hadoop.fs.defaultFS", "hdfs://127.0.0.1:10000");
        stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        PrivilegeChecker.check(stmt, connectContext);
        resource = (SparkResource) Resource.fromStmt(stmt);
        Assert.assertTrue(resource.isYarnMaster());
        Map<String, String> map = resource.getSparkConfigs();
        Assert.assertEquals(7, map.size());
        // test getProcNodeData
        BaseProcResult result = new BaseProcResult();
        resource.getProcNodeData(result);
        Assert.assertEquals(9, result.getRows().size());

        // master: yarn, deploy_mode: cluster
        // yarn resource manager ha
        properties.clear();
        properties.put("type", type);
        properties.put("spark.master", "yarn");
        properties.put("spark.submit.deployMode", "cluster");
        properties.put("spark.hadoop.yarn.resourcemanager.ha.enabled", "true");
        properties.put("spark.hadoop.yarn.resourcemanager.ha.rm-ids", "rm1,rm2");
        properties.put("spark.hadoop.yarn.resourcemanager.hostname.rm1", "host1");
        properties.put("spark.hadoop.yarn.resourcemanager.hostname.rm2", "host2");
        properties.put("spark.hadoop.fs.defaultFS", "hdfs://127.0.0.1:10000");
        stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        PrivilegeChecker.check(stmt, connectContext);
        resource = (SparkResource) Resource.fromStmt(stmt);
        Assert.assertTrue(resource.isYarnMaster());
        map = resource.getSparkConfigs();
        Assert.assertEquals(7, map.size());
    }

    @Test(expected = DdlException.class)
    public void testYarnHaExceptionFromStmt(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr,
                                            @Injectable Auth auth) throws UserException {
        new Expectations() {
            {
                PrivilegeActions.checkSystemAction(connectContext, PrivilegeType.CREATE_RESOURCE);
                result = true;
            }
        };

        // master: yarn, deploy_mode: cluster
        // yarn resource manager ha
        properties.put("spark.master", "yarn");
        properties.put("spark.submit.deployMode", "cluster");
        properties.put("spark.hadoop.yarn.resourcemanager.ha.enabled", "true");
        properties.put("spark.hadoop.yarn.resourcemanager.ha.rm-ids", "rm1,rm2");
        properties.put("spark.hadoop.yarn.resourcemanager.hostname.rm1", "host1");
        properties.put("spark.hadoop.yarn.resourcemanager.hostname.rm3", "host3");
        properties.put("spark.hadoop.fs.defaultFS", "hdfs://127.0.0.1:10000");
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        PrivilegeChecker.check(stmt, connectContext);
        Resource.fromStmt(stmt);
    }

    @Test
    public void testUpdate(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr,
                           @Injectable Auth auth)
            throws UserException {
        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
                PrivilegeActions.checkSystemAction(connectContext, PrivilegeType.CREATE_RESOURCE);
                result = true;
            }
        };

        properties.put("spark.master", "yarn");
        properties.put("spark.submit.deployMode", "cluster");
        properties.put("spark.driver.memory", "1g");
        properties.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        properties.put("spark.hadoop.fs.defaultFS", "hdfs://127.0.0.1:10000");
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        PrivilegeChecker.check(stmt, connectContext);
        SparkResource resource = (SparkResource) Resource.fromStmt(stmt);
        SparkResource copiedResource = resource.getCopiedResource();
        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put("spark.executor.memory", "1g");
        newProperties.put("spark.driver.memory", "2g");
        ResourceDesc resourceDesc = new ResourceDesc(name, newProperties);
        copiedResource.update(resourceDesc);
        Map<String, String> map = copiedResource.getSparkConfigs();
        Assert.assertEquals(5, resource.getSparkConfigs().size());
        Assert.assertEquals("1g", resource.getSparkConfigs().get("spark.driver.memory"));
        Assert.assertEquals(6, map.size());
        Assert.assertEquals("2g", copiedResource.getSparkConfigs().get("spark.driver.memory"));
    }

    @Test(expected = DdlException.class)
    public void testNoBroker(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr,
                             @Injectable Auth auth)
            throws UserException {
        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = false;
                PrivilegeActions.checkSystemAction(connectContext, PrivilegeType.CREATE_RESOURCE);
                result = true;
                result = true;
            }
        };

        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        PrivilegeChecker.check(stmt, connectContext);
        Resource.fromStmt(stmt);
    }
}
