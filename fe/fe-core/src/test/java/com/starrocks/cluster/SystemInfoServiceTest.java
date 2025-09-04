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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/cluster/SystemInfoServiceTest.java

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

package com.starrocks.cluster;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SystemInfoServiceTest {
    private String hostPort;
    private SystemInfoService systemInfoService;

    @BeforeEach
    public void setUp() throws IOException {
        systemInfoService = new SystemInfoService();
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    public void createHostAndPort(int type) {
        switch (type) {
            case 1:
                // missing ip
                hostPort = "12346";
                break;
            case 2:
                // invalid ip
                hostPort = "asdasd:12345";
                break;
            case 3:
                // invalid port
                hostPort = "10.1.2.3:123467";
                break;
            case 4:
                // normal
                hostPort = "127.0.0.1:12345";
                break;
            default:
                break;
        }
    }

    public void clearAllBackend() {
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropAllBackend();
    }

    @Test
    public void validHostAndPortTest1() {
        assertThrows(SemanticException.class, () -> {
            createHostAndPort(1);
            SystemInfoService.validateHostAndPort(hostPort, false);
        });
    }

    @Test
    public void validHostAndPortTest3() {
        assertThrows(SemanticException.class, () -> {
            createHostAndPort(3);
            SystemInfoService.validateHostAndPort(hostPort, false);
        });
    }

    @Test
    public void validHostAndPortTest4() throws Exception {
        createHostAndPort(4);
        SystemInfoService.validateHostAndPort(hostPort, false);
    }

    @Test
    public void addBackendTest() throws AnalysisException {
        clearAllBackend();
        AddBackendClause stmt = new AddBackendClause(Lists.newArrayList("192.168.0.1:1234"),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        stmt.getHostPortPairs().add(Pair.create("192.168.0.1", 1234));
        try {
            systemInfoService.addBackends(stmt);
        } catch (DdlException e) {
            Assertions.fail();
        }

        try {
            systemInfoService.addBackends(stmt);
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("already exists"));
        }

        Backend backend = systemInfoService.getBackendWithHeartbeatPort("192.168.0.1", 1234);
        Assertions.assertNotNull(backend);

        Assertions.assertEquals(0L, systemInfoService.getBackendReportVersion(backend.getId()));

        Database database = new Database();
        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(long dbId) {
                return database;
            }
        };
        systemInfoService.updateBackendReportVersion(backend.getId(), 2L, 20000L);
        Assertions.assertEquals(2L, systemInfoService.getBackendReportVersion(backend.getId()));
    }

    @Test
    public void addComputeNodeTest() throws AnalysisException {
        clearAllBackend();
        AddComputeNodeClause stmt = new AddComputeNodeClause(Lists.newArrayList("192.168.0.1:1234"),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME, "", NodePosition.ZERO);

        stmt.getHostPortPairs().add(Pair.create("192.168.0.1", 1234));

        try {
            systemInfoService.addComputeNodes(stmt);
        } catch (DdlException e) {
            Assertions.fail();
        }

        Assertions.assertNotNull(systemInfoService.getComputeNodeWithHeartbeatPort("192.168.0.1", 1234));

        try {
            systemInfoService.addComputeNodes(stmt);
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("Compute node already exists with same host"));
        }
    }

    @Test
    public void removeBackendTest() throws AnalysisException {
        clearAllBackend();
        AddBackendClause stmt = new AddBackendClause(Lists.newArrayList("192.168.0.1:1234"),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        stmt.getHostPortPairs().add(Pair.create("192.168.0.1", 1234));
        try {
            systemInfoService.addBackends(stmt);
        } catch (DdlException e) {
            e.printStackTrace();
        }

        DropBackendClause dropStmt =
                new DropBackendClause(Lists.newArrayList("192.168.0.1:1234"), true, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        dropStmt.getHostPortPairs().add(Pair.create("192.168.0.1", 1234));
        try {
            systemInfoService.dropBackends(dropStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assertions.fail();
        }

        try {
            systemInfoService.dropBackends(dropStmt);
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("does not exist"));
        }

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        StarOSAgent starosAgent = new StarOSAgent();
        new Expectations(starosAgent) {
            {
                try {
                    starosAgent.removeWorker("192.168.0.1:1235", StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                    minTimes = 0;
                    result = null;
                } catch (DdlException e) {
                    e.printStackTrace();
                }
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            StarOSAgent getStarOSAgent() {
                return starosAgent;
            }
        };

        AddBackendClause stmt2 = new AddBackendClause(Lists.newArrayList("192.168.0.1:1235"),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        stmt2.getHostPortPairs().add(Pair.create("192.168.0.1", 1235));

        try {
            systemInfoService.addBackends(stmt2);
        } catch (DdlException e) {
            e.printStackTrace();
        }

        DropBackendClause dropStmt2 =
                new DropBackendClause(Lists.newArrayList("192.168.0.1:1235"), true, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        dropStmt2.getHostPortPairs().add(Pair.create("192.168.0.1", 1235));

        try {
            systemInfoService.dropBackends(dropStmt2);
        } catch (DdlException e) {
            e.printStackTrace();
            Assertions.assertTrue(e.getMessage()
                    .contains("starletPort has not been updated by heartbeat from this backend"));
        }

        try {
            systemInfoService.dropBackends(dropStmt2);
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testSeqChooseComputeNodes() {
        clearAllBackend();
        AddComputeNodeClause stmt = new AddComputeNodeClause(Lists.newArrayList("192.168.0.1:1234"),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME, "", NodePosition.ZERO);
        stmt.getHostPortPairs().add(Pair.create("192.168.0.1", 1234));

        try {
            systemInfoService.addComputeNodes(stmt);
        } catch (DdlException e) {
            Assertions.fail();
        }

        Assertions.assertNotNull(systemInfoService.getComputeNodeWithHeartbeatPort("192.168.0.1", 1234));

        List<Long> longList = systemInfoService.getNodeSelector()
                .seqChooseComputeNodes(1, false, false);
        Assertions.assertEquals(1, longList.size());
        ComputeNode computeNode = new ComputeNode();
        computeNode.setHost("192.168.0.1");
        computeNode.setHttpPort(9030);
        computeNode.setAlive(true);
        systemInfoService.addComputeNode(computeNode);
        List<Long> computeNods = systemInfoService.getNodeSelector()
                .seqChooseComputeNodes(1, true, false);
        Assertions.assertEquals(1, computeNods.size());

        // test seqChooseBackendOrComputeId func
        Exception exception = Assertions.assertThrows(StarRocksException.class, () -> {
            systemInfoService.getNodeSelector().seqChooseBackendOrComputeId();
        });
        Assertions.assertTrue(exception.getMessage().contains("No backend alive."));

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
        new MockUp<NodeSelector>() {
            @Mock
            public List<Long> seqChooseComputeNodes(int computeNodeNum,
                                                    boolean needAvailable, boolean isCreate) {
                return new ArrayList<>();
            }
        };

        exception = Assertions.assertThrows(StarRocksException.class, () ->
                systemInfoService.getNodeSelector().seqChooseBackendOrComputeId());
        Assertions.assertTrue(exception.getMessage().contains("No backend or compute node alive."));
    }

    @Test
    public void testGetDecommissionedBackends() throws Exception {
        for (int i = 100; i < 200; i++) {
            Backend be = new Backend(i, "decommissionedHost", 1000);
            be.setStarletPort(i);
            systemInfoService.addBackend(be);
            be.setDecommissioned(true);
        }
        Assertions.assertTrue(systemInfoService.getDecommissionedBackendIds().size() == 100);
    }

}
