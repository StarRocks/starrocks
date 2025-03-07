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
import com.starrocks.analysis.Analyzer;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SystemInfoServiceTest {

    @Mocked
    private EditLog editLog;
    @Mocked
    private GlobalStateMgr globalStateMgr;

    private LocalMetastore localMetastore;
    private NodeMgr nodeMgr;
    private SystemInfoService systemInfoService;
    private TabletInvertedIndex invertedIndex;

    @Mocked
    private Database db;

    private Analyzer analyzer;

    private String hostPort;

    private long backendId = 10000L;

    @Before
    public void setUp() throws IOException {
        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();

        new Expectations() {
            {
                editLog.logAddBackend((Backend) any);
                minTimes = 0;

                editLog.logDropBackend((Backend) any);
                minTimes = 0;

                editLog.logBackendStateChange((Backend) any);
                minTimes = 0;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = backendId;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = db;

                globalStateMgr.clear();
                minTimes = 0;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                localMetastore = new LocalMetastore(globalStateMgr, null, null);
                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;

                nodeMgr = new NodeMgr();
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                invertedIndex = new TabletInvertedIndex();
                globalStateMgr.getTabletInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;
            }
        };

        new Expectations(localMetastore) {
            {
                localMetastore.getDb(anyLong);
                minTimes = 0;
                result = db;
            }
        };

        new Expectations(nodeMgr) {
            {
                systemInfoService = new SystemInfoService();
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        analyzer = new Analyzer(globalStateMgr, new ConnectContext(null));
    }

    public void mkdir(String dirString) {
        File dir = new File(dirString);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void deleteDir(String metaDir) {
        File dir = new File(metaDir);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
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

    @Test(expected = SemanticException.class)
    public void validHostAndPortTest1() throws Exception {
        createHostAndPort(1);
        systemInfoService.validateHostAndPort(hostPort, false);
    }

    @Test(expected = SemanticException.class)
    public void validHostAndPortTest3() throws Exception {
        createHostAndPort(3);
        systemInfoService.validateHostAndPort(hostPort, false);
    }

    @Test
    public void validHostAndPortTest4() throws Exception {
        createHostAndPort(4);
        systemInfoService.validateHostAndPort(hostPort, false);
    }

    @Test
    public void addBackendTest() throws AnalysisException {
        clearAllBackend();
        AddBackendClause stmt = new AddBackendClause(Lists.newArrayList("192.168.0.1:1234"),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        com.starrocks.sql.analyzer.Analyzer analyzer = new com.starrocks.sql.analyzer.Analyzer(
                com.starrocks.sql.analyzer.Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt), new ConnectContext(null));
        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackends(stmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackends(stmt);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("already exists"));
        }

        Assert.assertNotNull(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(backendId));
        Assert.assertNotNull(
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendWithHeartbeatPort("192.168.0.1", 1234));

        Assert.assertTrue(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getTotalBackendNumber() == 1);
        Assert.assertTrue(
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(false).get(0) == backendId);

        Assert.assertTrue(
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendReportVersion(backendId) == 0L);

        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().updateBackendReportVersion(backendId, 2L, 20000L);
        Assert.assertTrue(
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendReportVersion(backendId) == 2L);
    }

    @Test
    public void addComputeNodeTest() throws AnalysisException {
        clearAllBackend();
        AddComputeNodeClause stmt = new AddComputeNodeClause(Lists.newArrayList("192.168.0.1:1234"),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME, NodePosition.ZERO);

        com.starrocks.sql.analyzer.Analyzer analyzer = new com.starrocks.sql.analyzer.Analyzer(
                com.starrocks.sql.analyzer.Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt), new ConnectContext(null));

        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNodes(stmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        Assert.assertNotNull(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().
                getComputeNodeWithHeartbeatPort("192.168.0.1", 1234));

        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNodes(stmt);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Compute node already exists with same host"));
        }
    }

    @Test
    public void removeBackendTest() throws AnalysisException {
        clearAllBackend();
        AddBackendClause stmt = new AddBackendClause(Lists.newArrayList("192.168.0.1:1234"),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        com.starrocks.sql.analyzer.Analyzer analyzer = new com.starrocks.sql.analyzer.Analyzer(
                com.starrocks.sql.analyzer.Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt), new ConnectContext(null));
        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackends(stmt);
        } catch (DdlException e) {
            e.printStackTrace();
        }

        DropBackendClause dropStmt =
                new DropBackendClause(Lists.newArrayList("192.168.0.1:1234"), true, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(dropStmt), new ConnectContext(null));
        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackends(dropStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackends(dropStmt);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
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
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt2), new ConnectContext(null));

        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackends(stmt2);
        } catch (DdlException e) {
            e.printStackTrace();
        }

        DropBackendClause dropStmt2 =
                new DropBackendClause(Lists.newArrayList("192.168.0.1:1235"), true, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(dropStmt2), new ConnectContext(null));

        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackends(dropStmt2);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.assertTrue(e.getMessage()
                    .contains("starletPort has not been updated by heartbeat from this backend"));
        }

        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackends(dropStmt2);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testSeqChooseComputeNodes() {
        clearAllBackend();
        AddComputeNodeClause stmt = new AddComputeNodeClause(Lists.newArrayList("192.168.0.1:1234"),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME, NodePosition.ZERO);

        com.starrocks.sql.analyzer.Analyzer analyzer = new com.starrocks.sql.analyzer.Analyzer(
                com.starrocks.sql.analyzer.Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt), new ConnectContext(null));

        try {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNodes(stmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        Assert.assertNotNull(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().
                getComputeNodeWithHeartbeatPort("192.168.0.1", 1234));

        List<Long> longList = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getNodeSelector()
                .seqChooseComputeNodes(1, false, false);
        Assert.assertEquals(1, longList.size());
        ComputeNode computeNode = new ComputeNode();
        computeNode.setHost("192.168.0.1");
        computeNode.setHttpPort(9030);
        computeNode.setAlive(true);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(computeNode);
        List<Long> computeNods = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getNodeSelector()
                .seqChooseComputeNodes(1, true, false);
        Assert.assertEquals(1, computeNods.size());

        // test seqChooseBackendOrComputeId func
        Exception exception = Assertions.assertThrows(StarRocksException.class, () -> {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getNodeSelector().seqChooseBackendOrComputeId();
        });
        Assert.assertTrue(exception.getMessage().contains("No backend alive."));

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

        exception = Assert.assertThrows(StarRocksException.class, () -> {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getNodeSelector().seqChooseBackendOrComputeId();
        });
        Assert.assertTrue(exception.getMessage().contains("No backend or compute node alive."));
    }

    @Test
    public void testGetDecommissionedBackends() throws Exception {
        for (int i = 100; i < 200; i++) {
            Backend be = new Backend(i, "decommissionedHost", 1000);
            be.setStarletPort(i);
            systemInfoService.addBackend(be);
            be.setDecommissioned(true);
        }
        Assert.assertTrue(systemInfoService.getDecommissionedBackendIds().size() == 100);
    }

}
