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


package com.starrocks.alter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.DecommissionBackendClause;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.DropComputeNodeClause;
import com.starrocks.sql.ast.ModifyBackendClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SystemHandlerTest {

    private SystemHandler systemHandler;
    private GlobalStateMgr globalStateMgr;
    private static FakeEditLog fakeEditLog;
    private static FakeGlobalStateMgr fakeGlobalStateMgr;

    @BeforeEach
    public void setUp() throws Exception {
        fakeEditLog = new FakeEditLog();
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = GlobalStateMgrTestUtil.createTestState();
        globalStateMgr.getWarehouseMgr().initDefaultWarehouse();
        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        systemHandler = new SystemHandler();
    }

    @Test
    public void testModifyBackendAddressLogic() {
        assertThrows(RuntimeException.class, () -> {
            ModifyBackendClause clause = new ModifyBackendClause("127.0.0.1", "sandbox-fqdn");
            List<AlterClause> clauses = new ArrayList<>();
            clauses.add(clause);
            systemHandler.process(clauses, null, null);
        });
    }

    @Test
    public void testModifyFrontendAddressLogic() {
        assertThrows(NullPointerException.class, () -> {
            ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("127.0.0.1", "sandbox-fqdn");
            List<AlterClause> clauses = new ArrayList<>();
            clauses.add(clause);
            systemHandler.process(clauses, null, null);
        });
    }

    @Test
    public void testDecommissionInvalidBackend() {
        List<String> hostAndPorts = Lists.newArrayList("192.168.1.11:1234");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        Throwable exception = assertThrows(RuntimeException.class, () -> {
            systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
        });
        assertThat(exception.getMessage(), containsString("Backend does not exist"));
    }

    @Test
    public void testDecommissionBackendsReplicasRequirement() {
        List<String> hostAndPorts = Lists.newArrayList("host1:123");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        Throwable exception = assertThrows(RuntimeException.class, () -> {
            systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
        });
        assertThat(exception.getMessage(), containsString("It will cause insufficient BE number"));
    }

    @Test
    public void testDecommissionBackendsSpaceRequirement() {
        List<String> hostAndPorts = Lists.newArrayList("host1:123");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        DiskInfo diskInfo = new DiskInfo("/data");
        diskInfo.setAvailableCapacityB(100);
        diskInfo.setDataUsedCapacityB(900);
        diskInfo.setTotalCapacityB(1000);
        Map<String, DiskInfo> diskInfoMap = Maps.newHashMap();
        diskInfoMap.put("/data", diskInfo);

        for (Backend backend : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends()) {
            backend.setDisks(ImmutableMap.copyOf(diskInfoMap));
        }

        Throwable exception = assertThrows(RuntimeException.class, () -> {
            systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
        });
        assertThat(exception.getMessage(), containsString("It will cause insufficient disk space"));
    }

    @Test
    public void testDecommissionBackends() throws StarRocksException {
        List<String> hostAndPorts = Lists.newArrayList("host1:123");
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(hostAndPorts);
        Analyzer.analyze(new AlterSystemStmt(decommissionBackendClause), new ConnectContext());

        Backend backend4 = new Backend(100, "host4", 123);
        backend4.setAlive(true);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend4);

        DiskInfo diskInfo = new DiskInfo("/data");
        diskInfo.setAvailableCapacityB(900);
        diskInfo.setDataUsedCapacityB(100);
        diskInfo.setTotalCapacityB(1000);
        Map<String, DiskInfo> diskInfoMap = Maps.newHashMap();
        diskInfoMap.put("/data", diskInfo);

        for (Backend backend : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends()) {
            backend.setDisks(ImmutableMap.copyOf(diskInfoMap));
        }

        systemHandler.process(Lists.newArrayList(decommissionBackendClause), null, null);
    }

    void skipIfNotTheDefaultWarehouseManagerImplementation() {
        WarehouseManager mgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Assumptions.assumeTrue(WarehouseManager.class.getName().equals(mgr.getClass().getName()));
    }

    @Test
    public void testAddBackendIntoCNGroup() throws StarRocksException {
        String warehouseName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        {
            List<String> hostAndPorts = Lists.newArrayList("127.0.0.1:13567");
            AddBackendClause clause = new AddBackendClause(hostAndPorts, warehouseName, "", NodePosition.ZERO);
            Analyzer.analyze(new AlterSystemStmt(clause), new ConnectContext());
            systemHandler.process(Lists.newArrayList(clause), null, null);
            Backend node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .getBackendWithHeartbeatPort("127.0.0.1", 13567);
            Assertions.assertNotNull(node);
            Warehouse warehouse =
                    GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
            Assertions.assertEquals(warehouse.getId(), node.getWarehouseId());
            Assertions.assertEquals(node.getWorkerGroupId(), (long) warehouse.getWorkerGroupIds().get(0));
        }

        // The following code is only true for the open source WarehouseManager implementation.
        skipIfNotTheDefaultWarehouseManagerImplementation();
        {
            List<String> hostAndPorts = Lists.newArrayList("127.0.0.1:1234");
            AddBackendClause clause = new AddBackendClause(hostAndPorts, warehouseName, "cngroup1", NodePosition.ZERO);
            Analyzer.analyze(new AlterSystemStmt(clause), new ConnectContext());
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> systemHandler.process(Lists.newArrayList(clause), null, null));
            Assertions.assertTrue(exception.getMessage().contains(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED.formatErrorMsg()));
            ConnectContext connCtx = ConnectContext.get();
            Assertions.assertEquals(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED, connCtx.getState().getErrorCode());
        }
    }

    @Test
    public void testAddComputeNodeIntoCNGroup() throws StarRocksException {
        String warehouseName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        {
            List<String> hostAndPorts = Lists.newArrayList("127.0.0.1:13567");
            AddComputeNodeClause clause = new AddComputeNodeClause(hostAndPorts, warehouseName, "", NodePosition.ZERO);
            Analyzer.analyze(new AlterSystemStmt(clause), new ConnectContext());
            systemHandler.process(Lists.newArrayList(clause), null, null);
            ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .getComputeNodeWithHeartbeatPort("127.0.0.1", 13567);
            Assertions.assertNotNull(node);
            Warehouse warehouse =
                    GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
            Assertions.assertEquals(warehouse.getId(), node.getWarehouseId());
            Assertions.assertEquals(node.getWorkerGroupId(), (long) warehouse.getWorkerGroupIds().get(0));
        }

        // The following code is only true for the open source WarehouseManager implementation.
        skipIfNotTheDefaultWarehouseManagerImplementation();
        {
            List<String> hostAndPorts = Lists.newArrayList("127.0.0.1:1234");
            AddComputeNodeClause clause = new AddComputeNodeClause(hostAndPorts, warehouseName, "cngroup1", NodePosition.ZERO);
            Analyzer.analyze(new AlterSystemStmt(clause), new ConnectContext());
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> systemHandler.process(Lists.newArrayList(clause), null, null));
            Assertions.assertTrue(exception.getMessage().contains(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED.formatErrorMsg()));
            ConnectContext connCtx = ConnectContext.get();
            Assertions.assertEquals(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED, connCtx.getState().getErrorCode());
        }
    }

    @Test
    public void testDropBackendFromCNGroup() throws StarRocksException {
        String warehouseName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();

        SystemInfoService sysInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

        long nodeId = 10086;
        Backend backend = new Backend(nodeId, "127.0.0.1", 10086);
        String hostPort = String.format("%s:%d", backend.getHost(), backend.getHeartbeatPort());
        {
            ExceptionChecker.expectThrowsNoException(() -> sysInfo.addBackend(backend));

            List<String> hostAndPorts = Lists.newArrayList(hostPort);
            DropBackendClause clause = new DropBackendClause(hostAndPorts, false, warehouseName, "", NodePosition.ZERO);
            Analyzer.analyze(new AlterSystemStmt(clause), new ConnectContext());
            systemHandler.process(Lists.newArrayList(clause), null, null);

            Backend node = sysInfo.getBackend(nodeId);
            // removed successfully
            Assertions.assertNull(node);
        }

        // The following code is only true for the open source WarehouseManager implementation.
        skipIfNotTheDefaultWarehouseManagerImplementation();
        {
            ExceptionChecker.expectThrowsNoException(() -> sysInfo.addBackend(backend));
            List<String> hostAndPorts = Lists.newArrayList(hostPort);
            DropBackendClause clause = new DropBackendClause(hostAndPorts, false, warehouseName, "cngroup1", NodePosition.ZERO);
            Analyzer.analyze(new AlterSystemStmt(clause), new ConnectContext());
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> systemHandler.process(Lists.newArrayList(clause), null, null));
            Assertions.assertTrue(exception.getMessage().contains(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED.formatErrorMsg()));
            ConnectContext connCtx = ConnectContext.get();
            Assertions.assertEquals(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED, connCtx.getState().getErrorCode());

            Backend node = sysInfo.getBackend(nodeId);
            // The node is still there not removed
            Assertions.assertNotNull(node);
        }
    }

    @Test
    public void testDropComputeNodeFromCNGroup() throws StarRocksException {
        String warehouseName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();

        SystemInfoService sysInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

        long nodeId = 10086;
        ComputeNode cnNode = new ComputeNode(nodeId, "127.0.0.1", 10086);
        String hostPort = String.format("%s:%d", cnNode.getHost(), cnNode.getHeartbeatPort());
        {
            ExceptionChecker.expectThrowsNoException(() -> sysInfo.addComputeNode(cnNode));

            List<String> hostAndPorts = Lists.newArrayList(hostPort);
            DropComputeNodeClause clause = new DropComputeNodeClause(hostAndPorts, warehouseName, "", NodePosition.ZERO);
            Analyzer.analyze(new AlterSystemStmt(clause), new ConnectContext());
            systemHandler.process(Lists.newArrayList(clause), null, null);

            ComputeNode node = sysInfo.getComputeNode(nodeId);
            // removed successfully
            Assertions.assertNull(node);
        }

        // The following code is only true for the open source WarehouseManager implementation.
        skipIfNotTheDefaultWarehouseManagerImplementation();
        {
            ExceptionChecker.expectThrowsNoException(() -> sysInfo.addComputeNode(cnNode));

            List<String> hostAndPorts = Lists.newArrayList(hostPort);
            DropComputeNodeClause clause = new DropComputeNodeClause(hostAndPorts, warehouseName, "cngroup1", NodePosition.ZERO);
            Analyzer.analyze(new AlterSystemStmt(clause), new ConnectContext());
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> systemHandler.process(Lists.newArrayList(clause), null, null));
            Assertions.assertTrue(exception.getMessage().contains(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED.formatErrorMsg()));
            ConnectContext connCtx = ConnectContext.get();
            Assertions.assertEquals(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED, connCtx.getState().getErrorCode());

            ComputeNode node = sysInfo.getComputeNode(nodeId);
            // The node is still there not removed
            Assertions.assertNotNull(node);
        }
    }
}
