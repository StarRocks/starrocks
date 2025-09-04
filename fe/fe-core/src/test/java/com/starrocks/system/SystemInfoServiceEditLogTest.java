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

package com.starrocks.system;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.leader.CheckpointController;
import com.starrocks.persist.CancelDecommissionDiskInfo;
import com.starrocks.persist.DecommissionDiskInfo;
import com.starrocks.persist.DisableDiskInfo;
import com.starrocks.persist.DropBackendInfo;
import com.starrocks.persist.DropComputeNodeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.UpdateBackendInfo;
import com.starrocks.persist.UpdateHistoricalNodeLog;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.DecommissionBackendClause;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.DropComputeNodeClause;
import com.starrocks.sql.ast.ModifyBackendClause;
import com.starrocks.thrift.TDisk;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.MockJournal;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class SystemInfoServiceEditLogTest {
    private SystemInfoService masterSystemInfoService;
    private SystemInfoService followerSystemInfoService;
    private final String warehouse = "default_warehouse";
    private final String cnGroupName = "";

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create SystemInfoService instances
        masterSystemInfoService = new SystemInfoService();
        followerSystemInfoService = new SystemInfoService();
        
        GlobalStateMgr.getCurrentState().setHaProtocol(new MockJournal.MockProtocol());
        GlobalStateMgr.getCurrentState().setCheckpointController(new CheckpointController("controller", new MockJournal(), ""));
        GlobalStateMgr.getCurrentState().initDefaultWarehouse();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // ==================== Add Compute Nodes Tests ====================

    @Test
    public void testAddComputeNodesNormalCase() throws Exception {
        // 1. Prepare test data
        List<String> hostPorts = Arrays.asList("192.168.1.100:9050", "192.168.1.101:9051");

        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9050));
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9051));

        // 2. Verify initial state
        Assertions.assertEquals(0, masterSystemInfoService.getComputeNodes().size());

        // 3. Execute addComputeNodes operation (master side)
        masterSystemInfoService.addComputeNodes(addComputeNodeClause);

        // 4. Verify master state
        List<ComputeNode> computeNodes = masterSystemInfoService.getComputeNodes();
        Assertions.assertEquals(2, computeNodes.size());
        
        // Verify first compute node
        ComputeNode firstNode = computeNodes.get(0);
        Assertions.assertEquals("192.168.1.100", firstNode.getHost());
        Assertions.assertEquals(9050, firstNode.getHeartbeatPort());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, firstNode.getWarehouseId());
        Assertions.assertTrue(firstNode.getId() > 0);

        // Verify second compute node
        ComputeNode secondNode = computeNodes.get(1);
        Assertions.assertEquals("192.168.1.101", secondNode.getHost());
        Assertions.assertEquals(9051, secondNode.getHeartbeatPort());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, secondNode.getWarehouseId());
        Assertions.assertTrue(secondNode.getId() > 0);

        // Verify compute nodes have different IDs
        Assertions.assertNotEquals(firstNode.getId(), secondNode.getId());

        // 5. Test follower replay functionality
        // Verify follower initial state
        Assertions.assertEquals(0, followerSystemInfoService.getComputeNodes().size());

        // Replay first compute node addition
        ComputeNode replayFirstNode = (ComputeNode) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_COMPUTE_NODE);
        followerSystemInfoService.replayAddComputeNode(replayFirstNode);

        // Replay second compute node addition
        ComputeNode replaySecondNode = (ComputeNode) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_COMPUTE_NODE);
        followerSystemInfoService.replayAddComputeNode(replaySecondNode);

        // 6. Verify follower state is consistent with master
        List<ComputeNode> followerComputeNodes = followerSystemInfoService.getComputeNodes();
        Assertions.assertEquals(2, followerComputeNodes.size());
        
        ComputeNode followerFirstNode = followerComputeNodes.get(0);
        ComputeNode followerSecondNode = followerComputeNodes.get(1);
        
        Assertions.assertEquals(firstNode.getId(), followerFirstNode.getId());
        Assertions.assertEquals(firstNode.getHost(), followerFirstNode.getHost());
        Assertions.assertEquals(firstNode.getHeartbeatPort(), followerFirstNode.getHeartbeatPort());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, followerFirstNode.getWarehouseId());

        Assertions.assertEquals(secondNode.getId(), followerSecondNode.getId());
        Assertions.assertEquals(secondNode.getHost(), followerSecondNode.getHost());
        Assertions.assertEquals(secondNode.getHeartbeatPort(), followerSecondNode.getHeartbeatPort());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, followerSecondNode.getWarehouseId());
    }

    @Test
    public void testAddComputeNodesEditLogException() throws Exception {
        // 1. Prepare test data
        List<String> hostPorts = List.of("192.168.1.200:9050");

        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.200", 9050));

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        EditLog spyEditLog = spy(new EditLog(null));
        
        // 3. Mock EditLog.logAddComputeNode to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAddComputeNode(any(ComputeNode.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionSystemInfoService.getComputeNodes().size());

        // 4. Execute addComputeNodes operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.addComputeNodes(addComputeNodeClause);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionSystemInfoService.getComputeNodes().size());
        
        // Verify no compute node was accidentally added
        List<ComputeNode> computeNodes = exceptionSystemInfoService.getComputeNodes();
        Assertions.assertTrue(computeNodes.isEmpty());
    }

    @Test
    public void testAddComputeNodesMultipleNodes() throws Exception {
        // 1. Add multiple compute nodes
        List<String> hostPorts = Arrays.asList("192.168.1.100:9050", "192.168.1.101:9051", "192.168.1.102:9052");

        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9050));
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9051));
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.102", 9052));
        masterSystemInfoService.addComputeNodes(addComputeNodeClause);

        // 2. Verify all compute nodes are added
        Assertions.assertEquals(3, masterSystemInfoService.getComputeNodes().size());
        
        List<ComputeNode> computeNodes = masterSystemInfoService.getComputeNodes();
        
        // Verify each compute node has unique ID and correct properties
        for (int i = 0; i < computeNodes.size(); i++) {
            for (int j = i + 1; j < computeNodes.size(); j++) {
                Assertions.assertNotEquals(computeNodes.get(i).getId(), computeNodes.get(j).getId());
            }
            Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, computeNodes.get(i).getWarehouseId());
        }
    }

    @Test
    public void testAddComputeNodesReplayMultipleNodes() throws Exception {
        // 1. Add multiple compute nodes to master
        List<String> hostPorts = Arrays.asList("192.168.1.100:9050", "192.168.1.101:9051", "192.168.1.102:9052");

        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9050));
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9051));
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.102", 9052));
        masterSystemInfoService.addComputeNodes(addComputeNodeClause);

        // 2. Verify follower initial state
        Assertions.assertEquals(0, followerSystemInfoService.getComputeNodes().size());

        // 3. Replay all compute node additions
        for (int i = 0; i < 3; i++) {
            ComputeNode replayNode = (ComputeNode) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_COMPUTE_NODE);
            followerSystemInfoService.replayAddComputeNode(replayNode);
        }

        // 4. Verify follower state matches master
        Assertions.assertEquals(3, followerSystemInfoService.getComputeNodes().size());
        
        List<ComputeNode> masterComputeNodes = masterSystemInfoService.getComputeNodes();
        List<ComputeNode> followerComputeNodes = followerSystemInfoService.getComputeNodes();
        
        for (int i = 0; i < masterComputeNodes.size(); i++) {
            ComputeNode masterNode = masterComputeNodes.get(i);
            ComputeNode followerNode = followerComputeNodes.get(i);
            
            Assertions.assertEquals(masterNode.getId(), followerNode.getId());
            Assertions.assertEquals(masterNode.getHost(), followerNode.getHost());
            Assertions.assertEquals(masterNode.getHeartbeatPort(), followerNode.getHeartbeatPort());
            Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, followerNode.getWarehouseId());
        }
    }

    // ==================== Add Backends Tests ====================

    @Test
    public void testAddBackendsNormalCase() throws Exception {
        // 1. Prepare test data
        List<String> hostPorts = Arrays.asList("192.168.1.100:9060", "192.168.1.101:9061");
        String warehouse = "default_warehouse";
        String cnGroupName = "";
        
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9060));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9061));

        // 2. Verify initial state
        Assertions.assertEquals(0, masterSystemInfoService.getBackends().size());

        // 3. Execute addBackends operation (master side)
        masterSystemInfoService.addBackends(addBackendClause);

        // 4. Verify master state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Assertions.assertEquals(2, backends.size());
        
        // Verify first backend
        Backend firstBackend = backends.get(0);
        Assertions.assertEquals("192.168.1.100", firstBackend.getHost());
        Assertions.assertEquals(9060, firstBackend.getHeartbeatPort());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, firstBackend.getWarehouseId());
        Assertions.assertTrue(firstBackend.getId() > 0);
        Assertions.assertEquals(Backend.BackendState.using, firstBackend.getBackendState());

        // Verify second backend
        Backend secondBackend = backends.get(1);
        Assertions.assertEquals("192.168.1.101", secondBackend.getHost());
        Assertions.assertEquals(9061, secondBackend.getHeartbeatPort());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, secondBackend.getWarehouseId());
        Assertions.assertTrue(secondBackend.getId() > 0);
        Assertions.assertEquals(Backend.BackendState.using, secondBackend.getBackendState());

        // Verify backends have different IDs
        Assertions.assertNotEquals(firstBackend.getId(), secondBackend.getId());

        // 5. Test follower replay functionality
        // Verify follower initial state
        Assertions.assertEquals(0, followerSystemInfoService.getBackends().size());

        // Replay first backend addition
        Backend replayFirstBackend = (Backend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
        followerSystemInfoService.replayAddBackend(replayFirstBackend);

        // Replay second backend addition
        Backend replaySecondBackend = (Backend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
        followerSystemInfoService.replayAddBackend(replaySecondBackend);

        // 6. Verify follower state is consistent with master
        List<Backend> followerBackends = followerSystemInfoService.getBackends();
        Assertions.assertEquals(2, followerBackends.size());
        
        Backend followerFirstBackend = followerBackends.get(0);
        Backend followerSecondBackend = followerBackends.get(1);
        
        Assertions.assertEquals(firstBackend.getId(), followerFirstBackend.getId());
        Assertions.assertEquals(firstBackend.getHost(), followerFirstBackend.getHost());
        Assertions.assertEquals(firstBackend.getHeartbeatPort(), followerFirstBackend.getHeartbeatPort());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, followerFirstBackend.getWarehouseId());
        Assertions.assertEquals(firstBackend.getBackendState(), followerFirstBackend.getBackendState());
        
        Assertions.assertEquals(secondBackend.getId(), followerSecondBackend.getId());
        Assertions.assertEquals(secondBackend.getHost(), followerSecondBackend.getHost());
        Assertions.assertEquals(secondBackend.getHeartbeatPort(), followerSecondBackend.getHeartbeatPort());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, followerSecondBackend.getWarehouseId());
        Assertions.assertEquals(secondBackend.getBackendState(), followerSecondBackend.getBackendState());
    }

    @Test
    public void testAddBackendsEditLogException() throws Exception {
        // 1. Prepare test data
        List<String> hostPorts = Arrays.asList("192.168.1.200:9060");
        String warehouse = "default_warehouse";
        String cnGroupName = "";
        
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.200", 9060));

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        EditLog spyEditLog = spy(new EditLog(null));
        
        // 3. Mock EditLog.logAddBackend to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAddBackend(any(Backend.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionSystemInfoService.getBackends().size());

        // 4. Execute addBackends operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.addBackends(addBackendClause);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionSystemInfoService.getBackends().size());
        
        // Verify no backend was accidentally added
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Assertions.assertTrue(backends.isEmpty());
    }

    @Test
    public void testAddBackendsMultipleBackends() throws Exception {
        // 1. Add multiple backends
        List<String> hostPorts = Arrays.asList("192.168.1.100:9060", "192.168.1.101:9061", "192.168.1.102:9062");
        String warehouse = "default_warehouse";
        String cnGroupName = "";
        
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9060));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9061));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.102", 9062));
        masterSystemInfoService.addBackends(addBackendClause);

        // 2. Verify all backends are added
        Assertions.assertEquals(3, masterSystemInfoService.getBackends().size());
        
        List<Backend> backends = masterSystemInfoService.getBackends();
        
        // Verify each backend has unique ID and correct properties
        for (int i = 0; i < backends.size(); i++) {
            for (int j = i + 1; j < backends.size(); j++) {
                Assertions.assertNotEquals(backends.get(i).getId(), backends.get(j).getId());
            }
            Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, backends.get(i).getWarehouseId());
            Assertions.assertEquals(Backend.BackendState.using, backends.get(i).getBackendState());
        }
    }

    @Test
    public void testAddBackendsReplayMultipleBackends() throws Exception {
        // 1. Add multiple backends to master
        List<String> hostPorts = Arrays.asList("192.168.1.100:9060", "192.168.1.101:9061", "192.168.1.102:9062");
        String warehouse = "default_warehouse";
        String cnGroupName = "";
        
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9060));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9061));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.102", 9062));
        masterSystemInfoService.addBackends(addBackendClause);

        // 2. Verify follower initial state
        Assertions.assertEquals(0, followerSystemInfoService.getBackends().size());

        // 3. Replay all backend additions
        for (int i = 0; i < 3; i++) {
            Backend replayBackend = (Backend) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
            followerSystemInfoService.replayAddBackend(replayBackend);
        }

        // 4. Verify follower state matches master
        Assertions.assertEquals(3, followerSystemInfoService.getBackends().size());
        
        List<Backend> masterBackends = masterSystemInfoService.getBackends();
        List<Backend> followerBackends = followerSystemInfoService.getBackends();
        
        for (int i = 0; i < masterBackends.size(); i++) {
            Backend masterBackend = masterBackends.get(i);
            Backend followerBackend = followerBackends.get(i);
            
            Assertions.assertEquals(masterBackend.getId(), followerBackend.getId());
            Assertions.assertEquals(masterBackend.getHost(), followerBackend.getHost());
            Assertions.assertEquals(masterBackend.getHeartbeatPort(), followerBackend.getHeartbeatPort());
            Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, followerBackend.getWarehouseId());
            Assertions.assertEquals(masterBackend.getBackendState(), followerBackend.getBackendState());
        }
    }

    // ==================== Drop Backends Tests ====================

    @Test
    public void testDropBackendsNormalCase() throws Exception {
        // 1. Prepare test data and add backends first
        List<String> hostPorts = Arrays.asList("192.168.1.200:9060", "192.168.1.201:9061");
        String warehouse = "default_warehouse";
        String cnGroupName = "";
        
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.200", 9060));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.201", 9061));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(2, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state after adding
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend firstBackend = backends.get(0);
        Backend secondBackend = backends.get(1);
        Assertions.assertEquals("192.168.1.200", firstBackend.getHost());
        Assertions.assertEquals(9060, firstBackend.getHeartbeatPort());
        Assertions.assertEquals("192.168.1.201", secondBackend.getHost());
        Assertions.assertEquals(9061, secondBackend.getHeartbeatPort());

        // 3. Execute dropBackends operation (master side)
        List<String> dropHostPorts = Arrays.asList("192.168.1.200:9060");
        DropBackendClause dropBackendClause = new DropBackendClause(dropHostPorts, false, warehouse, cnGroupName, null);
        dropBackendClause.getHostPortPairs().add(Pair.create("192.168.1.200", 9060));
        masterSystemInfoService.dropBackends(dropBackendClause);

        // 4. Verify master state after dropping
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());
        
        // Verify first backend is removed
        Backend retrievedBackend = masterSystemInfoService.getBackend(firstBackend.getId());
        Assertions.assertNull(retrievedBackend);
        
        // Verify second backend still exists
        Backend remainingBackend = masterSystemInfoService.getBackends().get(0);
        Assertions.assertEquals(secondBackend.getId(), remainingBackend.getId());
        Assertions.assertEquals(secondBackend.getHost(), remainingBackend.getHost());

        // 5. Test follower replay functionality
        // First add the same backends to follower
        followerSystemInfoService.replayAddBackend(firstBackend);
        followerSystemInfoService.replayAddBackend(secondBackend);
        Assertions.assertEquals(2, followerSystemInfoService.getBackends().size());

        DropBackendInfo info  = (DropBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_BACKEND_V2);
        
        // Execute follower replay
        followerSystemInfoService.replayDropBackend(info);

        // 6. Verify follower state is consistent with master
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());
        
        Backend followerRetrievedBackend = followerSystemInfoService.getBackend(firstBackend.getId());
        Assertions.assertNull(followerRetrievedBackend);
        
        Backend followerRemainingBackend = followerSystemInfoService.getBackends().get(0);
        Assertions.assertEquals(secondBackend.getId(), followerRemainingBackend.getId());
    }

    @Test
    public void testDropBackendsEditLogException() throws Exception {
        // 1. Prepare test data and add backend first
        List<String> hostPorts = Arrays.asList("192.168.1.300:9060");
        String warehouse = "default_warehouse";
        String cnGroupName = "";
        
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.300", 9060));

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());

        // Mock EditLog.logDropBackend to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropBackend(any(DropBackendInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute dropBackends operation and expect exception
        List<String> dropHostPorts = Arrays.asList("192.168.1.300:9060");
        DropBackendClause dropBackendClause = new DropBackendClause(dropHostPorts, false, warehouse, cnGroupName, null);
        dropBackendClause.getHostPortPairs().add(Pair.create("192.168.1.300", 9060));
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.dropBackends(dropBackendClause);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());
        
        // Verify backend still exists
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Backend backend = backends.get(0);
        Assertions.assertEquals("192.168.1.300", backend.getHost());
        Assertions.assertEquals(9060, backend.getHeartbeatPort());
    }

    @Test
    public void testDropBackendsNotExist() throws Exception {
        // 1. Test dropping non-existent backend
        List<String> dropHostPorts = Arrays.asList("192.168.1.400:9060");
        String warehouse = "default_warehouse";
        String cnGroupName = "";

        // 2. Verify initial state
        Assertions.assertEquals(0, masterSystemInfoService.getBackends().size());

        // 3. Execute dropBackends operation and expect DdlException
        DropBackendClause dropBackendClause = new DropBackendClause(dropHostPorts, false, warehouse, cnGroupName, null);
        dropBackendClause.getHostPortPairs().add(Pair.create("192.168.1.400", 9060));
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterSystemInfoService.dropBackends(dropBackendClause);
        });
        Assertions.assertTrue(exception.getMessage().contains("backend does not exists"));

        // 4. Verify state remains unchanged
        Assertions.assertEquals(0, masterSystemInfoService.getBackends().size());
    }

    @Test
    public void testDropBackendsWrongWarehouse() throws Exception {
        // 1. Add a backend to a specific warehouse
        List<String> hostPorts = Arrays.asList("192.168.1.500:9060");
        String warehouse = "default_warehouse";
        String cnGroupName = "";
        
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.500", 9060));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Try to drop with wrong warehouse
        String wrongWarehouse = "wrong_warehouse";
        List<String> dropHostPorts = Arrays.asList("192.168.1.500:9060");
        DropBackendClause dropBackendClause = new DropBackendClause(dropHostPorts, false, wrongWarehouse, cnGroupName, null);
        dropBackendClause.getHostPortPairs().add(Pair.create("192.168.1.500", 9060));

        // 3. Expect DdlException to be thrown
        Assertions.assertThrows(DdlException.class, () -> {
            masterSystemInfoService.dropBackends(dropBackendClause);
        });

        // 4. Verify state remains unchanged
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());
        List<Backend> backends = masterSystemInfoService.getBackends();
        Assertions.assertEquals("192.168.1.500", backends.get(0).getHost());
        Assertions.assertEquals(9060, backends.get(0).getHeartbeatPort());
    }

    @Test
    public void testDropBackendsMultipleBackends() throws Exception {
        // 1. Add multiple backends
        List<String> hostPorts = Arrays.asList("192.168.1.100:9060", "192.168.1.101:9061", "192.168.1.102:9062");
        String warehouse = "default_warehouse";
        String cnGroupName = "";
        
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9060));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9061));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.102", 9062));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(3, masterSystemInfoService.getBackends().size());

        // 2. Drop one backend
        List<String> dropHostPorts = Arrays.asList("192.168.1.100:9060");
        DropBackendClause dropBackendClause = new DropBackendClause(dropHostPorts, false, warehouse, cnGroupName, null);
        dropBackendClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9060));
        masterSystemInfoService.dropBackends(dropBackendClause);
        Assertions.assertEquals(2, masterSystemInfoService.getBackends().size());

        // 3. Verify remaining backends
        List<Backend> backends = masterSystemInfoService.getBackends();
        Assertions.assertEquals(2, backends.size());
        
        // Verify the remaining nodes are the correct ones
        boolean hasNode1 = false;
        boolean hasNode2 = false;
        for (Backend backend : backends) {
            if (backend.getHost().equals("192.168.1.101") && backend.getHeartbeatPort() == 9061) {
                hasNode1 = true;
            }
            if (backend.getHost().equals("192.168.1.102") && backend.getHeartbeatPort() == 9062) {
                hasNode2 = true;
            }
        }
        Assertions.assertTrue(hasNode1);
        Assertions.assertTrue(hasNode2);
    }

    @Test
    public void testDropBackendsReplayMultipleBackends() throws Exception {
        // 1. Add multiple backends to master
        List<String> hostPorts = Arrays.asList("192.168.1.100:9060", "192.168.1.101:9061", "192.168.1.102:9062");
        String warehouse = "default_warehouse";
        String cnGroupName = "";
        
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9060));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9061));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.102", 9062));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(3, masterSystemInfoService.getBackends().size());

        // 2. Drop one backend
        List<String> dropHostPorts = Arrays.asList("192.168.1.100:9060");
        DropBackendClause dropBackendClause = new DropBackendClause(dropHostPorts, false, warehouse, cnGroupName, null);
        dropBackendClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9060));
        masterSystemInfoService.dropBackends(dropBackendClause);
        Assertions.assertEquals(2, masterSystemInfoService.getBackends().size());

        // 3. Create follower SystemInfoService
        Assertions.assertEquals(0, followerSystemInfoService.getBackends().size());

        // 4. Replay all operations (3 adds + 1 drop)
        for (int i = 0; i < 3; i++) {
            Backend replayBackend = (Backend) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
            followerSystemInfoService.replayAddBackend(replayBackend);
        }
        Assertions.assertEquals(3, followerSystemInfoService.getBackends().size());

        // Replay drop operation
        DropBackendInfo info = (DropBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_BACKEND_V2);
        followerSystemInfoService.replayDropBackend(info);
        Assertions.assertEquals(2, followerSystemInfoService.getBackends().size());

        // 5. Verify follower state matches master
        List<Backend> masterBackends = masterSystemInfoService.getBackends();
        List<Backend> followerBackends = followerSystemInfoService.getBackends();
        
        Assertions.assertEquals(2, masterBackends.size());
        Assertions.assertEquals(2, followerBackends.size());
        
        for (int i = 0; i < masterBackends.size(); i++) {
            Backend masterBackend = masterBackends.get(i);
            Backend followerBackend = followerBackends.get(i);
            
            Assertions.assertEquals(masterBackend.getId(), followerBackend.getId());
            Assertions.assertEquals(masterBackend.getHost(), followerBackend.getHost());
            Assertions.assertEquals(masterBackend.getHeartbeatPort(), followerBackend.getHeartbeatPort());
            Assertions.assertEquals(masterBackend.getWarehouseId(), followerBackend.getWarehouseId());
        }
    }

    // ==================== Update Historical Nodes Tests ====================

    @Test
    public void testUpdateHistoricalComputeNodesNormalCase() throws Exception {
        // 1. Prepare test data
        long warehouseId = 1L;
        long workerGroupId = 1L;
        long updateTime = System.currentTimeMillis();

        // 2. Add some compute nodes first
        List<String> hostPorts = Arrays.asList("192.168.1.100:9050", "192.168.1.101:9051");
        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9050));
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9051));
        masterSystemInfoService.addComputeNodes(addComputeNodeClause);
        Assertions.assertEquals(2, masterSystemInfoService.getComputeNodes().size());

        // 3. Execute updateHistoricalComputeNodes operation (master side)
        masterSystemInfoService.updateHistoricalComputeNodes(warehouseId, workerGroupId, updateTime);

        // 4. Test follower replay functionality
        // Verify follower initial state
        Assertions.assertEquals(0, followerSystemInfoService.getComputeNodes().size());

        // Replay compute node additions first
        for (int i = 0; i < 2; i++) {
            ComputeNode replayNode = (ComputeNode) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_COMPUTE_NODE);
            followerSystemInfoService.replayAddComputeNode(replayNode);
        }
        Assertions.assertEquals(2, followerSystemInfoService.getComputeNodes().size());

        // Replay historical node update
        UpdateHistoricalNodeLog replayLog = (UpdateHistoricalNodeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_HISTORICAL_NODE);
        followerSystemInfoService.replayUpdateHistoricalNode(replayLog);

        // 5. Verify the historical node update was logged correctly
        // The actual verification would depend on how historical nodes are stored and accessed
        // For now, we verify that the operation was logged and replayed without exception
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals(warehouseId, replayLog.getWarehouseId());
        Assertions.assertEquals(workerGroupId, replayLog.getWorkerGroupId());
        Assertions.assertEquals(updateTime, replayLog.getUpdateTime());
        Assertions.assertNotNull(replayLog.getComputeNodeIds());
        Assertions.assertEquals(2, replayLog.getComputeNodeIds().size());
    }

    @Test
    public void testUpdateHistoricalComputeNodesEditLogException() throws Exception {
        // 1. Prepare test data
        long warehouseId = 1L;
        long workerGroupId = 1L;
        long updateTime = System.currentTimeMillis();

        // 2. Add some compute nodes first
        List<String> hostPorts = Arrays.asList("192.168.1.200:9050");
        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.200", 9050));

        // 3. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addComputeNodes(addComputeNodeClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getComputeNodes().size());

        // Mock EditLog.logUpdateHistoricalNode to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logUpdateHistoricalNode(any(UpdateHistoricalNodeLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute updateHistoricalComputeNodes operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.updateHistoricalComputeNodes(warehouseId, workerGroupId, updateTime);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify compute nodes remain unchanged after exception
        Assertions.assertEquals(1, exceptionSystemInfoService.getComputeNodes().size());
        List<ComputeNode> computeNodes = exceptionSystemInfoService.getComputeNodes();
        Assertions.assertEquals("192.168.1.200", computeNodes.get(0).getHost());
        Assertions.assertEquals(9050, computeNodes.get(0).getHeartbeatPort());
    }

    @Test
    public void testUpdateHistoricalComputeNodesEmptyNodes() throws Exception {
        // 1. Prepare test data
        long warehouseId = 1L;
        long workerGroupId = 1L;
        long updateTime = System.currentTimeMillis();

        // 2. Verify initial state (no compute nodes)
        Assertions.assertEquals(0, masterSystemInfoService.getComputeNodes().size());

        // 3. Execute updateHistoricalComputeNodes operation with no compute nodes
        masterSystemInfoService.updateHistoricalComputeNodes(warehouseId, workerGroupId, updateTime);

        // 4. Test follower replay functionality
        UpdateHistoricalNodeLog replayLog = (UpdateHistoricalNodeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_HISTORICAL_NODE);
        followerSystemInfoService.replayUpdateHistoricalNode(replayLog);

        // 5. Verify the historical node update was logged correctly
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals(warehouseId, replayLog.getWarehouseId());
        Assertions.assertEquals(workerGroupId, replayLog.getWorkerGroupId());
        Assertions.assertEquals(updateTime, replayLog.getUpdateTime());
        Assertions.assertNotNull(replayLog.getComputeNodeIds());
        Assertions.assertEquals(0, replayLog.getComputeNodeIds().size());
    }

    @Test
    public void testUpdateHistoricalBackendsNormalCase() throws Exception {
        // 1. Prepare test data
        long warehouseId = 1L;
        long workerGroupId = 1L;
        long updateTime = System.currentTimeMillis();

        // 2. Add some backends first
        List<String> hostPorts = Arrays.asList("192.168.1.100:9060", "192.168.1.101:9061");
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9060));
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9061));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(2, masterSystemInfoService.getBackends().size());

        // 3. Execute updateHistoricalBackends operation (master side)
        masterSystemInfoService.updateHistoricalBackends(warehouseId, workerGroupId, updateTime);

        // 4. Test follower replay functionality
        // Verify follower initial state
        Assertions.assertEquals(0, followerSystemInfoService.getBackends().size());

        // Replay backend additions first
        for (int i = 0; i < 2; i++) {
            Backend replayBackend = (Backend) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
            followerSystemInfoService.replayAddBackend(replayBackend);
        }
        Assertions.assertEquals(2, followerSystemInfoService.getBackends().size());

        // Replay historical node update
        UpdateHistoricalNodeLog replayLog = (UpdateHistoricalNodeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_HISTORICAL_NODE);
        followerSystemInfoService.replayUpdateHistoricalNode(replayLog);

        // 5. Verify the historical node update was logged correctly
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals(warehouseId, replayLog.getWarehouseId());
        Assertions.assertEquals(workerGroupId, replayLog.getWorkerGroupId());
        Assertions.assertEquals(updateTime, replayLog.getUpdateTime());
        Assertions.assertNotNull(replayLog.getBackendIds());
        Assertions.assertEquals(2, replayLog.getBackendIds().size());
    }

    @Test
    public void testUpdateHistoricalBackendsEditLogException() throws Exception {
        // 1. Prepare test data
        long warehouseId = 1L;
        long workerGroupId = 1L;
        long updateTime = System.currentTimeMillis();

        // 2. Add some backends first
        List<String> hostPorts = Arrays.asList("192.168.1.200:9060");
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.200", 9060));

        // 3. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());

        // Mock EditLog.logUpdateHistoricalNode to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logUpdateHistoricalNode(any(UpdateHistoricalNodeLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute updateHistoricalBackends operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.updateHistoricalBackends(warehouseId, workerGroupId, updateTime);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify backends remain unchanged after exception
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Assertions.assertEquals("192.168.1.200", backends.get(0).getHost());
        Assertions.assertEquals(9060, backends.get(0).getHeartbeatPort());
    }

    @Test
    public void testUpdateHistoricalBackendsEmptyNodes() throws Exception {
        // 1. Prepare test data
        long warehouseId = 1L;
        long workerGroupId = 1L;
        long updateTime = System.currentTimeMillis();

        // 2. Verify initial state (no backends)
        Assertions.assertEquals(0, masterSystemInfoService.getBackends().size());

        // 3. Execute updateHistoricalBackends operation with no backends
        masterSystemInfoService.updateHistoricalBackends(warehouseId, workerGroupId, updateTime);

        // 4. Test follower replay functionality
        UpdateHistoricalNodeLog replayLog = (UpdateHistoricalNodeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_HISTORICAL_NODE);
        followerSystemInfoService.replayUpdateHistoricalNode(replayLog);

        // 5. Verify the historical node update was logged correctly
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals(warehouseId, replayLog.getWarehouseId());
        Assertions.assertEquals(workerGroupId, replayLog.getWorkerGroupId());
        Assertions.assertEquals(updateTime, replayLog.getUpdateTime());
        Assertions.assertNotNull(replayLog.getBackendIds());
        Assertions.assertEquals(0, replayLog.getBackendIds().size());
    }

    @Test
    public void testUpdateHistoricalNodesReplayMultipleOperations() throws Exception {
        // 1. Prepare test data
        long warehouseId = 1L;
        long workerGroupId = 1L;
        long updateTime1 = System.currentTimeMillis();
        long updateTime2 = updateTime1 + 1000;

        // 2. Add compute nodes and backends
        List<String> computeHostPorts = Arrays.asList("192.168.1.100:9050");
        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(computeHostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9050));
        masterSystemInfoService.addComputeNodes(addComputeNodeClause);

        List<String> backendHostPorts = Arrays.asList("192.168.1.200:9060");
        AddBackendClause addBackendClause = new AddBackendClause(backendHostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create("192.168.1.200", 9060));
        masterSystemInfoService.addBackends(addBackendClause);

        // 3. Execute multiple historical node updates
        masterSystemInfoService.updateHistoricalComputeNodes(warehouseId, workerGroupId, updateTime1);
        masterSystemInfoService.updateHistoricalBackends(warehouseId, workerGroupId, updateTime2);

        // 4. Test follower replay functionality
        // Replay compute node addition
        ComputeNode replayComputeNode = (ComputeNode) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_COMPUTE_NODE);
        followerSystemInfoService.replayAddComputeNode(replayComputeNode);

        // Replay backend addition
        Backend replayBackend = (Backend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
        followerSystemInfoService.replayAddBackend(replayBackend);

        // Replay first historical node update (compute nodes)
        UpdateHistoricalNodeLog replayLog1 = (UpdateHistoricalNodeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_HISTORICAL_NODE);
        followerSystemInfoService.replayUpdateHistoricalNode(replayLog1);

        // Replay second historical node update (backends)
        UpdateHistoricalNodeLog replayLog2 = (UpdateHistoricalNodeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_HISTORICAL_NODE);
        followerSystemInfoService.replayUpdateHistoricalNode(replayLog2);

        // 5. Verify both historical node updates were logged correctly
        Assertions.assertNotNull(replayLog1);
        Assertions.assertEquals(warehouseId, replayLog1.getWarehouseId());
        Assertions.assertEquals(workerGroupId, replayLog1.getWorkerGroupId());
        Assertions.assertEquals(updateTime1, replayLog1.getUpdateTime());
        Assertions.assertNotNull(replayLog1.getComputeNodeIds());
        Assertions.assertEquals(1, replayLog1.getComputeNodeIds().size());

        Assertions.assertNotNull(replayLog2);
        Assertions.assertEquals(warehouseId, replayLog2.getWarehouseId());
        Assertions.assertEquals(workerGroupId, replayLog2.getWorkerGroupId());
        Assertions.assertEquals(updateTime2, replayLog2.getUpdateTime());
        Assertions.assertNotNull(replayLog2.getBackendIds());
        Assertions.assertEquals(1, replayLog2.getBackendIds().size());
    }

    // ==================== Modify Backend Tests ====================

    @Test
    public void testModifyBackendHostNormalCase() throws Exception {
        // 1. Prepare test data and add backend first
        String originalHost = "192.168.1.100";
        String newHost = "192.168.1.200";
        int heartbeatPort = 9060;
        
        List<String> hostPorts = Arrays.asList(originalHost + ":" + heartbeatPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(originalHost, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend originalBackend = backends.get(0);
        Assertions.assertEquals(originalHost, originalBackend.getHost());
        Assertions.assertEquals(heartbeatPort, originalBackend.getHeartbeatPort());

        // 3. Execute modifyBackendHost operation (master side)
        ModifyBackendClause modifyBackendClause = new ModifyBackendClause(originalHost, newHost);
        ShowResultSet result = masterSystemInfoService.modifyBackendHost(modifyBackendClause);

        // 4. Verify master state after modification
        List<Backend> updatedBackends = masterSystemInfoService.getBackends();
        Assertions.assertEquals(1, updatedBackends.size());
        
        Backend modifiedBackend = updatedBackends.get(0);
        Assertions.assertEquals(newHost, modifiedBackend.getHost());
        Assertions.assertEquals(heartbeatPort, modifiedBackend.getHeartbeatPort());
        Assertions.assertEquals(originalBackend.getId(), modifiedBackend.getId());

        // 5. Test follower replay functionality
        // First add the same backend to follower
        followerSystemInfoService.replayAddBackend(originalBackend);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        UpdateBackendInfo info = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        
        // Execute follower replay
        followerSystemInfoService.replayBackendStateChange(info);

        // 6. Verify follower state is consistent with master
        List<Backend> followerBackends = followerSystemInfoService.getBackends();
        Assertions.assertEquals(1, followerBackends.size());
        
        Backend followerBackend = followerBackends.get(0);
        Assertions.assertEquals(newHost, followerBackend.getHost());
        Assertions.assertEquals(heartbeatPort, followerBackend.getHeartbeatPort());
        Assertions.assertEquals(originalBackend.getId(), followerBackend.getId());
    }

    @Test
    public void testModifyBackendHostEditLogException() throws Exception {
        // 1. Prepare test data and add backend first
        String originalHost = "192.168.1.300";
        String newHost = "192.168.1.400";
        int heartbeatPort = 9060;
        
        List<String> hostPorts = Arrays.asList(originalHost + ":" + heartbeatPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(originalHost, heartbeatPort));

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());

        // Mock EditLog.logBackendStateChange to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logBackendStateChange(any(UpdateBackendInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute modifyBackendHost operation and expect exception
        ModifyBackendClause modifyBackendClause = new ModifyBackendClause(originalHost, newHost);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.modifyBackendHost(modifyBackendClause);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify backend host remains unchanged after exception
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Backend backend = backends.get(0);
        Assertions.assertEquals(originalHost, backend.getHost());
        Assertions.assertEquals(heartbeatPort, backend.getHeartbeatPort());
    }

    @Test
    public void testModifyBackendPropertyNormalCase() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.100";
        int heartbeatPort = 9060;
        
        List<String> hostPorts = Arrays.asList(host + ":" + heartbeatPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend originalBackend = backends.get(0);
        Assertions.assertEquals(host, originalBackend.getHost());
        Assertions.assertEquals(heartbeatPort, originalBackend.getHeartbeatPort());

        // 3. Execute modifyBackendProperty operation (master side)
        String backendHostPort = host + ":" + heartbeatPort;
        Map<String, String> properties = new HashMap<>();
        properties.put("labels.location", "zone:zone1");
        ModifyBackendClause modifyBackendClause = new ModifyBackendClause(backendHostPort, properties);
        ShowResultSet result = masterSystemInfoService.modifyBackendProperty(modifyBackendClause);

        // 4. Verify master state after modification
        List<Backend> updatedBackends = masterSystemInfoService.getBackends();
        Assertions.assertEquals(1, updatedBackends.size());
        
        Backend modifiedBackend = updatedBackends.get(0);
        Assertions.assertEquals(host, modifiedBackend.getHost());
        Assertions.assertEquals(heartbeatPort, modifiedBackend.getHeartbeatPort());
        Assertions.assertEquals(originalBackend.getId(), modifiedBackend.getId());
        Assertions.assertNotNull(modifiedBackend.getLocation());
        Assertions.assertEquals("zone1", modifiedBackend.getLocation().get("zone"));

        // 5. Test follower replay functionality
        // First add the same backend to follower
        followerSystemInfoService.replayAddBackend(originalBackend);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        UpdateBackendInfo info = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        
        // Execute follower replay
        followerSystemInfoService.replayBackendStateChange(info);

        // 6. Verify follower state is consistent with master
        List<Backend> followerBackends = followerSystemInfoService.getBackends();
        Assertions.assertEquals(1, followerBackends.size());
        
        Backend followerBackend = followerBackends.get(0);
        Assertions.assertEquals(host, followerBackend.getHost());
        Assertions.assertEquals(heartbeatPort, followerBackend.getHeartbeatPort());
        Assertions.assertEquals(originalBackend.getId(), followerBackend.getId());
        Assertions.assertNotNull(followerBackend.getLocation());
        Assertions.assertEquals("zone1", followerBackend.getLocation().get("zone"));
    }

    @Test
    public void testModifyBackendPropertyEditLogException() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.300";
        int heartbeatPort = 9060;
        
        List<String> hostPorts = Arrays.asList(host + ":" + heartbeatPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());

        // Mock EditLog.logBackendStateChange to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logBackendStateChange(any(UpdateBackendInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute modifyBackendProperty operation and expect exception
        String backendHostPort = host + ":" + heartbeatPort;
        Map<String, String> properties = new HashMap<>();
        properties.put("labels.location", "zone:zone1");
        ModifyBackendClause modifyBackendClause = new ModifyBackendClause(backendHostPort, properties);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.modifyBackendProperty(modifyBackendClause);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify backend properties remain unchanged after exception
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Backend backend = backends.get(0);
        Assertions.assertEquals(host, backend.getHost());
        Assertions.assertEquals(heartbeatPort, backend.getHeartbeatPort());
        // Location should remain unchanged or null
        Assertions.assertTrue(backend.getLocation().isEmpty());
    }

    @Test
    public void testDecommissionBackendNormalCase() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.100";
        int heartbeatPort = 9060;
        
        List<String> hostPorts = Arrays.asList(host + ":" + heartbeatPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend originalBackend = backends.get(0);
        Assertions.assertEquals(host, originalBackend.getHost());
        Assertions.assertEquals(heartbeatPort, originalBackend.getHeartbeatPort());
        Assertions.assertFalse(originalBackend.isDecommissioned());

        // 3. Execute decommissionBackend operation (master side)
        List<String> decommissionHostPorts = Arrays.asList(host + ":" + heartbeatPort);
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(decommissionHostPorts);
        decommissionBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.decommissionBackend(decommissionBackendClause);

        // 4. Verify master state after decommission
        List<Backend> updatedBackends = masterSystemInfoService.getBackends();
        Assertions.assertEquals(1, updatedBackends.size());
        
        Backend decommissionedBackend = updatedBackends.get(0);
        Assertions.assertEquals(host, decommissionedBackend.getHost());
        Assertions.assertEquals(heartbeatPort, decommissionedBackend.getHeartbeatPort());
        Assertions.assertEquals(originalBackend.getId(), decommissionedBackend.getId());
        Assertions.assertTrue(decommissionedBackend.isDecommissioned());

        // 5. Test follower replay functionality
        // First add the same backend to follower
        followerSystemInfoService.replayAddBackend(originalBackend);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        UpdateBackendInfo info = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        
        // Execute follower replay
        followerSystemInfoService.replayBackendStateChange(info);

        // 6. Verify follower state is consistent with master
        List<Backend> followerBackends = followerSystemInfoService.getBackends();
        Assertions.assertEquals(1, followerBackends.size());
        
        Backend followerBackend = followerBackends.get(0);
        Assertions.assertEquals(host, followerBackend.getHost());
        Assertions.assertEquals(heartbeatPort, followerBackend.getHeartbeatPort());
        Assertions.assertEquals(originalBackend.getId(), followerBackend.getId());
        Assertions.assertTrue(followerBackend.isDecommissioned());
    }

    @Test
    public void testDecommissionBackendEditLogException() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.300";
        int heartbeatPort = 9060;
        
        List<String> hostPorts = Arrays.asList(host + ":" + heartbeatPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());

        // Mock EditLog.logBackendStateChange to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logBackendStateChange(any(UpdateBackendInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute decommissionBackend operation and expect exception
        List<String> decommissionHostPorts = Arrays.asList(host + ":" + heartbeatPort);
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(decommissionHostPorts);
        decommissionBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.decommissionBackend(decommissionBackendClause);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify backend state remains unchanged after exception
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Backend backend = backends.get(0);
        Assertions.assertEquals(host, backend.getHost());
        Assertions.assertEquals(heartbeatPort, backend.getHeartbeatPort());
        Assertions.assertFalse(backend.isDecommissioned());
    }

    @Test
    public void testCancelDecommissionBackendNormalCase() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.100";
        int heartbeatPort = 9060;
        
        List<String> hostPorts = Arrays.asList(host + ":" + heartbeatPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Decommission the backend first
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend decommissionedBackend = backends.get(0);
        decommissionedBackend.setDecommissioned(true);

        // 3. Execute cancelDecommissionBackend operation (master side)
        List<String> cancelHostPorts = Arrays.asList(host + ":" + heartbeatPort);
        CancelAlterSystemStmt cancelAlterSystemStmt = new CancelAlterSystemStmt(cancelHostPorts);
        cancelAlterSystemStmt.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.cancelDecommissionBackend(cancelAlterSystemStmt);

        // 4. Verify master state after cancel decommission
        List<Backend> updatedBackends = masterSystemInfoService.getBackends();
        Assertions.assertEquals(1, updatedBackends.size());
        
        Backend cancelledBackend = updatedBackends.get(0);
        Assertions.assertEquals(host, cancelledBackend.getHost());
        Assertions.assertEquals(heartbeatPort, cancelledBackend.getHeartbeatPort());
        Assertions.assertEquals(decommissionedBackend.getId(), cancelledBackend.getId());
        Assertions.assertFalse(cancelledBackend.isDecommissioned());

        // 5. Test follower replay functionality
        // First add the same backend to follower and replay decommission
        followerSystemInfoService.replayAddBackend(decommissionedBackend);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        UpdateBackendInfo info = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        
        // Execute follower replay
        followerSystemInfoService.replayBackendStateChange(info);

        // 6. Verify follower state is consistent with master
        List<Backend> followerBackends = followerSystemInfoService.getBackends();
        Assertions.assertEquals(1, followerBackends.size());
        
        Backend followerBackend = followerBackends.get(0);
        Assertions.assertEquals(host, followerBackend.getHost());
        Assertions.assertEquals(heartbeatPort, followerBackend.getHeartbeatPort());
        Assertions.assertEquals(decommissionedBackend.getId(), followerBackend.getId());
        Assertions.assertFalse(followerBackend.isDecommissioned());
    }

    @Test
    public void testCancelDecommissionBackendEditLogException() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.300";
        int heartbeatPort = 9060;
        
        List<String> hostPorts = Arrays.asList(host + ":" + heartbeatPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));

        // 2. Decommission the backend first
        List<String> decommissionHostPorts = Arrays.asList(host + ":" + heartbeatPort);
        DecommissionBackendClause decommissionBackendClause = new DecommissionBackendClause(decommissionHostPorts);
        decommissionBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));

        // 3. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addBackends(addBackendClause);
        exceptionSystemInfoService.decommissionBackend(decommissionBackendClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());

        // Mock EditLog.logBackendStateChange to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logBackendStateChange(any(UpdateBackendInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute cancelDecommissionBackend operation and expect exception
        List<String> cancelHostPorts = Arrays.asList(host + ":" + heartbeatPort);
        CancelAlterSystemStmt cancelAlterSystemStmt = new CancelAlterSystemStmt(cancelHostPorts);
        cancelAlterSystemStmt.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.cancelDecommissionBackend(cancelAlterSystemStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify backend state remains decommissioned after exception
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Backend backend = backends.get(0);
        Assertions.assertEquals(host, backend.getHost());
        Assertions.assertEquals(heartbeatPort, backend.getHeartbeatPort());
        Assertions.assertTrue(backend.isDecommissioned());
    }

    // ==================== Drop Compute Nodes Tests ====================

    @Test
    public void testDropComputeNodesNormalCase() throws Exception {
        // 1. Prepare test data and add compute node first
        List<String> hostPorts = Arrays.asList("192.168.1.200:9050");

        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.200", 9050));
        masterSystemInfoService.addComputeNodes(addComputeNodeClause);
        Assertions.assertEquals(1, masterSystemInfoService.getComputeNodes().size());

        // 2. Verify initial state after adding
        List<ComputeNode> computeNodes = masterSystemInfoService.getComputeNodes();
        ComputeNode addedNode = computeNodes.get(0);
        Assertions.assertEquals("192.168.1.200", addedNode.getHost());
        Assertions.assertEquals(9050, addedNode.getHeartbeatPort());

        // 3. Execute dropComputeNodes operation (master side)
        DropComputeNodeClause dropComputeNodeClause = new DropComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        dropComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.200", 9050));
        masterSystemInfoService.dropComputeNodes(dropComputeNodeClause);

        // 4. Verify master state after dropping
        Assertions.assertEquals(0, masterSystemInfoService.getComputeNodes().size());
        
        // Verify compute node is removed from all collections
        ComputeNode retrievedNode = masterSystemInfoService.getComputeNode(addedNode.getId());
        Assertions.assertNull(retrievedNode);

        // 5. Test follower replay functionality
        // First add the same compute node to follower
        followerSystemInfoService.replayAddComputeNode(addedNode);
        Assertions.assertEquals(1, followerSystemInfoService.getComputeNodes().size());

        DropComputeNodeLog replayLog = (DropComputeNodeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_COMPUTE_NODE);
        
        // Execute follower replay
        followerSystemInfoService.replayDropComputeNode(replayLog);

        // 6. Verify follower state is consistent with master
        Assertions.assertEquals(0, followerSystemInfoService.getComputeNodes().size());
        
        ComputeNode followerRetrievedNode = followerSystemInfoService.getComputeNode(addedNode.getId());
        Assertions.assertNull(followerRetrievedNode);
    }

    @Test
    public void testDropComputeNodesEditLogException() throws Exception {
        // 1. Prepare test data and add compute node first
        List<String> hostPorts = Arrays.asList("192.168.1.300:9050");

        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.300", 9050));
        masterSystemInfoService.addComputeNodes(addComputeNodeClause);
        Assertions.assertEquals(1, masterSystemInfoService.getComputeNodes().size());

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addComputeNodes(addComputeNodeClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getComputeNodes().size());

        // Mock EditLog.logDropComputeNode to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropComputeNode(any(DropComputeNodeLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute dropComputeNodes operation and expect exception
        DropComputeNodeClause dropComputeNodeClause = new DropComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        dropComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.300", 9050));
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.dropComputeNodes(dropComputeNodeClause);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionSystemInfoService.getComputeNodes().size());
        
        // Verify compute node still exists
        List<ComputeNode> computeNodes = exceptionSystemInfoService.getComputeNodes();
        ComputeNode computeNode = computeNodes.get(0);
        Assertions.assertEquals("192.168.1.300", computeNode.getHost());
        Assertions.assertEquals(9050, computeNode.getHeartbeatPort());
    }

    @Test
    public void testDropComputeNodesNotExist() throws Exception {
        // 1. Test dropping non-existent compute node
        List<String> hostPorts = Arrays.asList("192.168.1.400:9050");

        // 2. Verify initial state
        Assertions.assertEquals(0, masterSystemInfoService.getComputeNodes().size());

        // 3. Execute dropComputeNodes operation and expect DdlException
        DropComputeNodeClause dropComputeNodeClause = new DropComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        dropComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.400", 9050));
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterSystemInfoService.dropComputeNodes(dropComputeNodeClause);
        });
        Assertions.assertTrue(exception.getMessage().contains("compute node does not exists"));

        // 4. Verify state remains unchanged
        Assertions.assertEquals(0, masterSystemInfoService.getComputeNodes().size());
    }

    @Test
    public void testDropComputeNodesWrongWarehouse() throws Exception {
        // 1. Add a compute node to a specific warehouse
        List<String> hostPorts = Arrays.asList("192.168.1.500:9050");

        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.500", 9050));
        masterSystemInfoService.addComputeNodes(addComputeNodeClause);
        Assertions.assertEquals(1, masterSystemInfoService.getComputeNodes().size());

        // 2. Try to drop with wrong warehouse
        String wrongWarehouse = "wrong_warehouse";
        DropComputeNodeClause dropComputeNodeClause = new DropComputeNodeClause(hostPorts, wrongWarehouse, cnGroupName, null);
        dropComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.500", 9050));

        // 3. Expect DdlException to be thrown
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterSystemInfoService.dropComputeNodes(dropComputeNodeClause);
        });

        // 4. Verify state remains unchanged
        Assertions.assertEquals(1, masterSystemInfoService.getComputeNodes().size());
        List<ComputeNode> computeNodes = masterSystemInfoService.getComputeNodes();
        Assertions.assertEquals("192.168.1.500", computeNodes.get(0).getHost());
        Assertions.assertEquals(9050, computeNodes.get(0).getHeartbeatPort());
    }

    @Test
    public void testDropComputeNodesMultipleNodes() throws Exception {
        // 1. Add multiple compute nodes
        List<String> hostPorts = Arrays.asList("192.168.1.100:9050", "192.168.1.101:9051", "192.168.1.102:9052");

        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9050));
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9051));
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.102", 9052));
        masterSystemInfoService.addComputeNodes(addComputeNodeClause);
        Assertions.assertEquals(3, masterSystemInfoService.getComputeNodes().size());

        // 2. Drop one compute node
        List<String> dropHostPorts = Arrays.asList("192.168.1.100:9050");
        DropComputeNodeClause dropComputeNodeClause = new DropComputeNodeClause(dropHostPorts, warehouse, cnGroupName, null);
        dropComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9050));
        masterSystemInfoService.dropComputeNodes(dropComputeNodeClause);
        Assertions.assertEquals(2, masterSystemInfoService.getComputeNodes().size());

        // 3. Verify remaining compute nodes
        List<ComputeNode> computeNodes = masterSystemInfoService.getComputeNodes();
        Assertions.assertEquals(2, computeNodes.size());
        
        // Verify the remaining nodes are the correct ones
        boolean hasNode1 = false;
        boolean hasNode2 = false;
        for (ComputeNode node : computeNodes) {
            if (node.getHost().equals("192.168.1.101") && node.getHeartbeatPort() == 9051) {
                hasNode1 = true;
            }
            if (node.getHost().equals("192.168.1.102") && node.getHeartbeatPort() == 9052) {
                hasNode2 = true;
            }
        }
        Assertions.assertTrue(hasNode1);
        Assertions.assertTrue(hasNode2);
    }

    @Test
    public void testDropComputeNodesReplayMultipleNodes() throws Exception {
        // 1. Add multiple compute nodes to master
        List<String> hostPorts = Arrays.asList("192.168.1.100:9050", "192.168.1.101:9051", "192.168.1.102:9052");
        AddComputeNodeClause addComputeNodeClause = new AddComputeNodeClause(hostPorts, warehouse, cnGroupName, null);
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9050));
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.101", 9051));
        addComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.102", 9052));
        masterSystemInfoService.addComputeNodes(addComputeNodeClause);
        Assertions.assertEquals(3, masterSystemInfoService.getComputeNodes().size());

        // 2. Drop one compute node
        List<String> dropHostPorts = Arrays.asList("192.168.1.100:9050");
        DropComputeNodeClause dropComputeNodeClause = new DropComputeNodeClause(dropHostPorts, warehouse, cnGroupName, null);
        dropComputeNodeClause.getHostPortPairs().add(Pair.create("192.168.1.100", 9050));
        masterSystemInfoService.dropComputeNodes(dropComputeNodeClause);
        Assertions.assertEquals(2, masterSystemInfoService.getComputeNodes().size());

        // 3. Create follower SystemInfoService
        Assertions.assertEquals(0, followerSystemInfoService.getComputeNodes().size());

        // 4. Replay all operations (3 adds + 1 drop)
        for (int i = 0; i < 3; i++) {
            ComputeNode replayNode = (ComputeNode) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_COMPUTE_NODE);
            followerSystemInfoService.replayAddComputeNode(replayNode);
        }
        Assertions.assertEquals(3, followerSystemInfoService.getComputeNodes().size());

        // Replay drop operation
        DropComputeNodeLog replayLog = (DropComputeNodeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_COMPUTE_NODE);
        followerSystemInfoService.replayDropComputeNode(replayLog);
        Assertions.assertEquals(2, followerSystemInfoService.getComputeNodes().size());

        // 5. Verify follower state matches master
        List<ComputeNode> masterComputeNodes = masterSystemInfoService.getComputeNodes();
        List<ComputeNode> followerComputeNodes = followerSystemInfoService.getComputeNodes();
        
        Assertions.assertEquals(2, masterComputeNodes.size());
        Assertions.assertEquals(2, followerComputeNodes.size());
        
        for (int i = 0; i < masterComputeNodes.size(); i++) {
            ComputeNode masterNode = masterComputeNodes.get(i);
            ComputeNode followerNode = followerComputeNodes.get(i);
            
            Assertions.assertEquals(masterNode.getId(), followerNode.getId());
            Assertions.assertEquals(masterNode.getHost(), followerNode.getHost());
            Assertions.assertEquals(masterNode.getHeartbeatPort(), followerNode.getHeartbeatPort());
            Assertions.assertEquals(masterNode.getWarehouseId(), followerNode.getWarehouseId());
        }
    }

    // ==================== Disk Management Tests ====================

    @Test
    public void testDecommissionDisksNormalCase() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.100";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;
        
        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend originalBackend = backends.get(0);
        Assertions.assertEquals(host, originalBackend.getHost());
        Assertions.assertEquals(heartbeatPort, originalBackend.getHeartbeatPort());

        // 3. Execute decommissionDisks operation (master side)
        originalBackend.setDisks(ImmutableMap.of("/data1", new DiskInfo("/data1"), "/data2", new DiskInfo("/data2")));
        List<String> diskList = Arrays.asList("/data1", "/data2");
        masterSystemInfoService.decommissionDisks(beHostPort, diskList);

        // 4. Test follower replay functionality
        // First add the same backend to follower
        Backend replayBackendInfo = (Backend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
        followerSystemInfoService.replayAddBackend(replayBackendInfo);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        DecommissionDiskInfo replayInfo = (DecommissionDiskInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DECOMMISSION_DISK);
        
        // Execute follower replay
        followerSystemInfoService.replayDecommissionDisks(replayInfo);

        // 5. Verify the decommission disk operation was logged correctly
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(originalBackend.getId(), replayInfo.getBeId());
        Assertions.assertNotNull(replayInfo.getDiskList());
        Assertions.assertEquals(2, replayInfo.getDiskList().size());
        Assertions.assertTrue(replayInfo.getDiskList().contains("/data1"));
        Assertions.assertTrue(replayInfo.getDiskList().contains("/data2"));
    }

    @Test
    public void testDecommissionDisksEditLogException() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.300";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;
        
        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());

        Backend originalBackend = exceptionSystemInfoService.getBackends().get(0);
        originalBackend.setDisks(ImmutableMap.of("/data1", new DiskInfo("/data1"), "/data2", new DiskInfo("/data2")));

        // Mock EditLog.logDecommissionDisk to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDecommissionDisk(any(DecommissionDiskInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute decommissionDisks operation and expect exception
        List<String> diskList = Arrays.asList("/data1", "/data2");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.decommissionDisks(beHostPort, diskList);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify backend remains unchanged after exception
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Backend backend = backends.get(0);
        Assertions.assertEquals(host, backend.getHost());
        Assertions.assertEquals(heartbeatPort, backend.getHeartbeatPort());
    }

    @Test
    public void testCancelDecommissionDisksNormalCase() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.100";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;
        
        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend originalBackend = backends.get(0);
        originalBackend.setDisks(ImmutableMap.of("/data1", new DiskInfo("/data1"), "/data2", new DiskInfo("/data2")));
        Assertions.assertEquals(host, originalBackend.getHost());
        Assertions.assertEquals(heartbeatPort, originalBackend.getHeartbeatPort());

        masterSystemInfoService.decommissionDisks(beHostPort, Arrays.asList("/data1", "/data2"));

        // 3. Execute cancelDecommissionDisks operation (master side)
        List<String> diskList = Arrays.asList("/data1", "/data2");
        masterSystemInfoService.cancelDecommissionDisks(beHostPort, diskList);

        // 4. Test follower replay functionality
        // First add the same backend to follower
        Backend replayBackendInfo = (Backend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
        followerSystemInfoService.replayAddBackend(replayBackendInfo);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        CancelDecommissionDiskInfo replayInfo = (CancelDecommissionDiskInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CANCEL_DECOMMISSION_DISK);
        
        // Execute follower replay
        followerSystemInfoService.replayCancelDecommissionDisks(replayInfo);

        // 5. Verify the cancel decommission disk operation was logged correctly
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(originalBackend.getId(), replayInfo.getBeId());
        Assertions.assertNotNull(replayInfo.getDiskList());
        Assertions.assertEquals(2, replayInfo.getDiskList().size());
        Assertions.assertTrue(replayInfo.getDiskList().contains("/data1"));
        Assertions.assertTrue(replayInfo.getDiskList().contains("/data2"));
    }

    @Test
    public void testCancelDecommissionDisksEditLogException() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.300";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;
        
        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());

        Backend originalBackend = exceptionSystemInfoService.getBackends().get(0);
        originalBackend.setDisks(ImmutableMap.of("/data1", new DiskInfo("/data1"), "/data2", new DiskInfo("/data2")));
        exceptionSystemInfoService.decommissionDisks(beHostPort, Arrays.asList("/data1", "/data2"));

        // Mock EditLog.logCancelDecommissionDisk to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCancelDecommissionDisk(any(CancelDecommissionDiskInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute cancelDecommissionDisks operation and expect exception
        List<String> diskList = Arrays.asList("/data1", "/data2");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.cancelDecommissionDisks(beHostPort, diskList);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify backend remains unchanged after exception
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Backend backend = backends.get(0);
        Assertions.assertEquals(host, backend.getHost());
        Assertions.assertEquals(heartbeatPort, backend.getHeartbeatPort());
    }

    @Test
    public void testDisableDisksNormalCase() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.100";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;
        
        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend originalBackend = backends.get(0);
        Assertions.assertEquals(host, originalBackend.getHost());
        Assertions.assertEquals(heartbeatPort, originalBackend.getHeartbeatPort());

        originalBackend.setDisks(ImmutableMap.of("/data1", new DiskInfo("/data1"), "/data2", new DiskInfo("/data2")));

        // 3. Execute disableDisks operation (master side)
        List<String> diskList = Arrays.asList("/data1", "/data2");
        masterSystemInfoService.disableDisks(beHostPort, diskList);

        // 4. Test follower replay functionality
        // First add the same backend to follower
        Backend replayBackendInfo = (Backend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
        followerSystemInfoService.replayAddBackend(replayBackendInfo);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        DisableDiskInfo replayInfo = (DisableDiskInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DISABLE_DISK);
        
        // Execute follower replay
        followerSystemInfoService.replayDisableDisks(replayInfo);

        // 5. Verify the disable disk operation was logged correctly
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(originalBackend.getId(), replayInfo.getBeId());
        Assertions.assertNotNull(replayInfo.getDiskList());
        Assertions.assertEquals(2, replayInfo.getDiskList().size());
        Assertions.assertTrue(replayInfo.getDiskList().contains("/data1"));
        Assertions.assertTrue(replayInfo.getDiskList().contains("/data2"));
    }

    @Test
    public void testDisableDisksEditLogException() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.300";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;
        
        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());

        Backend originalBackend = exceptionSystemInfoService.getBackends().get(0);
        originalBackend.setDisks(ImmutableMap.of("/data1", new DiskInfo("/data1"), "/data2", new DiskInfo("/data2")));

        // Mock EditLog.logDisableDisk to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDisableDisk(any(DisableDiskInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute disableDisks operation and expect exception
        List<String> diskList = Arrays.asList("/data1", "/data2");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionSystemInfoService.disableDisks(beHostPort, diskList);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify backend remains unchanged after exception
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Backend backend = backends.get(0);
        Assertions.assertEquals(host, backend.getHost());
        Assertions.assertEquals(heartbeatPort, backend.getHeartbeatPort());
    }

    // ==================== Backend Update Disks Tests ====================

    @Test
    public void testBackendUpdateDisksNormalCase() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.100";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;

        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend originalBackend = backends.get(0);
        Assertions.assertEquals(host, originalBackend.getHost());
        Assertions.assertEquals(heartbeatPort, originalBackend.getHeartbeatPort());

        // 3. Create TDisk objects for update
        Map<String, TDisk> backendDisks = new HashMap<>();
        TDisk disk1 = new TDisk("/data1/", 1000, 800, true);
        disk1.setDisk_available_capacity(200);
        disk1.setPath_hash(1111);
        disk1.setStorage_medium(TStorageMedium.HDD);
        TDisk disk2 = new TDisk("/data2/", 2000, 700, true);
        disk2.setDisk_available_capacity(1300);
        disk2.setPath_hash(2222);
        disk2.setStorage_medium(TStorageMedium.HDD);
        TDisk disk3 = new TDisk("/data3/", 3000, 600, false);
        disk3.setDisk_available_capacity(2400);
        disk3.setPath_hash(3333);
        disk3.setStorage_medium(TStorageMedium.SSD);

        backendDisks.put(disk1.getRoot_path(), disk1);
        backendDisks.put(disk2.getRoot_path(), disk2);
        backendDisks.put(disk3.getRoot_path(), disk3);

        // 4. Execute updateDisks operation (master side)
        originalBackend.updateDisks(backendDisks, masterSystemInfoService);
        Assertions.assertEquals(3, originalBackend.getDisks().size());
        DiskInfo masterDisk1 = originalBackend.getDisks().get("/data1/");
        DiskInfo masterDisk2 = originalBackend.getDisks().get("/data2/");
        DiskInfo masterDisk3 = originalBackend.getDisks().get("/data3/");
        Assertions.assertNotNull(masterDisk1);
        Assertions.assertNotNull(masterDisk2);
        Assertions.assertNotNull(masterDisk3);
        Assertions.assertEquals(1000L, masterDisk1.getTotalCapacityB());
        Assertions.assertEquals(2000L, masterDisk2.getTotalCapacityB());
        Assertions.assertEquals(3000L, masterDisk3.getTotalCapacityB());
        Assertions.assertEquals(800L, masterDisk1.getDataUsedCapacityB());
        Assertions.assertEquals(700L, masterDisk2.getDataUsedCapacityB());
        Assertions.assertEquals(600L, masterDisk3.getDataUsedCapacityB());
        Assertions.assertEquals(200, masterDisk1.getAvailableCapacityB());
        Assertions.assertEquals(1300, masterDisk2.getAvailableCapacityB());
        Assertions.assertEquals(2400, masterDisk3.getAvailableCapacityB());
        Assertions.assertEquals(DiskInfo.DiskState.ONLINE, masterDisk1.getState());
        Assertions.assertEquals(DiskInfo.DiskState.ONLINE, masterDisk2.getState());
        Assertions.assertEquals(DiskInfo.DiskState.OFFLINE, masterDisk3.getState());
        Assertions.assertEquals(1111, masterDisk1.getPathHash());
        Assertions.assertEquals(2222, masterDisk2.getPathHash());
        Assertions.assertEquals(3333, masterDisk3.getPathHash());
        Assertions.assertEquals(TStorageMedium.HDD, masterDisk1.getStorageMedium());
        Assertions.assertEquals(TStorageMedium.HDD, masterDisk2.getStorageMedium());
        Assertions.assertEquals(TStorageMedium.SSD, masterDisk3.getStorageMedium());

        // 5. Test follower replay functionality
        // First add the same backend to follower
        Backend replayBackendInfo = (Backend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
        followerSystemInfoService.replayAddBackend(replayBackendInfo);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        UpdateBackendInfo info = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);

        // Execute follower replay
        followerSystemInfoService.replayBackendStateChange(info);

        Backend backend = followerSystemInfoService.getBackend(originalBackend.getId());
        // 6. Verify the update disks operation was logged correctly
        Assertions.assertNotNull(backend);
        Assertions.assertEquals(originalBackend.getId(), backend.getId());
        Assertions.assertEquals(host, backend.getHost());
        Assertions.assertEquals(heartbeatPort, backend.getHeartbeatPort());

        // Verify disk information
        Map<String, DiskInfo> disks = backend.getDisks();
        Assertions.assertNotNull(disks);
        Assertions.assertEquals(3, disks.size());
        Assertions.assertTrue(disks.containsKey("/data1/"));
        Assertions.assertTrue(disks.containsKey("/data2/"));
        Assertions.assertTrue(disks.containsKey("/data3/"));
    }

    @Test
    public void testBackendUpdateDisksEditLogException() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.300";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;
        
        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));

        // 2. Create a separate SystemInfoService for exception testing
        SystemInfoService exceptionSystemInfoService = new SystemInfoService();
        exceptionSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, exceptionSystemInfoService.getBackends().size());

        Backend originalBackend = exceptionSystemInfoService.getBackends().get(0);

        // 3. Create TDisk objects for update
        Map<String, TDisk> backendDisks = new HashMap<>();
        TDisk disk1 = new TDisk("/data1/", 1000, 800, true);
        TDisk disk2 = new TDisk("/data2/", 2000, 700, true);
        
        backendDisks.put(disk1.getRoot_path(), disk1);
        backendDisks.put(disk2.getRoot_path(), disk2);

        // Mock EditLog.logBackendStateChange to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logBackendStateChange(any(UpdateBackendInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute updateDisks operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            originalBackend.updateDisks(backendDisks, exceptionSystemInfoService);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify backend remains unchanged after exception
        List<Backend> backends = exceptionSystemInfoService.getBackends();
        Backend backend = backends.get(0);
        Assertions.assertEquals(host, backend.getHost());
        Assertions.assertEquals(heartbeatPort, backend.getHeartbeatPort());
        
        // Verify no disks were added due to exception
        Map<String, DiskInfo> disks = backend.getDisks();
        Assertions.assertTrue(disks == null || disks.isEmpty());
    }

    // ==================== Backend Disk State Update Tests ====================

    @Test
    public void testBackendUpdateDiskStateAndVerifyFollower() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.100";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;

        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend originalBackend = backends.get(0);
        Assertions.assertEquals(host, originalBackend.getHost());
        Assertions.assertEquals(heartbeatPort, originalBackend.getHeartbeatPort());

        // 3. Create initial TDisk objects
        Map<String, TDisk> initialDisks = new HashMap<>();
        TDisk disk1 = new TDisk("/data1/", 1000, 800, true);
        TDisk disk2 = new TDisk("/data2/", 2000, 700, true);
        initialDisks.put(disk1.getRoot_path(), disk1);
        initialDisks.put(disk2.getRoot_path(), disk2);

        // 4. First update with initial disks
        originalBackend.updateDisks(initialDisks, masterSystemInfoService);
        Assertions.assertEquals(2, originalBackend.getDisks().size());

        // 5. Update disk state (change disk2 to offline)
        Map<String, TDisk> updatedDisks = new HashMap<>();
        TDisk updatedDisk1 = new TDisk("/data1/", 1000, 800, true);
        TDisk updatedDisk2 = new TDisk("/data2/", 2000, 700, false); // Changed to offline
        updatedDisks.put(updatedDisk1.getRoot_path(), updatedDisk1);
        updatedDisks.put(updatedDisk2.getRoot_path(), updatedDisk2);

        // 6. Execute disk state update (master side)
        originalBackend.updateDisks(updatedDisks, masterSystemInfoService);

        // 7. Test follower replay functionality
        // First add the same backend to follower
        Backend replayBackendInfo = (Backend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
        followerSystemInfoService.replayAddBackend(originalBackend);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        // Replay first disk update (initial disks)
        UpdateBackendInfo info1 = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        followerSystemInfoService.replayBackendStateChange(info1);

        // Replay second disk update (state change)
        UpdateBackendInfo info2 = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        followerSystemInfoService.replayBackendStateChange(info2);

        // 8. Verify follower state is consistent with master
        Backend followerBackend = followerSystemInfoService.getBackend(originalBackend.getId());
        Assertions.assertNotNull(followerBackend);
        Assertions.assertEquals(originalBackend.getId(), followerBackend.getId());
        Assertions.assertEquals(host, followerBackend.getHost());
        Assertions.assertEquals(heartbeatPort, followerBackend.getHeartbeatPort());

        // Verify disk information and state
        Map<String, DiskInfo> followerDisks = followerBackend.getDisks();
        Assertions.assertNotNull(followerDisks);
        Assertions.assertEquals(2, followerDisks.size());
        
        DiskInfo followerDisk1 = followerDisks.get("/data1/");
        DiskInfo followerDisk2 = followerDisks.get("/data2/");
        Assertions.assertNotNull(followerDisk1);
        Assertions.assertNotNull(followerDisk2);
        
        // Verify disk1 is still online
        Assertions.assertEquals(DiskInfo.DiskState.ONLINE, followerDisk1.getState());
        // Verify disk2 is now offline
        Assertions.assertEquals(DiskInfo.DiskState.OFFLINE, followerDisk2.getState());
    }

    @Test
    public void testBackendAddDiskAndVerifyFollower() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.100";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;

        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend originalBackend = backends.get(0);
        Assertions.assertEquals(host, originalBackend.getHost());
        Assertions.assertEquals(heartbeatPort, originalBackend.getHeartbeatPort());

        // 3. Create initial TDisk objects with one disk
        Map<String, TDisk> initialDisks = new HashMap<>();
        TDisk disk1 = new TDisk("/data1/", 1000, 800, true);
        initialDisks.put(disk1.getRoot_path(), disk1);

        // 4. First update with initial disk
        originalBackend.updateDisks(initialDisks, masterSystemInfoService);
        Assertions.assertEquals(1, originalBackend.getDisks().size());

        // 5. Add new disk
        Map<String, TDisk> updatedDisks = new HashMap<>();
        TDisk disk2 = new TDisk("/data2/", 2000, 700, true);
        updatedDisks.put(disk1.getRoot_path(), disk1);
        updatedDisks.put(disk2.getRoot_path(), disk2);

        // 6. Execute add disk operation (master side)
        originalBackend.updateDisks(updatedDisks, masterSystemInfoService);
        Assertions.assertEquals(2, originalBackend.getDisks().size());

        // 7. Test follower replay functionality
        // First add the same backend to follower
        Backend replayBackendInfo = (Backend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
        followerSystemInfoService.replayAddBackend(replayBackendInfo);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        // Replay first disk update (initial disk)
        UpdateBackendInfo info1 = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        followerSystemInfoService.replayBackendStateChange(info1);

        // Replay second disk update (add new disk)
        UpdateBackendInfo info2 = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        followerSystemInfoService.replayBackendStateChange(info2);

        // 8. Verify follower state is consistent with master
        Backend followerBackend = followerSystemInfoService.getBackend(originalBackend.getId());
        Assertions.assertNotNull(followerBackend);
        Assertions.assertEquals(originalBackend.getId(), followerBackend.getId());
        Assertions.assertEquals(host, followerBackend.getHost());
        Assertions.assertEquals(heartbeatPort, followerBackend.getHeartbeatPort());

        // Verify disk information
        Map<String, DiskInfo> followerDisks = followerBackend.getDisks();
        Assertions.assertNotNull(followerDisks);
        Assertions.assertEquals(2, followerDisks.size());
        
        // Verify both disks exist
        Assertions.assertTrue(followerDisks.containsKey("/data1/"));
        Assertions.assertTrue(followerDisks.containsKey("/data2/"));
        
        DiskInfo followerDisk1 = followerDisks.get("/data1/");
        DiskInfo followerDisk2 = followerDisks.get("/data2/");
        Assertions.assertNotNull(followerDisk1);
        Assertions.assertNotNull(followerDisk2);
        
        // Verify disk properties
        Assertions.assertEquals(DiskInfo.DiskState.ONLINE, followerDisk1.getState());
        Assertions.assertEquals(DiskInfo.DiskState.ONLINE, followerDisk2.getState());
    }

    @Test
    public void testBackendRemoveDiskAndVerifyFollower() throws Exception {
        // 1. Prepare test data and add backend first
        String host = "192.168.1.100";
        int heartbeatPort = 9060;
        String beHostPort = host + ":" + heartbeatPort;

        List<String> hostPorts = Arrays.asList(beHostPort);
        AddBackendClause addBackendClause = new AddBackendClause(hostPorts, warehouse, cnGroupName, null);
        addBackendClause.getHostPortPairs().add(Pair.create(host, heartbeatPort));
        masterSystemInfoService.addBackends(addBackendClause);
        Assertions.assertEquals(1, masterSystemInfoService.getBackends().size());

        // 2. Verify initial state
        List<Backend> backends = masterSystemInfoService.getBackends();
        Backend originalBackend = backends.get(0);
        Assertions.assertEquals(host, originalBackend.getHost());
        Assertions.assertEquals(heartbeatPort, originalBackend.getHeartbeatPort());

        // 3. Create initial TDisk objects with multiple disks
        Map<String, TDisk> initialDisks = new HashMap<>();
        TDisk disk1 = new TDisk("/data1/", 1000, 800, true);
        TDisk disk2 = new TDisk("/data2/", 2000, 700, true);
        TDisk disk3 = new TDisk("/data3/", 3000, 600, true);
        initialDisks.put(disk1.getRoot_path(), disk1);
        initialDisks.put(disk2.getRoot_path(), disk2);
        initialDisks.put(disk3.getRoot_path(), disk3);

        // 4. First update with initial disks
        originalBackend.updateDisks(initialDisks, masterSystemInfoService);
        Assertions.assertEquals(3, originalBackend.getDisks().size());

        // 5. Remove one disk (disk2)
        Map<String, TDisk> updatedDisks = new HashMap<>();
        updatedDisks.put(disk1.getRoot_path(), disk1);
        updatedDisks.put(disk3.getRoot_path(), disk3);
        // Note: disk2 is not included, so it will be removed

        // 6. Execute remove disk operation (master side)
        originalBackend.updateDisks(updatedDisks, masterSystemInfoService);
        Assertions.assertEquals(2, originalBackend.getDisks().size());

        // 7. Test follower replay functionality
        // First add the same backend to follower
        Backend replayBackendInfo = (Backend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BACKEND_V2);
        followerSystemInfoService.replayAddBackend(replayBackendInfo);
        Assertions.assertEquals(1, followerSystemInfoService.getBackends().size());

        // Replay first disk update (initial disks)
        UpdateBackendInfo info1 = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        followerSystemInfoService.replayBackendStateChange(info1);

        // Replay second disk update (remove disk)
        UpdateBackendInfo info2 = (UpdateBackendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        followerSystemInfoService.replayBackendStateChange(info2);

        // 8. Verify follower state is consistent with master
        Backend followerBackend = followerSystemInfoService.getBackend(originalBackend.getId());
        Assertions.assertNotNull(followerBackend);
        Assertions.assertEquals(originalBackend.getId(), followerBackend.getId());
        Assertions.assertEquals(host, followerBackend.getHost());
        Assertions.assertEquals(heartbeatPort, followerBackend.getHeartbeatPort());

        // Verify disk information
        Map<String, DiskInfo> followerDisks = followerBackend.getDisks();
        Assertions.assertNotNull(followerDisks);
        Assertions.assertEquals(2, followerDisks.size());
        
        // Verify disk1 and disk3 still exist
        Assertions.assertTrue(followerDisks.containsKey("/data1/"));
        Assertions.assertTrue(followerDisks.containsKey("/data3/"));
        
        // Verify disk2 was removed
        Assertions.assertFalse(followerDisks.containsKey("/data2/"));
        
        DiskInfo followerDisk1 = followerDisks.get("/data1/");
        DiskInfo followerDisk3 = followerDisks.get("/data3/");
        Assertions.assertNotNull(followerDisk1);
        Assertions.assertNotNull(followerDisk3);
        
        // Verify remaining disk properties
        Assertions.assertEquals(DiskInfo.DiskState.ONLINE, followerDisk1.getState());
        Assertions.assertEquals(DiskInfo.DiskState.ONLINE, followerDisk3.getState());
    }
}
