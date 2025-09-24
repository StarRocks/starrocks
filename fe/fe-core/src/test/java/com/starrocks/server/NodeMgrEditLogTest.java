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

package com.starrocks.server;

import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.leader.CheckpointController;
import com.starrocks.persist.DropFrontendInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.UpdateFrontendInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.system.Frontend;
import com.starrocks.utframe.MockJournal;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class NodeMgrEditLogTest {
    private NodeMgr masterNodeMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create NodeMgr instance
        masterNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "master", Pair.create("192.168.1.2", 9010));
        GlobalStateMgr.getCurrentState().setHaProtocol(new MockJournal.MockProtocol());
        GlobalStateMgr.getCurrentState().setCheckpointController(new CheckpointController("controller", new MockJournal(), ""));
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAddFrontendNormalCase() throws Exception {
        // 1. Prepare test data
        FrontendNodeType role = FrontendNodeType.FOLLOWER;
        String host = "192.168.1.100";
        int editLogPort = 9010;

        // 2. Verify initial state
        Assertions.assertEquals(0, masterNodeMgr.getAllFrontends().size());

        // 3. Execute addFrontend operation (master side)
        masterNodeMgr.addFrontend(role, host, editLogPort);

        // 4. Verify master state
        List<Frontend> frontends = masterNodeMgr.getAllFrontends();
        Assertions.assertEquals(1, frontends.size());
        
        Frontend addedFrontend = frontends.get(0);
        Assertions.assertEquals(role, addedFrontend.getRole());
        Assertions.assertEquals(host, addedFrontend.getHost());
        Assertions.assertEquals(editLogPort, addedFrontend.getEditLogPort());
        Assertions.assertNotNull(addedFrontend.getNodeName());
        Assertions.assertTrue(addedFrontend.getFid() > 0);

        // Verify frontend can be retrieved by ID
        Frontend retrievedFrontend = masterNodeMgr.getFrontend(addedFrontend.getFid());
        Assertions.assertNotNull(retrievedFrontend);
        Assertions.assertEquals(addedFrontend.getNodeName(), retrievedFrontend.getNodeName());

        // Verify frontend can be retrieved by role
        List<Frontend> followerFrontends = masterNodeMgr.getFrontends(FrontendNodeType.FOLLOWER);
        Assertions.assertEquals(1, followerFrontends.size());
        Assertions.assertEquals(addedFrontend.getNodeName(), followerFrontends.get(0).getNodeName());

        // 5. Test follower replay functionality
        NodeMgr followerNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "follower", Pair.create("192.168.1.3", 9010));
        
        // Verify follower initial state
        Assertions.assertEquals(0, followerNodeMgr.getAllFrontends().size());

        Frontend replayFrontend = (Frontend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_FRONTEND_V2);
        
        // Execute follower replay
        followerNodeMgr.replayAddFrontend(replayFrontend);

        // 6. Verify follower state is consistent with master
        List<Frontend> followerFrontendsList = followerNodeMgr.getAllFrontends();
        Assertions.assertEquals(1, followerFrontendsList.size());
        
        Frontend followerFrontend = followerFrontendsList.get(0);
        Assertions.assertEquals(role, followerFrontend.getRole());
        Assertions.assertEquals(host, followerFrontend.getHost());
        Assertions.assertEquals(editLogPort, followerFrontend.getEditLogPort());
        Assertions.assertEquals(addedFrontend.getNodeName(), followerFrontend.getNodeName());
        Assertions.assertEquals(addedFrontend.getFid(), followerFrontend.getFid());
    }

    @Test
    public void testAddFrontendEditLogException() throws Exception {
        // 1. Prepare test data
        FrontendNodeType role = FrontendNodeType.OBSERVER;
        String host = "192.168.1.200";
        int editLogPort = 9011;

        // 2. Create a separate NodeMgr for exception testing
        NodeMgr exceptionNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "exception_master", Pair.create("192.168.1.3", 9010));
        EditLog spyEditLog = spy(new EditLog(null));
        
        // 3. Mock EditLog.logAddFrontend to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAddFrontend(any(Frontend.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionNodeMgr.getAllFrontends().size());

        // 4. Execute addFrontend operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionNodeMgr.addFrontend(role, host, editLogPort);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionNodeMgr.getAllFrontends().size());
        
        // Verify no frontend was accidentally added
        List<Frontend> frontends = exceptionNodeMgr.getAllFrontends();
        Assertions.assertTrue(frontends.isEmpty());
    }

    @Test
    public void testAddFrontendDuplicateHost() throws Exception {
        // 1. First add a frontend
        FrontendNodeType role1 = FrontendNodeType.FOLLOWER;
        String host = "192.168.1.150";
        int editLogPort1 = 9010;
        
        masterNodeMgr.addFrontend(role1, host, editLogPort1);
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());

        // 2. Try to add frontend with same host but different port
        FrontendNodeType role2 = FrontendNodeType.OBSERVER;
        int editLogPort2 = 9011;

        // 3. Expect DdlException to be thrown
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterNodeMgr.addFrontend(role2, host, editLogPort2);
        });
        Assertions.assertTrue(exception.getMessage().contains("FE with the same host"));

        // 4. Verify state remains unchanged (only the original frontend)
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());
        List<Frontend> frontends = masterNodeMgr.getAllFrontends();
        Assertions.assertEquals(role1, frontends.get(0).getRole());
        Assertions.assertEquals(editLogPort1, frontends.get(0).getEditLogPort());
    }

    @Test
    public void testAddFrontendDifferentRoles() throws Exception {
        // 1. Add FOLLOWER frontend
        FrontendNodeType followerRole = FrontendNodeType.FOLLOWER;
        String followerHost = "192.168.1.160";
        int followerPort = 9010;
        
        masterNodeMgr.addFrontend(followerRole, followerHost, followerPort);
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());

        // 2. Add OBSERVER frontend with different host
        FrontendNodeType observerRole = FrontendNodeType.OBSERVER;
        String observerHost = "192.168.1.161";
        int observerPort = 9011;
        
        masterNodeMgr.addFrontend(observerRole, observerHost, observerPort);

        // 3. Verify both frontends exist
        Assertions.assertEquals(2, masterNodeMgr.getAllFrontends().size());
        
        List<Frontend> followerFrontends = masterNodeMgr.getFrontends(FrontendNodeType.FOLLOWER);
        List<Frontend> observerFrontends = masterNodeMgr.getFrontends(FrontendNodeType.OBSERVER);
        
        Assertions.assertEquals(1, followerFrontends.size());
        Assertions.assertEquals(1, observerFrontends.size());
        
        Assertions.assertEquals(followerHost, followerFrontends.get(0).getHost());
        Assertions.assertEquals(followerPort, followerFrontends.get(0).getEditLogPort());
        Assertions.assertEquals(observerHost, observerFrontends.get(0).getHost());
        Assertions.assertEquals(observerPort, observerFrontends.get(0).getEditLogPort());
    }

    @Test
    public void testAddFrontendMultipleFrontends() throws Exception {
        // 1. Add multiple frontends
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        masterNodeMgr.addFrontend(FrontendNodeType.OBSERVER, "192.168.1.101", 9011);
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.102", 9012);

        // 2. Verify all frontends are added
        Assertions.assertEquals(3, masterNodeMgr.getAllFrontends().size());
        
        List<Frontend> followerFrontends = masterNodeMgr.getFrontends(FrontendNodeType.FOLLOWER);
        List<Frontend> observerFrontends = masterNodeMgr.getFrontends(FrontendNodeType.OBSERVER);
        
        Assertions.assertEquals(2, followerFrontends.size());
        Assertions.assertEquals(1, observerFrontends.size());

        // 3. Verify each frontend has unique ID
        List<Frontend> allFrontends = masterNodeMgr.getAllFrontends();
        for (int i = 0; i < allFrontends.size(); i++) {
            for (int j = i + 1; j < allFrontends.size(); j++) {
                Assertions.assertNotEquals(allFrontends.get(i).getFid(), allFrontends.get(j).getFid());
                Assertions.assertNotEquals(allFrontends.get(i).getNodeName(), allFrontends.get(j).getNodeName());
            }
        }
    }

    @Test
    public void testAddFrontendReplayMultipleFrontends() throws Exception {
        // 1. Add multiple frontends to master
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        masterNodeMgr.addFrontend(FrontendNodeType.OBSERVER, "192.168.1.101", 9011);
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.102", 9012);

        // 2. Create follower NodeMgr
        NodeMgr followerNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "follower", Pair.create("192.168.1.1", 9010));
        Assertions.assertEquals(0, followerNodeMgr.getAllFrontends().size());

        // 3. Replay all frontend additions
        for (int i = 0; i < 3; i++) {
            Frontend replayFrontend = (Frontend) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_FRONTEND_V2);
            followerNodeMgr.replayAddFrontend(replayFrontend);
        }

        // 4. Verify follower state matches master
        Assertions.assertEquals(3, followerNodeMgr.getAllFrontends().size());
        
        List<Frontend> masterFrontends = masterNodeMgr.getAllFrontends();
        List<Frontend> followerFrontends = followerNodeMgr.getAllFrontends();
        
        for (int i = 0; i < masterFrontends.size(); i++) {
            Frontend masterFrontend = masterFrontends.get(i);
            Frontend followerFrontend = followerFrontends.get(i);
            
            Assertions.assertEquals(masterFrontend.getRole(), followerFrontend.getRole());
            Assertions.assertEquals(masterFrontend.getHost(), followerFrontend.getHost());
            Assertions.assertEquals(masterFrontend.getEditLogPort(), followerFrontend.getEditLogPort());
            Assertions.assertEquals(masterFrontend.getNodeName(), followerFrontend.getNodeName());
            Assertions.assertEquals(masterFrontend.getFid(), followerFrontend.getFid());
        }
    }

    // ==================== Drop Frontend Tests ====================

    @Test
    public void testDropFrontendNormalCase() throws Exception {
        // 1. Prepare test data and add frontend first
        FrontendNodeType role = FrontendNodeType.FOLLOWER;
        String host = "192.168.1.200";
        int editLogPort = 9010;

        // Add frontend first
        masterNodeMgr.addFrontend(role, host, editLogPort);
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());

        // 2. Verify initial state after adding
        List<Frontend> frontends = masterNodeMgr.getAllFrontends();
        Frontend addedFrontend = frontends.get(0);
        Assertions.assertEquals(role, addedFrontend.getRole());
        Assertions.assertEquals(host, addedFrontend.getHost());
        Assertions.assertEquals(editLogPort, addedFrontend.getEditLogPort());

        // 3. Execute dropFrontend operation (master side)
        masterNodeMgr.dropFrontend(role, host, editLogPort);

        // 4. Verify master state after dropping
        Assertions.assertEquals(0, masterNodeMgr.getAllFrontends().size());
        
        // Verify frontend is removed from all collections
        Frontend retrievedFrontend = masterNodeMgr.getFrontend(addedFrontend.getFid());
        Assertions.assertNull(retrievedFrontend);
        
        List<Frontend> followerFrontends = masterNodeMgr.getFrontends(FrontendNodeType.FOLLOWER);
        Assertions.assertEquals(0, followerFrontends.size());

        // 5. Test follower replay functionality
        NodeMgr followerNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "follower", Pair.create("192.168.1.1", 9010));
        
        // First add the same frontend to follower
        followerNodeMgr.replayAddFrontend(addedFrontend);
        Assertions.assertEquals(1, followerNodeMgr.getAllFrontends().size());

        DropFrontendInfo dropFrontendInfo = (DropFrontendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_REMOVE_FRONTEND_V2);
        
        // Execute follower replay
        followerNodeMgr.replayDropFrontend(dropFrontendInfo);

        // 6. Verify follower state is consistent with master
        Assertions.assertEquals(0, followerNodeMgr.getAllFrontends().size());
        
        Frontend followerRetrievedFrontend = followerNodeMgr.getFrontend(addedFrontend.getFid());
        Assertions.assertNull(followerRetrievedFrontend);
        
        List<Frontend> followerFollowerFrontends = followerNodeMgr.getFrontends(FrontendNodeType.FOLLOWER);
        Assertions.assertEquals(0, followerFollowerFrontends.size());
    }

    @Test
    public void testDropFrontendEditLogException() throws Exception {
        // 1. Prepare test data and add frontend first
        FrontendNodeType role = FrontendNodeType.OBSERVER;
        String host = "192.168.1.3";
        int editLogPort = 9010;

        // Create a separate NodeMgr for exception testing
        NodeMgr exceptionNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER,
                "exception_master", Pair.create("192.168.1.2", 9010));
        
        // Add frontend first
        exceptionNodeMgr.addFrontend(role, host, editLogPort);
        Assertions.assertEquals(1, exceptionNodeMgr.getAllFrontends().size());

        // 2. Mock EditLog.logRemoveFrontend to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logRemoveFrontend(any(DropFrontendInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute dropFrontend operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionNodeMgr.dropFrontend(role, host, editLogPort);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionNodeMgr.getAllFrontends().size());
        
        // Verify frontend still exists
        List<Frontend> frontends = exceptionNodeMgr.getAllFrontends();
        Frontend frontend = frontends.get(0);
        Assertions.assertEquals(role, frontend.getRole());
        Assertions.assertEquals(host, frontend.getHost());
        Assertions.assertEquals(editLogPort, frontend.getEditLogPort());
    }

    @Test
    public void testDropFrontendNotExist() throws Exception {
        // 1. Test dropping non-existent frontend
        FrontendNodeType role = FrontendNodeType.FOLLOWER;
        String host = "192.168.1.400";
        int editLogPort = 9010;

        // 2. Verify initial state
        Assertions.assertEquals(0, masterNodeMgr.getAllFrontends().size());

        // 3. Execute dropFrontend operation and expect DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterNodeMgr.dropFrontend(role, host, editLogPort);
        });
        Assertions.assertTrue(exception.getMessage().contains("frontend does not exist"));

        // 4. Verify state remains unchanged
        Assertions.assertEquals(0, masterNodeMgr.getAllFrontends().size());
    }

    @Test
    public void testDropFrontendWrongRole() throws Exception {
        // 1. Add a FOLLOWER frontend
        FrontendNodeType followerRole = FrontendNodeType.FOLLOWER;
        String host = "192.168.1.5";
        int editLogPort = 9010;
        
        masterNodeMgr.addFrontend(followerRole, host, editLogPort);
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());

        // 2. Try to drop with wrong role (OBSERVER)
        FrontendNodeType wrongRole = FrontendNodeType.OBSERVER;

        // 3. Expect DdlException to be thrown
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterNodeMgr.dropFrontend(wrongRole, host, editLogPort);
        });
        Assertions.assertTrue(exception.getMessage().contains("OBSERVER does not exist"));

        // 4. Verify state remains unchanged
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());
        List<Frontend> frontends = masterNodeMgr.getAllFrontends();
        Assertions.assertEquals(followerRole, frontends.get(0).getRole());
    }

    @Test
    public void testDropFrontendMultipleFrontends() throws Exception {
        // 1. Add multiple frontends
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        masterNodeMgr.addFrontend(FrontendNodeType.OBSERVER, "192.168.1.101", 9011);
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.102", 9012);
        Assertions.assertEquals(3, masterNodeMgr.getAllFrontends().size());

        // 2. Drop one frontend
        masterNodeMgr.dropFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        Assertions.assertEquals(2, masterNodeMgr.getAllFrontends().size());

        // 3. Verify remaining frontends
        List<Frontend> followerFrontends = masterNodeMgr.getFrontends(FrontendNodeType.FOLLOWER);
        List<Frontend> observerFrontends = masterNodeMgr.getFrontends(FrontendNodeType.OBSERVER);
        
        Assertions.assertEquals(1, followerFrontends.size());
        Assertions.assertEquals(1, observerFrontends.size());
        
        Assertions.assertEquals("192.168.1.102", followerFrontends.get(0).getHost());
        Assertions.assertEquals("192.168.1.101", observerFrontends.get(0).getHost());
    }

    // ==================== Modify Frontend Host Tests ====================

    @Test
    public void testModifyFrontendHostNormalCase() throws Exception {
        // 1. Prepare test data and add frontend first
        FrontendNodeType role = FrontendNodeType.FOLLOWER;
        String originalHost = "192.168.1.100";
        String newHost = "192.168.1.200";
        int editLogPort = 9010;

        // Add frontend first
        masterNodeMgr.addFrontend(role, originalHost, editLogPort);
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());

        // 2. Verify initial state
        List<Frontend> frontends = masterNodeMgr.getAllFrontends();
        Frontend originalFrontend = frontends.get(0);
        Assertions.assertEquals(originalHost, originalFrontend.getHost());

        // 3. Execute modifyFrontendHost operation
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause(originalHost, newHost);
        masterNodeMgr.modifyFrontendHost(clause);

        // 4. Verify master state after modification
        List<Frontend> updatedFrontends = masterNodeMgr.getAllFrontends();
        Assertions.assertEquals(1, updatedFrontends.size());
        
        Frontend modifiedFrontend = updatedFrontends.get(0);
        Assertions.assertEquals(newHost, modifiedFrontend.getHost());
        Assertions.assertEquals(editLogPort, modifiedFrontend.getEditLogPort());
        Assertions.assertEquals(role, modifiedFrontend.getRole());
        Assertions.assertEquals(originalFrontend.getNodeName(), modifiedFrontend.getNodeName());
        Assertions.assertEquals(originalFrontend.getFid(), modifiedFrontend.getFid());

        // 5. Test follower replay functionality
        NodeMgr followerNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "follower", Pair.create("1", 9010));
        
        // First add the same frontend to follower
        followerNodeMgr.replayAddFrontend(originalFrontend);
        Assertions.assertEquals(1, followerNodeMgr.getAllFrontends().size());

        UpdateFrontendInfo updateFrontendInfo = (UpdateFrontendInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_FRONTEND_V2);
        
        // Execute follower replay
        followerNodeMgr.replayUpdateFrontend(updateFrontendInfo);

        // 6. Verify follower state is consistent with master
        List<Frontend> followerFrontends = followerNodeMgr.getAllFrontends();
        Assertions.assertEquals(1, followerFrontends.size());
        
        Frontend followerModifiedFrontend = followerFrontends.get(0);
        Assertions.assertEquals(newHost, followerModifiedFrontend.getHost());
        Assertions.assertEquals(editLogPort, followerModifiedFrontend.getEditLogPort());
        Assertions.assertEquals(role, followerModifiedFrontend.getRole());
        Assertions.assertEquals(originalFrontend.getNodeName(), followerModifiedFrontend.getNodeName());
        Assertions.assertEquals(originalFrontend.getFid(), followerModifiedFrontend.getFid());
    }

    @Test
    public void testModifyFrontendHostEditLogException() throws Exception {
        // 1. Prepare test data and add frontend first
        FrontendNodeType role = FrontendNodeType.OBSERVER;
        String originalHost = "192.168.1.3";
        String newHost = "192.168.1.4";
        int editLogPort = 9010;

        // Create a separate NodeMgr for exception testing
        NodeMgr exceptionNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "exception_master", Pair.create("1", 9010));
        
        // Add frontend first
        exceptionNodeMgr.addFrontend(role, originalHost, editLogPort);
        Assertions.assertEquals(1, exceptionNodeMgr.getAllFrontends().size());

        // 2. Mock EditLog.logUpdateFrontend to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logUpdateFrontend(any(UpdateFrontendInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute modifyFrontendHost operation and expect exception
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause(originalHost, newHost);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionNodeMgr.modifyFrontendHost(clause);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionNodeMgr.getAllFrontends().size());
        
        // Verify frontend host remains unchanged
        List<Frontend> frontends = exceptionNodeMgr.getAllFrontends();
        Frontend frontend = frontends.get(0);
        Assertions.assertEquals(originalHost, frontend.getHost());
    }

    @Test
    public void testModifyFrontendHostNotExist() throws Exception {
        // 1. Test modifying non-existent frontend
        String nonExistentHost = "192.168.1.500";
        String newHost = "192.168.1.600";

        // 2. Verify initial state
        Assertions.assertEquals(0, masterNodeMgr.getAllFrontends().size());

        // 3. Execute modifyFrontendHost operation and expect DdlException
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause(nonExistentHost, newHost);
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterNodeMgr.modifyFrontendHost(clause);
        });
        Assertions.assertTrue(exception.getMessage().contains("frontend [192.168.1.500] not found"));

        // 4. Verify state remains unchanged
        Assertions.assertEquals(0, masterNodeMgr.getAllFrontends().size());
    }

    @Test
    public void testModifyFrontendHostDuplicateHost() throws Exception {
        // 1. Add two frontends
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        masterNodeMgr.addFrontend(FrontendNodeType.OBSERVER, "192.168.1.101", 9011);
        Assertions.assertEquals(2, masterNodeMgr.getAllFrontends().size());

        // 2. Try to modify first frontend to use the same host as second frontend
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("192.168.1.100", "192.168.1.101");

        // 3. Expect DdlException to be thrown
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterNodeMgr.modifyFrontendHost(clause);
        });
        Assertions.assertTrue(exception.getMessage().contains("frontend with host [192.168.1.101] already exists"));

        // 4. Verify state remains unchanged
        Assertions.assertEquals(2, masterNodeMgr.getAllFrontends().size());
        List<Frontend> frontends = masterNodeMgr.getAllFrontends();
        HashSet<String> hostSet = new HashSet<>();
        hostSet.add(frontends.get(0).getHost());
        hostSet.add(frontends.get(1).getHost());
        Assertions.assertTrue(hostSet.contains("192.168.1.100"));
        Assertions.assertTrue(hostSet.contains("192.168.1.101"));
    }

    @Test
    public void testModifyFrontendHostMultipleModifications() throws Exception {
        // 1. Add frontend
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());

        // 2. First modification
        ModifyFrontendAddressClause clause1 = new ModifyFrontendAddressClause("192.168.1.100", "192.168.1.200");
        masterNodeMgr.modifyFrontendHost(clause1);
        
        List<Frontend> frontends = masterNodeMgr.getAllFrontends();
        Assertions.assertEquals("192.168.1.200", frontends.get(0).getHost());

        // 3. Second modification
        ModifyFrontendAddressClause clause2 = new ModifyFrontendAddressClause("192.168.1.200", "192.168.1.300");
        masterNodeMgr.modifyFrontendHost(clause2);
        
        frontends = masterNodeMgr.getAllFrontends();
        Assertions.assertEquals("192.168.1.300", frontends.get(0).getHost());
        Assertions.assertEquals(1, frontends.size());
    }

    // ==================== Reset Frontends Tests ====================

    @Test
    public void testResetFrontendsNormalCase() throws Exception {
        // 1. Prepare test data and add multiple frontends first
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        masterNodeMgr.addFrontend(FrontendNodeType.OBSERVER, "192.168.1.101", 9011);
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.102", 9012);
        Assertions.assertEquals(3, masterNodeMgr.getAllFrontends().size());

        // 2. Verify initial state
        List<Frontend> frontends = masterNodeMgr.getAllFrontends();
        Assertions.assertEquals(3, frontends.size());
        
        List<Frontend> followerFrontends = masterNodeMgr.getFrontends(FrontendNodeType.FOLLOWER);
        List<Frontend> observerFrontends = masterNodeMgr.getFrontends(FrontendNodeType.OBSERVER);
        Assertions.assertEquals(2, followerFrontends.size());
        Assertions.assertEquals(1, observerFrontends.size());

        // 3. Execute resetFrontends operation (master side)
        masterNodeMgr.resetFrontends();

        // 4. Verify master state after reset
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());
        
        List<Frontend> resetFrontends = masterNodeMgr.getAllFrontends();
        Frontend resetFrontend = resetFrontends.get(0);
        
        // Verify the reset frontend is the self node
        Assertions.assertEquals(FrontendNodeType.FOLLOWER, resetFrontend.getRole());
        Assertions.assertEquals("master", resetFrontend.getNodeName());
        Assertions.assertEquals("192.168.1.2", resetFrontend.getHost());
        Assertions.assertEquals(9010, resetFrontend.getEditLogPort());
        Assertions.assertTrue(resetFrontend.getFid() > 0);
        Assertions.assertTrue(resetFrontend.isAlive());

        // Verify all other frontends are removed
        List<Frontend> followerFrontendsAfterReset = masterNodeMgr.getFrontends(FrontendNodeType.FOLLOWER);
        List<Frontend> observerFrontendsAfterReset = masterNodeMgr.getFrontends(FrontendNodeType.OBSERVER);
        Assertions.assertEquals(1, followerFrontendsAfterReset.size());
        Assertions.assertEquals(0, observerFrontendsAfterReset.size());
        
        // Verify the remaining frontend is the reset one
        Assertions.assertEquals(resetFrontend.getNodeName(), followerFrontendsAfterReset.get(0).getNodeName());

        // 5. Test follower replay functionality
        NodeMgr followerNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "follower", Pair.create("192.168.1.3", 9010));
        
        // First add the same frontends to follower
        for (Frontend fe : frontends) {
            followerNodeMgr.replayAddFrontend(fe);
        }
        Assertions.assertEquals(3, followerNodeMgr.getAllFrontends().size());

        Frontend replayFrontend = (Frontend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESET_FRONTENDS);
        
        // Execute follower replay
        followerNodeMgr.applyResetFrontends(replayFrontend);

        // 6. Verify follower state is consistent with master
        Assertions.assertEquals(1, followerNodeMgr.getAllFrontends().size());
        
        List<Frontend> followerResetFrontends = followerNodeMgr.getAllFrontends();
        Frontend followerResetFrontend = followerResetFrontends.get(0);
        
        Assertions.assertEquals(FrontendNodeType.FOLLOWER, followerResetFrontend.getRole());
        Assertions.assertEquals("master", followerResetFrontend.getNodeName());
        Assertions.assertEquals("192.168.1.2", followerResetFrontend.getHost());
        Assertions.assertEquals(9010, followerResetFrontend.getEditLogPort());
        Assertions.assertEquals(resetFrontend.getFid(), followerResetFrontend.getFid());
        Assertions.assertTrue(followerResetFrontend.isAlive());

        // Verify all other frontends are removed in follower too
        List<Frontend> followerFollowerFrontends = followerNodeMgr.getFrontends(FrontendNodeType.FOLLOWER);
        List<Frontend> followerObserverFrontends = followerNodeMgr.getFrontends(FrontendNodeType.OBSERVER);
        Assertions.assertEquals(1, followerFollowerFrontends.size());
        Assertions.assertEquals(0, followerObserverFrontends.size());
    }

    @Test
    public void testResetFrontendsEditLogException() throws Exception {
        // 1. Prepare test data and add frontends first
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        masterNodeMgr.addFrontend(FrontendNodeType.OBSERVER, "192.168.1.101", 9011);
        Assertions.assertEquals(2, masterNodeMgr.getAllFrontends().size());

        // 2. Create a separate NodeMgr for exception testing
        NodeMgr exceptionNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "exception_master", Pair.create("192.168.1.2", 9010));
        
        // Add frontends to exception NodeMgr
        exceptionNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        exceptionNodeMgr.addFrontend(FrontendNodeType.OBSERVER, "192.168.1.101", 9011);
        Assertions.assertEquals(2, exceptionNodeMgr.getAllFrontends().size());

        // 3. Mock EditLog.logResetFrontends to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logResetFrontends(any(Frontend.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute resetFrontends operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionNodeMgr.resetFrontends();
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(2, exceptionNodeMgr.getAllFrontends().size());
        
        // Verify frontends still exist
        List<Frontend> frontends = exceptionNodeMgr.getAllFrontends();
        Assertions.assertEquals(2, frontends.size());
        
        List<Frontend> followerFrontends = exceptionNodeMgr.getFrontends(FrontendNodeType.FOLLOWER);
        List<Frontend> observerFrontends = exceptionNodeMgr.getFrontends(FrontendNodeType.OBSERVER);
        Assertions.assertEquals(1, followerFrontends.size());
        Assertions.assertEquals(1, observerFrontends.size());
    }

    @Test
    public void testResetFrontendsEmptyFrontends() throws Exception {
        // 1. Verify initial state (no frontends)
        Assertions.assertEquals(0, masterNodeMgr.getAllFrontends().size());

        // 2. Execute resetFrontends operation
        masterNodeMgr.resetFrontends();

        // 3. Verify state after reset
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());
        
        List<Frontend> resetFrontends = masterNodeMgr.getAllFrontends();
        Frontend resetFrontend = resetFrontends.get(0);
        
        // Verify the reset frontend is the self node
        Assertions.assertEquals(FrontendNodeType.FOLLOWER, resetFrontend.getRole());
        Assertions.assertEquals("master", resetFrontend.getNodeName());
        Assertions.assertEquals("192.168.1.2", resetFrontend.getHost());
        Assertions.assertEquals(9010, resetFrontend.getEditLogPort());
        Assertions.assertTrue(resetFrontend.getFid() > 0);
        Assertions.assertTrue(resetFrontend.isAlive());

        // 4. Test follower replay functionality
        NodeMgr followerNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "follower", Pair.create("192.168.1.3", 9010));
        Assertions.assertEquals(0, followerNodeMgr.getAllFrontends().size());

        Frontend replayFrontend = (Frontend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESET_FRONTENDS);
        
        // Execute follower replay
        followerNodeMgr.applyResetFrontends(replayFrontend);

        // 5. Verify follower state is consistent with master
        Assertions.assertEquals(1, followerNodeMgr.getAllFrontends().size());
        
        List<Frontend> followerResetFrontends = followerNodeMgr.getAllFrontends();
        Frontend followerResetFrontend = followerResetFrontends.get(0);
        
        Assertions.assertEquals(FrontendNodeType.FOLLOWER, followerResetFrontend.getRole());
        Assertions.assertEquals("master", followerResetFrontend.getNodeName());
        Assertions.assertEquals("192.168.1.2", followerResetFrontend.getHost());
        Assertions.assertEquals(9010, followerResetFrontend.getEditLogPort());
        Assertions.assertEquals(resetFrontend.getFid(), followerResetFrontend.getFid());
        Assertions.assertTrue(followerResetFrontend.isAlive());
    }

    @Test
    public void testResetFrontendsMultipleResets() throws Exception {
        // 1. Add frontends
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        masterNodeMgr.addFrontend(FrontendNodeType.OBSERVER, "192.168.1.101", 9011);
        Assertions.assertEquals(2, masterNodeMgr.getAllFrontends().size());

        // 2. First reset
        masterNodeMgr.resetFrontends();
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());
        
        List<Frontend> firstResetFrontends = masterNodeMgr.getAllFrontends();
        Frontend firstResetFrontend = firstResetFrontends.get(0);
        Assertions.assertEquals("master", firstResetFrontend.getNodeName());

        // 3. Add more frontends after first reset
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.200", 9020);
        masterNodeMgr.addFrontend(FrontendNodeType.OBSERVER, "192.168.1.201", 9021);
        Assertions.assertEquals(3, masterNodeMgr.getAllFrontends().size());

        // 4. Second reset
        masterNodeMgr.resetFrontends();
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());
        
        List<Frontend> secondResetFrontends = masterNodeMgr.getAllFrontends();
        Frontend secondResetFrontend = secondResetFrontends.get(0);
        Assertions.assertEquals("master", secondResetFrontend.getNodeName());
        Assertions.assertEquals("192.168.1.2", secondResetFrontend.getHost());
        Assertions.assertEquals(9010, secondResetFrontend.getEditLogPort());
        
        // Verify the second reset frontend has a different ID (new allocation)
        Assertions.assertNotEquals(firstResetFrontend.getFid(), secondResetFrontend.getFid());
    }

    @Test
    public void testResetFrontendsReplayMultipleResets() throws Exception {
        // 1. Add frontends to master
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.100", 9010);
        masterNodeMgr.addFrontend(FrontendNodeType.OBSERVER, "192.168.1.101", 9011);
        Assertions.assertEquals(2, masterNodeMgr.getAllFrontends().size());

        // 2. First reset
        masterNodeMgr.resetFrontends();
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());

        // 3. Add more frontends and second reset
        masterNodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.200", 9020);
        masterNodeMgr.resetFrontends();
        Assertions.assertEquals(1, masterNodeMgr.getAllFrontends().size());

        // 4. Create follower NodeMgr
        NodeMgr followerNodeMgr = new NodeMgr(FrontendNodeType.FOLLOWER, "follower", Pair.create("192.168.1.3", 9010));
        Assertions.assertEquals(0, followerNodeMgr.getAllFrontends().size());

        // 5. Replay all operations (2 adds + 2 resets)
        for (int i = 0; i < 2; i++) {
            Frontend replayFrontend = (Frontend) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_FRONTEND_V2);
            followerNodeMgr.replayAddFrontend(replayFrontend);
        }
        Assertions.assertEquals(2, followerNodeMgr.getAllFrontends().size());

        // Replay first reset
        Frontend firstResetReplayFrontend = (Frontend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESET_FRONTENDS);
        followerNodeMgr.applyResetFrontends(firstResetReplayFrontend);
        Assertions.assertEquals(1, followerNodeMgr.getAllFrontends().size());

        // Replay third add
        Frontend thirdAddReplayFrontend = (Frontend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_FRONTEND_V2);
        followerNodeMgr.replayAddFrontend(thirdAddReplayFrontend);
        Assertions.assertEquals(2, followerNodeMgr.getAllFrontends().size());

        // Replay second reset
        Frontend secondResetReplayFrontend = (Frontend) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESET_FRONTENDS);
        followerNodeMgr.applyResetFrontends(secondResetReplayFrontend);
        Assertions.assertEquals(1, followerNodeMgr.getAllFrontends().size());

        // 6. Verify follower state matches master
        List<Frontend> masterFrontends = masterNodeMgr.getAllFrontends();
        List<Frontend> followerFrontends = followerNodeMgr.getAllFrontends();
        
        Assertions.assertEquals(1, masterFrontends.size());
        Assertions.assertEquals(1, followerFrontends.size());
        
        Frontend masterFrontend = masterFrontends.get(0);
        Frontend followerFrontend = followerFrontends.get(0);
        
        Assertions.assertEquals(masterFrontend.getRole(), followerFrontend.getRole());
        Assertions.assertEquals(masterFrontend.getNodeName(), followerFrontend.getNodeName());
        Assertions.assertEquals(masterFrontend.getHost(), followerFrontend.getHost());
        Assertions.assertEquals(masterFrontend.getEditLogPort(), followerFrontend.getEditLogPort());
        Assertions.assertEquals(masterFrontend.getFid(), followerFrontend.getFid());
    }
}

