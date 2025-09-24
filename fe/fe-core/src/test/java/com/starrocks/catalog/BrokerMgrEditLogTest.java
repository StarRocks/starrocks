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

import com.google.common.collect.ArrayListMultimap;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.persist.DropBrokerLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyBrokerInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class BrokerMgrEditLogTest {
    private BrokerMgr masterBrokerMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create BrokerMgr instance
        masterBrokerMgr = new BrokerMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAddBrokersNormalCase() throws Exception {
        // 1. Prepare test data
        String brokerName = "test_broker";
        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        addresses.add(new Pair<>("192.168.1.100", 8000));
        addresses.add(new Pair<>("192.168.1.101", 8001));

        // 2. Verify initial state
        Assertions.assertFalse(masterBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(0, masterBrokerMgr.getAllBrokers().size());

        // 3. Execute addBrokers operation (master side)
        masterBrokerMgr.addBrokers(brokerName, addresses);

        // 4. Verify master state
        Assertions.assertTrue(masterBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(2, masterBrokerMgr.getAllBrokers().size());
        
        // Verify broker details
        FsBroker broker1 = masterBrokerMgr.getBroker(brokerName, "192.168.1.100", 8000);
        FsBroker broker2 = masterBrokerMgr.getBroker(brokerName, "192.168.1.101", 8001);
        Assertions.assertNotNull(broker1);
        Assertions.assertNotNull(broker2);
        Assertions.assertEquals("192.168.1.100", broker1.ip);
        Assertions.assertEquals(8000, broker1.port);
        Assertions.assertEquals("192.168.1.101", broker2.ip);
        Assertions.assertEquals(8001, broker2.port);

        // Verify internal data structures
        Map<String, ArrayListMultimap<String, FsBroker>> brokersMap = masterBrokerMgr.getBrokersMap();
        Assertions.assertTrue(brokersMap.containsKey(brokerName));
        ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(brokerName);
        Assertions.assertEquals(1, brokerAddrsMap.get("192.168.1.100").size());
        Assertions.assertEquals(1, brokerAddrsMap.get("192.168.1.101").size());

        // 5. Test follower replay functionality
        BrokerMgr followerBrokerMgr = new BrokerMgr();
        
        // Verify follower initial state
        Assertions.assertFalse(followerBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(0, followerBrokerMgr.getAllBrokers().size());

        ModifyBrokerInfo replayInfo = (ModifyBrokerInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_BROKER_V2);
        
        // Execute follower replay
        followerBrokerMgr.replayAddBrokers(replayInfo);

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(2, followerBrokerMgr.getAllBrokers().size());
        
        FsBroker followerBroker1 = followerBrokerMgr.getBroker(brokerName, "192.168.1.100", 8000);
        FsBroker followerBroker2 = followerBrokerMgr.getBroker(brokerName, "192.168.1.101", 8001);
        Assertions.assertNotNull(followerBroker1);
        Assertions.assertNotNull(followerBroker2);
        Assertions.assertEquals("192.168.1.100", followerBroker1.ip);
        Assertions.assertEquals(8000, followerBroker1.port);
        Assertions.assertEquals("192.168.1.101", followerBroker2.ip);
        Assertions.assertEquals(8001, followerBroker2.port);

        // Verify follower internal data structures
        Map<String, ArrayListMultimap<String, FsBroker>> followerBrokersMap = followerBrokerMgr.getBrokersMap();
        Assertions.assertTrue(followerBrokersMap.containsKey(brokerName));
        ArrayListMultimap<String, FsBroker> followerBrokerAddrsMap = followerBrokersMap.get(brokerName);
        Assertions.assertEquals(1, followerBrokerAddrsMap.get("192.168.1.100").size());
        Assertions.assertEquals(1, followerBrokerAddrsMap.get("192.168.1.101").size());
    }

    @Test
    public void testAddBrokersEditLogException() throws Exception {
        // 1. Prepare test data
        String brokerName = "exception_broker";
        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        addresses.add(new Pair<>("192.168.1.200", 8000));

        // 2. Create a separate BrokerMgr for exception testing
        BrokerMgr exceptionBrokerMgr = new BrokerMgr();
        EditLog spyEditLog = spy(new EditLog(null));
        
        // 3. Mock EditLog.logAddBroker to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAddBroker(any(ModifyBrokerInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertFalse(exceptionBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(0, exceptionBrokerMgr.getAllBrokers().size());
        
        // Save initial state snapshot
        Map<String, ArrayListMultimap<String, FsBroker>> initialBrokersMap = exceptionBrokerMgr.getBrokersMap();
        Map<String, List<FsBroker>> initialBrokerListMap = exceptionBrokerMgr.getBrokerListMap();

        // 4. Execute addBrokers operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionBrokerMgr.addBrokers(brokerName, addresses);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertFalse(exceptionBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(0, exceptionBrokerMgr.getAllBrokers().size());
        
        // Verify internal data structures were not modified
        Map<String, ArrayListMultimap<String, FsBroker>> currentBrokersMap = exceptionBrokerMgr.getBrokersMap();
        Map<String, List<FsBroker>> currentBrokerListMap = exceptionBrokerMgr.getBrokerListMap();
        
        Assertions.assertEquals(initialBrokersMap.size(), currentBrokersMap.size());
        Assertions.assertEquals(initialBrokerListMap.size(), currentBrokerListMap.size());
        Assertions.assertFalse(currentBrokersMap.containsKey(brokerName));
        Assertions.assertFalse(currentBrokerListMap.containsKey(brokerName));

        // 6. Verify no broker was accidentally added
        FsBroker broker = exceptionBrokerMgr.getBroker(brokerName, "192.168.1.200", 8000);
        Assertions.assertNull(broker);
    }

    @Test
    public void testAddBrokersDuplicateAddress() throws Exception {
        // 1. First add a broker
        String brokerName = "duplicate_test_broker";
        Collection<Pair<String, Integer>> addresses1 = new ArrayList<>();
        addresses1.add(new Pair<>("192.168.1.150", 8000));
        
        masterBrokerMgr.addBrokers(brokerName, addresses1);
        Assertions.assertTrue(masterBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(1, masterBrokerMgr.getAllBrokers().size());

        // 2. Try to add duplicate address
        Collection<Pair<String, Integer>> addresses2 = new ArrayList<>();
        addresses2.add(new Pair<>("192.168.1.150", 8000)); // Duplicate address
        addresses2.add(new Pair<>("192.168.1.151", 8001)); // New address

        // 3. Expect DdlException to be thrown
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterBrokerMgr.addBrokers(brokerName, addresses2);
        });
        Assertions.assertTrue(exception.getMessage().contains("has already in brokers"));

        // 4. Verify state remains unchanged (only the original broker)
        Assertions.assertEquals(1, masterBrokerMgr.getAllBrokers().size());
        FsBroker existingBroker = masterBrokerMgr.getBroker(brokerName, "192.168.1.150", 8000);
        FsBroker newBroker = masterBrokerMgr.getBroker(brokerName, "192.168.1.151", 8001);
        Assertions.assertNotNull(existingBroker);
        Assertions.assertNull(newBroker); // New broker should not be added
    }

    @Test
    public void testAddBrokersToExistingBrokerName() throws Exception {
        // 1. First add a broker
        String brokerName = "existing_broker";
        Collection<Pair<String, Integer>> addresses1 = new ArrayList<>();
        addresses1.add(new Pair<>("192.168.1.160", 8000));
        
        masterBrokerMgr.addBrokers(brokerName, addresses1);
        Assertions.assertEquals(1, masterBrokerMgr.getAllBrokers().size());

        // 2. Add a different address broker to the same broker name
        Collection<Pair<String, Integer>> addresses2 = new ArrayList<>();
        addresses2.add(new Pair<>("192.168.1.161", 8001));
        
        masterBrokerMgr.addBrokers(brokerName, addresses2);

        // 3. Verify both brokers exist
        Assertions.assertEquals(2, masterBrokerMgr.getAllBrokers().size());
        FsBroker broker1 = masterBrokerMgr.getBroker(brokerName, "192.168.1.160", 8000);
        FsBroker broker2 = masterBrokerMgr.getBroker(brokerName, "192.168.1.161", 8001);
        Assertions.assertNotNull(broker1);
        Assertions.assertNotNull(broker2);

        // 4. Verify they belong to the same broker name
        Map<String, ArrayListMultimap<String, FsBroker>> brokersMap = masterBrokerMgr.getBrokersMap();
        Assertions.assertEquals(1, brokersMap.size());
        Assertions.assertTrue(brokersMap.containsKey(brokerName));
        ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(brokerName);
        Assertions.assertEquals(2, brokerAddrsMap.size()); // Two different IP addresses
    }

    @Test
    public void testDropBrokersNormalCase() throws Exception {
        // 1. Prepare test data and add brokers first
        String brokerName = "drop_test_broker";
        Collection<Pair<String, Integer>> addAddresses = new ArrayList<>();
        addAddresses.add(new Pair<>("192.168.1.200", 8000));
        addAddresses.add(new Pair<>("192.168.1.201", 8001));
        addAddresses.add(new Pair<>("192.168.1.202", 8002));

        // Add brokers first
        masterBrokerMgr.addBrokers(brokerName, addAddresses);

        // 2. Verify initial state after adding
        Assertions.assertTrue(masterBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(3, masterBrokerMgr.getAllBrokers().size());
        
        FsBroker broker1 = masterBrokerMgr.getBroker(brokerName, "192.168.1.200", 8000);
        FsBroker broker2 = masterBrokerMgr.getBroker(brokerName, "192.168.1.201", 8001);
        FsBroker broker3 = masterBrokerMgr.getBroker(brokerName, "192.168.1.202", 8002);
        Assertions.assertNotNull(broker1);
        Assertions.assertNotNull(broker2);
        Assertions.assertNotNull(broker3);

        // 3. Prepare addresses to drop
        Collection<Pair<String, Integer>> dropAddresses = new ArrayList<>();
        dropAddresses.add(new Pair<>("192.168.1.200", 8000)); // Drop first broker
        dropAddresses.add(new Pair<>("192.168.1.202", 8002)); // Drop third broker

        // 4. Execute dropBrokers operation (master side)
        masterBrokerMgr.dropBrokers(brokerName, dropAddresses);

        // 5. Verify master state after dropping
        Assertions.assertTrue(masterBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(1, masterBrokerMgr.getAllBrokers().size());
        
        // Verify dropped brokers are gone
        FsBroker droppedBroker1 = masterBrokerMgr.getBroker(brokerName, "192.168.1.200", 8000);
        FsBroker droppedBroker3 = masterBrokerMgr.getBroker(brokerName, "192.168.1.202", 8002);
        Assertions.assertNull(droppedBroker1);
        Assertions.assertNull(droppedBroker3);
        
        // Verify remaining broker still exists
        FsBroker remainingBroker = masterBrokerMgr.getBroker(brokerName, "192.168.1.201", 8001);
        Assertions.assertNotNull(remainingBroker);
        Assertions.assertEquals("192.168.1.201", remainingBroker.ip);
        Assertions.assertEquals(8001, remainingBroker.port);

        // Verify internal data structures
        Map<String, ArrayListMultimap<String, FsBroker>> brokersMap = masterBrokerMgr.getBrokersMap();
        Assertions.assertTrue(brokersMap.containsKey(brokerName));
        ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(brokerName);
        Assertions.assertEquals(0, brokerAddrsMap.get("192.168.1.200").size());
        Assertions.assertEquals(1, brokerAddrsMap.get("192.168.1.201").size());
        Assertions.assertEquals(0, brokerAddrsMap.get("192.168.1.202").size());

        // 6. Test follower replay functionality
        BrokerMgr followerBrokerMgr = new BrokerMgr();
        
        // First add the same brokers to follower
        followerBrokerMgr.addBrokers(brokerName, addAddresses);
        Assertions.assertEquals(3, followerBrokerMgr.getAllBrokers().size());

        ModifyBrokerInfo replayInfo = (ModifyBrokerInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_BROKER_V2);
        
        // Execute follower replay
        followerBrokerMgr.replayDropBrokers(replayInfo);

        // 7. Verify follower state is consistent with master
        Assertions.assertTrue(followerBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(1, followerBrokerMgr.getAllBrokers().size());
        
        FsBroker followerDroppedBroker1 = followerBrokerMgr.getBroker(brokerName, "192.168.1.200", 8000);
        FsBroker followerDroppedBroker3 = followerBrokerMgr.getBroker(brokerName, "192.168.1.202", 8002);
        Assertions.assertNull(followerDroppedBroker1);
        Assertions.assertNull(followerDroppedBroker3);
        
        FsBroker followerRemainingBroker = followerBrokerMgr.getBroker(brokerName, "192.168.1.201", 8001);
        Assertions.assertNotNull(followerRemainingBroker);
        Assertions.assertEquals("192.168.1.201", followerRemainingBroker.ip);
        Assertions.assertEquals(8001, followerRemainingBroker.port);

        // Verify follower internal data structures
        Map<String, ArrayListMultimap<String, FsBroker>> followerBrokersMap = followerBrokerMgr.getBrokersMap();
        Assertions.assertTrue(followerBrokersMap.containsKey(brokerName));
        ArrayListMultimap<String, FsBroker> followerBrokerAddrsMap = followerBrokersMap.get(brokerName);
        Assertions.assertEquals(0, followerBrokerAddrsMap.get("192.168.1.200").size());
        Assertions.assertEquals(1, followerBrokerAddrsMap.get("192.168.1.201").size());
        Assertions.assertEquals(0, followerBrokerAddrsMap.get("192.168.1.202").size());
    }

    @Test
    public void testDropBrokersEditLogException() throws Exception {
        // 1. Prepare test data and add brokers first
        String brokerName = "drop_exception_broker";
        Collection<Pair<String, Integer>> addAddresses = new ArrayList<>();
        addAddresses.add(new Pair<>("192.168.1.300", 8000));
        addAddresses.add(new Pair<>("192.168.1.301", 8001));

        // Create a separate BrokerMgr for exception testing
        BrokerMgr exceptionBrokerMgr = new BrokerMgr();
        
        // Add brokers first
        exceptionBrokerMgr.addBrokers(brokerName, addAddresses);
        Assertions.assertEquals(2, exceptionBrokerMgr.getAllBrokers().size());

        // 2. Prepare addresses to drop
        Collection<Pair<String, Integer>> dropAddresses = new ArrayList<>();
        dropAddresses.add(new Pair<>("192.168.1.300", 8000));

        // 3. Mock EditLog.logDropBroker to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropBroker(any(ModifyBrokerInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        Map<String, ArrayListMultimap<String, FsBroker>> initialBrokersMap = exceptionBrokerMgr.getBrokersMap();
        Map<String, List<FsBroker>> initialBrokerListMap = exceptionBrokerMgr.getBrokerListMap();

        // 4. Execute dropBrokers operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionBrokerMgr.dropBrokers(brokerName, dropAddresses);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(exceptionBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(2, exceptionBrokerMgr.getAllBrokers().size());
        
        // Verify both brokers still exist
        FsBroker broker1 = exceptionBrokerMgr.getBroker(brokerName, "192.168.1.300", 8000);
        FsBroker broker2 = exceptionBrokerMgr.getBroker(brokerName, "192.168.1.301", 8001);
        Assertions.assertNotNull(broker1);
        Assertions.assertNotNull(broker2);
        
        // Verify internal data structures were not modified
        Map<String, ArrayListMultimap<String, FsBroker>> currentBrokersMap = exceptionBrokerMgr.getBrokersMap();
        Map<String, List<FsBroker>> currentBrokerListMap = exceptionBrokerMgr.getBrokerListMap();
        
        Assertions.assertEquals(initialBrokersMap.size(), currentBrokersMap.size());
        Assertions.assertEquals(initialBrokerListMap.size(), currentBrokerListMap.size());
        Assertions.assertTrue(currentBrokersMap.containsKey(brokerName));
        Assertions.assertTrue(currentBrokerListMap.containsKey(brokerName));
        
        // Verify broker counts remain the same
        ArrayListMultimap<String, FsBroker> brokerAddrsMap = currentBrokersMap.get(brokerName);
        Assertions.assertEquals(1, brokerAddrsMap.get("192.168.1.300").size());
        Assertions.assertEquals(1, brokerAddrsMap.get("192.168.1.301").size());
    }

    @Test
    public void testDropAllBrokerNormalCase() throws Exception {
        // 1. Prepare test data and add brokers first
        String brokerName = "drop_all_test_broker";
        Collection<Pair<String, Integer>> addAddresses = new ArrayList<>();
        addAddresses.add(new Pair<>("192.168.1.400", 8000));
        addAddresses.add(new Pair<>("192.168.1.401", 8001));
        addAddresses.add(new Pair<>("192.168.1.402", 8002));

        // Add brokers first
        masterBrokerMgr.addBrokers(brokerName, addAddresses);

        // 2. Verify initial state after adding
        Assertions.assertTrue(masterBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(3, masterBrokerMgr.getAllBrokers().size());
        
        FsBroker broker1 = masterBrokerMgr.getBroker(brokerName, "192.168.1.400", 8000);
        FsBroker broker2 = masterBrokerMgr.getBroker(brokerName, "192.168.1.401", 8001);
        FsBroker broker3 = masterBrokerMgr.getBroker(brokerName, "192.168.1.402", 8002);
        Assertions.assertNotNull(broker1);
        Assertions.assertNotNull(broker2);
        Assertions.assertNotNull(broker3);

        // 3. Execute dropAllBroker operation (master side)
        masterBrokerMgr.dropAllBroker(brokerName);

        // 4. Verify master state after dropping all brokers
        Assertions.assertFalse(masterBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(0, masterBrokerMgr.getAllBrokers().size());
        
        // Verify all brokers are gone
        FsBroker droppedBroker1 = masterBrokerMgr.getBroker(brokerName, "192.168.1.400", 8000);
        FsBroker droppedBroker2 = masterBrokerMgr.getBroker(brokerName, "192.168.1.401", 8001);
        FsBroker droppedBroker3 = masterBrokerMgr.getBroker(brokerName, "192.168.1.402", 8002);
        Assertions.assertNull(droppedBroker1);
        Assertions.assertNull(droppedBroker2);
        Assertions.assertNull(droppedBroker3);

        // Verify internal data structures
        Map<String, ArrayListMultimap<String, FsBroker>> brokersMap = masterBrokerMgr.getBrokersMap();
        Map<String, List<FsBroker>> brokerListMap = masterBrokerMgr.getBrokerListMap();
        Assertions.assertFalse(brokersMap.containsKey(brokerName));
        Assertions.assertFalse(brokerListMap.containsKey(brokerName));

        // 5. Test follower replay functionality
        BrokerMgr followerBrokerMgr = new BrokerMgr();
        
        // First add the same brokers to follower
        followerBrokerMgr.addBrokers(brokerName, addAddresses);
        Assertions.assertEquals(3, followerBrokerMgr.getAllBrokers().size());

        DropBrokerLog dropBrokerLog = (DropBrokerLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_ALL_BROKER_V2);
        
        // Execute follower replay
        followerBrokerMgr.replayDropAllBroker(dropBrokerLog.getBrokerName());

        // 6. Verify follower state is consistent with master
        Assertions.assertFalse(followerBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(0, followerBrokerMgr.getAllBrokers().size());
        
        FsBroker followerDroppedBroker1 = followerBrokerMgr.getBroker(brokerName, "192.168.1.400", 8000);
        FsBroker followerDroppedBroker2 = followerBrokerMgr.getBroker(brokerName, "192.168.1.401", 8001);
        FsBroker followerDroppedBroker3 = followerBrokerMgr.getBroker(brokerName, "192.168.1.402", 8002);
        Assertions.assertNull(followerDroppedBroker1);
        Assertions.assertNull(followerDroppedBroker2);
        Assertions.assertNull(followerDroppedBroker3);

        // Verify follower internal data structures
        Map<String, ArrayListMultimap<String, FsBroker>> followerBrokersMap = followerBrokerMgr.getBrokersMap();
        Map<String, List<FsBroker>> followerBrokerListMap = followerBrokerMgr.getBrokerListMap();
        Assertions.assertFalse(followerBrokersMap.containsKey(brokerName));
        Assertions.assertFalse(followerBrokerListMap.containsKey(brokerName));
    }

    @Test
    public void testDropAllBrokerEditLogException() throws Exception {
        // 1. Prepare test data and add brokers first
        String brokerName = "drop_all_exception_broker";
        Collection<Pair<String, Integer>> addAddresses = new ArrayList<>();
        addAddresses.add(new Pair<>("192.168.1.500", 8000));
        addAddresses.add(new Pair<>("192.168.1.501", 8001));

        // Create a separate BrokerMgr for exception testing
        BrokerMgr exceptionBrokerMgr = new BrokerMgr();
        
        // Add brokers first
        exceptionBrokerMgr.addBrokers(brokerName, addAddresses);
        Assertions.assertEquals(2, exceptionBrokerMgr.getAllBrokers().size());

        // 2. Mock EditLog.logDropAllBroker to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropAllBroker(any(String.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        Map<String, ArrayListMultimap<String, FsBroker>> initialBrokersMap = exceptionBrokerMgr.getBrokersMap();
        Map<String, List<FsBroker>> initialBrokerListMap = exceptionBrokerMgr.getBrokerListMap();

        // 3. Execute dropAllBroker operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionBrokerMgr.dropAllBroker(brokerName);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(exceptionBrokerMgr.containsBroker(brokerName));
        Assertions.assertEquals(2, exceptionBrokerMgr.getAllBrokers().size());
        
        // Verify both brokers still exist
        FsBroker broker1 = exceptionBrokerMgr.getBroker(brokerName, "192.168.1.500", 8000);
        FsBroker broker2 = exceptionBrokerMgr.getBroker(brokerName, "192.168.1.501", 8001);
        Assertions.assertNotNull(broker1);
        Assertions.assertNotNull(broker2);
        
        // Verify internal data structures were not modified
        Map<String, ArrayListMultimap<String, FsBroker>> currentBrokersMap = exceptionBrokerMgr.getBrokersMap();
        Map<String, List<FsBroker>> currentBrokerListMap = exceptionBrokerMgr.getBrokerListMap();
        
        Assertions.assertEquals(initialBrokersMap.size(), currentBrokersMap.size());
        Assertions.assertEquals(initialBrokerListMap.size(), currentBrokerListMap.size());
        Assertions.assertTrue(currentBrokersMap.containsKey(brokerName));
        Assertions.assertTrue(currentBrokerListMap.containsKey(brokerName));
        
        // Verify broker counts remain the same
        ArrayListMultimap<String, FsBroker> brokerAddrsMap = currentBrokersMap.get(brokerName);
        Assertions.assertEquals(1, brokerAddrsMap.get("192.168.1.500").size());
        Assertions.assertEquals(1, brokerAddrsMap.get("192.168.1.501").size());
    }

    @Test
    public void testDropAllBrokerUnknownBrokerName() throws Exception {
        // 1. Test dropping non-existent broker name
        String unknownBrokerName = "unknown_broker_name";

        // 2. Verify initial state
        Assertions.assertFalse(masterBrokerMgr.containsBroker(unknownBrokerName));
        Assertions.assertEquals(0, masterBrokerMgr.getAllBrokers().size());

        // 3. Execute dropAllBroker operation and expect DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterBrokerMgr.dropAllBroker(unknownBrokerName);
        });
        Assertions.assertTrue(exception.getMessage().contains("Unknown broker name"));

        // 4. Verify state remains unchanged
        Assertions.assertFalse(masterBrokerMgr.containsBroker(unknownBrokerName));
        Assertions.assertEquals(0, masterBrokerMgr.getAllBrokers().size());
    }
}
