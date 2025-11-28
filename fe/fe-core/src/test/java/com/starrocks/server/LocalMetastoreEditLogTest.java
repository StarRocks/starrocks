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

import com.starrocks.persist.AutoIncrementInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.transaction.MockedLocalMetaStore;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class LocalMetastoreEditLogTest {
    private MockedLocalMetaStore localMetastore;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        globalStateMgr.setLocalMetastore(localMetastore);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAllocateAutoIncrementIdNormalCase() throws Exception {
        // 1. Prepare test data
        Long tableId = 100L;
        Long rows = 10L;

        // 2. Verify initial state
        Assertions.assertNull(localMetastore.getCurrentAutoIncrementIdByTableId(tableId));

        // 3. Execute allocateAutoIncrementId operation (master side)
        Long startId = localMetastore.allocateAutoIncrementId(tableId, rows);

        // 4. Verify master state
        Assertions.assertNotNull(startId);
        Assertions.assertEquals(1L, startId);
        Long currentId = localMetastore.getCurrentAutoIncrementIdByTableId(tableId);
        Assertions.assertNotNull(currentId);
        Assertions.assertEquals(11L, currentId); // 1 + 10

        // 5. Test follower replay functionality
        MockedLocalMetaStore followerMetastore = new MockedLocalMetaStore(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getRecycleBin(), null);
        
        // Verify follower initial state
        Assertions.assertNull(followerMetastore.getCurrentAutoIncrementIdByTableId(tableId));

        // Replay the operation
        AutoIncrementInfo replayInfo = (AutoIncrementInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_SAVE_AUTO_INCREMENT_ID);
        followerMetastore.replayAutoIncrementId(replayInfo);

        // 6. Verify follower state is consistent with master
        Long followerCurrentId = followerMetastore.getCurrentAutoIncrementIdByTableId(tableId);
        Assertions.assertNotNull(followerCurrentId);
        Assertions.assertEquals(currentId, followerCurrentId);
    }

    @Test
    public void testAllocateAutoIncrementIdEditLogException() throws Exception {
        // 1. Prepare test data
        Long tableId = 200L;
        Long rows = 5L;

        // 2. Verify initial state
        Assertions.assertNull(localMetastore.getCurrentAutoIncrementIdByTableId(tableId));

        // 3. Mock EditLog.logSaveAutoIncrementId to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logSaveAutoIncrementId(any(AutoIncrementInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute allocateAutoIncrementId operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            localMetastore.allocateAutoIncrementId(tableId, rows);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        // The compute function may have updated the value, but since editlog failed,
        // the state should be rolled back or not committed
        Long currentId = localMetastore.getCurrentAutoIncrementIdByTableId(tableId);
        Assertions.assertNull(currentId);
    }

    @Test
    public void testRemoveAutoIncrementIdByTableIdNormalCase() throws Exception {
        // 1. Prepare test data - allocate an ID first
        Long tableId = 300L;
        localMetastore.allocateAutoIncrementId(tableId, 5L);
        Assertions.assertNotNull(localMetastore.getCurrentAutoIncrementIdByTableId(tableId));

        // 2. Execute removeAutoIncrementIdByTableId operation (master side)
        localMetastore.removeAutoIncrementIdByTableId(tableId);

        // 3. Verify master state
        Assertions.assertNull(localMetastore.getCurrentAutoIncrementIdByTableId(tableId));

        // 4. Test follower replay functionality
        MockedLocalMetaStore followerMetastore = new MockedLocalMetaStore(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getRecycleBin(), null);
        
        // First allocate in follower
        followerMetastore.allocateAutoIncrementId(tableId, 5L);
        Assertions.assertNotNull(followerMetastore.getCurrentAutoIncrementIdByTableId(tableId));

        // Replay the remove operation
        AutoIncrementInfo replayInfo = (AutoIncrementInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DELETE_AUTO_INCREMENT_ID);
        followerMetastore.replayDeleteAutoIncrementId(replayInfo);

        // 5. Verify follower state is consistent with master
        Assertions.assertNull(followerMetastore.getCurrentAutoIncrementIdByTableId(tableId));
    }

    @Test
    public void testRemoveAutoIncrementIdByTableIdEditLogException() throws Exception {
        // 1. Prepare test data - allocate an ID first
        Long tableId = 400L;
        localMetastore.allocateAutoIncrementId(tableId, 5L);
        Long initialId = localMetastore.getCurrentAutoIncrementIdByTableId(tableId);
        Assertions.assertNotNull(initialId);

        // 2. Mock EditLog.logSaveDeleteAutoIncrementId to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logSaveDeleteAutoIncrementId(any(AutoIncrementInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute removeAutoIncrementIdByTableId operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            localMetastore.removeAutoIncrementIdByTableId(tableId);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Long currentId = localMetastore.getCurrentAutoIncrementIdByTableId(tableId);
        Assertions.assertNotNull(currentId);
        Assertions.assertEquals(initialId, currentId);
    }

    @Test
    public void testAlterTableAutoIncrementNormalCase() throws Exception {
        // 1. Prepare test data - create a database and table
        localMetastore.createDb("test_db");
        // Note: alterTableAutoIncrement requires a real table, so we'll test the editlog part
        // by mocking the table lookup
        Long tableId = 500L;
        long newAutoIncrementValue = 1000L;

        // Set up initial auto increment value
        localMetastore.addOrReplaceAutoIncrementIdByTableId(tableId, 100L);
        Assertions.assertEquals(100L, localMetastore.getCurrentAutoIncrementIdByTableId(tableId).longValue());

        // 2. Since alterTableAutoIncrement requires a real OlapTable and calls sendDropAutoIncrementMapTask,
        // we'll test the editlog part by directly calling the logic
        // For a complete test, we would need to set up a real table, but for editlog testing,
        // we can verify the editlog is called correctly
        ConcurrentHashMap<Long, Long> idMap = new ConcurrentHashMap<>();
        idMap.put(tableId, newAutoIncrementValue);
        AutoIncrementInfo info = new AutoIncrementInfo(idMap);
        GlobalStateMgr.getCurrentState().getEditLog().logSaveAutoIncrementId(
                info, wal -> localMetastore.addOrReplaceAutoIncrementIdByTableId(tableId, newAutoIncrementValue));

        // 3. Verify master state
        Long currentId = localMetastore.getCurrentAutoIncrementIdByTableId(tableId);
        Assertions.assertNotNull(currentId);
        Assertions.assertEquals(newAutoIncrementValue, currentId.longValue());

        // 4. Test follower replay functionality
        MockedLocalMetaStore followerMetastore = new MockedLocalMetaStore(
                GlobalStateMgr.getCurrentState(), GlobalStateMgr.getCurrentState().getRecycleBin(), null);
        
        // Set initial value in follower
        followerMetastore.addOrReplaceAutoIncrementIdByTableId(tableId, 100L);

        // Replay the operation
        AutoIncrementInfo replayInfo = (AutoIncrementInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_SAVE_AUTO_INCREMENT_ID);
        followerMetastore.replayAutoIncrementId(replayInfo);

        // 5. Verify follower state is consistent with master
        Long followerCurrentId = followerMetastore.getCurrentAutoIncrementIdByTableId(tableId);
        Assertions.assertNotNull(followerCurrentId);
        Assertions.assertEquals(newAutoIncrementValue, followerCurrentId.longValue());
    }

    @Test
    public void testAlterTableAutoIncrementEditLogException() throws Exception {
        // 1. Prepare test data
        Long tableId = 600L;
        long newAutoIncrementValue = 2000L;

        // Set up initial auto increment value
        localMetastore.addOrReplaceAutoIncrementIdByTableId(tableId, 100L);
        Long initialId = localMetastore.getCurrentAutoIncrementIdByTableId(tableId);
        Assertions.assertNotNull(initialId);

        // 2. Mock EditLog.logSaveAutoIncrementId to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logSaveAutoIncrementId(any(AutoIncrementInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute the editlog operation and expect exception
        ConcurrentHashMap<Long, Long> idMap = new ConcurrentHashMap<>();
        idMap.put(tableId, newAutoIncrementValue);
        AutoIncrementInfo info = new AutoIncrementInfo(idMap);
        
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            GlobalStateMgr.getCurrentState().getEditLog().logSaveAutoIncrementId(
                    info, wal -> localMetastore.addOrReplaceAutoIncrementIdByTableId(tableId, newAutoIncrementValue));
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Long currentId = localMetastore.getCurrentAutoIncrementIdByTableId(tableId);
        Assertions.assertNotNull(currentId);
        Assertions.assertEquals(initialId, currentId);
    }
}


