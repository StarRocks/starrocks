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

import com.google.common.collect.Lists;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.persist.DropDictionaryInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.UpdateDictionaryMgrLog;
import com.starrocks.proto.PProcessDictionaryCacheRequest;
import com.starrocks.proto.PProcessDictionaryCacheResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class DictionaryMgrEditLogTest {
    private DictionaryMgr masterDictionaryMgr;
    private static final String TEST_DICTIONARY_NAME = "test_dictionary";
    private static final String TEST_CATALOG_NAME = "default_catalog";
    private static final String TEST_DB_NAME = "test_db";

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create DictionaryMgr instance
        masterDictionaryMgr = new DictionaryMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private CreateDictionaryStmt createTestDictionaryStmt(String dictionaryName, Map<String, String> properties) {
        List<String> dictionaryKeys = Lists.newArrayList("key1");
        List<String> dictionaryValues = Lists.newArrayList("value1");
        return new CreateDictionaryStmt(
                dictionaryName,
                "test_table",
                dictionaryKeys,
                dictionaryValues,
                properties,
                NodePosition.ZERO);
    }

    // ========== scheduleTasks Tests ==========

    @Test
    public void testScheduleTasksNormalCase() throws Exception {
        // 1. Create a dictionary with refresh interval
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_refresh_interval", "10"); // 10 seconds
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt stmt = createTestDictionaryStmt(TEST_DICTIONARY_NAME, properties);
        
        masterDictionaryMgr.createDictionary(stmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        // 2. Verify initial state
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        Assertions.assertNotNull(dictionary);
        
        // 3. Set next schedulable time to past to trigger scheduling
        // Ensure dictionary is not already in unfinishedRefreshTasks
        masterDictionaryMgr.getUnfinishedRefreshTasks().clear();
        // Set refresh interval to a positive value so it can be scheduled
        dictionary.setNextSchedulableTime(System.currentTimeMillis() - 1000); // Set to past
        
        // 4. Execute scheduleTasks operation (master side)
        masterDictionaryMgr.scheduleTasks();
        
        // 5. Verify master state - dictionary should be in unfinishedRefreshTasks or EditLog was written
        Set<Long> unfinishedTasks = masterDictionaryMgr.getUnfinishedRefreshTasks();
        Assertions.assertTrue(unfinishedTasks.contains(dictionary.getDictionaryId()));
        Assertions.assertTrue(dictionary.getState() == Dictionary.DictionaryState.REFRESHING);
        
        // 6. Test follower replay functionality
        DictionaryMgr followerDictionaryMgr = new DictionaryMgr();

        Dictionary replayDictionary = (Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY);
        followerDictionaryMgr.replayCreateDictionary(replayDictionary);

        // Replay the schedule tasks operation
        UpdateDictionaryMgrLog replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);
        
        // 7. Verify follower state
        Dictionary followerDictionary = followerDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        Assertions.assertTrue(followerDictionary.getState() == Dictionary.DictionaryState.REFRESHING);
    }

    @Test
    public void testScheduleTasksEditLogException() throws Exception {
        // 1. Create a dictionary with refresh interval
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_refresh_interval", "10");
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt stmt = createTestDictionaryStmt(TEST_DICTIONARY_NAME, properties);
        
        masterDictionaryMgr.createDictionary(stmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        // Ensure dictionary is not already in unfinishedRefreshTasks
        masterDictionaryMgr.getUnfinishedRefreshTasks().clear();
        dictionary.setNextSchedulableTime(System.currentTimeMillis() - 1000); // Set to past
        
        // 2. Mock EditLog.logModifyDictionaryMgr to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logModifyDictionaryMgr(any(UpdateDictionaryMgrLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // Save initial state
        Set<Long> initialUnfinishedTasks = masterDictionaryMgr.getUnfinishedRefreshTasks();
        int initialSize = initialUnfinishedTasks.size();

        // 3. Execute scheduleTasks operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            masterDictionaryMgr.scheduleTasks();
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Set<Long> currentUnfinishedTasks = masterDictionaryMgr.getUnfinishedRefreshTasks();
        Assertions.assertEquals(initialSize, currentUnfinishedTasks.size());
        Assertions.assertFalse(currentUnfinishedTasks.contains(dictionary.getDictionaryId()));
    }

    // ========== createDictionary Tests ==========

    @Test
    public void testCreateDictionaryNormalCase() throws Exception {
        // 1. Prepare test data
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt stmt = createTestDictionaryStmt(TEST_DICTIONARY_NAME, properties);
        
        // 2. Verify initial state
        Assertions.assertFalse(masterDictionaryMgr.isExist(TEST_DICTIONARY_NAME));
        Assertions.assertEquals(0, masterDictionaryMgr.getDictionariesMapById().size());
        
        // 3. Execute createDictionary operation (master side)
        masterDictionaryMgr.createDictionary(stmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        // 4. Verify master state
        Assertions.assertTrue(masterDictionaryMgr.isExist(TEST_DICTIONARY_NAME));
        Assertions.assertEquals(1, masterDictionaryMgr.getDictionariesMapById().size());
        
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        Assertions.assertNotNull(dictionary);
        Assertions.assertEquals(TEST_DICTIONARY_NAME, dictionary.getDictionaryName());
        Assertions.assertEquals(TEST_CATALOG_NAME, dictionary.getCatalogName());
        Assertions.assertEquals(TEST_DB_NAME, dictionary.getDbName());
        
        // 5. Test follower replay functionality
        DictionaryMgr followerDictionaryMgr = new DictionaryMgr();
        
        // Verify follower initial state
        Assertions.assertFalse(followerDictionaryMgr.isExist(TEST_DICTIONARY_NAME));
        
        // Replay the operation
        Dictionary replayDictionary = (Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY);
        followerDictionaryMgr.replayCreateDictionary(replayDictionary);
        
        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerDictionaryMgr.isExist(TEST_DICTIONARY_NAME));
        Dictionary followerDictionary = followerDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        Assertions.assertNotNull(followerDictionary);
        Assertions.assertEquals(TEST_DICTIONARY_NAME, followerDictionary.getDictionaryName());
    }

    @Test
    public void testCreateDictionaryEditLogException() throws Exception {
        // 1. Prepare test data
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt stmt = createTestDictionaryStmt("exception_dictionary", properties);
        
        // 2. Mock EditLog.logCreateDictionary to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateDictionary(any(Dictionary.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // Verify initial state
        Assertions.assertFalse(masterDictionaryMgr.isExist("exception_dictionary"));
        Assertions.assertEquals(0, masterDictionaryMgr.getDictionariesMapById().size());
        
        // Save initial state snapshot
        int initialDictionaryCount = masterDictionaryMgr.getDictionariesMapById().size();
        
        // 3. Execute createDictionary operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            masterDictionaryMgr.createDictionary(stmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertFalse(masterDictionaryMgr.isExist("exception_dictionary"));
        Assertions.assertEquals(initialDictionaryCount, masterDictionaryMgr.getDictionariesMapById().size());
    }

    // ========== dropDictionary Tests ==========

    @Test
    public void testDropDictionaryNormalCase() throws Exception {
        // 1. Prepare test data - create a dictionary first
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt(TEST_DICTIONARY_NAME, properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Assertions.assertTrue(masterDictionaryMgr.isExist(TEST_DICTIONARY_NAME));
        
        // 2. Execute dropDictionary operation (master side)
        masterDictionaryMgr.dropDictionary(TEST_DICTIONARY_NAME, false);
        
        // 3. Verify master state
        Assertions.assertFalse(masterDictionaryMgr.isExist(TEST_DICTIONARY_NAME));
        Assertions.assertEquals(0, masterDictionaryMgr.getDictionariesMapById().size());
        
        // 4. Test follower replay functionality
        DictionaryMgr followerDictionaryMgr = new DictionaryMgr();
        
        // First create the dictionary in follower
        Dictionary replayDictionary = new Dictionary(
                1L,
                TEST_DICTIONARY_NAME,
                "test_table",
                TEST_CATALOG_NAME,
                TEST_DB_NAME,
                Lists.newArrayList("key1"),
                Lists.newArrayList("value1"),
                properties);
        replayDictionary.buildDictionaryProperties();
        followerDictionaryMgr.replayCreateDictionary(replayDictionary);
        Assertions.assertTrue(followerDictionaryMgr.isExist(TEST_DICTIONARY_NAME));
        
        // Replay the drop operation
        DropDictionaryInfo replayInfo = (DropDictionaryInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_DICTIONARY);
        // Use reflection to access package-private getDictionaryName() method
        Method getDictionaryNameMethod = DropDictionaryInfo.class.getDeclaredMethod("getDictionaryName");
        getDictionaryNameMethod.setAccessible(true);
        String dictionaryName = (String) getDictionaryNameMethod.invoke(replayInfo);
        followerDictionaryMgr.replayDropDictionary(dictionaryName);
        
        // 5. Verify follower state is consistent with master
        Assertions.assertFalse(followerDictionaryMgr.isExist(TEST_DICTIONARY_NAME));
    }

    @Test
    public void testDropDictionaryEditLogException() throws Exception {
        // 1. Prepare test data - create a dictionary first
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt("drop_exception_dictionary", properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Assertions.assertTrue(masterDictionaryMgr.isExist("drop_exception_dictionary"));
        
        // 2. Mock EditLog.logDropDictionary to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropDictionary(any(DropDictionaryInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // Save initial state snapshot
        Assertions.assertTrue(masterDictionaryMgr.isExist("drop_exception_dictionary"));
        int initialDictionaryCount = masterDictionaryMgr.getDictionariesMapById().size();
        
        // 3. Execute dropDictionary operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            masterDictionaryMgr.dropDictionary("drop_exception_dictionary", false);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(masterDictionaryMgr.isExist("drop_exception_dictionary"));
        Assertions.assertEquals(initialDictionaryCount, masterDictionaryMgr.getDictionariesMapById().size());
    }

    @Test
    public void testDropDictionaryCacheOnly() throws Exception {
        // 1. Prepare test data - create a dictionary first
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt(TEST_DICTIONARY_NAME, properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Assertions.assertTrue(masterDictionaryMgr.isExist(TEST_DICTIONARY_NAME));
        
        // 2. Execute dropDictionary with cacheOnly=true (should not write EditLog)
        masterDictionaryMgr.dropDictionary(TEST_DICTIONARY_NAME, true);
        
        // 3. Verify dictionary still exists (only cache cleared)
        Assertions.assertTrue(masterDictionaryMgr.isExist(TEST_DICTIONARY_NAME));
    }

    // ========== refreshDictionary Tests ==========

    @Test
    public void testRefreshDictionaryNormalCase() throws Exception {
        // 1. Prepare test data - create a dictionary first
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt(TEST_DICTIONARY_NAME, properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        Assertions.assertNotNull(dictionary);
        
        // 2. Execute refreshDictionary operation (master side)
        masterDictionaryMgr.refreshDictionary(TEST_DICTIONARY_NAME);
        
        // 3. Verify master state
        Set<Long> unfinishedTasks = masterDictionaryMgr.getUnfinishedRefreshTasks();
        Assertions.assertTrue(unfinishedTasks.contains(dictionary.getDictionaryId()));
        Assertions.assertEquals(Dictionary.DictionaryState.REFRESHING, dictionary.getState());

        // 4. Test follower replay functionality
        DictionaryMgr followerDictionaryMgr = new DictionaryMgr();

        Dictionary replayDictionary = (Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY);
        followerDictionaryMgr.replayCreateDictionary(replayDictionary);

        // Replay the refresh operation
        UpdateDictionaryMgrLog replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);
        
        // 5. Verify follower state
        Dictionary followerDictionary = followerDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        Assertions.assertNotNull(followerDictionary);
        Assertions.assertEquals(Dictionary.DictionaryState.REFRESHING, followerDictionary.getState());
    }

    @Test
    public void testRefreshDictionaryEditLogException() throws Exception {
        // 1. Prepare test data - create a dictionary first
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt("refresh_exception_dictionary", properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName("refresh_exception_dictionary");
        Assertions.assertNotNull(dictionary);
        
        // 2. Mock EditLog.logModifyDictionaryMgr to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logModifyDictionaryMgr(any(UpdateDictionaryMgrLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // Save initial state
        Set<Long> initialUnfinishedTasks = masterDictionaryMgr.getUnfinishedRefreshTasks();
        int initialSize = initialUnfinishedTasks.size();
        
        // 3. Execute refreshDictionary operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            masterDictionaryMgr.refreshDictionary("refresh_exception_dictionary");
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 4. Verify leader memory state remains unchanged after exception
        Set<Long> currentUnfinishedTasks = masterDictionaryMgr.getUnfinishedRefreshTasks();
        Assertions.assertEquals(initialSize, currentUnfinishedTasks.size());
        Assertions.assertFalse(currentUnfinishedTasks.contains(dictionary.getDictionaryId()));
        Assertions.assertEquals(Dictionary.DictionaryState.UNINITIALIZED, dictionary.getState());
    }

    @Test
    public void testRefreshDictionaryNonExistent() throws Exception {
        // 1. Test refreshing non-existent dictionary
        String nonExistentName = "non_existent_dictionary";
        
        // 2. Verify initial state
        Assertions.assertFalse(masterDictionaryMgr.isExist(nonExistentName));
        
        // 3. Execute refreshDictionary operation and expect MetaNotFoundException
        MetaNotFoundException exception = Assertions.assertThrows(MetaNotFoundException.class, () -> {
            masterDictionaryMgr.refreshDictionary(nonExistentName);
        });
        Assertions.assertTrue(exception.getMessage().contains("refreshed dictionary not found"));
    }

    // ========== getAndIncrementTxnIdUnlocked Tests ==========

    @Test
    public void testGetAndIncrementTxnIdUnlockedNormalCase() throws Exception {
        // 1. Verify initial state
        long initialTxnId = masterDictionaryMgr.getNextTxnId();
        
        // 2. Execute getAndIncrementTxnIdUnlocked operation (master side)
        long returnedTxnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        
        // 3. Verify master state
        Assertions.assertEquals(initialTxnId, returnedTxnId);
        Assertions.assertEquals(initialTxnId + 1, masterDictionaryMgr.getNextTxnId());
        
        // 4. Test follower replay functionality
        DictionaryMgr followerDictionaryMgr = new DictionaryMgr();
        
        // Replay the operation
        UpdateDictionaryMgrLog replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);
        
        // 5. Verify follower state is consistent with master
        Assertions.assertEquals(initialTxnId + 1, followerDictionaryMgr.getNextTxnId());
    }

    @Test
    public void testGetAndIncrementTxnIdUnlockedEditLogException() throws Exception {
        // 1. Mock EditLog.logModifyDictionaryMgr to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logModifyDictionaryMgr(any(UpdateDictionaryMgrLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // Save initial state
        long initialTxnId = masterDictionaryMgr.getNextTxnId();
        
        // 2. Execute getAndIncrementTxnIdUnlocked operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 3. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(initialTxnId, masterDictionaryMgr.getNextTxnId());
    }

    // ========== getAndIncrementDictionaryId Tests ==========

    @Test
    public void testGetAndIncrementDictionaryIdNormalCase() throws Exception {
        // 1. Verify initial state
        long initialDictionaryId = masterDictionaryMgr.getNextDictionaryId();
        
        // 2. Execute getAndIncrementDictionaryId operation (master side)
        // This is a private method, so we test it indirectly through createDictionary
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt stmt = createTestDictionaryStmt("test_dict_id", properties);
        masterDictionaryMgr.createDictionary(stmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        // 3. Verify master state - dictionary ID should be incremented
        Assertions.assertEquals(initialDictionaryId + 1, masterDictionaryMgr.getNextDictionaryId());
        
        // 4. Test follower replay functionality
        DictionaryMgr followerDictionaryMgr = new DictionaryMgr();

        // The dictionary ID increment happens inside createDictionary before the EditLog is written
        // So the ID increment log should be written before the create dictionary log
        // Replay in the correct order: first the ID increment, then the dictionary creation

        // replay the modify log for dictionary ID increment
        UpdateDictionaryMgrLog idIncrementLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        followerDictionaryMgr.replayModifyDictionaryMgr(idIncrementLog);

        // Replay the create dictionary operation
        Dictionary replayDictionary = (Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY);
        followerDictionaryMgr.replayCreateDictionary(replayDictionary);
        
        long followerId = followerDictionaryMgr.getNextDictionaryId();
        Assertions.assertEquals(followerId, initialDictionaryId + 1);
    }

    @Test
    public void testGetAndIncrementDictionaryIdEditLogException() throws Exception {
        // 1. Mock EditLog.logModifyDictionaryMgr to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logModifyDictionaryMgr(any(UpdateDictionaryMgrLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // Save initial state
        long initialDictionaryId = masterDictionaryMgr.getNextDictionaryId();
        
        // 2. Execute createDictionary operation (which calls getAndIncrementDictionaryId) and expect exception
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt stmt = createTestDictionaryStmt("exception_dict_id", properties);
        
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            masterDictionaryMgr.createDictionary(stmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 3. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(initialDictionaryId, masterDictionaryMgr.getNextDictionaryId());
    }

    // ========== RefreshDictionaryCacheWorker.commit Tests ==========

    @Test
    public void testRefreshDictionaryCacheWorkerCommitNormalCase() throws Exception {
        // 1. Create a dictionary first
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt(TEST_DICTIONARY_NAME, properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        Assertions.assertNotNull(dictionary);
        
        // 2. Create a RefreshDictionaryCacheWorker and test commit method using reflection
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        
        // Mock DictionaryMgr static methods to avoid BE call and simulate a successful result
        new MockUp<DictionaryMgr>() {
            @Mock
            public static void fillBackendsOrComputeNodes(List<TNetworkAddress> nodes) {
                // Add a dummy backend node to avoid null beNodes
                nodes.add(new TNetworkAddress("127.0.0.1", 1234));
            }

            @Mock
            public static Pair<Boolean, String> processDictionaryCacheInteranl(PProcessDictionaryCacheRequest request,
                                                                               List<TNetworkAddress> beNodes,
                                                                               List<PProcessDictionaryCacheResult> results) {
                // Simulate successful result
                return Pair.create(false, "");
            }
        };
        
        DictionaryMgr.RefreshDictionaryCacheWorker worker = 
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);
        
        worker.commit();
        Dictionary.DictionaryState state = dictionary.getState();
        Assertions.assertEquals(Dictionary.DictionaryState.COMMITTING, state);

        // 5. Test follower replay functionality
        DictionaryMgr followerDictionaryMgr = new DictionaryMgr();

        // Replay the create dictionary operation
        Dictionary replayDictionary = (Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY);
        followerDictionaryMgr.replayCreateDictionary(replayDictionary);

        UpdateDictionaryMgrLog replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        // Replay the commit operation: first for the txn ID increment
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);

        replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        // Replay the commit operation: second for the commit state change
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);

        // 6. Verify follower state
        Dictionary followerDictionary = followerDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        Assertions.assertEquals(Dictionary.DictionaryState.COMMITTING, followerDictionary.getState());
    }

    @Test
    public void testRefreshDictionaryCacheWorkerCommitEditLogException() throws Exception {
        // 1. Create a dictionary first
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt("commit_exception_dict", properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName("commit_exception_dict");
        Assertions.assertNotNull(dictionary);
        
        // 2. Create a RefreshDictionaryCacheWorker first (before mocking EditLog)
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker = 
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);
        
        // Set error to false so commit will proceed
        java.lang.reflect.Field errorField = DictionaryMgr.RefreshDictionaryCacheWorker.class.getDeclaredField("error");
        errorField.setAccessible(true);
        errorField.setBoolean(worker, false);
        
        // 3. Mock EditLog.logModifyDictionaryMgr to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logModifyDictionaryMgr(any(UpdateDictionaryMgrLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        // Save initial state
        Dictionary.DictionaryState initialState = dictionary.getState();
        
        // 4. Execute commit operation and expect exception
        // Reflection will wrap the exception in InvocationTargetException
        Exception exception = Assertions.assertThrows(Exception.class, worker::commit);
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        
        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(initialState, dictionary.getState());
    }

    // ========== RefreshDictionaryCacheWorker.finish Tests ==========

    @Test
    public void testRefreshDictionaryCacheWorkerFinishNormalCase() throws Exception {
        // 1. Create a dictionary first
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt(TEST_DICTIONARY_NAME, properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        Assertions.assertNotNull(dictionary);
        
        // 2. Create a RefreshDictionaryCacheWorker and test finish method using reflection
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker = 
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);

        worker.setError(false, "");
        worker.finish();

        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, dictionary.getState());
        Assertions.assertEquals(txnId, dictionary.getLastSuccessVersion());

        // 5. Test follower replay functionality
        DictionaryMgr followerDictionaryMgr = new DictionaryMgr();
        Dictionary replayDictionary = (Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY);
        followerDictionaryMgr.replayCreateDictionary(replayDictionary);

        UpdateDictionaryMgrLog replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        // Replay the commit operation: first for the txn ID increment
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);

        replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        // Replay the commit operation: second for the commit state change
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);

        // 6. Verify follower state
        Dictionary followerDictionary = followerDictionaryMgr.getDictionaryByName(TEST_DICTIONARY_NAME);
        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, followerDictionary.getState());
        Assertions.assertEquals(txnId, followerDictionary.getLastSuccessVersion());
    }

    @Test
    public void testRefreshDictionaryCacheWorkerFinishEditLogException() throws Exception {
        // 1. Create a dictionary first
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt("finish_exception_dict", properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName("finish_exception_dict");
        Assertions.assertNotNull(dictionary);
        
        // 2. Create a RefreshDictionaryCacheWorker first (before mocking EditLog)
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker = 
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);
        
        // Set error state
        java.lang.reflect.Field errorField = DictionaryMgr.RefreshDictionaryCacheWorker.class.getDeclaredField("error");
        errorField.setAccessible(true);
        errorField.setBoolean(worker, false);
        
        // 3. Mock EditLog.logModifyDictionaryMgr to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logModifyDictionaryMgr(any(UpdateDictionaryMgrLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // Save initial state
        Dictionary.DictionaryState initialState = dictionary.getState();
        
        // 4. Execute finish operation and expect exception
        // Reflection will wrap the exception in InvocationTargetException
        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            worker.finish();
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        
        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(initialState, dictionary.getState());
    }

    @Test
    public void testRefreshDictionaryCacheWorkerFinishWithIgnoreFailedRefresh() throws Exception {
        // 1. Create a dictionary with ignore_failed_refresh enabled
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        properties.put("dictionary_ignore_failed_refresh", "true");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt("ignore_failed_dict", properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName("ignore_failed_dict");
        Assertions.assertNotNull(dictionary);
        Assertions.assertTrue(dictionary.getIgnoreFailedRefresh());
        
        // 2. Set dictionary state to REFRESHING (simulate a refresh in progress)
        Dictionary.DictionaryState previousState = dictionary.getState();
        dictionary.setRefreshing(System.currentTimeMillis());
        Assertions.assertEquals(Dictionary.DictionaryState.REFRESHING, dictionary.getState());
        
        // 3. Create a RefreshDictionaryCacheWorker and test finish method with error
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker = 
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);

        worker.setError(true, "Test error message");
        worker.finish();

        // 4. Verify dictionary state was reset to previous state
        Assertions.assertEquals(previousState, dictionary.getState());
        // Use reflection to access private runtimeErrMsg field
        String errorMsg = dictionary.getRuntimeErrMsg();
        Assertions.assertTrue(errorMsg.contains("Cancelled and rollback to previous state"));
        Assertions.assertTrue(errorMsg.contains("Test error message"));

        // 5. Test follower replay functionality
        DictionaryMgr followerDictionaryMgr = new DictionaryMgr();
        Dictionary replayDictionary = (Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY);
        followerDictionaryMgr.replayCreateDictionary(replayDictionary);

        UpdateDictionaryMgrLog replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        // Replay the txn ID increment
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);

        // Set follower dictionary to REFRESHING state to match master before finish
        Dictionary followerDictionary = followerDictionaryMgr.getDictionaryByName("ignore_failed_dict");
        Dictionary.DictionaryState followerPreviousState = followerDictionary.getState();
        followerDictionary.setRefreshing(System.currentTimeMillis());

        replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        // Replay the finish operation with reset state
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);

        // 6. Verify follower state
        Assertions.assertEquals(followerPreviousState, followerDictionary.getState());
        String followerErrorMsg = followerDictionary.getRuntimeErrMsg();
        Assertions.assertTrue(followerErrorMsg.contains("Cancelled and rollback to previous state"));
    }

    @Test
    public void testRefreshDictionaryCacheWorkerFinishWithErrorCancelled() throws Exception {
        // 1. Create a dictionary without ignore_failed_refresh (default is false)
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        CreateDictionaryStmt createStmt = createTestDictionaryStmt("cancelled_dict", properties);
        masterDictionaryMgr.createDictionary(createStmt, TEST_CATALOG_NAME, TEST_DB_NAME);
        
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName("cancelled_dict");
        Assertions.assertNotNull(dictionary);
        Assertions.assertFalse(dictionary.getIgnoreFailedRefresh());
        
        // 2. Create a RefreshDictionaryCacheWorker and test finish method with error
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker = 
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);

        String errorMessage = "Test cancellation error";
        worker.setError(true, errorMessage);
        worker.finish();

        // 3. Verify dictionary state was set to CANCELLED
        Assertions.assertEquals(Dictionary.DictionaryState.CANCELLED, dictionary.getState());
        // Use reflection to access private runtimeErrMsg field
        String actualErrorMsg = dictionary.getRuntimeErrMsg();
        Assertions.assertEquals(errorMessage, actualErrorMsg);

        // 4. Test follower replay functionality
        DictionaryMgr followerDictionaryMgr = new DictionaryMgr();
        Dictionary replayDictionary = (Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY);
        followerDictionaryMgr.replayCreateDictionary(replayDictionary);

        UpdateDictionaryMgrLog replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        // Replay the txn ID increment
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);

        replayLog = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        // Replay the finish operation with CANCELLED state
        followerDictionaryMgr.replayModifyDictionaryMgr(replayLog);

        // 5. Verify follower state
        Dictionary followerDictionary = followerDictionaryMgr.getDictionaryByName("cancelled_dict");
        Assertions.assertEquals(Dictionary.DictionaryState.CANCELLED, followerDictionary.getState());
        String followerErrorMsg = followerDictionary.getRuntimeErrMsg();
        Assertions.assertEquals(errorMessage, followerErrorMsg);
    }
}

