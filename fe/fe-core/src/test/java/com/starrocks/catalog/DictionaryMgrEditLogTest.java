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
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.persist.DropDictionaryInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.UpdateDictionaryLog;
import com.starrocks.persist.UpdateDictionaryMgrLog;
import com.starrocks.proto.PProcessDictionaryCacheRequest;
import com.starrocks.proto.PProcessDictionaryCacheRequestType;
import com.starrocks.proto.PProcessDictionaryCacheResult;
import com.starrocks.proto.StatusPB;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class DictionaryMgrEditLogTest {
    private DictionaryMgr masterDictionaryMgr;
    private static final String TEST_DICTIONARY_NAME = "test_dictionary";
    private static final String TEST_CATALOG_NAME = "default_catalog";
    private static final String TEST_DB_NAME = "test_db";
    private boolean oldLeaveOutFailedNodes;
    private int oldResyncMinInterval;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create DictionaryMgr instance
        masterDictionaryMgr = new DictionaryMgr();

        // Both are off by default. Most tests below are about the behavior when they are on.
        oldLeaveOutFailedNodes = Config.dictionary_refresh_leave_out_failed_nodes;
        oldResyncMinInterval = Config.dictionary_node_resync_min_interval_second;
        Config.dictionary_refresh_leave_out_failed_nodes = true;
        Config.dictionary_node_resync_min_interval_second = 30;
    }

    @AfterEach
    public void tearDown() {
        Config.dictionary_refresh_leave_out_failed_nodes = oldLeaveOutFailedNodes;
        Config.dictionary_node_resync_min_interval_second = oldResyncMinInterval;
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
                                                                               List<PProcessDictionaryCacheResult> results,
                                                                               List<TNetworkAddress> failedNodes) {
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

    // ========== A node that fails during a refresh ==========

    private static final TNetworkAddress NODE_A = new TNetworkAddress("127.0.0.1", 8060);
    private static final TNetworkAddress NODE_B = new TNetworkAddress("127.0.0.2", 8060);
    private static final TNetworkAddress NODE_C = new TNetworkAddress("127.0.0.3", 8060);

    // Three nodes. Node B fails the request types in failOnB, and every node fails the request types in failOnAll.
    private void mockThreeNodes(Set<PProcessDictionaryCacheRequestType> failOnB,
                                Set<PProcessDictionaryCacheRequestType> failOnAll) {
        mockThreeNodes(failOnB, failOnAll, new HashMap<>(Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L)));
    }

    // The test can change startTimes later to show a node that restarted.
    private void mockThreeNodes(Set<PProcessDictionaryCacheRequestType> failOnB,
                                Set<PProcessDictionaryCacheRequestType> failOnAll,
                                Map<TNetworkAddress, Long> startTimes) {
        new MockUp<DictionaryMgr>() {
            @Mock
            public void fillBackendsOrComputeNodes(List<TNetworkAddress> nodes) {
                nodes.add(NODE_A);
                nodes.add(NODE_B);
                nodes.add(NODE_C);
            }

            @Mock
            public Map<TNetworkAddress, Long> getAvailableNodeStartTimes() {
                return new HashMap<>(startTimes);
            }

            @Mock
            public Pair<Boolean, String> processDictionaryCacheInteranl(PProcessDictionaryCacheRequest request,
                                                                        List<TNetworkAddress> beNodes,
                                                                        List<PProcessDictionaryCacheResult> results,
                                                                        List<TNetworkAddress> failedNodes) {
                // failedNodes is null for the callers that stop at the first failed node.
                if (failOnAll.contains(request.type)) {
                    if (failedNodes != null) {
                        failedNodes.addAll(beNodes);
                    }
                    return Pair.create(true, "all nodes failed");
                }
                if (failOnB.contains(request.type) && beNodes.contains(NODE_B)) {
                    if (failedNodes != null) {
                        failedNodes.add(NODE_B);
                    }
                    return Pair.create(true, "node B restarted");
                }
                return Pair.create(false, "");
            }
        };
    }

    private Dictionary createRefreshingDictionary(String name, boolean ignoreFailedRefresh) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        properties.put("dictionary_ignore_failed_refresh", String.valueOf(ignoreFailedRefresh));
        masterDictionaryMgr.createDictionary(createTestDictionaryStmt(name, properties), TEST_CATALOG_NAME, TEST_DB_NAME);
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName(name);
        dictionary.setRefreshing(System.currentTimeMillis());
        return dictionary;
    }

    @Test
    public void testRefreshGoesOnWhenBeginFailsOnOneNode() throws Exception {
        mockThreeNodes(Set.of(PProcessDictionaryCacheRequestType.BEGIN), Set.of());
        Dictionary dictionary = createRefreshingDictionary("begin_fails_on_one_node", false);
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);

        worker.begin();
        worker.commit();
        worker.finish();

        Assertions.assertEquals(Lists.newArrayList(NODE_A, NODE_C), worker.getBeNodes());
        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, dictionary.getState());
        Assertions.assertEquals(txnId, dictionary.getLastSuccessVersion());
    }

    @Test
    public void testRefreshGoesOnWhenCommitFailsOnOneNode() throws Exception {
        // A failed COMMIT used to set CANCELLED, even with dictionary_ignore_failed_refresh.
        mockThreeNodes(Set.of(PProcessDictionaryCacheRequestType.COMMIT), Set.of());
        Dictionary dictionary = createRefreshingDictionary("commit_fails_on_one_node", true);
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);

        worker.begin();
        worker.commit();
        worker.finish();

        Assertions.assertEquals(Lists.newArrayList(NODE_A, NODE_C), worker.getBeNodes());
        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, dictionary.getState());
        Assertions.assertEquals(txnId, dictionary.getLastSuccessVersion());
    }

    @Test
    public void testRefreshFailsWhenCommitFailsOnOneNodeWithTheDefaultConfiguration() throws Exception {
        Config.dictionary_refresh_leave_out_failed_nodes = false;
        mockThreeNodes(Set.of(PProcessDictionaryCacheRequestType.COMMIT), Set.of());
        Dictionary dictionary = createRefreshingDictionary("commit_fails_by_default", false);
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);

        worker.begin();
        worker.commit();
        worker.finish();

        Assertions.assertEquals(Lists.newArrayList(NODE_A, NODE_B, NODE_C), worker.getBeNodes());
        Assertions.assertEquals(Dictionary.DictionaryState.CANCELLED, dictionary.getState());
        Assertions.assertEquals("node B restarted", dictionary.getRuntimeErrMsg());
    }

    // Runs the real request loop. Only the RPC to the node is mocked. Node B fails with an error status.
    private List<TNetworkAddress> mockNodeRpc(List<TNetworkAddress> calledNodes) {
        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PProcessDictionaryCacheResult> processDictionaryCache(
                    TNetworkAddress address, PProcessDictionaryCacheRequest request) {
                calledNodes.add(address);
                PProcessDictionaryCacheResult result = new PProcessDictionaryCacheResult();
                result.status = new StatusPB();
                result.status.statusCode = NODE_B.equals(address) ? TStatusCode.INTERNAL_ERROR.getValue()
                        : TStatusCode.OK.getValue();
                result.status.errorMsgs = NODE_B.equals(address) ? List.of("node B restarted") : List.of();
                return CompletableFuture.completedFuture(result);
            }
        };
        return calledNodes;
    }

    @Test
    public void testRequestLoopReachesNodesAfterTheFailedOne() {
        List<TNetworkAddress> calledNodes = mockNodeRpc(Lists.newArrayList());
        List<TNetworkAddress> failedNodes = Lists.newArrayList();
        List<PProcessDictionaryCacheResult> results = Lists.newArrayList();

        Pair<Boolean, String> ret = DictionaryMgr.processDictionaryCacheInteranl(new PProcessDictionaryCacheRequest(),
                Lists.newArrayList(NODE_A, NODE_B, NODE_C), results, failedNodes);

        Assertions.assertTrue(ret.first);
        Assertions.assertEquals("node B restarted", ret.second);
        Assertions.assertEquals(Lists.newArrayList(NODE_A, NODE_B, NODE_C), calledNodes);
        Assertions.assertEquals(Lists.newArrayList(NODE_B), failedNodes);
        Assertions.assertEquals(2, results.size());
    }

    @Test
    public void testRequestLoopStopsAtTheFailedNodeWithoutAFailedList() {
        List<TNetworkAddress> calledNodes = mockNodeRpc(Lists.newArrayList());

        Pair<Boolean, String> ret = DictionaryMgr.processDictionaryCacheInteranl(new PProcessDictionaryCacheRequest(),
                Lists.newArrayList(NODE_A, NODE_B, NODE_C), null);

        Assertions.assertTrue(ret.first);
        Assertions.assertEquals(Lists.newArrayList(NODE_A, NODE_B), calledNodes);
    }

    @Test
    public void testRefreshIsCancelledWhenCommitFailsOnAllNodes() throws Exception {
        mockThreeNodes(Set.of(), Set.of(PProcessDictionaryCacheRequestType.COMMIT));
        Dictionary dictionary = createRefreshingDictionary("commit_fails_on_all_nodes", true);
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);

        worker.begin();
        worker.commit();
        worker.finish();

        Assertions.assertEquals(Dictionary.DictionaryState.CANCELLED, dictionary.getState());
        Assertions.assertEquals("all nodes failed", dictionary.getRuntimeErrMsg());
    }

    @Test
    public void testRefreshRollsBackWhenBeginFailsOnOneNodeWithIgnoreFailedRefresh() throws Exception {
        Map<TNetworkAddress, Long> startTimes = new HashMap<>(Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L));
        Set<PProcessDictionaryCacheRequestType> failOnB = new HashSet<>();
        mockThreeNodes(failOnB, Set.of(), startTimes);
        Dictionary dictionary = createRefreshingDictionary("begin_fails_rolls_back", true);
        long firstTxnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker first =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, firstTxnId);
        first.begin();
        first.commit();
        first.finish();
        Map<TNetworkAddress, Long> record = Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L);
        Assertions.assertEquals(record, masterDictionaryMgr.getSyncedNodeStartTimes(dictionary.getDictionaryId()));

        // Node A restarted, and node B fails BEGIN.
        startTimes.put(NODE_A, 200L);
        failOnB.add(PProcessDictionaryCacheRequestType.BEGIN);
        dictionary.setRefreshing(System.currentTimeMillis());
        long secondTxnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker second =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, secondTxnId);
        second.begin();
        second.commit();
        second.finish();

        // Every node keeps the old version, so queries on node B still work.
        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, dictionary.getState());
        Assertions.assertEquals(firstTxnId, dictionary.getLastSuccessVersion());
        Assertions.assertTrue(dictionary.getRuntimeErrMsg().contains("node B restarted"));
        // The failed refresh keeps the record, so the next check still sees that node A has no copy.
        Assertions.assertEquals(record, masterDictionaryMgr.getSyncedNodeStartTimes(dictionary.getDictionaryId()));
    }

    @Test
    public void testCancelledRefreshDoesNotGoOnWithTheOtherNodes() throws Exception {
        mockThreeNodes(Set.of(PProcessDictionaryCacheRequestType.COMMIT), Set.of());
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        masterDictionaryMgr.createDictionary(createTestDictionaryStmt("cancelled_dict", properties),
                TEST_CATALOG_NAME, TEST_DB_NAME);
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName("cancelled_dict");
        masterDictionaryMgr.refreshDictionary("cancelled_dict");
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());

        worker.begin();
        // Node B fails COMMIT because of the cancel. Nodes A and C commit.
        masterDictionaryMgr.cancelRefreshDictionary("cancelled_dict");
        worker.commit();
        worker.finish();

        Assertions.assertEquals(Dictionary.DictionaryState.CANCELLED, dictionary.getState());
        Assertions.assertEquals("node B restarted", dictionary.getRuntimeErrMsg());

        // The cancel ends with that refresh. The next refresh leaves node B out again.
        masterDictionaryMgr.refreshDictionary("cancelled_dict");
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker next =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);
        next.begin();
        next.commit();
        next.finish();

        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, dictionary.getState());
        Assertions.assertEquals(txnId, dictionary.getLastSuccessVersion());
    }

    // ========== A node that missed part of the data ==========

    private List<PProcessDictionaryCacheRequest> mockThreeNodesWithRows(Map<TNetworkAddress, Long> txnRows,
                                                                        Set<TNetworkAddress> rpcFails) {
        return mockThreeNodesWithStatistic(txnRows, Map.of(), rpcFails);
    }

    // Three nodes with the real request loop. STATISTIC returns the rows in txnRows and the committed version
    // in versions. A node with no entry is an old BE that does not report it. The nodes in rpcFails fail
    // the STATISTIC RPC.
    private List<PProcessDictionaryCacheRequest> mockThreeNodesWithStatistic(Map<TNetworkAddress, Long> txnRows,
                                                                             Map<TNetworkAddress, Long> versions,
                                                                             Set<TNetworkAddress> rpcFails) {
        List<PProcessDictionaryCacheRequest> requests = Lists.newArrayList();
        new MockUp<DictionaryMgr>() {
            @Mock
            public void fillBackendsOrComputeNodes(List<TNetworkAddress> nodes) {
                nodes.add(NODE_A);
                nodes.add(NODE_B);
                nodes.add(NODE_C);
            }

            @Mock
            public Map<TNetworkAddress, Long> getAvailableNodeStartTimes() {
                return new HashMap<>(Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L));
            }
        };
        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PProcessDictionaryCacheResult> processDictionaryCache(
                    TNetworkAddress address, PProcessDictionaryCacheRequest request) {
                requests.add(request);
                if (request.type == PProcessDictionaryCacheRequestType.STATISTIC && rpcFails.contains(address)) {
                    CompletableFuture<PProcessDictionaryCacheResult> failed = new CompletableFuture<>();
                    failed.completeExceptionally(new RuntimeException("node unreachable"));
                    return failed;
                }
                PProcessDictionaryCacheResult result = new PProcessDictionaryCacheResult();
                result.status = new StatusPB();
                result.status.statusCode = TStatusCode.OK.getValue();
                result.status.errorMsgs = List.of();
                if (request.type == PProcessDictionaryCacheRequestType.STATISTIC) {
                    result.txnRows = txnRows.get(address);
                    result.dictionaryVersion = versions.get(address);
                }
                return CompletableFuture.completedFuture(result);
            }
        };
        return requests;
    }

    @Test
    public void testNodeWithFewerRowsIsLeftOutBeforeCommit() throws Exception {
        List<PProcessDictionaryCacheRequest> requests =
                mockThreeNodesWithRows(Map.of(NODE_A, 10L, NODE_B, 7L, NODE_C, 10L), Set.of());
        Dictionary dictionary = createRefreshingDictionary("fewer_rows_dict", false);
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);

        worker.begin();
        worker.leaveOutNodesWithMissingRows();
        worker.commit();
        worker.finish();

        PProcessDictionaryCacheRequest statistic = requests.get(3);
        Assertions.assertEquals(PProcessDictionaryCacheRequestType.STATISTIC, statistic.type);
        Assertions.assertEquals(txnId, statistic.txnId);
        Assertions.assertEquals(Lists.newArrayList(NODE_A, NODE_C), worker.getBeNodes());
        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, dictionary.getState());
        Assertions.assertEquals(Map.of(NODE_A, 100L, NODE_C, 100L),
                masterDictionaryMgr.getSyncedNodeStartTimes(dictionary.getDictionaryId()));
    }

    @Test
    public void testNodeLeftOutByTheRowCheckIsRefreshedAtOnce() throws Exception {
        mockThreeNodesWithRows(Map.of(NODE_A, 10L, NODE_B, 7L, NODE_C, 10L), Set.of());
        Dictionary dictionary = createRefreshingDictionary("row_check_backoff_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker first = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        first.begin();
        first.leaveOutNodesWithMissingRows();
        first.commit();
        first.finish();
        Map<TNetworkAddress, Long> allNodes = Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L);
        long now = System.currentTimeMillis();
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, now));

        dictionary.setRefreshing(now);
        DictionaryMgr.RefreshDictionaryCacheWorker regular = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        regular.begin();
        regular.leaveOutNodesWithMissingRows();
        regular.commit();
        regular.finish();
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, now + 1000L));
    }

    @Test
    public void testNodeWithoutTheTxnIsLeftOutBeforeCommit() throws Exception {
        // A new BE reports -1 when the txn is not open, for example after a restart after BEGIN.
        mockThreeNodesWithRows(Map.of(NODE_A, 10L, NODE_B, -1L, NODE_C, 10L), Set.of());
        Dictionary dictionary = createRefreshingDictionary("no_txn_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();

        worker.leaveOutNodesWithMissingRows();

        Assertions.assertEquals(Lists.newArrayList(NODE_A, NODE_C), worker.getBeNodes());
    }

    @Test
    public void testNodeThatFailsTheRowCheckRpcIsLeftOutBeforeCommit() throws Exception {
        mockThreeNodesWithRows(Map.of(NODE_A, 10L, NODE_B, 10L, NODE_C, 10L), Set.of(NODE_C));
        Dictionary dictionary = createRefreshingDictionary("row_check_rpc_fails_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();

        worker.leaveOutNodesWithMissingRows();

        Assertions.assertEquals(Lists.newArrayList(NODE_A, NODE_B), worker.getBeNodes());
    }

    @Test
    public void testRowCheckLeavesOutAnOldNodeThatDoesNotReportRows() throws Exception {
        // In a rolling upgrade, node B is an old BE. It may have missed chunks, so it must not commit.
        mockThreeNodesWithRows(Map.of(NODE_A, 10L, NODE_C, 10L), Set.of());
        Dictionary dictionary = createRefreshingDictionary("old_node_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();

        worker.leaveOutNodesWithMissingRows();

        Assertions.assertEquals(Lists.newArrayList(NODE_A, NODE_C), worker.getBeNodes());
    }

    // The FE cannot tell which node ran the sink, so nodes that report no rows never commit with leave-out on, even
    // when every node is old: the refresh fails instead.
    @Test
    public void testRowCheckFailsTheRefreshWhenNoNodeReportsRows() throws Exception {
        List<PProcessDictionaryCacheRequest> requests = mockThreeNodesWithRows(Map.of(), Set.of());
        Dictionary dictionary = createRefreshingDictionary("only_old_nodes_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();

        worker.leaveOutNodesWithMissingRows();
        worker.commit();
        worker.finish();

        Assertions.assertTrue(requests.stream().noneMatch(r -> r.type == PProcessDictionaryCacheRequestType.COMMIT));
        Assertions.assertEquals(Dictionary.DictionaryState.CANCELLED, dictionary.getState());
    }

    // A failed STATISTIC call can hide the new sink node, which honours skip_failed_nodes. The old nodes left
    // could then hold part of the data, so they are left out and the refresh fails.
    @Test
    public void testRowCheckLeavesOutOldNodesWhenANodeFailsTheCheck() throws Exception {
        List<PProcessDictionaryCacheRequest> requests = mockThreeNodesWithRows(Map.of(), Set.of(NODE_A));
        Dictionary dictionary = createRefreshingDictionary("hidden_sink_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();

        worker.leaveOutNodesWithMissingRows();
        worker.commit();
        worker.finish();

        Assertions.assertTrue(requests.stream().noneMatch(r -> r.type == PProcessDictionaryCacheRequestType.COMMIT));
        Assertions.assertEquals(Dictionary.DictionaryState.CANCELLED, dictionary.getState());
    }

    // A node left out at an earlier step, for example at BEGIN, can still run the sink fragment, so the old nodes
    // left could hold part of the data even though every one of them answers the check.
    @Test
    public void testRowCheckLeavesOutOldNodesWhenANodeWasLeftOutEarlier() throws Exception {
        List<PProcessDictionaryCacheRequest> requests = mockThreeNodesWithRows(Map.of(), Set.of());
        Dictionary dictionary = createRefreshingDictionary("left_out_sink_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();
        worker.getBeNodes().remove(NODE_A);

        worker.leaveOutNodesWithMissingRows();
        worker.commit();
        worker.finish();

        Assertions.assertTrue(requests.stream().noneMatch(r -> r.type == PProcessDictionaryCacheRequestType.COMMIT));
        Assertions.assertEquals(Dictionary.DictionaryState.CANCELLED, dictionary.getState());
    }

    @Test
    public void testNoRowCheckWhenFailedNodesAreNotLeftOut() throws Exception {
        List<PProcessDictionaryCacheRequest> requests =
                mockThreeNodesWithRows(Map.of(NODE_A, 10L, NODE_B, 7L, NODE_C, 10L), Set.of());
        // With dictionary_ignore_failed_refresh, BEGIN and the data push do not leave nodes out either.
        Dictionary dictionary = createRefreshingDictionary("no_row_check_dict", true);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();

        worker.leaveOutNodesWithMissingRows();

        Assertions.assertEquals(3, requests.size());
        Assertions.assertEquals(Lists.newArrayList(NODE_A, NODE_B, NODE_C), worker.getBeNodes());
    }

    // ========== A node that has no copy of the current version ==========

    @Test
    public void testNodeThatMissedTheCommitIsRefreshedEarly() throws Exception {
        mockThreeNodes(Set.of(PProcessDictionaryCacheRequestType.COMMIT), Set.of());
        Dictionary dictionary = createRefreshingDictionary("missed_commit_dict", true);
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);
        worker.begin();
        worker.commit();
        worker.finish();

        Assertions.assertEquals(Map.of(NODE_A, 100L, NODE_C, 100L),
                masterDictionaryMgr.getSyncedNodeStartTimes(dictionary.getDictionaryId()));

        Map<TNetworkAddress, Long> availableNodes = Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L);
        long now = System.currentTimeMillis();
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, availableNodes, now));
        // The next early refresh waits for dictionary_node_resync_min_interval_second.
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, availableNodes, now + 1000L));
        long later = now + Config.dictionary_node_resync_min_interval_second * 1000L;
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, availableNodes, later));
    }

    @Test
    public void testNodeLeftOutAtCommitIsRefreshedAtOnce() throws Exception {
        mockThreeNodes(Set.of(PProcessDictionaryCacheRequestType.COMMIT), Set.of());
        Dictionary dictionary = createRefreshingDictionary("left_out_at_commit_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker first = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        first.begin();
        first.commit();
        first.finish();
        Map<TNetworkAddress, Long> allNodes = Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L);
        long now = System.currentTimeMillis();
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, now));

        // A regular refresh leaves node B out at COMMIT just after that early refresh. Do not wait.
        dictionary.setRefreshing(now);
        DictionaryMgr.RefreshDictionaryCacheWorker regular = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        regular.begin();
        regular.commit();
        regular.finish();
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, now + 1000L));

        // The early refresh leaves node B out again, so the wait still applies.
        dictionary.setRefreshing(now);
        DictionaryMgr.RefreshDictionaryCacheWorker early = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        early.setEarlyRefresh(true);
        early.begin();
        early.commit();
        early.finish();
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, now + 2000L));
    }

    @Test
    public void testFailedEarlyRefreshKeepsTheOldVersion() throws Exception {
        mockThreeNodes(Set.of(), Set.of(PProcessDictionaryCacheRequestType.BEGIN));
        // Without dictionary_ignore_failed_refresh, a failed regular refresh ends in CANCELLED.
        Dictionary dictionary = createFinishedDictionary("failed_early_refresh_dict", 5L);
        dictionary.setRefreshing(System.currentTimeMillis());
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.setEarlyRefresh(true);

        worker.begin();
        worker.commit();
        worker.finish();

        // An early refresh only repairs nodes. The old version is still on every node, so queries still work.
        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, dictionary.getState());
        Assertions.assertEquals(5L, dictionary.getLastSuccessVersion());
        Assertions.assertTrue(dictionary.getRuntimeErrMsg().contains("all nodes failed"));
    }

    @Test
    public void testNoEarlyRefreshForADictionaryThatReadsTheLatestVersion() throws Exception {
        // dictionary_read_latest fails every query while the dictionary refreshes. An early refresh only repairs
        // nodes, so it must not cause that.
        mockThreeNodes(Set.of(PProcessDictionaryCacheRequestType.COMMIT), Set.of());
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        properties.put("dictionary_read_latest", "true");
        masterDictionaryMgr.createDictionary(createTestDictionaryStmt("read_latest_dict", properties),
                TEST_CATALOG_NAME, TEST_DB_NAME);
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName("read_latest_dict");
        long id = dictionary.getDictionaryId();
        Map<TNetworkAddress, Long> allNodes = Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L);
        long now = System.currentTimeMillis();

        // No record: no question to the nodes.
        dictionary.setFinished(now, 5L);
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, now));
        Assertions.assertFalse(masterDictionaryMgr.isNodeProbePending(id));

        // Node B misses the COMMIT: no early refresh.
        dictionary.setRefreshing(now);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();
        worker.commit();
        worker.finish();
        Assertions.assertEquals(Map.of(NODE_A, 100L, NODE_C, 100L), masterDictionaryMgr.getSyncedNodeStartTimes(id));
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, now));
    }

    @Test
    public void testRestartedOrNewNodeIsRefreshedEarly() throws Exception {
        mockThreeNodes(Set.of(), Set.of());
        Dictionary dictionary = createRefreshingDictionary("restarted_node_dict", true);
        long txnId = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker worker =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, txnId);
        worker.begin();
        worker.commit();
        worker.finish();

        long now = System.currentTimeMillis();
        Map<TNetworkAddress, Long> sameNodes = Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L);
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, sameNodes, now));
        // A node that is down is not a reason to refresh.
        Map<TNetworkAddress, Long> nodeDown = Map.of(NODE_A, 100L, NODE_C, 100L);
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, nodeDown, now));

        Map<TNetworkAddress, Long> restarted = Map.of(NODE_A, 100L, NODE_B, 200L, NODE_C, 100L);
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, restarted, now));

        TNetworkAddress newNode = new TNetworkAddress("127.0.0.4", 8060);
        Map<TNetworkAddress, Long> scaledOut = Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L, newNode, 100L);
        long later = now + Config.dictionary_node_resync_min_interval_second * 1000L;
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, scaledOut, later));
    }

    @Test
    public void testNoEarlyRefreshWithoutARecordOrWhenTurnedOff() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        masterDictionaryMgr.createDictionary(createTestDictionaryStmt("no_record_dict", properties),
                TEST_CATALOG_NAME, TEST_DB_NAME);
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName("no_record_dict");
        long now = System.currentTimeMillis();
        Map<TNetworkAddress, Long> nodes = Map.of(NODE_A, 100L);

        long id = dictionary.getDictionaryId();

        // Not FINISHED: the regular schedule handles it.
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, nodes, now));

        // Turned off: no early refresh, and no question to the nodes.
        dictionary.setFinished(now, 1L);
        Config.dictionary_node_resync_min_interval_second = -1;
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, nodes, now));
        Assertions.assertFalse(masterDictionaryMgr.isNodeProbePending(id));
        Config.dictionary_node_resync_min_interval_second = 30;

        // A new leader has no record. It does not refresh, and it asks the nodes which version they have.
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, nodes, now));
        Assertions.assertNull(masterDictionaryMgr.getSyncedNodeStartTimes(id));
        Assertions.assertTrue(masterDictionaryMgr.isNodeProbePending(id));

        masterDictionaryMgr.dropDictionary("no_record_dict", false);
        Assertions.assertNull(masterDictionaryMgr.getSyncedNodeStartTimes(id));
        Assertions.assertFalse(masterDictionaryMgr.isNodeProbePending(id));
    }

    @Test
    public void testEarlyRefreshWaitsLongerEachTimeANodeStaysWithoutACopy() throws Exception {
        mockThreeNodes(Set.of(PProcessDictionaryCacheRequestType.COMMIT), Set.of());
        Dictionary dictionary = createRefreshingDictionary("backoff_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();
        worker.commit();
        worker.finish();

        int oldMin = Config.dictionary_node_resync_min_interval_second;
        int oldMax = Config.dictionary_node_resync_max_interval_second;
        try {
            Config.dictionary_node_resync_min_interval_second = 10;
            Config.dictionary_node_resync_max_interval_second = 25;
            Map<TNetworkAddress, Long> allNodes = Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L);
            long t = System.currentTimeMillis();

            Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t));
            Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 9_999L));
            Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 10_000L));
            // The wait doubles to 20 seconds.
            Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 29_999L));
            Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 30_000L));
            // 40 seconds is more than the max, so the wait is 25 seconds.
            Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 54_999L));
            Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 55_000L));

            // Node B is down, so every available node has a copy. The wait starts again from the min.
            Map<TNetworkAddress, Long> nodeBDown = Map.of(NODE_A, 100L, NODE_C, 100L);
            Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, nodeBDown, t + 56_000L));
            Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 56_001L));
        } finally {
            Config.dictionary_node_resync_min_interval_second = oldMin;
            Config.dictionary_node_resync_max_interval_second = oldMax;
        }
    }

    @Test
    public void testEarlyRefreshWaitStartsAtOneSchedulerPassWithAMinOfZero() throws Exception {
        mockThreeNodes(Set.of(PProcessDictionaryCacheRequestType.COMMIT), Set.of());
        Dictionary dictionary = createRefreshingDictionary("zero_min_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();
        worker.commit();
        worker.finish();

        // With 0, the doubling would stay at 0, and a node that keeps failing would cause a refresh every pass.
        Config.dictionary_node_resync_min_interval_second = 0;
        Map<TNetworkAddress, Long> allNodes = Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L);
        long t = System.currentTimeMillis();
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t));
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 1_000L));
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 10_000L));
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 29_999L));
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, t + 30_000L));
    }

    @Test
    public void testScheduleTasksStartsAnEarlyRefreshForARestartedNode() throws Exception {
        Map<TNetworkAddress, Long> startTimes = new HashMap<>(Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L));
        mockThreeNodes(Set.of(), Set.of(), startTimes);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        // The real refresh needs a table and live nodes. The test only needs to see that it starts.
        CountDownLatch started = new CountDownLatch(1);
        List<Boolean> earlyFlags = Lists.newArrayList();
        new MockUp<DictionaryMgr.RefreshDictionaryCacheWorker>() {
            @Mock
            public void run(Invocation invocation) {
                earlyFlags.add(((DictionaryMgr.RefreshDictionaryCacheWorker) invocation.getInvokedInstance())
                        .isEarlyRefresh());
                started.countDown();
            }
        };
        // No automatic refresh, so only a node resync can start one.
        Dictionary dictionary = createRefreshingDictionary("schedule_dict", false);
        DictionaryMgr.RefreshDictionaryCacheWorker worker = masterDictionaryMgr.new RefreshDictionaryCacheWorker(
                dictionary, masterDictionaryMgr.getAndIncrementTxnIdUnlocked());
        worker.begin();
        worker.commit();
        worker.finish();
        long id = dictionary.getDictionaryId();

        masterDictionaryMgr.scheduleTasks();
        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, dictionary.getState());
        Assertions.assertFalse(masterDictionaryMgr.getUnfinishedRefreshTasks().contains(id));

        startTimes.put(NODE_B, 200L);
        masterDictionaryMgr.scheduleTasks();
        Assertions.assertEquals(Dictionary.DictionaryState.REFRESHING, dictionary.getState());
        Assertions.assertTrue(masterDictionaryMgr.getUnfinishedRefreshTasks().contains(id));
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
        Assertions.assertEquals(List.of(true), earlyFlags);
    }

    // ========== A new leader ==========

    private Dictionary createFinishedDictionary(String name, long version) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_warm_up", "false");
        masterDictionaryMgr.createDictionary(createTestDictionaryStmt(name, properties), TEST_CATALOG_NAME, TEST_DB_NAME);
        Dictionary dictionary = masterDictionaryMgr.getDictionaryByName(name);
        dictionary.setFinished(System.currentTimeMillis(), version);
        return dictionary;
    }

    @Test
    public void testNewLeaderRefreshesANodeOnAnOldVersion() throws Exception {
        // Node B has an old version. Node C is an old BE that does not report its version.
        mockThreeNodesWithStatistic(Map.of(), Map.of(NODE_A, 5L, NODE_B, 4L), Set.of());
        Dictionary dictionary = createFinishedDictionary("new_leader_old_version_dict", 5L);
        Map<TNetworkAddress, Long> allNodes = Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L);
        long now = System.currentTimeMillis();
        Assertions.assertFalse(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, now));

        masterDictionaryMgr.probeNodeVersions(dictionary);

        Assertions.assertEquals(Map.of(NODE_A, 100L, NODE_C, 100L),
                masterDictionaryMgr.getSyncedNodeStartTimes(dictionary.getDictionaryId()));
        Assertions.assertFalse(masterDictionaryMgr.isNodeProbePending(dictionary.getDictionaryId()));
        Assertions.assertTrue(masterDictionaryMgr.needNodeResyncUnlocked(dictionary, allNodes, now));
    }

    @Test
    public void testNewLeaderRefreshesANodeWithNoCopy() throws Exception {
        // A new BE reports -1 when it has no copy, for example after a restart.
        mockThreeNodesWithStatistic(Map.of(), Map.of(NODE_A, 5L, NODE_B, -1L, NODE_C, 5L), Set.of());
        Dictionary dictionary = createFinishedDictionary("new_leader_no_copy_dict", 5L);

        masterDictionaryMgr.probeNodeVersions(dictionary);

        Assertions.assertEquals(Map.of(NODE_A, 100L, NODE_C, 100L),
                masterDictionaryMgr.getSyncedNodeStartTimes(dictionary.getDictionaryId()));
    }

    @Test
    public void testNewLeaderRefreshesANodeThatDoesNotAnswer() throws Exception {
        mockThreeNodesWithStatistic(Map.of(), Map.of(NODE_A, 5L, NODE_B, 5L, NODE_C, 5L), Set.of(NODE_B));
        Dictionary dictionary = createFinishedDictionary("new_leader_no_answer_dict", 5L);

        masterDictionaryMgr.probeNodeVersions(dictionary);

        Assertions.assertEquals(Map.of(NODE_A, 100L, NODE_C, 100L),
                masterDictionaryMgr.getSyncedNodeStartTimes(dictionary.getDictionaryId()));
    }

    @Test
    public void testNewLeaderDoesNotRefreshWhenEveryNodeHasTheCurrentVersion() throws Exception {
        mockThreeNodesWithStatistic(Map.of(), Map.of(NODE_A, 5L, NODE_B, 5L, NODE_C, 5L), Set.of());
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        Dictionary dictionary = createFinishedDictionary("new_leader_current_dict", 5L);
        long id = dictionary.getDictionaryId();

        // The first pass asks the nodes on another thread.
        masterDictionaryMgr.scheduleTasks();
        long deadline = System.currentTimeMillis() + 10_000L;
        while (masterDictionaryMgr.isNodeProbePending(id) && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        Assertions.assertEquals(Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L),
                masterDictionaryMgr.getSyncedNodeStartTimes(id));

        masterDictionaryMgr.scheduleTasks();
        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, dictionary.getState());
        Assertions.assertFalse(masterDictionaryMgr.getUnfinishedRefreshTasks().contains(id));
    }

    private static void replayNextModifyLogs(DictionaryMgr follower, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            follower.replayModifyDictionaryMgr((UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2));
        }
    }

    @Test
    public void testNewLeaderRollsBackAnEarlyRefreshThatFailsBeforeCommit() throws Exception {
        Map<TNetworkAddress, Long> startTimes = new HashMap<>(Map.of(NODE_A, 100L, NODE_B, 100L, NODE_C, 100L));
        Set<PProcessDictionaryCacheRequestType> failOnAll = new HashSet<>();
        mockThreeNodes(Set.of(), failOnAll, startTimes);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        BlockingQueue<DictionaryMgr.RefreshDictionaryCacheWorker> started = new LinkedBlockingQueue<>();
        new MockUp<DictionaryMgr.RefreshDictionaryCacheWorker>() {
            @Mock
            public void run(Invocation invocation) {
                started.add(invocation.getInvokedInstance());
            }
        };
        Dictionary dictionary = createRefreshingDictionary("failover_early_dict", false);
        long id = dictionary.getDictionaryId();
        DictionaryMgr follower = new DictionaryMgr();
        follower.replayCreateDictionary((Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY));
        long version = masterDictionaryMgr.getAndIncrementTxnIdUnlocked();
        DictionaryMgr.RefreshDictionaryCacheWorker first =
                masterDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, version);
        first.begin();
        first.commit();
        first.finish();
        // txn id, COMMITTING, FINISHED
        replayNextModifyLogs(follower, 3);

        // Node B restarted, so the leader starts an early refresh. Then the leader goes down.
        startTimes.put(NODE_B, 200L);
        masterDictionaryMgr.scheduleTasks();
        Assertions.assertTrue(started.poll(10, TimeUnit.SECONDS).isEarlyRefresh());
        // REFRESHING, txn id
        replayNextModifyLogs(follower, 2);

        // The follower becomes the leader and goes on with the refresh, which fails on every node at BEGIN.
        follower.scheduleTasks();
        DictionaryMgr.RefreshDictionaryCacheWorker resumed = started.poll(10, TimeUnit.SECONDS);
        failOnAll.add(PProcessDictionaryCacheRequestType.BEGIN);
        resumed.begin();
        resumed.commit();
        resumed.finish();

        Dictionary newLeaderDictionary = follower.getDictionaryByName("failover_early_dict");
        Assertions.assertEquals(Dictionary.DictionaryState.FINISHED, newLeaderDictionary.getState());
        Assertions.assertEquals(version, newLeaderDictionary.getLastSuccessVersion());
        Assertions.assertTrue(newLeaderDictionary.getRuntimeErrMsg().contains("all nodes failed"));
        Assertions.assertTrue(resumed.isEarlyRefresh());
        Assertions.assertFalse(follower.isEarlyRefreshTask(id));
    }

    @Test
    public void testFollowerKeepsTheEarlyRefreshMarkUntilTheRefreshEnds() throws Exception {
        createFinishedDictionary("replay_early_mark_dict", 5L);
        DictionaryMgr follower = new DictionaryMgr();
        Dictionary dictionary = (Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY);
        follower.replayCreateDictionary(dictionary);
        long id = dictionary.getDictionaryId();
        long ts = System.currentTimeMillis();

        UpdateDictionaryLog finished = new UpdateDictionaryLog(id, ts);
        finished.setState(Dictionary.DictionaryState.FINISHED);
        finished.setLastSuccessVersion(6L);
        UpdateDictionaryLog cancelled = new UpdateDictionaryLog(id, ts);
        cancelled.setState(Dictionary.DictionaryState.CANCELLED);
        UpdateDictionaryLog rolledBack = new UpdateDictionaryLog(id, ts);
        rolledBack.setResetStateBeforeRefresh(true);

        for (UpdateDictionaryLog end : List.of(finished, cancelled, rolledBack)) {
            UpdateDictionaryLog early = new UpdateDictionaryLog(id, ts);
            early.setState(Dictionary.DictionaryState.REFRESHING);
            early.setEarlyRefresh(true);
            follower.replayModifyDictionaryMgr(new UpdateDictionaryMgrLog(Lists.newArrayList(early)));
            Assertions.assertTrue(follower.isEarlyRefreshTask(id));

            follower.replayModifyDictionaryMgr(new UpdateDictionaryMgrLog(Lists.newArrayList(end)));
            Assertions.assertFalse(follower.isEarlyRefreshTask(id));
        }

        UpdateDictionaryLog regular = new UpdateDictionaryLog(id, ts);
        regular.setState(Dictionary.DictionaryState.REFRESHING);
        follower.replayModifyDictionaryMgr(new UpdateDictionaryMgrLog(Lists.newArrayList(regular)));
        Assertions.assertFalse(follower.isEarlyRefreshTask(id));
    }
}
