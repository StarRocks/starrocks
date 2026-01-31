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
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.UpdateDictionaryMgrLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class DictionaryMgrEditLogTest2 {
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
}
