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
// test

package com.starrocks.alter;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MockedLocalMetaStore;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.MockedMetadataMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class AlterJobMgrEditLogTest {
    private AlterJobMgr alterJobMgr;
    private MockedLocalMetaStore localMetastore;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        globalStateMgr.setLocalMetastore(localMetastore);

        MockedMetadataMgr mockedMetadataMgr = new MockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(mockedMetadataMgr);

        alterJobMgr = globalStateMgr.getAlterJobMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testSetViewSecurityNormalCase() throws Exception {
        // 1. Prepare test data - create a database and view
        localMetastore.createDb("test_db");
        Database db = localMetastore.getDb("test_db");
        Assertions.assertNotNull(db);

        // Create a simple view
        List<Column> columns = new ArrayList<>();
        View view = new View(1L, "test_view", columns);
        db.registerTableUnlocked(view);
        
        Table table = localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.isView());
        View testView = (View) table;
        Assertions.assertFalse(testView.isSecurity());

        // 2. Prepare AlterViewInfo
        AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(), true);

        // 3. Execute setViewSecurity operation (master side)
        alterJobMgr.setViewSecurity(alterViewInfo);

        // 4. Verify master state
        View updatedView = (View) localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(updatedView);
        Assertions.assertTrue(updatedView.isSecurity());

        // 5. Test follower replay functionality
        // Use the same alterJobMgr for replay since we're testing the replay method
        AlterJobMgr followerAlterJobMgr = alterJobMgr;
        
        // Create follower view with initial state
        List<Column> followerColumns = new ArrayList<>();
        View followerView = new View(view.getId(), "test_view", followerColumns);
        followerView.setSecurity(false);
        db.registerTableUnlocked(followerView);
        
        Assertions.assertFalse(followerView.isSecurity());

        // Replay the operation
        AlterViewInfo replayInfo = (AlterViewInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_SET_VIEW_SECURITY_LOG);
        followerAlterJobMgr.updateViewSecurity(replayInfo);

        // 6. Verify follower state is consistent with master
        View followerUpdatedView = (View) localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(followerUpdatedView);
        Assertions.assertTrue(followerUpdatedView.isSecurity());
    }

    @Test
    public void testSetViewSecurityEditLogException() throws Exception {
        // 1. Prepare test data - create a database and view
        localMetastore.createDb("test_db");
        Database db = localMetastore.getDb("test_db");
        Assertions.assertNotNull(db);

        // Create a simple view
        List<Column> columns = new ArrayList<>();
        View view = new View(1L, "test_view", columns);
        db.registerTableUnlocked(view);
        
        Table table = localMetastore.getTable("test_db", "test_view");
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.isView());
        View testView = (View) table;
        Assertions.assertFalse(testView.isSecurity());

        // 2. Prepare AlterViewInfo
        AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(), true);

        // 3. Mock EditLog.logJsonObject to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logJsonObject(any(Short.class), any(), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute setViewSecurity operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            alterJobMgr.setViewSecurity(alterViewInfo);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify view security remains unchanged after exception
        Assertions.assertNotNull(exception);
        Assertions.assertFalse(testView.isSecurity());
    }
}

