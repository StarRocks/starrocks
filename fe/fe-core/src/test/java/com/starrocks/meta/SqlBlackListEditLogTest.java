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

package com.starrocks.meta;

import com.starrocks.meta.BlackListSql;
import com.starrocks.persist.DeleteSqlBlackLists;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.SqlBlackListPersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class SqlBlackListEditLogTest {
    private SqlBlackList sqlBlackList;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        sqlBlackList = GlobalStateMgr.getCurrentState().getSqlBlackList();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAddBlackSqlNormalCase() throws Exception {
        // 1. Prepare test data
        String sqlPattern = "select.*from.*test";
        Pattern pattern = Pattern.compile(sqlPattern);

        // 2. Verify initial state
        List<BlackListSql> initialBlackLists = sqlBlackList.getBlackLists();
        int initialCount = initialBlackLists.size();

        // 3. Execute put operation (which calls addBlackSql internally via editlog)
        long id = sqlBlackList.put(pattern);

        // 4. Verify master state
        List<BlackListSql> blackLists = sqlBlackList.getBlackLists();
        Assertions.assertEquals(initialCount + 1, blackLists.size());
        Assertions.assertTrue(id >= 0);

        // 5. Test follower replay functionality
        SqlBlackList followerBlackList = new SqlBlackList();
        
        // Verify follower initial state
        Assertions.assertEquals(0, followerBlackList.getBlackLists().size());

        // Replay the operation
        SqlBlackListPersistInfo replayInfo = (SqlBlackListPersistInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_SQL_QUERY_BLACK_LIST);
        followerBlackList.put(replayInfo.id, Pattern.compile(replayInfo.pattern));

        // 6. Verify follower state is consistent with master
        List<BlackListSql> followerBlackLists = followerBlackList.getBlackLists();
        Assertions.assertEquals(1, followerBlackLists.size());
        Assertions.assertEquals(id, followerBlackLists.get(0).id);
        Assertions.assertEquals(sqlPattern, followerBlackLists.get(0).pattern.pattern());
    }

    @Test
    public void testAddBlackSqlEditLogException() throws Exception {
        // 1. Prepare test data
        String sqlPattern = "select.*from.*exception";
        Pattern pattern = Pattern.compile(sqlPattern);

        // 2. Verify initial state
        int initialCount = sqlBlackList.getBlackLists().size();

        // 3. Mock EditLog.logAddSQLBlackList to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAddSQLBlackList(any(SqlBlackListPersistInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        int savedCount = initialCount;

        // 4. Execute put operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            sqlBlackList.put(pattern);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        int currentCount = sqlBlackList.getBlackLists().size();
        Assertions.assertEquals(savedCount, currentCount);
    }

    @Test
    public void testDeleteBlackSqlNormalCase() throws Exception {
        // 1. Prepare test data - add a blacklist entry first
        String sqlPattern = "select.*from.*delete";
        Pattern pattern = Pattern.compile(sqlPattern);
        long id = sqlBlackList.put(pattern);
        
        Assertions.assertNotNull(sqlBlackList.getBlackLists().stream()
                .filter(b -> b.id == id).findFirst().orElse(null));

        // 2. Prepare delete statement
        List<Long> indexs = new ArrayList<>();
        indexs.add(id);
        DelSqlBlackListStmt delStmt = new DelSqlBlackListStmt(indexs);

        // 3. Execute deleteBlackSql operation (master side)
        sqlBlackList.deleteBlackSql(delStmt);

        // 4. Verify master state
        Assertions.assertNull(sqlBlackList.getBlackLists().stream()
                .filter(b -> b.id == id).findFirst().orElse(null));

        // 5. Test follower replay functionality
        SqlBlackList followerBlackList = new SqlBlackList();
        
        // First add the blacklist entry in follower
        followerBlackList.put(id, pattern);
        Assertions.assertNotNull(followerBlackList.getBlackLists().stream()
                .filter(b -> b.id == id).findFirst().orElse(null));

        // Replay the delete operation
        DeleteSqlBlackLists replayInfo = (DeleteSqlBlackLists) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DELETE_SQL_QUERY_BLACK_LIST);
        for (long deleteId : replayInfo.ids) {
            followerBlackList.delete(deleteId);
        }

        // 6. Verify follower state is consistent with master
        Assertions.assertNull(followerBlackList.getBlackLists().stream()
                .filter(b -> b.id == id).findFirst().orElse(null));
    }

    @Test
    public void testDeleteBlackSqlEditLogException() throws Exception {
        // 1. Prepare test data - add a blacklist entry first
        String sqlPattern = "select.*from.*delete_exception";
        Pattern pattern = Pattern.compile(sqlPattern);
        long id = sqlBlackList.put(pattern);
        
        BlackListSql initialEntry = sqlBlackList.getBlackLists().stream()
                .filter(b -> b.id == id).findFirst().orElse(null);
        Assertions.assertNotNull(initialEntry);

        // 2. Prepare delete statement
        List<Long> indexs = new ArrayList<>();
        indexs.add(id);
        DelSqlBlackListStmt delStmt = new DelSqlBlackListStmt(indexs);

        // 3. Mock EditLog.logDeleteSQLBlackList to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDeleteSQLBlackList(any(DeleteSqlBlackLists.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        BlackListSql savedEntry = initialEntry;

        // 4. Execute deleteBlackSql operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            sqlBlackList.deleteBlackSql(delStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        BlackListSql currentEntry = sqlBlackList.getBlackLists().stream()
                .filter(b -> b.id == id).findFirst().orElse(null);
        Assertions.assertNotNull(currentEntry);
        Assertions.assertEquals(savedEntry.id, currentEntry.id);
    }
}

