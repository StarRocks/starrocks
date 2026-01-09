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

import com.starrocks.persist.DeleteSqlDigestBlackLists;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.SqlDigestBlackListPersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class SqlDigestBlackListEditLogTest {
    private SqlDigestBlackList sqlBlackList;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        sqlBlackList = GlobalStateMgr.getCurrentState().getSqlDigestBlackList();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAddDigestBlackSqlNormalCase() throws Exception {
        // 1. Prepare test data
        String digest = "abcd";

        // 2. Verify initial state
        Assertions.assertEquals(0, sqlBlackList.getDigests().size());

        // 3. Execute put operation (which calls addBlackSql internally via editlog)
        sqlBlackList.addDigest(digest);

        // 4. Verify master state
        Set<String> blackLists = sqlBlackList.getDigests();
        Assertions.assertEquals(1, blackLists.size());

        // 5. Test follower replay functionality
        SqlDigestBlackList followerBlackList = new SqlDigestBlackList();

        // Verify follower initial state
        Assertions.assertEquals(0, followerBlackList.getDigests().size());

        // Replay the operation
        SqlDigestBlackListPersistInfo replayInfo = (SqlDigestBlackListPersistInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_SQL_DIGEST_BLACK_LIST);
        followerBlackList.put(replayInfo.digest);

        // 6. Verify follower state is consistent with master
        Set<String> followerBlackLists = followerBlackList.getDigests();
        Assertions.assertEquals(1, followerBlackLists.size());
        Assertions.assertTrue(followerBlackLists.contains(digest));
    }

    @Test
    public void testAddDigestBlackSqlEditLogException() throws Exception {
        // 1. Prepare test data
        String digest = "abcd";

        // 2. Verify initial state
        int initialCount = sqlBlackList.getDigests().size();

        // 3. Mock EditLog.logAddSQLDigestBlackList to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddSqlDigestBlackList(any(SqlDigestBlackListPersistInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        int savedCount = initialCount;

        // 4. Execute put operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            sqlBlackList.addDigest(digest);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        int currentCount = sqlBlackList.getDigests().size();
        Assertions.assertEquals(savedCount, currentCount);
    }

    @Test
    public void testDeleteDigestBlackSqlNormalCase() throws Exception {
        // 1. Prepare test data - add a blacklist entry first
        String digest = "abcd";
        sqlBlackList.addDigest(digest);

        Assertions.assertFalse(sqlBlackList.getDigests().isEmpty());

        // 2. Prepare delete statement
        List<String> digests = new ArrayList<>();
        digests.add(digest);

        // 3. Execute deleteBlackSql operation (master side)
        sqlBlackList.deleteDigests(digests);

        // 4. Verify master state
        Assertions.assertTrue(sqlBlackList.getDigests().isEmpty());

        // 5. Test follower replay functionality
        SqlDigestBlackList followerBlackList = new SqlDigestBlackList();

        // First add the blacklist entry in follower
        followerBlackList.addDigest(digest);
        Assertions.assertFalse(followerBlackList.getDigests().isEmpty());

        // Replay the delete operation
        DeleteSqlDigestBlackLists replayInfo = (DeleteSqlDigestBlackLists) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DELETE_SQL_DIGEST_BLACK_LIST);
        followerBlackList.deleteAll(replayInfo.digests);

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerBlackList.getDigests().isEmpty());
    }

    @Test
    public void testDeleteDigestBlackSqlEditLogException() throws Exception {
        // 1. Prepare test data - add a blacklist entry first
        String digest = "abcd";
        sqlBlackList.addDigest(digest);

        String initialEntry = sqlBlackList.getDigests().stream().findFirst().orElse(null);
        Assertions.assertNotNull(initialEntry);

        // 2. Prepare delete statement
        List<String> digests = new ArrayList<>();
        digests.add(digest);

        // 3. Mock EditLog.logDeleteSQLDigestBlackList to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDeleteSqlDigestBlackList(any(DeleteSqlDigestBlackLists.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        String savedEntry = initialEntry;

        // 4. Execute deleteBlackSql operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            sqlBlackList.deleteDigests(digests);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        String currentEntry = sqlBlackList.getDigests().stream().findFirst().orElse(null);
        Assertions.assertEquals(savedEntry, currentEntry);

        // 6. Clean
        sqlBlackList.deleteAll(digests);
        Assertions.assertTrue(sqlBlackList.getDigests().isEmpty());
    }
}
