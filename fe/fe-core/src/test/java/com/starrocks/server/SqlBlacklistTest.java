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

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.meta.BlackListSql;
import com.starrocks.meta.SqlBlackList;
import com.starrocks.persist.DeleteSqlBlackLists;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.SqlBlackListPersistInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.ShowSqlBlackListStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.regex.Pattern;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.parseSql;

public class SqlBlacklistTest {
    GlobalStateMgr state;
    SqlBlackList sqlBlackList;
    EditLog editLog;
    ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void beforeEach() {
        state = Deencapsulation.newInstance(GlobalStateMgr.class);
        sqlBlackList = new SqlBlackList();
        connectContext = UtFrameUtils.createDefaultCtx();
        editLog = Mockito.mock(EditLog.class);
        connectContext.setQueryId(UUIDUtil.genUUID());
    }

    @Test
    public void testAddSQLBlacklist() throws Exception {
        mockupGlobalState();

        ArgumentCaptor<SqlBlackListPersistInfo> addBlacklistEditLogArgument = ArgumentCaptor
                .forClass(SqlBlackListPersistInfo.class);

        AddSqlBlackListStmt addStatement = (AddSqlBlackListStmt) parseSql("ADD SQLBLACKLIST \".+\";");
        Assertions.assertEquals(addStatement.getSql(), ".+");

        StmtExecutor addStatementExecutor = new StmtExecutor(connectContext, addStatement);
        addStatementExecutor.execute();
        List<BlackListSql> blackLists = sqlBlackList.getBlackLists();
        Assertions.assertEquals(1, blackLists.size());
        Assertions.assertEquals(0, blackLists.get(0).id);
        Assertions.assertEquals(".+", blackLists.get(0).pattern.pattern());

        Mockito.verify(editLog).logAddSQLBlackList(addBlacklistEditLogArgument.capture());

        Assertions.assertEquals(0, addBlacklistEditLogArgument.getValue().id);
        Assertions.assertEquals(".+", addBlacklistEditLogArgument.getValue().pattern);
    }

    @Test
    public void testShowBlacklist() {
        mockupGlobalState();
        sqlBlackList.put(Pattern.compile("qwert"));
        sqlBlackList.put(Pattern.compile("abcde"));

        ShowSqlBlackListStmt showSqlStatement = (ShowSqlBlackListStmt) parseSql("SHOW SQLBLACKLIST");

        ShowResultSet resultSet = ShowExecutor.execute(showSqlStatement, connectContext);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(0L, resultSet.getLong(0));
        Assertions.assertEquals("qwert", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1L, resultSet.getLong(0));
        Assertions.assertEquals("abcde", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    public void testBlackListReturnsSameIdIfPatternAlreadyExists() {
        mockupGlobalState();
        Pattern p = Pattern.compile("qwert");
        long id = sqlBlackList.put(p);

        Assertions.assertEquals(id, sqlBlackList.put(p));
    }

    @Test
    public void testDeleteSqlBlacklist() throws Exception {
        mockupGlobalState();
        long id1 = sqlBlackList.put(Pattern.compile("qwert"));
        long id2 = sqlBlackList.put(Pattern.compile("abcde"));

        ArgumentCaptor<DeleteSqlBlackLists> deleteBlacklistsEditLogArgument =
                ArgumentCaptor.forClass(DeleteSqlBlackLists.class);

        StmtExecutor deleteStatementExecutor = new StmtExecutor(connectContext, new DelSqlBlackListStmt(List.of(id1, id2)));
        deleteStatementExecutor.execute();
        Assertions.assertTrue(sqlBlackList
                .getBlackLists().stream().noneMatch(x -> x.id == id1 || x.id != id2));

        Mockito.verify(editLog).logDeleteSQLBlackList(deleteBlacklistsEditLogArgument.capture());

        Assertions.assertEquals(List.of(id1, id2), deleteBlacklistsEditLogArgument.getValue().ids);
    }

    @Test
    public void testSaveLoadBlackListImage() throws Exception {
        SqlBlackList originalBlacklist = new SqlBlackList();
        originalBlacklist.put(Pattern.compile("zxcvbqwert"));
        originalBlacklist.put(Pattern.compile("qwdsad"));

        UtFrameUtils.PseudoImage testImage = new UtFrameUtils.PseudoImage();
        originalBlacklist.save(testImage.getImageWriter());

        SqlBlackList recoveredBlackList = new SqlBlackList();
        recoveredBlackList.load(testImage.getMetaBlockReader());

        Assertions.assertEquals(originalBlacklist.getBlackLists().size(), recoveredBlackList.getBlackLists().size());
        Assertions.assertEquals(originalBlacklist.getBlackLists().get(0).id, recoveredBlackList.getBlackLists().get(0).id);
        Assertions.assertEquals(
                originalBlacklist.getBlackLists().get(0).pattern.pattern(),
                recoveredBlackList.getBlackLists().get(0).pattern.pattern()
        );
        Assertions.assertEquals(originalBlacklist.getBlackLists().get(1).id, recoveredBlackList.getBlackLists().get(1).id);
        Assertions.assertEquals(
                originalBlacklist.getBlackLists().get(1).pattern.pattern(),
                recoveredBlackList.getBlackLists().get(1).pattern.pattern()
        );
    }

    @Test
    public void testSqlBlacklistJournalOperations() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        // add blacklists

        GlobalStateMgr.getCurrentState().getEditLog().logAddSQLBlackList(new SqlBlackListPersistInfo(123, "p1"));
        GlobalStateMgr.getCurrentState().getEditLog().logAddSQLBlackList(new SqlBlackListPersistInfo(1234, "p2"));
        UtFrameUtils.PseudoJournalReplayer.replayJournalToEnd();

        List<BlackListSql> resultBlackLists = GlobalStateMgr.getCurrentState().getSqlBlackList().getBlackLists();
        Assertions.assertEquals(2, resultBlackLists.size());
        Assertions.assertEquals(123L, resultBlackLists.get(0).id);
        Assertions.assertEquals("p1", resultBlackLists.get(0).pattern.pattern());
        Assertions.assertEquals(1234L, resultBlackLists.get(1).id);
        Assertions.assertEquals("p2", resultBlackLists.get(1).pattern.pattern());

        // delete blacklists

        GlobalStateMgr.getCurrentState().getEditLog().logDeleteSQLBlackList(new DeleteSqlBlackLists(List.of(123L, 1234L)));
        UtFrameUtils.PseudoJournalReplayer.replayJournalToEnd();

        Assertions.assertTrue(
                sqlBlackList.getBlackLists().stream()
                        .noneMatch(x -> x.id == 123L || x.id == 1234L)
        );

    }

    private void mockupGlobalState() {
        MockUp<GlobalStateMgr> mockUp = new MockUp<GlobalStateMgr>() {
            @Mock
            GlobalStateMgr getCurrentState() {
                return state;
            }

            @Mock
            public SqlBlackList getSqlBlackList() {
                return sqlBlackList;
            }

            @Mock
            public boolean isLeader() {
                return true;
            }

            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };
    }
}
