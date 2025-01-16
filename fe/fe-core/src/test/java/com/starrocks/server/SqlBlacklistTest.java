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

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.common.jmockit.Deencapsulation;
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
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.parseSql;

public class SqlBlacklistTest {
    GlobalStateMgr state;
    SqlBlackList sqlBlackList;
    EditLog editLog;
    ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Before
    public void beforeEach() {
        state = Deencapsulation.newInstance(GlobalStateMgr.class);
        sqlBlackList = new SqlBlackList();
        connectContext = UtFrameUtils.createDefaultCtx();
        editLog = Mockito.mock(EditLog.class);
        connectContext.setQueryId(UUID.randomUUID());
    }

    @Test
    public void testAddSQLBlacklist() throws Exception {
        mockupGlobalState();

        new Expectations() {
            {
                editLog.logAddSQLBlackList(new SqlBlackListPersistInfo(0, ".+"));
                times = 1;
            }
        };

        AddSqlBlackListStmt addStatement = (AddSqlBlackListStmt) parseSql("ADD SQLBLACKLIST \".+\";");
        Assert.assertEquals(addStatement.getSql(), ".+");

        StmtExecutor addStatementExecutor = new StmtExecutor(connectContext, addStatement);
        addStatementExecutor.execute();
        List<BlackListSql> blackLists = sqlBlackList.getBlackLists();
        Assert.assertEquals(1, blackLists.size());
        Assert.assertEquals(0, blackLists.get(0).id);
        Assert.assertEquals(".+", blackLists.get(0).pattern.pattern());
    }

    @Test
    public void testShowBlacklist() {
        mockupGlobalState();
        sqlBlackList.put(Pattern.compile("qwert"));
        sqlBlackList.put(Pattern.compile("abcde"));

        ShowSqlBlackListStmt showSqlStatement = (ShowSqlBlackListStmt) parseSql("SHOW SQLBLACKLIST");

        ShowResultSet resultSet = ShowExecutor.execute(showSqlStatement, connectContext);
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(0L, resultSet.getLong(0));
        Assert.assertEquals("qwert", resultSet.getString(1));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(1L, resultSet.getLong(0));
        Assert.assertEquals("abcde", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testDeleteSqlBlacklist() throws Exception {
        mockupGlobalState();
        long id1 = sqlBlackList.put(Pattern.compile("qwert"));
        long id2 = sqlBlackList.put(Pattern.compile("abcde"));

        new Expectations() {
            {
                editLog.logDeleteSQLBlackList(new DeleteSqlBlackLists(List.of(id1, id2)));
                times = 1;
            }
        };

        StmtExecutor deleteStatementExecutor = new StmtExecutor(connectContext, new DelSqlBlackListStmt(List.of(id1, id2)));
        deleteStatementExecutor.execute();
        Assert.assertTrue(sqlBlackList
                .getBlackLists().stream().noneMatch(x -> x.id == id1 || x.id != id2));
    }

    @Test
    public void testRedirectStatus() {
        Assert.assertEquals(
                new AddSqlBlackListStmt("ADD SQLBLACKLIST \".+\";").getRedirectStatus(),
                RedirectStatus.FORWARD_NO_SYNC
        );
        Assert.assertEquals(
                new DelSqlBlackListStmt(List.of(1L, 2L)).getRedirectStatus(),
                RedirectStatus.FORWARD_NO_SYNC
        );
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

        Assertions.assertIterableEquals(originalBlacklist.getBlackLists(), recoveredBlackList.getBlackLists());
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

        Assertions.assertIterableEquals(
                List.of(new BlackListSql(Pattern.compile("p1"), 123L),
                        new BlackListSql(Pattern.compile("p2"), 1234L)),
                GlobalStateMgr.getCurrentState().getSqlBlackList().getBlackLists()
        );

        // delete blacklists

        GlobalStateMgr.getCurrentState().getEditLog().logDeleteSQLBlackList(new DeleteSqlBlackLists(List.of(123L, 1234L)));
        UtFrameUtils.PseudoJournalReplayer.replayJournalToEnd();

        Assert.assertTrue(
                GlobalStateMgr.getCurrentState().getSqlBlackList().getBlackLists().stream()
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
