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

import com.starrocks.ha.FrontendNodeType;
import com.starrocks.meta.BlackListSql;
import com.starrocks.meta.SqlBlackList;
import com.starrocks.persist.AddSqlBlackList;
import com.starrocks.persist.DeleteSqlBlackLists;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.parseSql;

public class SqlBlacklistTest {
    @BeforeClass
    public static void beforeEach() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        UtFrameUtils.setUpForPersistTest();
    }

    @Test
    public void testAddSQLBlacklist(@Mocked EditLog editLog) throws Exception {
        GlobalStateMgr.getCurrentState().waitForReady();

        SqlBlackList sqlBlackList = new SqlBlackList();

        MockUp<GlobalStateMgr> mockUp = new MockUp<GlobalStateMgr>() {
            @Mock
            public SqlBlackList getSqlBlackList() {
                return sqlBlackList;
            }
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new Expectations(editLog) {
            {
                editLog.logAddSQLBlackList(new AddSqlBlackList(0, ".+"));
                times = 1;
            }
        };

        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUID.randomUUID());

        StatementBase addStatement = parseSql("ADD SQLBLACKLIST \".+\";");

        StmtExecutor addStatementExecutor = new StmtExecutor(connectContext, addStatement);
        addStatementExecutor.execute();
        List<BlackListSql> blackLists = sqlBlackList.getBlackLists();
        Assert.assertEquals(1, blackLists.size());
        Assert.assertEquals(0, blackLists.get(0).id);
        Assert.assertEquals(".+", blackLists.get(0).pattern.pattern());
    }

    @Test
    public void testDeleteSqlBlacklist(@Mocked EditLog editLog) throws Exception {
        SqlBlackList sqlBlackList = new SqlBlackList();
        long id1 = sqlBlackList.put(Pattern.compile("qwert"));
        long id2 = sqlBlackList.put(Pattern.compile("abcde"));

        MockUp<GlobalStateMgr> mockUp = new MockUp<GlobalStateMgr>() {
            @Mock
            public SqlBlackList getSqlBlackList() {
                return sqlBlackList;
            }
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new Expectations(editLog) {
            {
                editLog.logDeleteSQLBlackList(new DeleteSqlBlackLists(List.of(id1, id2)));
                times = 1;
            }
        };

        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUID.randomUUID());

        StmtExecutor deleteStatementExecutor = new StmtExecutor(connectContext, new DelSqlBlackListStmt(List.of(id1, id2)));
        deleteStatementExecutor.execute();
        Assert.assertTrue(sqlBlackList
                .getBlackLists().stream().noneMatch(x -> x.id == id1 || x.id != id2));
    }

    @Test
    public void testRedirectBlacklistOperationIfNotLeader() {
        MockUp<GlobalStateMgr> mockUp = new MockUp<GlobalStateMgr>() {
            @Mock
            public FrontendNodeType getFeType() {
                return FrontendNodeType.FOLLOWER;
            }

            @Mock
            public boolean isLeader() {
                return false;
            }
        };
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        StatementBase addStatement = parseSql("ADD SQLBLACKLIST \".+\";");
        StmtExecutor addStatementExecutor = new StmtExecutor(connectContext, addStatement);
        Assert.assertTrue(addStatementExecutor.isForwardToLeader());

        StatementBase deleteStatement = parseSql("DELETE SQLBLACKLIST 1,2,3;");
        StmtExecutor deleteStatementExecutor = new StmtExecutor(connectContext, deleteStatement);
        Assert.assertTrue(deleteStatementExecutor.isForwardToLeader());
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
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        // add blacklists

        GlobalStateMgr.getCurrentState().getEditLog().logAddSQLBlackList(new AddSqlBlackList(123, "p1"));
        GlobalStateMgr.getCurrentState().getEditLog().logAddSQLBlackList(new AddSqlBlackList(1234, "p2"));
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

}
