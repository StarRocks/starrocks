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

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.DeleteSqlDigestBlackLists;
import com.starrocks.persist.SqlDigestBlackListPersistInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AddSqlDigestBlackListStmt;
import com.starrocks.sql.ast.DelSqlDigestBlackListStmt;
import com.starrocks.sql.ast.ShowSqlDigestBlackListStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.parseSql;

public class SqlDigestBlacklistTest {
    SqlDigestBlackList sqlDigestBlackList;
    ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void beforeEach() {
        sqlDigestBlackList = GlobalStateMgr.getCurrentState().getSqlDigestBlackList();
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
        sqlDigestBlackList.cleanup();
    }

    @Test
    public void testAddDigestSqlBlacklist() throws Exception {
        AddSqlDigestBlackListStmt stmt =
                (AddSqlDigestBlackListStmt) parseSql("ADD SQL DIGEST BLACKLIST 389d2ef8d98994a4290b5d2e1d5838aa");
        Assertions.assertEquals(stmt.getDigest(), "389d2ef8d98994a4290b5d2e1d5838aa");

        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, stmt);
        stmtExecutor.execute();
        Set<String> blackLists = sqlDigestBlackList.getDigests();
        Assertions.assertEquals(1, blackLists.size());
        Assertions.assertTrue(blackLists.contains("389d2ef8d98994a4290b5d2e1d5838aa"));
    }

    @Test
    public void testShowSqlDigestBlacklist() {
        sqlDigestBlackList.addDigest("qwert");
        sqlDigestBlackList.addDigest("abcde");

        ShowSqlDigestBlackListStmt stmt = (ShowSqlDigestBlackListStmt) parseSql("SHOW SQL DIGEST BLACKLIST");

        ShowResultSet resultSet = ShowExecutor.execute(stmt, connectContext);
        Set<String> actualDigests = new HashSet<>();
        while (resultSet.next()) {
            actualDigests.add(resultSet.getString(0));
        }
        Assertions.assertEquals(Set.of("qwert", "abcde"), actualDigests);
    }

    @Test
    public void testDelDigestSqlBlacklist() throws Exception {
        sqlDigestBlackList.addDigest("qwert");
        StmtExecutor stmtExecutor =
                new StmtExecutor(connectContext, new DelSqlDigestBlackListStmt(List.of("qwert")));
        stmtExecutor.execute();
        Assertions.assertFalse(sqlDigestBlackList.getDigests().contains("qwert"));
    }

    @Test
    public void testSaveLoadBlackListImage() throws Exception {
        SqlDigestBlackList originalBlacklist = new SqlDigestBlackList();
        originalBlacklist.addDigest("asdf");

        UtFrameUtils.PseudoImage testImage = new UtFrameUtils.PseudoImage();
        originalBlacklist.save(testImage.getImageWriter());

        SqlDigestBlackList recoveredBlackList = new SqlDigestBlackList();
        recoveredBlackList.load(testImage.getMetaBlockReader());

        Assertions.assertEquals(originalBlacklist.getDigests().size(), recoveredBlackList.getDigests().size());
        Assertions.assertEquals(originalBlacklist.getDigests(), recoveredBlackList.getDigests());
        GlobalStateMgr.getCurrentState().getSqlDigestBlackList().deleteDigests(List.of("asdf"));
    }

    @Test
    public void testSqlDigestBlackListJournalOperations() throws Exception {
        // add blacklists
        GlobalStateMgr.getCurrentState().getEditLog()
                .logAddSqlDigestBlackList(new SqlDigestBlackListPersistInfo("a"), wal -> {});
        GlobalStateMgr.getCurrentState().getEditLog()
                .logAddSqlDigestBlackList(new SqlDigestBlackListPersistInfo("b"), wal -> {});
        UtFrameUtils.PseudoJournalReplayer.replayJournalToEnd();

        Set<String> resultBlackLists = GlobalStateMgr.getCurrentState().getSqlDigestBlackList().getDigests();
        Assertions.assertEquals(2, resultBlackLists.size());
        Assertions.assertTrue(resultBlackLists.contains("a"));
        Assertions.assertTrue(resultBlackLists.contains("b"));

        // delete blacklists
        GlobalStateMgr.getCurrentState().getEditLog()
                .logDeleteSqlDigestBlackList(new DeleteSqlDigestBlackLists(List.of("a", "b")), wal -> {});
        UtFrameUtils.PseudoJournalReplayer.replayJournalToEnd();

        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getSqlDigestBlackList().getDigests().isEmpty());
    }
}
