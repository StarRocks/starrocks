// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.persist;

import com.starrocks.alter.AlterJobExecutor;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.journal.JournalEntity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.transaction.MockedLocalMetaStore;
import com.starrocks.transaction.MockedMetadataMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AlterViewTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @Before
    public void before() throws Exception {
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
    }

    @AfterClass
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAlterViewSecurity() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(globalStateMgr);

        MockedLocalMetaStore localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        globalStateMgr.setLocalMetastore(localMetastore);

        MockedMetadataMgr mockedMetadataMgr = new MockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(mockedMetadataMgr);

        localMetastore.createDb("db1");

        String createTable = "create table db1.tbl1 (c1 bigint, c2 bigint, c3 bigint)";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) SqlParser.parseSingleStatement(createTable, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(createTableStmt, context);
        localMetastore.createTable(createTableStmt);

        String createView = "create view db1.view1 as select * from db1.tbl1";
        CreateViewStmt createViewStmt =
                (CreateViewStmt) SqlParser.parseSingleStatement(createView, SqlModeHelper.MODE_DEFAULT);
        Analyzer.analyze(createViewStmt, context);
        localMetastore.createView(createViewStmt);

        Table table = localMetastore.getTable("db1", "view1");
        Assert.assertTrue(table.isView());
        Assert.assertFalse(((View) table).isSecurity());

        String alterView = "alter view db1.view1 set security invoker";
        AlterViewStmt alterViewStmt = (AlterViewStmt) SqlParser.parseSingleStatement(alterView, SqlModeHelper.MODE_DEFAULT);
        new AlterJobExecutor().process(alterViewStmt, context);
        Assert.assertTrue(((View) table).isSecurity());

        AlterViewInfo info =
                (AlterViewInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_SET_VIEW_SECURITY_LOG);
        Assert.assertEquals(localMetastore.getDb("db1").getId(), info.getDbId());
        Assert.assertEquals(table.getId(), info.getTableId());
        Assert.assertTrue(info.getSecurity());
    }

    @Test
    public void testReplayAlterViewSecurity() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(globalStateMgr);

        MockedLocalMetaStore localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        globalStateMgr.setLocalMetastore(localMetastore);

        MockedMetadataMgr mockedMetadataMgr = new MockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(mockedMetadataMgr);

        localMetastore.createDb("db1");

        String createTable = "create table db1.tbl1 (c1 bigint, c2 bigint, c3 bigint)";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) SqlParser.parseSingleStatement(createTable, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(createTableStmt, context);
        localMetastore.createTable(createTableStmt);

        String createView = "create view db1.view1 as select * from db1.tbl1";
        CreateViewStmt createViewStmt =
                (CreateViewStmt) SqlParser.parseSingleStatement(createView, SqlModeHelper.MODE_DEFAULT);
        Analyzer.analyze(createViewStmt, context);
        localMetastore.createView(createViewStmt);

        Table table = localMetastore.getTable("db1", "view1");
        Assert.assertTrue(table.isView());
        Assert.assertFalse(((View) table).isSecurity());

        Database database = localMetastore.getDb("db1");

        AlterViewInfo alterViewInfo = new AlterViewInfo(database.getId(), table.getId(), true);
        EditLog editLog = new EditLog(null);

        JournalEntity journalEntity = new JournalEntity();
        journalEntity.setOpCode(OperationType.OP_SET_VIEW_SECURITY_LOG);
        journalEntity.setData(alterViewInfo);

        editLog.loadJournal(GlobalStateMgr.getCurrentState(), journalEntity);

        Assert.assertTrue(((View) table).isSecurity());
    }
}
