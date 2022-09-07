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

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ShowDbStmtTest {

    private static StarRocksAssert starRocksAssert;
    private ConnectContext ctx;
    private GlobalStateMgr globalStateMgr;

    @Before
    public void setUp() throws Exception {
        ctx = new ConnectContext(null);
        ctx.setCommand(MysqlCommand.COM_SLEEP);


        // mock database
        Database db = new Database();
        new Expectations(db) {
            {
                db.readLock();
                minTimes = 0;

                db.readUnlock();
                minTimes = 0;

                db.getTable(anyString);
                minTimes = 0;
            }
        };

        // mock auth
        Auth auth = AccessTestUtil.fetchAdminAccess();

        // mock globalStateMgr.
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getDb("testCluster:testDb");
                minTimes = 0;
                result = db;

                globalStateMgr.getDb("testCluster:emptyDb");
                minTimes = 0;
                result = null;

                globalStateMgr.getAuth();
                minTimes = 0;
                result = auth;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };

        // mock scheduler
        ConnectScheduler scheduler = new ConnectScheduler(10);
        new Expectations(scheduler) {
            {
                scheduler.listConnection("testCluster:testUser");
                minTimes = 0;
                result = Lists.newArrayList(ctx.toThreadInfo());
            }
        };

        ctx.setConnectScheduler(scheduler);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        ctx.setQualifiedUser("testCluster:testUser");

        new Expectations(ctx) {
            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        final Analyzer analyzer = AccessTestUtil.fetchBlockAnalyzer();
        ShowDbStmt stmt = new ShowDbStmt(null);
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getPattern());
        Assert.assertEquals(1, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());

        stmt = new ShowDbStmt("abc");
        stmt.analyze(analyzer);
        Assert.assertEquals("abc", stmt.getPattern());
        Assert.assertEquals(1, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());

        stmt = new ShowDbStmt(null, "hive_catalog_1");
        stmt.analyze(analyzer);
        Assert.assertEquals("hive_catalog_1", stmt.getCatalogName());

        stmt = new ShowDbStmt(null, null, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        ShowResultSetMetaData metaData = resultSet.getMetaData();
        Assert.assertEquals(metaData.getColumn(0).getName(), "Database");
    }

    @Test
    public void testShowSchemas() throws Exception {
        ctx.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
        String showSQL = "show schemas";
        ShowDbStmt showDbStmt = (ShowDbStmt) UtFrameUtils.parseStmtWithNewParser(showSQL, ctx);
    }

}