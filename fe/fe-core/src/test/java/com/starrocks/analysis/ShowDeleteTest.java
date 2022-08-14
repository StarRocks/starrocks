// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowDeleteTest {
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
        ShowDeleteStmt stmt = new ShowDeleteStmt(null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW DELETE FROM `testCluster:testDb`", stmt.toString());
        Assert.assertEquals(5, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("TableName", stmt.getMetaData().getColumn(0).getName());
    }

    @Test
    public void testSqlParse() throws Exception {
        ctx.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
        String showSQL = "SHOW DELETE FROM testDb";
        ShowDeleteStmt stmt = (ShowDeleteStmt) UtFrameUtils.parseStmtWithNewParser(showSQL, ctx);
        Assert.assertEquals("testDb", stmt.getDbName());
        Assert.assertEquals("SHOW DELETE FROM `testDb`", stmt.toSql());
    }
}
