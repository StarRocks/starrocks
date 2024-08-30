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

package com.starrocks.lake.qe;

import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShowExecutorTest {
    private static ConnectContext ctx;

    private GlobalStateMgr globalStateMgr;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Before
    public void setUp() throws Exception {
        ctx = new ConnectContext(null);
        ctx.setCommand(MysqlCommand.COM_SLEEP);

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        // mock internal database.
        Database db = new Database();
        new Expectations(db) {
            {
                db.getFullName();
                minTimes = 0;
                result = "testDb";
            }};

        // mock external database.
        Database db1 = new Database();
        new Expectations(db1) {
            {
                db1.getFullName();
                minTimes = 0;
                result = "testDb1";

                db1.getCatalogName();
                minTimes = 0;
                result = "catalog";
            }};

        // mock globalStateMgr.
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLocalMetastore().getDb("testDb");
                minTimes = 0;
                result = db;

                globalStateMgr.getLocalMetastore().getDb("testDb1");
                minTimes = 0;
                result = db1;
            }
        };
    }

    @Test
    public void testShowCreateDb() throws DdlException {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");

        ShowCreateDbStmt stmt = new ShowCreateDbStmt("testDb");
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testDb", resultSet.getString(0));
        Assert.assertEquals(resultSet.getString(1), "CREATE DATABASE `testDb`\n" +
                        "PROPERTIES (\"storage_volume\" = \"builtin_storage_volume\")");
        Assert.assertFalse(resultSet.next());

        stmt = new ShowCreateDbStmt("testDb1");
        resultSet = ShowExecutor.execute(stmt, ctx);
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testDb1", resultSet.getString(0));
        Assert.assertEquals("CREATE DATABASE `testDb1`", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
    }
}
