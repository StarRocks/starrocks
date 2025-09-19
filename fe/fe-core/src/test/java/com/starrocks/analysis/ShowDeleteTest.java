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

package com.starrocks.analysis;

import com.starrocks.catalog.Database;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowDeleteStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ShowDeleteTest {
    private static StarRocksAssert starRocksAssert;
    private ConnectContext ctx;
    private GlobalStateMgr globalStateMgr;

    @BeforeEach
    public void setUp() throws Exception {
        ctx = new ConnectContext(null);
        ctx.setCommand(MysqlCommand.COM_SLEEP);


        // mock database
        Database db = new Database();
        new Expectations(db) {
            {
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), anyString);
                minTimes = 0;
            }
        };

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
    public void testSqlParse() throws Exception {
        ctx.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
        String showSQL = "SHOW DELETE FROM testDb";
        ShowDeleteStmt stmt = (ShowDeleteStmt) UtFrameUtils.parseStmtWithNewParser(showSQL, ctx);
        Assertions.assertEquals("testDb", stmt.getDbName());
        Assertions.assertEquals(5, new ShowResultMetaFactory().getMetadata(stmt).getColumnCount());
        Assertions.assertEquals("TableName", new ShowResultMetaFactory().getMetadata(stmt).getColumn(0).getName());
    }
}
