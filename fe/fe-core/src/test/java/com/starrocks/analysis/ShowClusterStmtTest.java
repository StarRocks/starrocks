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

import com.starrocks.qe.ConnectContext;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowClusterStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowClusterStmtTest {
    private ConnectContext ctx;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testNormal(@Mocked WarehouseManager warehouseMgr) throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setCurrentWarehouse("testWh");

        new Expectations() {
            {
                warehouseMgr.warehouseExists("testWh");
                result = true;
                minTimes = 0;
            }
        };

        ShowClusterStmt  stmt = new ShowClusterStmt("testWh");

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testWh", stmt.getWhName());
        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());
    }

    @Test(expected = SemanticException.class)
    public void testNoDb() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ShowClusterStmt stmt = new ShowClusterStmt("");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws");
    }
}
