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

import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterWarehouseStmt;
import com.starrocks.sql.ast.CreateWarehouseStmt;
import com.starrocks.sql.ast.ShowClustersStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShowClusterStmtTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        String createWarehouse = "CREATE WAREHOUSE testWh";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(createWarehouse);
        Assert.assertTrue(stmt instanceof CreateWarehouseStmt);
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        CreateWarehouseStmt statement = (CreateWarehouseStmt) stmt;
        DDLStmtExecutor.execute(statement, connectCtx);
        starRocksAssert = new StarRocksAssert();

        ctx = new ConnectContext(null);
    }

    @Test
    public void testNormal(@Mocked StarOSAgent starOSAgent) throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getCurrentStarOSAgent() {
                return starOSAgent;
            }
        };

        new Expectations() {
            {
                starOSAgent.createWorkerGroup(anyString);
                result = -1L;
                minTimes = 0;
            }
        };

        AlterWarehouseStmt addClusterStmt = (AlterWarehouseStmt)
                UtFrameUtils.parseStmtWithNewParser("alter warehouse testWh add cluster", ctx);
        GlobalStateMgr.getCurrentState().getWarehouseMgr().alterWarehouse(addClusterStmt);

        ShowClustersStmt stmt = new ShowClustersStmt("testWh");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testWh", stmt.getWarehouseName());
        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());

        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        ShowResultSetMetaData metaData = resultSet.getMetaData();
        Assert.assertEquals("ClusterId", metaData.getColumn(0).getName());
        Assert.assertEquals("Pending", metaData.getColumn(1).getName());
        Assert.assertEquals("Running", metaData.getColumn(2).getName());
    }

    @Test(expected = SemanticException.class)
    public void testNoWh() throws Exception {
        ShowClustersStmt stmt = new ShowClustersStmt("");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws");
    }
}
