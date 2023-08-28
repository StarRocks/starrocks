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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.epack.sql.ast.ShowWarehousesStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Collections;
import java.util.HashSet;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ShowWarehousesStmtTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        starRocksAssert = new StarRocksAssert();

        ctx = new ConnectContext(null);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(new HashSet<>(Collections.singletonList(-1L)));
    }

    @Test
    public void testShowWarehousesParserAndAnalyzer() {
        String sql_1 = "SHOW WAREHOUSES";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof ShowWarehousesStmt);
    }

    @Test
    public void testShowWarehousesNormal() throws AnalysisException, DdlException {
        ShowWarehousesStmt stmt = new ShowWarehousesStmt(null, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        ShowResultSetMetaData metaData = resultSet.getMetaData();
        Assert.assertEquals("Id", metaData.getColumn(0).getName());
        Assert.assertEquals("Name", metaData.getColumn(1).getName());
        Assert.assertEquals("State", metaData.getColumn(2).getName());
        Assert.assertEquals("Size", metaData.getColumn(3).getName());
        Assert.assertEquals("CurrentClusterCount", metaData.getColumn(4).getName());
        Assert.assertEquals("MaxClusterCount", metaData.getColumn(5).getName());
        Assert.assertEquals("StartedClusters", metaData.getColumn(6).getName());
        Assert.assertEquals("RunningSql", metaData.getColumn(7).getName());
        Assert.assertEquals("QueuedSql", metaData.getColumn(8).getName());
        Assert.assertEquals("CreatedOn", metaData.getColumn(9).getName());
        Assert.assertEquals("ResumedOn", metaData.getColumn(10).getName());
        Assert.assertEquals("UpdatedOn", metaData.getColumn(11).getName());
        Assert.assertEquals("Comment", metaData.getColumn(12).getName());
        Assert.assertEquals("AVAILABLE", resultSet.getResultRows().get(0).get(2).toString());
    }
}
