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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AlterWarehouseStmt;
import com.starrocks.sql.ast.CreateWarehouseStmt;
import com.starrocks.sql.ast.DropWarehouseStmt;
import com.starrocks.sql.ast.ResumeWarehouseStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SuspendWarehouseStmt;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class WarehouseStmtTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testCreateWarehouseParserAndAnalyzer() {
        String sql_1 = "CREATE WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof CreateWarehouseStmt);
        String sql_2 = "CREATE WAREHOUSE warehouse_2 properties(\"min_cluster\"=\"3\")";
        StatementBase stmt2 = AnalyzeTestUtil.analyzeSuccess(sql_2);
        Assert.assertEquals("CREATE WAREHOUSE 'warehouse_2' WITH PROPERTIES(\"min_cluster\"  =  \"3\")",
                stmt2.toSql());
    }

    @Test
    public void testDropWarehouseParserAndAnalyzer() {
        // test DROP WAREHOUSE warehouse_name
        String sql_1 = "DROP WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof DropWarehouseStmt);
        Assert.assertEquals("DROP WAREHOUSE 'warehouse_1'", stmt.toSql());
        String sql_2 = "DROP WAREHOUSE";
        AnalyzeTestUtil.analyzeFail(sql_2);

        // test DROP WAREHOUSE 'warehouse_name'
        String sql_3 = "DROP WAREHOUSE 'warehouse_1'";
        StatementBase stmt2 = AnalyzeTestUtil.analyzeSuccess(sql_3);
        Assert.assertTrue(stmt2 instanceof DropWarehouseStmt);
    }

    @Test
    public void testOpWarehouseParserAndAnalyzer() {
        String sql_1 = "SUSPEND WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof SuspendWarehouseStmt);
        String sql_2 = "RESUME WAREHOUSE warehouse_1";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql_2);
        Assert.assertTrue(stmt instanceof ResumeWarehouseStmt);
        String sql_3 = "ALTER WAREHOUSE warehouse_1 ADD CLUSTER";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql_3);
        Assert.assertTrue(stmt instanceof AlterWarehouseStmt);
    }

    @Test
    public void testCreateWarehouse(@Mocked StarOSAgent starOSAgent) throws Exception {
        String sql = "CREATE WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateWarehouseStmt);
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        CreateWarehouseStmt statement = (CreateWarehouseStmt) stmt;
        DDLStmtExecutor.execute(statement, connectCtx);
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Assert.assertTrue(warehouseMgr.warehouseExists("warehouse_1"));

        try {
            DDLStmtExecutor.execute(statement, connectCtx);
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("exists"));
        }

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getCurrentStarOSAgent() {
                return starOSAgent;
            }
        };

        new Expectations() {
            {
                starOSAgent.deleteWorkerGroup(anyLong);
                result = null;
                minTimes = 0;

                starOSAgent.createWorkerGroup(anyString);
                result = -1L;
                minTimes = 0;
            }
        };

        // test suspend/resume/alter warehouse
        String suspendSql = "SUSPEND WAREHOUSE warehouse_1";
        stmt = AnalyzeTestUtil.analyzeSuccess(suspendSql);
        Assert.assertTrue(stmt instanceof SuspendWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectCtx);
        Assert.assertEquals(Warehouse.WarehouseState.SUSPENDED,
                warehouseMgr.getWarehouse("warehouse_1").getState());

        String resumeSql = "RESUME WAREHOUSE warehouse_1";
        stmt = AnalyzeTestUtil.analyzeSuccess(resumeSql);
        Assert.assertTrue(stmt instanceof ResumeWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectCtx);
        Assert.assertEquals(Warehouse.WarehouseState.RUNNING,
                warehouseMgr.getWarehouse("warehouse_1").getState());

        String alterSql = "ALTER WAREHOUSE warehouse_1 set(\"size\"=\"medium\")";
        stmt = AnalyzeTestUtil.analyzeSuccess(alterSql);
        Assert.assertTrue(stmt instanceof AlterWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectCtx);
        Assert.assertEquals("medium", warehouseMgr.getWarehouse("warehouse_1").getSize());

        warehouseMgr.dropWarehouse(new DropWarehouseStmt(false,"warehouse_1"));
        Assert.assertFalse(warehouseMgr.warehouseExists("warehouse_1"));
    }

    @Test
    public void testDropWarehouse() throws Exception {
        // test DROP WAREHOUSE warehouse_name
        String createSql = "CREATE WAREHOUSE warehouse_1";
        String dropSql = "DROP WAREHOUSE warehouse_1";

        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        StatementBase createStmtBase = AnalyzeTestUtil.analyzeSuccess(createSql);
        Assert.assertTrue(createStmtBase instanceof CreateWarehouseStmt);
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        CreateWarehouseStmt createWarehouseStmt = (CreateWarehouseStmt) createStmtBase;
        DDLStmtExecutor.execute(createWarehouseStmt, connectCtx);
        Assert.assertTrue(warehouseMgr.warehouseExists("warehouse_1"));

        StatementBase dropStmtBase = AnalyzeTestUtil.analyzeSuccess(dropSql);
        Assert.assertTrue(dropStmtBase instanceof DropWarehouseStmt);
        DropWarehouseStmt dropWarehouseStmt = (DropWarehouseStmt) dropStmtBase;
        DDLStmtExecutor.execute(dropWarehouseStmt, connectCtx);
        Assert.assertFalse(warehouseMgr.warehouseExists("warehouse_1"));

        // test DROP WAREHOUSE 'warehouse_name'
        String dropSql_2 = "DROP WAREHOUSE 'warehouse_1'";

        DDLStmtExecutor.execute(createWarehouseStmt, connectCtx);
        Assert.assertTrue(warehouseMgr.warehouseExists("warehouse_1"));

        StatementBase dropStmtBase_2 = AnalyzeTestUtil.analyzeSuccess(dropSql_2);
        Assert.assertTrue(dropStmtBase_2 instanceof DropWarehouseStmt);
        DropWarehouseStmt dropWarehouseStmt_2 = (DropWarehouseStmt) dropStmtBase;
        DDLStmtExecutor.execute(dropWarehouseStmt_2, connectCtx);
        Assert.assertFalse(warehouseMgr.warehouseExists("warehouse_1"));
    }
}
