// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack;

import com.starrocks.common.DdlException;
import com.starrocks.epack.lake.StarOSAgentEpack;
import com.starrocks.epack.server.WarehouseManagerEpack;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.StatementBase;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WarehouseStmtTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.initWithoutTableAndDb();
    }

    @Before
    public void setUp() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
    }

    @Test
    public void testCreateWarehouseParserAndAnalyzer() {
        String sql1 = "CREATE WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql1);
        Assert.assertTrue(stmt instanceof CreateWarehouseStmt);
        String sql2 = "CREATE WAREHOUSE warehouse_2 properties(\"min_cluster\"=\"3\")";
        StatementBase stmt2 = AnalyzeTestUtil.analyzeSuccess(sql2);
        Assert.assertEquals("CREATE WAREHOUSE 'warehouse_2' WITH PROPERTIES(\"min_cluster\"  =  \"3\")",
                stmt2.toSql());
    }

    @Test
    public void testDropWarehouseParserAndAnalyzer() {
        // test DROP WAREHOUSE warehouse_name
        String sql1 = "DROP WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql1);
        Assert.assertTrue(stmt instanceof DropWarehouseStmt);
        Assert.assertEquals("DROP WAREHOUSE 'warehouse_1'", stmt.toSql());
        String sql2 = "DROP WAREHOUSE";
        AnalyzeTestUtil.analyzeFail(sql2);

        // test DROP WAREHOUSE 'warehouse_name'
        String sql3 = "DROP WAREHOUSE 'warehouse_1'";
        StatementBase stmt2 = AnalyzeTestUtil.analyzeSuccess(sql3);
        Assert.assertTrue(stmt2 instanceof DropWarehouseStmt);
    }

    @Test
    public void testOpWarehouseParserAndAnalyzer() {
        String sql1 = "SUSPEND WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql1);
        Assert.assertTrue(stmt instanceof SuspendWarehouseStmt);
        String sql2 = "RESUME WAREHOUSE warehouse_1";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql2);
        Assert.assertTrue(stmt instanceof ResumeWarehouseStmt);
    }

    @Test
    public void testOperateWarehouse(@Mocked StarOSAgentEpack starOSAgent) throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
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

        String sql = "CREATE WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateWarehouseStmt);
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        CreateWarehouseStmt statement = (CreateWarehouseStmt) stmt;
        DDLStmtExecutor.execute(statement, connectCtx);
        WarehouseManagerEpack warehouseMgr = (WarehouseManagerEpack) GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Assert.assertTrue(warehouseMgr.warehouseExists("warehouse_1"));

        try {
            DDLStmtExecutor.execute(statement, connectCtx);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("exists"));
        }

        // test suspend/resume/alter warehouse
        String suspendSql = "SUSPEND WAREHOUSE warehouse_1";
        stmt = AnalyzeTestUtil.analyzeSuccess(suspendSql);
        Assert.assertTrue(stmt instanceof SuspendWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectCtx);
        Assert.assertEquals(LocalWarehouse.WarehouseState.SUSPENDED,
                ((LocalWarehouse) warehouseMgr.getWarehouse("warehouse_1")).getState());

        String resumeSql = "RESUME WAREHOUSE warehouse_1";
        stmt = AnalyzeTestUtil.analyzeSuccess(resumeSql);
        Assert.assertTrue(stmt instanceof ResumeWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectCtx);
        Assert.assertEquals(LocalWarehouse.WarehouseState.AVAILABLE,
                ((LocalWarehouse) warehouseMgr.getWarehouse("warehouse_1")).getState());

        warehouseMgr.dropWarehouse(new DropWarehouseStmt(false, "warehouse_1"));
        Assert.assertFalse(warehouseMgr.warehouseExists("warehouse_1"));
    }

}
