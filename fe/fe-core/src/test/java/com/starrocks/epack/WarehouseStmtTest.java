// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack;

import com.starrocks.common.DdlException;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.epack.warehouse.WarehouseManagerEPack;
import com.starrocks.epack.warehouse.WarehouseProperty;
import com.starrocks.epack.warehouse.WarehouseSlotManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.BaseSlotTracker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class WarehouseStmtTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.initWithoutTableAndDb(RunMode.SHARED_DATA);
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
    public void testOperateWarehouse() throws Exception {
        String sql = "CREATE WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateWarehouseStmt);
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        CreateWarehouseStmt statement = (CreateWarehouseStmt) stmt;
        DDLStmtExecutor.execute(statement, connectCtx);
        WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) GlobalStateMgr.getCurrentState().getWarehouseMgr();
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

    @Test
    public void testOperateWarehouseWithQueryQueue1() throws Exception {
        String sql = "CREATE WAREHOUSE warehouse_1 PROPERTIES (\n" +
                "'enable_query_queue' = 'true',\n" +
                "'query_queue_max_queued_queries' = '100',\n" +
                "'query_queue_pending_timeout_second' = '600')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateWarehouseStmt);
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        CreateWarehouseStmt statement = (CreateWarehouseStmt) stmt;
        DDLStmtExecutor.execute(statement, connectCtx);
        WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Assert.assertTrue(warehouseMgr.warehouseExists("warehouse_1"));

        LocalWarehouse warehouse = (LocalWarehouse) warehouseMgr.getWarehouse("warehouse_1");
        long warehouseId = warehouse.getId();
        WarehouseProperty property = warehouse.getProperty();
        Assert.assertTrue(property.isEnableQueryQueue());
        Assert.assertEquals(100, property.getQueryQueueMaxQueuedQueries());
        Assert.assertEquals(600, property.getQueryQueuePendingTimeoutSecond());

        BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        Assert.assertTrue(slotManager instanceof WarehouseSlotManager);
        WarehouseSlotManager warehouseSlotManager = (WarehouseSlotManager) slotManager;
        Map<Long, BaseSlotTracker> warehouseIdToSlotTracker = warehouseSlotManager.getWarehouseIdToSlotTracker();
        Assert.assertTrue(warehouseIdToSlotTracker.containsKey(warehouseId));

        // test query options
        Assert.assertTrue(warehouseSlotManager.isEnableQueryQueueV2(warehouseId));
        Assert.assertEquals(100, warehouseSlotManager.getQueryQueueMaxQueuedQueries(warehouseId));
        Assert.assertEquals(600, warehouseSlotManager.getQueryQueuePendingTimeoutSecond(warehouseId));

        // recreate warehouse
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

        warehouseIdToSlotTracker = warehouseSlotManager.getWarehouseIdToSlotTracker();
        Assert.assertTrue(warehouseIdToSlotTracker.containsKey(warehouse.getId()));

        String resumeSql = "RESUME WAREHOUSE warehouse_1";
        stmt = AnalyzeTestUtil.analyzeSuccess(resumeSql);
        Assert.assertTrue(stmt instanceof ResumeWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectCtx);
        Assert.assertEquals(LocalWarehouse.WarehouseState.AVAILABLE,
                ((LocalWarehouse) warehouseMgr.getWarehouse("warehouse_1")).getState());
        warehouseIdToSlotTracker = warehouseSlotManager.getWarehouseIdToSlotTracker();
        Assert.assertTrue(warehouseIdToSlotTracker.containsKey(warehouse.getId()));

        warehouseMgr.dropWarehouse(new DropWarehouseStmt(false, "warehouse_1"));
        Assert.assertFalse(warehouseMgr.warehouseExists("warehouse_1"));
        warehouseIdToSlotTracker = warehouseSlotManager.getWarehouseIdToSlotTracker();
        Assert.assertFalse(warehouseIdToSlotTracker.containsKey(warehouse.getId()));
    }

    @Test
    public void testOperateWarehouseWithQueryQueue2() throws Exception {
        String sql = "CREATE WAREHOUSE warehouse_1;";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateWarehouseStmt);
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        CreateWarehouseStmt statement = (CreateWarehouseStmt) stmt;
        DDLStmtExecutor.execute(statement, connectCtx);
        WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Assert.assertTrue(warehouseMgr.warehouseExists("warehouse_1"));

        LocalWarehouse warehouse = (LocalWarehouse) warehouseMgr.getWarehouse("warehouse_1");
        long warehouseId = warehouse.getId();
        WarehouseProperty property = warehouse.getProperty();
        Assert.assertFalse(property.isEnableQueryQueue());

        BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        Assert.assertTrue(slotManager instanceof WarehouseSlotManager);
        WarehouseSlotManager warehouseSlotManager = (WarehouseSlotManager) slotManager;
        Map<Long, BaseSlotTracker> warehouseIdToSlotTracker = warehouseSlotManager.getWarehouseIdToSlotTracker();
        Assert.assertFalse(warehouseIdToSlotTracker.containsKey(warehouseId));

        // test query options
        Assert.assertFalse(warehouseSlotManager.isEnableQueryQueueV2(warehouseId));
        Assert.assertEquals(-1, warehouseSlotManager.getQueryQueueConcurrencyLimit(warehouseId));

        // alter warehouse
        sql = "ALTER WAREHOUSE warehouse_1\n" +
                "SET (\n" +
                "    'enable_query_queue' = 'true'\n" +
                ")";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof AlterWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectCtx);
        // refresh warehouse's property
        property = warehouse.getProperty();
        Assert.assertTrue(property.isEnableQueryQueue());
        // default values: 600
        Assert.assertEquals(Math.max(600, GlobalVariable.getQueryQueuePendingTimeoutSecond()),
                property.getQueryQueuePendingTimeoutSecond());

        // alter warehouse
        sql = "ALTER WAREHOUSE warehouse_1\n" +
                "SET (\n" +
                "    'enable_query_queue' = 'true',\n" +
                "    'query_queue_max_queued_queries' = '100',\n" +
                "    'query_queue_concurrency_limit' = '60',\n" +
                "    'query_queue_pending_timeout_second' = '600'\n" +
                ")";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof AlterWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectCtx);
        // refresh warehouse's property
        property = warehouse.getProperty();
        Assert.assertTrue(property.isEnableQueryQueue());
        Assert.assertEquals(100, property.getQueryQueueMaxQueuedQueries());
        Assert.assertEquals(600, property.getQueryQueuePendingTimeoutSecond());

        Assert.assertTrue(warehouseSlotManager.isEnableQueryQueueV2(warehouseId));
        Assert.assertEquals(100, warehouseSlotManager.getQueryQueueMaxQueuedQueries(warehouseId));
        Assert.assertEquals(600, warehouseSlotManager.getQueryQueuePendingTimeoutSecond(warehouseId));
        Assert.assertEquals(60, warehouseSlotManager.getQueryQueueConcurrencyLimit(warehouseId));

        // recreate warehouse
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
        warehouseIdToSlotTracker = warehouseSlotManager.getWarehouseIdToSlotTracker();
        Assert.assertTrue(warehouseIdToSlotTracker.containsKey(warehouse.getId()));

        String resumeSql = "RESUME WAREHOUSE warehouse_1";
        stmt = AnalyzeTestUtil.analyzeSuccess(resumeSql);
        Assert.assertTrue(stmt instanceof ResumeWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectCtx);
        Assert.assertEquals(LocalWarehouse.WarehouseState.AVAILABLE,
                ((LocalWarehouse) warehouseMgr.getWarehouse("warehouse_1")).getState());
        warehouseIdToSlotTracker = warehouseSlotManager.getWarehouseIdToSlotTracker();
        Assert.assertTrue(warehouseIdToSlotTracker.containsKey(warehouse.getId()));

        warehouseMgr.dropWarehouse(new DropWarehouseStmt(false, "warehouse_1"));
        Assert.assertFalse(warehouseMgr.warehouseExists("warehouse_1"));
        warehouseIdToSlotTracker = warehouseSlotManager.getWarehouseIdToSlotTracker();
        Assert.assertFalse(warehouseIdToSlotTracker.containsKey(warehouse.getId()));
    }

    @Test
    public void testOperateWarehouseWithQueryQueue3() throws Exception {
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) GlobalStateMgr.getCurrentState().getWarehouseMgr();
        LocalWarehouse warehouse = (LocalWarehouse) warehouseMgr.getWarehouse("default_warehouse");

        long warehouseId = warehouse.getId();
        WarehouseProperty property = warehouse.getProperty();
        Assert.assertFalse(property.isEnableQueryQueue());
        Assert.assertEquals(-1, property.getQueryQueueConcurrencyLimit());

        BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        Assert.assertTrue(slotManager instanceof WarehouseSlotManager);
        WarehouseSlotManager warehouseSlotManager = (WarehouseSlotManager) slotManager;
        Map<Long, BaseSlotTracker> warehouseIdToSlotTracker = warehouseSlotManager.getWarehouseIdToSlotTracker();
        Assert.assertFalse(warehouseIdToSlotTracker.containsKey(warehouseId));

        // test query options
        Assert.assertFalse(warehouseSlotManager.isEnableQueryQueueV2(warehouseId));

        // alter warehouse
        String sql = "ALTER WAREHOUSE default_warehouse \n" +
                "SET (\n" +
                "    'enable_query_queue' = 'true',\n" +
                "    'enable_query_queue_load' = 'true',\n" +
                "    'enable_query_queue_statistic' = 'true',\n" +
                "    'query_queue_max_queued_queries' = '100',\n" +
                "    'query_queue_concurrency_limit' = '10',\n" +
                "    'query_queue_pending_timeout_second' = '600'\n" +
                ")";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof AlterWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectCtx);

        // refresh warehouse's property
        property = warehouse.getProperty();
        Assert.assertTrue(property.isEnableQueryQueue());
        Assert.assertTrue(property.isEnableQueryQueueLoad());
        Assert.assertTrue(property.isEnableQueryQueueStatistic());
        Assert.assertEquals(100, property.getQueryQueueMaxQueuedQueries());
        Assert.assertEquals(600, property.getQueryQueuePendingTimeoutSecond());

        Assert.assertTrue(warehouseSlotManager.isEnableQueryQueueV2(warehouseId));
        Assert.assertEquals(100, warehouseSlotManager.getQueryQueueMaxQueuedQueries(warehouseId));
        Assert.assertEquals(600, warehouseSlotManager.getQueryQueuePendingTimeoutSecond(warehouseId));
        Assert.assertEquals(10, property.getQueryQueueConcurrencyLimit());
    }
}
