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

package com.starrocks.transaction;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ExplicitTxnWarehouseTest {

    private static final long WH_A_ID = 100L;
    private static final String WH_A_NAME = "wh_a";

    @BeforeAll
    public static void beforeAll() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        // Make all warehouses' resources available for testing.
        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return computeResource != null;
            }
        };

        // Add a second warehouse.
        GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .addWarehouse(new DefaultWarehouse(WH_A_ID, WH_A_NAME));

        MetricRepo.init();

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db_wh_test");
        starRocksAssert.useDatabase("db_wh_test");
        starRocksAssert.withTable(
                "create table db_wh_test.tbl1 (c1 bigint, c2 bigint, c3 bigint)" +
                " duplicate key(c1) distributed by hash(c1) buckets 1" +
                " properties('replication_num'='1')");
    }

    /** Stub out Coordinator so loadData() does not actually execute anything. */
    private void mockCoordinator() {
        new MockUp<DefaultCoordinator>() {
            @Mock
            public void exec() throws StarRocksException, RpcException, InterruptedException {
            }

            @Mock
            public boolean join(int timeoutSecond) {
                return true;
            }

            @Mock
            public boolean isDone() {
                return true;
            }

            @Mock
            public Status getExecStatus() {
                return Status.OK;
            }

            @Mock
            public Map<String, String> getLoadCounters() {
                Map<String, String> c = new HashMap<>();
                c.put(LoadEtlTask.DPP_NORMAL_ALL, "0");
                c.put(LoadEtlTask.DPP_ABNORMAL_ALL, "0");
                c.put(LoadJob.LOADED_BYTES, "0");
                return c;
            }
        };
    }

    private ConnectContext newContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setQualifiedUser("root");
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setExecutionId(new TUniqueId(
                UUID.randomUUID().getMostSignificantBits(),
                UUID.randomUUID().getLeastSignificantBits()));
        ctx.setLastQueryId(UUID.randomUUID());
        ctx.setDatabase("db_wh_test");
        // Pre-set compute resource so getCurrentComputeResource() returns it
        // without hitting the real acquisition path.
        ctx.setCurrentComputeResource(
                WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID));
        return ctx;
    }

    /**
     * After loadData(), txnState.computeResource must reflect the session's
     * current warehouse, not the warehouse captured at BEGIN time.
     */
    @Test
    public void testLoadDataSyncsComputeResource() {
        mockCoordinator();
        ConnectContext ctx = newContext();

        // BEGIN — binds txn to default warehouse.
        TransactionStmtExecutor.beginStmt(ctx, new BeginStmt(NodePosition.ZERO));
        long txnId = ctx.getTxnId();
        Assertions.assertNotEquals(0, txnId);

        ExplicitTxnState explicitTxnState = GlobalStateMgr.getCurrentState()
                .getGlobalTransactionMgr().getExplicitTxnState(txnId);
        TransactionState txnState = explicitTxnState.getTransactionState();
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                txnState.getComputeResource().getWarehouseId());

        // Simulate warehouse switch (SET warehouse / SET_VAR).
        ctx.getSessionVariable().setWarehouseName(WH_A_NAME);
        ctx.setCurrentComputeResource(WarehouseComputeResource.of(WH_A_ID));

        // Execute DML via loadData().
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db_wh_test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("db_wh_test", "tbl1");

        String sql = "insert into db_wh_test.tbl1 values(1,2,3)";
        DmlStmt stmt = (DmlStmt) SqlParser.parseSingleStatement(
                sql, ctx.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, ctx);

        TransactionStmtExecutor.loadData(
                database, table, new ExecPlan(), stmt, stmt.getOrigStmt(), ctx);
        Assertions.assertFalse(ctx.getState().isError(), ctx.getState().getErrorMessage());

        // The fix: txnState's compute resource must now be wh_a, not default.
        Assertions.assertEquals(WH_A_ID,
                txnState.getComputeResource().getWarehouseId());

        // Cleanup
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().clearExplicitTxnState(txnId);
        ctx.setTxnId(0);
    }

    /**
     * Two DMLs with different warehouses — txnState.computeResource must
     * reflect the LAST DML's warehouse (last-writer-wins).
     */
    @Test
    public void testMultipleDmlsDifferentWarehouses() {
        mockCoordinator();
        ConnectContext ctx = newContext();

        TransactionStmtExecutor.beginStmt(ctx, new BeginStmt(NodePosition.ZERO));
        long txnId = ctx.getTxnId();

        ExplicitTxnState explicitTxnState = GlobalStateMgr.getCurrentState()
                .getGlobalTransactionMgr().getExplicitTxnState(txnId);
        TransactionState txnState = explicitTxnState.getTransactionState();

        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db_wh_test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("db_wh_test", "tbl1");

        // First DML on wh_a.
        ctx.getSessionVariable().setWarehouseName(WH_A_NAME);
        ctx.setCurrentComputeResource(WarehouseComputeResource.of(WH_A_ID));

        String sql = "insert into db_wh_test.tbl1 values(1,2,3)";
        DmlStmt stmt = (DmlStmt) SqlParser.parseSingleStatement(
                sql, ctx.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, ctx);

        TransactionStmtExecutor.loadData(
                database, table, new ExecPlan(), stmt, stmt.getOrigStmt(), ctx);
        Assertions.assertFalse(ctx.getState().isError(), ctx.getState().getErrorMessage());

        Assertions.assertEquals(WH_A_ID,
                txnState.getComputeResource().getWarehouseId());

        // Second DML switches back to default warehouse.
        ctx.getSessionVariable().setWarehouseName(WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        ctx.setCurrentComputeResource(
                WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID));
        // Need fresh execution ID for the second DML.
        ctx.setExecutionId(new TUniqueId(
                UUID.randomUUID().getMostSignificantBits(),
                UUID.randomUUID().getLeastSignificantBits()));

        sql = "insert into db_wh_test.tbl1 values(4,5,6)";
        stmt = (DmlStmt) SqlParser.parseSingleStatement(
                sql, ctx.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, ctx);

        TransactionStmtExecutor.loadData(
                database, table, new ExecPlan(), stmt, stmt.getOrigStmt(), ctx);
        Assertions.assertFalse(ctx.getState().isError(), ctx.getState().getErrorMessage());

        // Last-writer-wins: default warehouse.
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                txnState.getComputeResource().getWarehouseId());

        // Cleanup
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().clearExplicitTxnState(txnId);
        ctx.setTxnId(0);
    }
}
