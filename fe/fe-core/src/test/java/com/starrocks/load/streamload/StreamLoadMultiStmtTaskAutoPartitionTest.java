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

package com.starrocks.load.streamload;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TImmutablePartitionRequest;
import com.starrocks.thrift.TImmutablePartitionResult;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.ExplicitTxnState;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TransactionStmtExecutor;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A multi-table transaction stream load registers its transaction with DatabaseTransactionMgr when a
 * table starts loading, not only at commit: the BE looks the transaction up there while it is writing
 * (createPartition for automatic partitioning, updateImmutablePartition for automatic bucketing), and
 * the abort of a failed sub-task has to find it there as well.
 */
public class StreamLoadMultiStmtTaskAutoPartitionTest {
    private static final String DB_NAME = "test_multi_stmt_auto_partition";

    @Mocked
    private ExecuteEnv exeEnv;

    private static Database db;
    private static GlobalTransactionMgr txnMgr;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME)
                .withTable("CREATE TABLE transaction_simple (\n"
                        + "    company_id LARGEINT NOT NULL,\n"
                        + "    txn_date DATE NOT NULL,\n"
                        + "    val STRING NULL\n"
                        + ")\n"
                        + "PRIMARY KEY (company_id, txn_date)\n"
                        + "PARTITION BY date_trunc('month', txn_date)\n"
                        + "DISTRIBUTED BY HASH(company_id) BUCKETS 6\n"
                        + "PROPERTIES (\"replication_num\" = \"1\");")
                .withTable("CREATE TABLE random_bucket (\n"
                        + "    event_day DATETIME NOT NULL,\n"
                        + "    site_id INT DEFAULT '10',\n"
                        + "    pv BIGINT DEFAULT '0'\n"
                        + ")\n"
                        + "DUPLICATE KEY(event_day, site_id)\n"
                        + "DISTRIBUTED BY RANDOM\n"
                        + "PROPERTIES (\"replication_num\" = \"1\");");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        txnMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
    }

    private static OlapTable getTable(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB_NAME, name);
    }

    private static StreamLoadMultiStmtTask beginMultiStmtTask(String label) {
        StreamLoadMultiStmtTask multiTask = new StreamLoadMultiStmtTask(
                GlobalStateMgr.getCurrentState().getNextId(), db, label, "root", "127.0.0.1",
                60_000L, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        TransactionResult resp = new TransactionResult();
        multiTask.beginTxn(resp);
        Assertions.assertTrue(resp.stateOK(), resp.msg);
        Assertions.assertNotEquals(0L, multiTask.getTxnId());
        return multiTask;
    }

    /** Runs a load request up to the point where the coordinator would be started (executeTask). */
    private static void tryLoad(StreamLoadMultiStmtTask multiTask, String tableName) throws StarRocksException {
        TransactionResult resp = new TransactionResult();
        Assertions.assertNull(multiTask.tryLoad(0, tableName, resp));
        Assertions.assertTrue(resp.stateOK(), resp.msg);
    }

    private static void rollback(StreamLoadMultiStmtTask multiTask) throws StarRocksException {
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertTrue(resp.stateOK(), resp.msg);
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
        Assertions.assertNull(txnMgr.getExplicitTxnState(multiTask.getTxnId()));
    }

    @Test
    public void testCreatePartitionFindsTransactionBeforeCommit() throws Exception {
        OlapTable table = getTable("transaction_simple");
        StreamLoadMultiStmtTask multiTask = beginMultiStmtTask("multi_stmt_auto_partition");
        long txnId = multiTask.getTxnId();
        // BEGIN only creates the explicit transaction state.
        Assertions.assertNull(txnMgr.getTransactionState(db.getId(), txnId));

        tryLoad(multiTask, "transaction_simple");
        TransactionState txnState = txnMgr.getTransactionState(db.getId(), txnId);
        Assertions.assertNotNull(txnState);
        Assertions.assertSame(txnMgr.getExplicitTxnState(txnId).getTransactionState(), txnState);
        Assertions.assertEquals(TransactionStatus.PREPARE, txnState.getTransactionStatus());
        Assertions.assertEquals(db.getId(), txnState.getDbId());
        Assertions.assertEquals(List.of(table.getId()), txnState.getTableIdList());

        // The BE asks the FE for the partition of a row it cannot place while it is still writing.
        List<List<String>> partitionValues = Lists.newArrayList();
        partitionValues.add(Lists.newArrayList("2026-10-05"));
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setTxn_id(txnId);
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult result = new FrontendServiceImpl(exeEnv).createPartition(request);
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(), String.valueOf(result.getStatus()));
        Assertions.assertNotNull(table.getPartition("p202610"));
        Assertions.assertEquals(1, result.getPartitions().size());
        Assertions.assertEquals(1, txnState.getPartitionNameToTPartition(table.getId()).size());

        // A second table joins the same transaction; a repeated load reuses the existing sub-task.
        OlapTable other = getTable("random_bucket");
        tryLoad(multiTask, "random_bucket");
        Assertions.assertEquals(List.of(table.getId(), other.getId()), txnState.getTableIdList());
        Assertions.assertEquals(2, multiTask.getTasks().size());
        tryLoad(multiTask, "transaction_simple");
        Assertions.assertEquals(2, multiTask.getTasks().size());

        // The commit-time registration is idempotent on top of the early one.
        ConnectContext context = Deencapsulation.getField(multiTask, "context");
        ExplicitTxnState.ExplicitTxnStateItem item = new ExplicitTxnState.ExplicitTxnStateItem();
        item.setTabletCommitInfos(Lists.newArrayList());
        item.setTabletFailInfos(Lists.newArrayList());
        TransactionStmtExecutor.loadData(db.getId(), table.getId(), item, context);
        Assertions.assertSame(txnState, txnMgr.getTransactionState(db.getId(), txnId));
        Assertions.assertEquals(List.of(table.getId(), other.getId()), txnState.getTableIdList());

        // Rollback reaches the registered transaction.
        rollback(multiTask);
        Assertions.assertEquals(TransactionStatus.ABORTED,
                txnMgr.getTransactionState(db.getId(), txnId).getTransactionStatus());
    }

    @Test
    public void testUpdateImmutablePartitionFindsTransactionBeforeCommit() throws Exception {
        OlapTable table = getTable("random_bucket");
        StreamLoadMultiStmtTask multiTask = beginMultiStmtTask("multi_stmt_immutable_partition");
        long txnId = multiTask.getTxnId();
        tryLoad(multiTask, "random_bucket");

        TImmutablePartitionRequest request = new TImmutablePartitionRequest();
        request.setTxn_id(txnId);
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_ids(table.getPhysicalPartitions().stream()
                .map(PhysicalPartition::getId).collect(Collectors.toList()));
        TImmutablePartitionResult result = new FrontendServiceImpl(exeEnv).updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(), String.valueOf(result.getStatus()));
        Assertions.assertEquals(2, table.getPhysicalPartitions().size());

        rollback(multiTask);
    }

    @Test
    public void testRollbackAbortsRegisteredTransactionWithoutItems() throws Exception {
        StreamLoadMultiStmtTask multiTask = beginMultiStmtTask("multi_stmt_rollback_without_items");
        long txnId = multiTask.getTxnId();
        tryLoad(multiTask, "transaction_simple");
        Assertions.assertEquals(TransactionStatus.PREPARE,
                txnMgr.getTransactionState(db.getId(), txnId).getTransactionStatus());

        // No load reached commit (no explicit transaction item), e.g. the user aborts or the task
        // times out: the rollback must abort the registered transaction instead of leaving it
        // PREPARE until the transaction timeout.
        rollback(multiTask);
        Assertions.assertEquals(TransactionStatus.ABORTED,
                txnMgr.getTransactionState(db.getId(), txnId).getTransactionStatus());
    }

    @Test
    public void testFailedSubTaskAbortsRegisteredTransaction() throws Exception {
        StreamLoadMultiStmtTask multiTask = beginMultiStmtTask("multi_stmt_sub_task_abort");
        long txnId = multiTask.getTxnId();
        tryLoad(multiTask, "transaction_simple");

        // StreamLoadTask.cancelTask aborts the shared transaction; before the transaction was
        // registered at load time this failed with "transaction not found".
        StreamLoadTask subTask = multiTask.getTasks().iterator().next();
        Assertions.assertNull(subTask.cancelTask("coordinator failed"));
        Assertions.assertEquals(TransactionStatus.ABORTED,
                txnMgr.getTransactionState(db.getId(), txnId).getTransactionStatus());

        // The parent's rollback still converges on the already aborted transaction.
        rollback(multiTask);
    }

    @Test
    public void testLoadFailsWithoutSubTaskWhenTransactionIsGone() throws Exception {
        StreamLoadMultiStmtTask multiTask = beginMultiStmtTask("multi_stmt_txn_gone");
        long txnId = multiTask.getTxnId();
        // e.g. removed by the transaction cleaner once the transaction timed out
        txnMgr.clearExplicitTxnState(txnId);

        Assertions.assertThrows(StarRocksException.class,
                () -> multiTask.tryLoad(0, "transaction_simple", new TransactionResult()));
        Assertions.assertTrue(multiTask.getTasks().isEmpty());
        Assertions.assertNull(txnMgr.getTransactionState(db.getId(), txnId));
    }
}
