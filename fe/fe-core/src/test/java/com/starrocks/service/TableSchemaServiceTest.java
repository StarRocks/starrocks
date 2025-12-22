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

package com.starrocks.service;

import com.starrocks.alter.AlterJobMgr;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.LakeTableAsyncFastSchemaChangeJob;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.lake.LakeTable;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TGetTableSchemaRequest;
import com.starrocks.thrift.TGetTableSchemaResponse;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTableSchemaKey;
import com.starrocks.thrift.TTableSchemaRequestSource;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class TableSchemaServiceTest extends StarRocksTestBase {
    private static final String DB_NAME = "test_table_schema_service";
    private static Database db;
    private static ConnectContext connectContext;

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();

        // Create test database
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        connectContext.setDatabase(DB_NAME);
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (connectContext != null) {
            GlobalStateMgr.getCurrentState().clear();
        }
    }

    // Helper methods
    private LakeTable createTable(String tableName) throws Exception {
        String sql = String.format("CREATE TABLE %s (c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) " +
                "BUCKETS 1 PROPERTIES('replication_num'='1', 'fast_schema_evolution'='true')", tableName);
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        return (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), createTableStmt.getTableName());
    }

    private LakeTableAsyncFastSchemaChangeJob executeAlterAndWaitFinish(LakeTable table, String sql)
            throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
        AlterJobMgr alterJobMgr = GlobalStateMgr.getCurrentState().getAlterJobMgr();
        List<AlterJobV2> jobs = alterJobMgr.getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        Assertions.assertEquals(1, jobs.size());
        AlterJobV2 alterJob = jobs.get(0);
        long startTime = System.currentTimeMillis();
        long timeoutMs = 10 * 60 * 1000; // 10 minutes timeout
        while (alterJob.getJobState() != AlterJobV2.JobState.FINISHED
                || table.getState() != OlapTable.OlapTableState.NORMAL) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - startTime > timeoutMs) {
                throw new RuntimeException(
                        String.format("Alter job timeout after 10 minutes. Job state: %s, table state: %s",
                                alterJob.getJobState(), table.getState()));
            }
            alterJob.run();
            Thread.sleep(100);
        }
        Assertions.assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, alterJob);
        return (LakeTableAsyncFastSchemaChangeJob) alterJob;
    }

    private Coordinator executeQueryAndRegister(String sql, TUniqueId queryId) throws Exception {
        connectContext.setExecutionId(queryId);
        DefaultCoordinator coordinator = UtFrameUtils.getPlanAndStartScheduling(connectContext, sql).second;
        QeProcessorImpl.INSTANCE.registerQuery(queryId, coordinator);
        return coordinator;
    }

    private static TTableSchemaKey createSchemaKey(long schemaId, long dbId, long tableId) {
        TTableSchemaKey schemaKey = new TTableSchemaKey();
        schemaKey.setSchema_id(schemaId);
        schemaKey.setDb_id(dbId);
        schemaKey.setTable_id(tableId);
        return schemaKey;
    }

    private static TGetTableSchemaRequest createScanRequest(long schemaId, long dbId, long tableId, TUniqueId queryId) {
        TGetTableSchemaRequest request = new TGetTableSchemaRequest();
        request.setSchema_key(createSchemaKey(schemaId, dbId, tableId));
        request.setSource(TTableSchemaRequestSource.SCAN);
        request.setQuery_id(queryId);
        return request;
    }

    private static TGetTableSchemaRequest createLoadRequest(long schemaId, long dbId, long tableId, long txnId) {
        TGetTableSchemaRequest request = new TGetTableSchemaRequest();
        request.setSchema_key(createSchemaKey(schemaId, dbId, tableId));
        request.setSource(TTableSchemaRequestSource.LOAD);
        request.setTxn_id(txnId);
        return request;
    }

    // Parameter Validation Tests
    @ParameterizedTest(name = "{0}")
    @MethodSource("parameterValidationTestCases")
    public void testParameterValidation(String testName, Consumer<TGetTableSchemaRequest> requestBuilder,
                                                       String expectedErrorMsg) {
        TGetTableSchemaRequest request = new TGetTableSchemaRequest();
        requestBuilder.accept(request);

        TGetTableSchemaResponse response = TableSchemaService.getTableSchema(request);
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, response.getStatus().getStatus_code());
        Assertions.assertTrue(response.getStatus().getError_msgs().get(0).contains(expectedErrorMsg));
    }

    private static Stream<Arguments> parameterValidationTestCases() {
        return Stream.of(
                Arguments.of("MissingSchemaKey",
                        (Consumer<TGetTableSchemaRequest>) request -> {
                            request.setSource(TTableSchemaRequestSource.SCAN);
                            request.setQuery_id(new TUniqueId(1, 1));
                        },
                        "schema key not set"),
                Arguments.of("MissingRequestSource",
                        (Consumer<TGetTableSchemaRequest>) request -> {
                            request.setSchema_key(createSchemaKey(1L, 1L, 1L));
                            request.setQuery_id(new TUniqueId(1, 1));
                        },
                        "request source not set"),
                Arguments.of("ScanWithoutQueryId",
                        (Consumer<TGetTableSchemaRequest>) request -> {
                            request.setSchema_key(createSchemaKey(1L, 1L, 1L));
                            request.setSource(TTableSchemaRequestSource.SCAN);
                        },
                        "query id not set for scan"),
                Arguments.of("LoadWithoutTxnId",
                        (Consumer<TGetTableSchemaRequest>) request -> {
                            request.setSchema_key(createSchemaKey(1L, 1L, 1L));
                            request.setSource(TTableSchemaRequestSource.LOAD);
                        },
                        "txn id not set for load")
        );
    }

    @Test
    public void testFoundInQueryCoordinator() throws Exception {
        LakeTable table = createTable("t_scan_coordinator");
        long indexId = table.getBaseIndexMetaId();
        SchemaInfo schemaInfo = SchemaInfo.fromMaterializedIndex(table, indexId, table.getIndexMetaByIndexId(indexId));

        // Execute query to create coordinator with scan nodes
        TUniqueId queryId = UUIDUtil.genTUniqueId();
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(queryId),
                "Query should not exist in QeProcessorImpl");
        String sql = "SELECT * FROM t_scan_coordinator";
        Coordinator coordinator = executeQueryAndRegister(sql, queryId);

        // Verify coordinator has scan nodes with schema
        List<OlapScanNode> scanNodes = coordinator.getScanNodes().stream()
                .filter(OlapScanNode.class::isInstance)
                .map(OlapScanNode.class::cast)
                .toList();
        Assertions.assertEquals(1, scanNodes.size());
        Assertions.assertEquals(schemaInfo, scanNodes.get(0).getSchema().orElse(null));

        // Request schema from coordinator
        TGetTableSchemaRequest request = createScanRequest(schemaInfo.getId(), db.getId(), table.getId(), queryId);
        TGetTableSchemaResponse response = TableSchemaService.getTableSchema(request);

        Assertions.assertEquals(TStatusCode.OK, response.getStatus().getStatus_code());
        Assertions.assertNotNull(response.getSchema());
        Assertions.assertEquals(schemaInfo.toTabletSchema(), response.getSchema());
    }

    @Test
    public void testFoundInCatalog() throws Exception {
        LakeTable table = createTable("t_found_in_catalog");
        long indexId = table.getBaseIndexMetaId();
        SchemaInfo schemaInfo = SchemaInfo.fromMaterializedIndex(table, indexId, table.getIndexMetaByIndexId(indexId));

        // query fallback to catalog
        TUniqueId queryId = UUIDUtil.genTUniqueId();
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(queryId),
                "Query should not exist in QeProcessorImpl");
        TGetTableSchemaRequest scanRequest = createScanRequest(schemaInfo.getId(), db.getId(), table.getId(), queryId);
        TGetTableSchemaResponse scanResponse = TableSchemaService.getTableSchema(scanRequest);
        Assertions.assertEquals(TStatusCode.OK, scanResponse.getStatus().getStatus_code());
        Assertions.assertNotNull(scanResponse.getSchema());
        Assertions.assertEquals(schemaInfo.toTabletSchema(), scanResponse.getSchema());

        // load request
        TGetTableSchemaRequest loadRequest = createLoadRequest(schemaInfo.getId(), db.getId(), table.getId(), 100);
        TGetTableSchemaResponse loadResponse = TableSchemaService.getTableSchema(loadRequest);
        Assertions.assertEquals(TStatusCode.OK, loadResponse.getStatus().getStatus_code());
        Assertions.assertNotNull(loadResponse.getSchema());
        Assertions.assertEquals(schemaInfo.toTabletSchema(), loadResponse.getSchema());
    }

    @Test
    public void testFoundInHistory() throws Exception {
        LakeTable table = createTable("t_found_in_history");
        long indexId = table.getBaseIndexMetaId();
        SchemaInfo oldSchemaInfo = SchemaInfo.fromMaterializedIndex(table, indexId, table.getIndexMetaByIndexId(indexId));

        // Begin a transaction before alter to prevent the history schema to be cleaned
        TransactionState.TxnCoordinator txnCoordinator = new TransactionState.TxnCoordinator(
                TransactionState.TxnSourceType.BE, "127.0.0.1");
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), List.of(table.getId()), UUID.randomUUID().toString(), txnCoordinator,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, 60000L);

        // Execute alter to create schema change job with history schema
        LakeTableAsyncFastSchemaChangeJob job = executeAlterAndWaitFinish(
                table, "ALTER TABLE t_found_in_history ADD COLUMN c1 BIGINT");

        // Verify history schema exists
        Optional<SchemaInfo> historySchema = job.getHistorySchema()
                .flatMap(history -> history.getSchemaBySchemaId(oldSchemaInfo.getId()));
        Assertions.assertTrue(historySchema.isPresent());
        Assertions.assertEquals(historySchema.get(), oldSchemaInfo);

        // Verify that oldSchemaId is not in the current table's schemas
        boolean oldSchemaIdInCurrentTable = table.getIndexMetaIdToMeta().values().stream()
                .anyMatch(indexMeta -> indexMeta.getSchemaId() == oldSchemaInfo.getId());
        Assertions.assertFalse(oldSchemaIdInCurrentTable,
                "Old schema ID should not exist in current table's schemas after alter");

        // query fallback to catalog
        TUniqueId queryId = UUIDUtil.genTUniqueId();
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(queryId), 
                "Query should not exist in QeProcessorImpl");
        TGetTableSchemaRequest scanRequest = createScanRequest(oldSchemaInfo.getId(), db.getId(), table.getId(), queryId);
        TGetTableSchemaResponse scanResponse = TableSchemaService.getTableSchema(scanRequest);

        Assertions.assertEquals(TStatusCode.OK, scanResponse.getStatus().getStatus_code());
        Assertions.assertNotNull(scanResponse.getSchema());
        Assertions.assertEquals(oldSchemaInfo.toTabletSchema(), scanResponse.getSchema());

        // load request
        TGetTableSchemaRequest loadRequest = createLoadRequest(oldSchemaInfo.getId(), db.getId(), table.getId(), 100);
        TGetTableSchemaResponse loadResponse = TableSchemaService.getTableSchema(loadRequest);
        Assertions.assertEquals(TStatusCode.OK, loadResponse.getStatus().getStatus_code());
        Assertions.assertNotNull(loadResponse.getSchema());
        Assertions.assertEquals(oldSchemaInfo.toTabletSchema(), loadResponse.getSchema());
    }

    @Test
    public void testTableNotFound() {
        TGetTableSchemaRequest request = createScanRequest(1L, db.getId(), -1L, new TUniqueId(1, 1));
        TGetTableSchemaResponse response = TableSchemaService.getTableSchema(request);
        Assertions.assertEquals(TStatusCode.TABLE_NOT_EXIST, response.getStatus().getStatus_code());
        Assertions.assertTrue(response.getStatus().getError_msgs().get(0).contains("table not found"));
    }

    @Test
    public void testScanNotFound() throws Exception {
        LakeTable table = createTable("t_scan_not_found");
        long indexId = table.getBaseIndexMetaId();
        SchemaInfo schemaInfo = SchemaInfo.fromMaterializedIndex(table, indexId, table.getIndexMetaByIndexId(indexId));

        // query not exists, and schema not found in catalog and history
        {
            TUniqueId queryId = UUIDUtil.genTUniqueId();
            TGetTableSchemaRequest request = createScanRequest(schemaInfo.getId() - 1, db.getId(), table.getId(), queryId);
            TGetTableSchemaResponse response = TableSchemaService.getTableSchema(request);

            Assertions.assertEquals(TStatusCode.QUERY_NOT_EXIST, response.getStatus().getStatus_code());
            Assertions.assertTrue(response.getStatus().getError_msgs().get(0).contains("query not found"));
        }

        // query exists, but not found which should not happen in reality
        {
            TUniqueId queryId = UUIDUtil.genTUniqueId();
            Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(queryId),
                    "Query should not exist in QeProcessorImpl");
            String sql = "SELECT * FROM t_scan_not_found";
            connectContext.setExecutionId(queryId);
            // just register, but not execute, so OlapScanNode.toThrift is not called, and there is no schema
            ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            DefaultCoordinator coordinator = new DefaultCoordinator.Factory().createQueryScheduler(
                    connectContext, execPlan.getFragments(), execPlan.getScanNodes(),
                    execPlan.getDescTbl().toThrift(), execPlan);
            QeProcessorImpl.INSTANCE.registerQuery(queryId, coordinator);

            TGetTableSchemaRequest request = createScanRequest(schemaInfo.getId() - 1, db.getId(), table.getId(), queryId);
            TGetTableSchemaResponse response = TableSchemaService.getTableSchema(request);
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, response.getStatus().getStatus_code());
            Assertions.assertTrue(response.getStatus().getError_msgs().get(0).contains(
                    "schema not found in query plan which should not happen"));
        }
    }

    @Test
    public void testLoadNotFound() throws Exception {
        LakeTable table = createTable("t_load_not_found");
        long invalidSchemaId = table.getIndexMetaByIndexId(table.getBaseIndexMetaId()).getSchemaId() - 1;

        // txn not exist
        {
            TGetTableSchemaRequest request = createLoadRequest(invalidSchemaId, db.getId(), table.getId(), -1);
            TGetTableSchemaResponse response = TableSchemaService.getTableSchema(request);
            Assertions.assertEquals(TStatusCode.TXN_NOT_EXISTS, response.getStatus().getStatus_code());
            Assertions.assertTrue(response.getStatus().getError_msgs().get(0).contains("transaction -1 not found"));
        }

        // txn aborted
        {
            GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
            TransactionState.TxnCoordinator txnCoordinator = new TransactionState.TxnCoordinator(
                    TransactionState.TxnSourceType.BE, "127.0.0.1");
            long txnId = transactionMgr.beginTransaction(db.getId(), List.of(table.getId()),
                    UUID.randomUUID().toString(), txnCoordinator,
                    TransactionState.LoadJobSourceType.BACKEND_STREAMING, 60000L);
            transactionMgr.abortTransaction(db.getId(), txnId, "artificial failure");
            TGetTableSchemaRequest request = createLoadRequest(invalidSchemaId, db.getId(), table.getId(), txnId);
            TGetTableSchemaResponse response = TableSchemaService.getTableSchema(request);
            Assertions.assertEquals(TStatusCode.TXN_NOT_EXISTS, response.getStatus().getStatus_code());
            Assertions.assertTrue(response.getStatus().getError_msgs().get(0).contains(
                    String.format("transaction %s has finished, status: %s", txnId, TransactionStatus.ABORTED)));
        }

        // should not happen in reality
        {
            GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
            TransactionState.TxnCoordinator txnCoordinator = new TransactionState.TxnCoordinator(
                    TransactionState.TxnSourceType.BE, "127.0.0.1");
            long txnId = transactionMgr.beginTransaction(db.getId(), List.of(table.getId()),
                    UUID.randomUUID().toString(), txnCoordinator,
                    TransactionState.LoadJobSourceType.BACKEND_STREAMING, 60000L);
            TGetTableSchemaRequest request = createLoadRequest(invalidSchemaId, db.getId(), table.getId(), txnId);
            TGetTableSchemaResponse response = TableSchemaService.getTableSchema(request);
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, response.getStatus().getStatus_code());
            Assertions.assertTrue(response.getStatus().getError_msgs().get(0).contains(
                    "schema for load not found which should not happen"));
        }
    }
}

