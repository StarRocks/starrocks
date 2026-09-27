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

package com.starrocks.alter;

import com.google.common.collect.Maps;
import com.google.gson.stream.JsonReader;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.service.TableSchemaService;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.thrift.TGetTableSchemaRequest;
import com.starrocks.thrift.TGetTableSchemaResponse;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTableSchemaKey;
import com.starrocks.thrift.TTableSchemaRequestSource;
import com.starrocks.transaction.ExplicitTxnState;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

public class CloudNativeFastSchemaEvolutionV2Test extends LakeFastSchemaChangeTestBase {

    private static final AtomicInteger TABLE_SUFFIX = new AtomicInteger(0);

    @Override
    protected boolean isFastSchemaEvolutionV2() {
        return true;
    }

    @Test
    public void testCreateTableProperty() throws Exception {
        LakeTable defaultTable = createLakeTable(uniqueName("create_default"), null);
        assertTableProperty(defaultTable, "true");

        LakeTable explicitTrue = createLakeTable(uniqueName("create_true"), "true");
        assertTableProperty(explicitTrue, "true");

        LakeTable explicitFalse = createLakeTable(uniqueName("create_false"), "false");
        assertTableProperty(explicitFalse, "false");

        Assertions.assertThrows(SemanticException.class,
                () -> createLakeTable(uniqueName("create_invalid"), "maybe"));

        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2, "true");
        Assertions.assertThrows(SemanticException.class,
                () -> PropertyAnalyzer.analyzeCloudNativeFastSchemaEvolutionV2(TableType.OLAP, properties, false));
    }

    @Test
    public void testAlterTableProperty() throws Exception {
        LakeTable table = createLakeTable(uniqueName("alter_all"), null);
        Map<Long, SchemaInfo> originalSchema = snapshotTableSchema(table);
        assertTableProperty(table, "true");

        // true -> true no-op
        alterTableProperty(table.getName(), "true");
        Assertions.assertTrue(getAlterJobs(table.getId()).isEmpty());
        assertTableProperty(table, "true");

        // true -> false requires async job
        alterTableProperty(table.getName(), "false");
        LakeTableAsyncFastSchemaChangeJob job = waitForAsyncSchemaChangeJob(table);
        Assertions.assertTrue(job.isDisableFastSchemaEvolutionV2());
        assertJobSchemaMatchesSnapshot(job, originalSchema);
        assertTableSchemaMatchesSnapshot(table, originalSchema);
        assertTableProperty(table, "false");

        // false -> false no-op
        alterTableProperty(table.getName(), "false");
        Assertions.assertTrue(getAlterJobs(table.getId()).isEmpty());
        assertTableProperty(table, "false");

        // false -> true metadata-only
        alterTableProperty(table.getName(), "true");
        Assertions.assertTrue(getAlterJobs(table.getId()).isEmpty());
        assertTableProperty(table, "true");

        // invalid alter value
        alterTableProperty(table.getName(), "maybe");
        Assertions.assertEquals(QueryState.ErrType.ANALYSIS_ERR, connectContext.getState().getErrType());
        Assertions.assertTrue(connectContext.getState().getErrorMessage()
                .contains("Invalid " + PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2));
    }

    @Test
    public void testReplayProperty() throws Exception {
        LakeTable table = createLakeTable(uniqueName("restore_property"), "false");
        Map<Long, SchemaInfo> schemaSnapshot = snapshotTableSchema(table);
        assertTableProperty(table, "false");
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage initialImage = new UtFrameUtils.PseudoImage();
        ImageWriter imageWriter = initialImage.getImageWriter();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(imageWriter);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().save(imageWriter);

        // false -> true
        alterTableProperty(table.getName(), "true");
        assertTableProperty(table, "true");

        // true -> false
        alterTableProperty(table.getName(), "false");
        LakeTableAsyncFastSchemaChangeJob job = waitForAsyncSchemaChangeJob(table);
        assertJobSchemaMatchesSnapshot(job, schemaSnapshot);
        Assertions.assertTrue(job.isDisableFastSchemaEvolutionV2());
        assertTableProperty(table, "false");

        LocalMetastore restoredMetastore =
                new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        AlterJobMgr restoredAlterJobMgr =
                new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(), new SystemHandler());
        JsonReader jsonReader = initialImage.getJsonReader();
        SRMetaBlockReader blockReader = new SRMetaBlockReaderV2(jsonReader);
        restoredMetastore.load(blockReader);
        blockReader.close();
        blockReader = new SRMetaBlockReaderV2(jsonReader);
        restoredAlterJobMgr.load(blockReader);
        blockReader.close();
        Database restoredDb = restoredMetastore.getDb(DB_NAME);
        Assertions.assertNotNull(restoredDb, "Restored database not found");
        LakeTable restoredTable = (LakeTable) restoredMetastore.getTable(DB_NAME, table.getName());
        Assertions.assertNotNull(restoredTable, "Restored table not found");
        assertTableProperty(restoredTable, "false");

        // replay false -> true
        ModifyTablePropertyOperationLog info = (ModifyTablePropertyOperationLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TABLE_PROPERTIES);
        restoredMetastore.replayModifyTableProperty(OperationType.OP_ALTER_TABLE_PROPERTIES, info);
        Assertions.assertTrue(restoredTable.isFastSchemaEvolutionV2());

        // replay true -> false
        SchemaChangeHandler restoredHandler = restoredAlterJobMgr.getSchemaChangeHandler();
        LocalMetastore originalMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        try {
            // restoredHandler can restore state to restoredMetastore
            GlobalStateMgr.getCurrentState().setLocalMetastore(restoredMetastore);
            // there is expected 4 edit log in the lifecycle of LakeTableAsyncFastSchemaChangeJob
            for (int i = 0; i < 4; i++) {
                AlterJobV2 alterJob = (AlterJobV2)
                        UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_JOB_V2);
                Assertions.assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, alterJob);
                Assertions.assertEquals(job.getJobId(), alterJob.getJobId());
                restoredHandler.replayAlterJobV2(alterJob);
            }
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(originalMetastore);
        }
        AlterJobV2 restoredJob = restoredHandler.getAlterJobsV2().get(job.getJobId());
        Assertions.assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, restoredJob);
        LakeTableAsyncFastSchemaChangeJob fseJob = (LakeTableAsyncFastSchemaChangeJob) restoredJob;
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, fseJob.getJobState());
        Assertions.assertTrue(fseJob.isDisableFastSchemaEvolutionV2());
        Assertions.assertFalse(restoredTable.isFastSchemaEvolutionV2());
    }

    private static void assertTableProperty(LakeTable table, String expectedValue) throws Exception {
        Assertions.assertEquals(Boolean.parseBoolean(expectedValue), table.isFastSchemaEvolutionV2());
        Assertions.assertEquals(expectedValue, table.getTableProperty().getProperties()
                .get(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2));
        String showStmt = showCreateTable(table.getName());
        String expectedFragment = String.format("\"%s\" = \"%s\"",
                PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2, expectedValue);
        Assertions.assertTrue(showStmt.contains(expectedFragment),
                () -> String.format("SHOW CREATE TABLE result [%s] missing fragment [%s]", showStmt, expectedFragment));
    }

    private static LakeTable createLakeTable(String tableName, @Nullable String propertyValue) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE ").append(DB_NAME).append(".").append(tableName)
                .append(" (c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 ")
                .append("PROPERTIES ('replication_num' = '1'");
        if (propertyValue != null) {
            builder.append(", '").append(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2)
                    .append("' = '").append(propertyValue).append("'");
        }
        builder.append(")");
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(builder.toString(), connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        return getLakeTable(tableName);
    }

    private static Database getDatabase() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
    }

    private static String uniqueName(String prefix) {
        return prefix + "_" + TABLE_SUFFIX.incrementAndGet();
    }

    private static String showCreateTable(String tableName) throws Exception {
        ShowCreateTableStmt stmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "SHOW CREATE TABLE " + DB_NAME + "." + tableName, connectContext);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, connectContext);
        return resultSet.getResultRows().get(0).get(1).toString();
    }

    private static void alterTableProperty(String tableName, String value) throws Exception {
        String sql = String.format("ALTER TABLE %s.%s SET ('%s' = '%s')",
                DB_NAME, tableName, PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2, value);
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.executeSql(sql);
    }

    private static List<AlterJobV2> getAlterJobs(long tableId) {
        return schemaChangeHandler.getUnfinishedAlterJobV2ByTableId(tableId);
    }

    private static LakeTable getLakeTable(String tableName) {
        Database db = getDatabase();
        return (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
    }

    private static LakeTableAsyncFastSchemaChangeJob waitForAsyncSchemaChangeJob(LakeTable table) throws Exception {
        List<AlterJobV2> jobs = getAlterJobs(table.getId());
        Assertions.assertEquals(1, jobs.size(), "Expected exactly one schema change job");
        AlterJobV2 job = jobs.get(0);
        Assertions.assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, job);
        long startTime = System.currentTimeMillis();
        long timeoutMs = 10 * 60 * 1000;
        while (job.getJobState() != AlterJobV2.JobState.FINISHED
                || table.getState() != OlapTable.OlapTableState.NORMAL) {
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                throw new RuntimeException(String.format(
                        "Alter job timeout. Job state: %s, table state: %s", job.getJobState(), table.getState()));
            }
            job.run();
            Thread.sleep(100);
        }
        return (LakeTableAsyncFastSchemaChangeJob) job;
    }

    private static Map<Long, SchemaInfo> snapshotTableSchema(LakeTable table) {
        Map<Long, SchemaInfo> snapshot = Maps.newHashMap();
        table.getIndexMetaIdToMeta().forEach((indexMetaId, meta) -> snapshot.put(indexMetaId, toSchemaInfo(meta)));
        return snapshot;
    }

    private static void assertTableSchemaMatchesSnapshot(LakeTable table, Map<Long, SchemaInfo> snapshot) {
        snapshot.forEach((indexMetaId, signature) -> {
            MaterializedIndexMeta meta = table.getIndexMetaIdToMeta().get(indexMetaId);
            Assertions.assertNotNull(meta, () -> "Table missing indexMetaId " + indexMetaId);
            assertSchemaInfoEquals(signature, toSchemaInfo(meta));
        });
    }

    private static void assertJobSchemaMatchesSnapshot(LakeTableAsyncFastSchemaChangeJob job,
                                                       Map<Long, SchemaInfo> snapshot) {
        List<SchemaInfo> jobSchemas = job.getSchemaInfoList();
        Assertions.assertEquals(snapshot.size(), jobSchemas.size());
        jobSchemas.forEach(schemaInfo -> {
            SchemaInfo expected = snapshot.get(schemaInfo.getId());
            assertSchemaInfoEquals(expected, schemaInfo);
        });
    }

    private static SchemaInfo toSchemaInfo(MaterializedIndexMeta meta) {
        return SchemaInfo.newBuilder()
                .setId(meta.getSchemaId())
                .setVersion(meta.getSchemaVersion())
                .setKeysType(meta.getKeysType())
                .setShortKeyColumnCount(meta.getShortKeyColumnCount())
                .setStorageType(meta.getStorageType())
                .addColumns(meta.getSchema())
                .build();
    }

    private static void assertSchemaInfoEquals(SchemaInfo expected, SchemaInfo actual) {
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(expected.getId(), actual.getId());
        Assertions.assertEquals(expected.getVersion(), actual.getVersion());
        Assertions.assertEquals(expected.getKeysType(), actual.getKeysType());
        Assertions.assertEquals(expected.getShortKeyColumnCount(), actual.getShortKeyColumnCount());
        Assertions.assertEquals(expected.getStorageType(), actual.getStorageType());
        Assertions.assertEquals(expected.getColumns(), actual.getColumns());
    }
    /**
     * The seam StarRocksTest#12167 failed at. The in-place index / bloom-filter fast path re-stamps an
     * index meta with a NEW schema id, so the previous id leaves the catalog. A load already bound to
     * it resolves its schema from FE at publish time (TableSchemaService, source = LOAD), and before
     * this fix that lookup fell through both the catalog and the history and returned INTERNAL_ERROR
     * "schema for load not found which should not happen" -- which the publish retried forever.
     *
     * Asserted here rather than in a SQL test on purpose: BE resolves the schema from its own cache
     * first (TableSchemaService::get_schema_for_load -> _get_local_schema), and in any self-contained
     * run that cache is warm, so the FE RPC is never reached. Calling the service directly is what
     * makes the coverage deterministic.
     */
    @Test
    public void testFastPathRetiredSchemaStaysResolvableForBoundLoad() throws Exception {
        LakeTable table = createTable(connectContext, """
                CREATE TABLE t_fastpath_retired (
                c0 INT,
                c1 VARCHAR(64)
                ) DUPLICATE KEY(c0)
                DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='true');""");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        long baseIndexMetaId = table.getBaseIndexMetaId();
        MaterializedIndexMeta baseMeta = table.getIndexMetaByMetaId(baseIndexMetaId);
        long retiredSchemaId = baseMeta.getSchemaId();

        // A load bound to the schema the flip is about to retire, still running when it happens.
        TransactionState.TxnCoordinator coordinator =
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, "127.0.0.1");
        long boundTxnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), List.of(table.getId()), UUIDUtil.genUUID().toString(), coordinator,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, 60000L);

        // What the fast path's FINISHED flip does: snapshot the metas it is about to re-stamp, then
        // re-stamp them. Both are the production calls, in the production order.
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(
                GlobalStateMgr.getCurrentState().getNextId(), db.getId(), table.getId(), table.getName(),
                60000L, List.of(), List.of());
        job.putNewSchema(baseIndexMetaId, GlobalStateMgr.getCurrentState().getNextId(),
                baseMeta.getSchemaVersion() + 1);
        job.historySchema = AlterJobV2.buildHistorySchema(table, List.of(baseIndexMetaId));
        job.applyCatalogMutation(table);
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().addAlterJobV2(job);

        // The retired id really did leave the catalog...
        Assertions.assertTrue(table.getIndexMetaIdToMeta().values().stream()
                .noneMatch(meta -> meta.getSchemaId() == retiredSchemaId));

        // ...and the publish-time lookup that used to fail now resolves it.
        TTableSchemaKey schemaKey = new TTableSchemaKey();
        schemaKey.setSchema_id(retiredSchemaId);
        schemaKey.setDb_id(db.getId());
        schemaKey.setTable_id(table.getId());
        TGetTableSchemaRequest request = new TGetTableSchemaRequest();
        request.setSchema_key(schemaKey);
        request.setSource(TTableSchemaRequestSource.LOAD);
        request.setTxn_id(boundTxnId);
        TGetTableSchemaResponse response = TableSchemaService.getTableSchema(request);
        Assertions.assertEquals(TStatusCode.OK, response.getStatus().getStatus_code());
        Assertions.assertEquals(retiredSchemaId, response.getSchema().getId());

        // Released once nothing can still be bound to it.
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .abortTransaction(db.getId(), boundTxnId, "done");
        job.isExpire();
        Assertions.assertTrue(job.getHistorySchema().orElseThrow().isExpired());
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
    }

    /**
     * A multi-statement Stream Load binds its sub-task's sink to a schema id before the transaction is
     * upserted into the DatabaseTransactionMgr (TransactionStmtExecutor.loadData(long, long, ...) upserts
     * only after the sub-task produced its item), so isPreviousTransactionsFinished -- which scans only
     * DatabaseTransactionMgr.idToRunningTransactionState -- cannot see it. Releasing the retired schema in
     * that window strands the load's publish exactly like StarRocksTest#12167.
     *
     * <p>The sibling test above uses BACKEND_STREAMING, which is registered in the database transaction
     * manager from the start and therefore never enters this window.
     */
    @Test
    public void testFastPathRetiredSchemaSurvivesUnregisteredExplicitTransaction() throws Exception {
        LakeTable table = createTable(connectContext, """
                CREATE TABLE t_fastpath_explicit (
                c0 INT,
                c1 VARCHAR(64)
                ) DUPLICATE KEY(c0)
                DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='true');""");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        long baseIndexMetaId = table.getBaseIndexMetaId();
        MaterializedIndexMeta baseMeta = table.getIndexMetaByMetaId(baseIndexMetaId);

        // Exactly what TransactionStmtExecutor.beginStmt builds: no dbId, no table list, status PREPARE.
        // Allocated before the snapshot so it sits below the history threshold.
        long explicitTxnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionIDGenerator().getNextTransactionId();
        TransactionState explicitTxn = new TransactionState(
                explicitTxnId, UUIDUtil.genUUID().toString(), null,
                TransactionState.LoadJobSourceType.MULTI_STATEMENT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"), 60000L);
        Assertions.assertEquals(0, explicitTxn.getDbId());
        Assertions.assertTrue(explicitTxn.isRunning());
        ExplicitTxnState explicitTxnState = new ExplicitTxnState();
        explicitTxnState.setTransactionState(explicitTxn);
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .addTransactionState(explicitTxnId, explicitTxnState);

        LakeTableAddIndexJob job = new LakeTableAddIndexJob(
                GlobalStateMgr.getCurrentState().getNextId(), db.getId(), table.getId(), table.getName(),
                60000L, List.of(), List.of());
        job.putNewSchema(baseIndexMetaId, GlobalStateMgr.getCurrentState().getNextId(),
                baseMeta.getSchemaVersion() + 1);
        job.historySchema = AlterJobV2.buildHistorySchema(table, List.of(baseIndexMetaId));
        job.applyCatalogMutation(table);
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().addAlterJobV2(job);

        // The DatabaseTransactionMgr scan on its own reports "nothing running" -- this is the blind spot.
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .isPreviousTransactionsFinished(job.getHistorySchema().orElseThrow().getHistoryTxnIdThreshold(),
                        db.getId(), List.of(table.getId())));

        // The payload must nevertheless be held: without the explicit-registry check this expires here.
        job.isExpire();
        Assertions.assertFalse(job.getHistorySchema().orElseThrow().isExpired());

        // Once the transaction is gone, normal expiry resumes.
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().clearExplicitTxnState(explicitTxnId);
        job.isExpire();
        Assertions.assertTrue(job.getHistorySchema().orElseThrow().isExpired());
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
    }

}
