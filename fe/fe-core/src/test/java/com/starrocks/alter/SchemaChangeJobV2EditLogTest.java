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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.SchemaVersionAndHash;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaChangeJobV2EditLogTest {
    private static final String DB_NAME = "test_schema_change_job_v2_editlog";
    private static final String TABLE_NAME = "tbl";

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testRunPendingJobEditLog() throws Exception {
        TableContext ctx = createTableContext(false, false);
        SchemaChangeJobV2 job = new TestSchemaChangeJobV2(
                GlobalStateMgr.getCurrentState().getNextId(),
                ctx.db.getId(),
                ctx.table.getId(),
                ctx.table.getName(),
                1000L);
        job.setJobState(AlterJobV2.JobState.PENDING);

        populateJobFields(job, ctx, ctx.shadowIndex);
        ctx.table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        job.runPendingJob();

        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        AlterJobV2 loggedJob = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ALTER_JOB_V2);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, loggedJob.getJobState());
        Assertions.assertEquals(job.getJobId(), loggedJob.getJobId());
    }

    @Test
    public void testRunPendingJobEditLogException() throws Exception {
        TableContext ctx = createTableContext(false, false);
        SchemaChangeJobV2 job = new TestSchemaChangeJobV2(
                GlobalStateMgr.getCurrentState().getNextId(),
                ctx.db.getId(),
                ctx.table.getId(),
                ctx.table.getName(),
                1000L);
        job.setJobState(AlterJobV2.JobState.PENDING);

        populateJobFields(job, ctx, ctx.shadowIndex);
        ctx.table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        com.starrocks.persist.EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        com.starrocks.persist.EditLog spyEditLog = org.mockito.Mockito.spy(originalEditLog);
        org.mockito.Mockito.doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterJob(org.mockito.ArgumentMatchers.any(AlterJobV2.class),
                        org.mockito.ArgumentMatchers.any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, job::runPendingJob);
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
            PhysicalPartition physicalPartition = ctx.table.getPartition(ctx.partitionId).getDefaultPhysicalPartition();
            Assertions.assertEquals(0, physicalPartition.getLatestMaterializedIndices(IndexExtState.SHADOW).size());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testRunPendingJobException() {
        SchemaChangeJobV2 job = new TestSchemaChangeJobV2(
                GlobalStateMgr.getCurrentState().getNextId(),
                -1L,
                -1L,
                "missing",
                1000L);
        job.setJobState(AlterJobV2.JobState.PENDING);

        AlterCancelException exception = Assertions.assertThrows(AlterCancelException.class, job::runPendingJob);
        Assertions.assertTrue(exception.getMessage().contains("does not exist"));
    }

    @Test
    public void testRunRunningJobEditLogAndPruneMeta() throws Exception {
        TableContext ctx = createTableContext(true, true);
        SchemaChangeJobV2 job = new SchemaChangeJobV2(
                GlobalStateMgr.getCurrentState().getNextId(),
                ctx.db.getId(),
                ctx.table.getId(),
                ctx.table.getName(),
                1000L);
        job.setJobState(AlterJobV2.JobState.RUNNING);

        populateJobFields(job, ctx, ctx.shadowIndex);
        ctx.table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        job.runRunningJob();

        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        AlterJobV2 loggedJob = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ALTER_JOB_V2);
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, loggedJob.getJobState());
        Assertions.assertEquals(job.getJobId(), loggedJob.getJobId());
        assertPruned(job);
    }

    @Test
    public void testRunRunningJobEditLogException() throws Exception {
        TableContext ctx = createTableContext(true, true);
        SchemaChangeJobV2 job = new SchemaChangeJobV2(
                GlobalStateMgr.getCurrentState().getNextId(),
                ctx.db.getId(),
                ctx.table.getId(),
                ctx.table.getName(),
                1000L);
        job.setJobState(AlterJobV2.JobState.RUNNING);

        populateJobFields(job, ctx, ctx.shadowIndex);
        ctx.table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        com.starrocks.persist.EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        com.starrocks.persist.EditLog spyEditLog = org.mockito.Mockito.spy(originalEditLog);
        org.mockito.Mockito.doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterJob(org.mockito.ArgumentMatchers.any(AlterJobV2.class),
                        org.mockito.ArgumentMatchers.any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, job::runRunningJob);
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
            Assertions.assertFalse(job.physicalPartitionIndexMap.isEmpty());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testRunRunningJobException() throws Exception {
        TableContext ctx = createTableContext(true, true);
        SchemaChangeJobV2 job = new SchemaChangeJobV2(
                GlobalStateMgr.getCurrentState().getNextId(),
                ctx.db.getId(),
                ctx.table.getId(),
                ctx.table.getName(),
                1000L);
        job.setJobState(AlterJobV2.JobState.RUNNING);

        populateJobFields(job, ctx, ctx.shadowIndex);
        ctx.db.dropTable(ctx.table.getId());

        AlterCancelException exception = Assertions.assertThrows(AlterCancelException.class, job::runRunningJob);
        Assertions.assertTrue(exception.getMessage().contains("does not exist"));
    }

    @Test
    public void testCancelImplEditLogAndPruneMeta() throws Exception {
        TableContext ctx = createTableContext(true, true);
        SchemaChangeJobV2 job = new SchemaChangeJobV2(
                GlobalStateMgr.getCurrentState().getNextId(),
                ctx.db.getId(),
                ctx.table.getId(),
                ctx.table.getName(),
                1000L);
        job.setJobState(AlterJobV2.JobState.RUNNING);

        populateJobFields(job, ctx, ctx.shadowIndex);
        ctx.table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        Assertions.assertTrue(job.cancelImpl("cancel"));
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());

        AlterJobV2 loggedJob = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ALTER_JOB_V2);
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, loggedJob.getJobState());
        Assertions.assertEquals(job.getJobId(), loggedJob.getJobId());
        assertPruned(job);
    }

    @Test
    public void testCancelImplException() throws Exception {
        TableContext ctx = createTableContext(true, true);
        SchemaChangeJobV2 job = new SchemaChangeJobV2(
                GlobalStateMgr.getCurrentState().getNextId(),
                ctx.db.getId(),
                ctx.table.getId(),
                ctx.table.getName(),
                1000L);
        job.setJobState(AlterJobV2.JobState.RUNNING);

        com.starrocks.persist.EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        com.starrocks.persist.EditLog spyEditLog = org.mockito.Mockito.spy(originalEditLog);
        org.mockito.Mockito.doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterJob(org.mockito.ArgumentMatchers.any(AlterJobV2.class),
                        org.mockito.ArgumentMatchers.any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> job.cancelImpl("cancel"));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    private static void populateJobFields(SchemaChangeJobV2 job, TableContext ctx, MaterializedIndex shadowIndex)
            throws Exception {
        Table<Long, Long, MaterializedIndex> partitionIndexMap = HashBasedTable.create();
        partitionIndexMap.put(ctx.physicalPartitionId, ctx.shadowIndexId, shadowIndex);
        job.physicalPartitionIndexMap = partitionIndexMap;

        Table<Long, Long, Map<Long, Long>> partitionIndexTabletMap = HashBasedTable.create();
        Map<Long, Long> tabletMap = new HashMap<>();
        tabletMap.put(ctx.shadowTabletId, ctx.baseTabletId);
        partitionIndexTabletMap.put(ctx.physicalPartitionId, ctx.shadowIndexId, tabletMap);
        job.physicalPartitionIndexTabletMap = partitionIndexTabletMap;

        Map<Long, Long> indexMetaIdMap = Maps.newHashMap();
        indexMetaIdMap.put(ctx.shadowIndexId, ctx.baseIndexId);
        job.indexMetaIdMap = indexMetaIdMap;

        Map<Long, String> indexMetaIdToName = Maps.newHashMap();
        indexMetaIdToName.put(ctx.shadowIndexId, ctx.shadowIndexName);
        job.indexMetaIdToName = indexMetaIdToName;

        Map<Long, List<Column>> indexMetaIdToSchema = Maps.newHashMap();
        indexMetaIdToSchema.put(ctx.shadowIndexId, ctx.table.getBaseSchema());
        job.indexMetaIdToSchema = indexMetaIdToSchema;

        Map<Long, SchemaVersionAndHash> schemaVersionMap = Maps.newHashMap();
        schemaVersionMap.put(ctx.shadowIndexId, new SchemaVersionAndHash(ctx.schemaVersion, ctx.schemaHash));
        job.indexMetaIdToSchemaVersionAndHash = schemaVersionMap;

        Map<Long, Short> shortKeyMap = Maps.newHashMap();
        shortKeyMap.put(ctx.shadowIndexId, (short) 1);
        job.indexMetaIdToShortKey = shortKeyMap;

        job.indexes = Lists.newArrayList();
        job.bfColumns = java.util.Collections.emptySet();
    }

    private static void assertPruned(SchemaChangeJobV2 job) throws Exception {
        Assertions.assertTrue(job.physicalPartitionIndexTabletMap.isEmpty());
        Assertions.assertTrue(job.physicalPartitionIndexMap.isEmpty());
        Assertions.assertTrue(job.indexMetaIdToSchema.isEmpty());
        Assertions.assertTrue(job.indexMetaIdToShortKey.isEmpty());
    }

    private static TableContext createTableContext(boolean addShadowIndexToTable, boolean shadowHasReplica) {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        long dbId = GlobalStateMgr.getCurrentState().getNextId();
        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        long partitionId = GlobalStateMgr.getCurrentState().getNextId();
        long physicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        long baseIndexId = GlobalStateMgr.getCurrentState().getNextId();
        long baseTabletId = GlobalStateMgr.getCurrentState().getNextId();
        long backendId = GlobalStateMgr.getCurrentState().getNextId();
        long replicaId = GlobalStateMgr.getCurrentState().getNextId();
        long shadowIndexId = GlobalStateMgr.getCurrentState().getNextId();
        long shadowTabletId = GlobalStateMgr.getCurrentState().getNextId();
        long shadowReplicaId = GlobalStateMgr.getCurrentState().getNextId();

        Database db = new Database(dbId, DB_NAME);
        metastore.unprotectCreateDb(db);
        Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(backendId);
        if (backend == null) {
            backend = new Backend(backendId, "127.0.0.1", 9050);
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend);
        }
        backend.setAlive(true);

        OlapTable table = createHashOlapTable(dbId, tableId, baseIndexId, baseTabletId,
                backendId, replicaId, partitionId, physicalPartitionId);
        db.registerTableUnlocked(table);

        MaterializedIndex shadowIndex = new MaterializedIndex(shadowIndexId, MaterializedIndex.IndexState.SHADOW);
        LocalTablet shadowTablet = new LocalTablet(shadowTabletId);
        if (shadowHasReplica) {
            shadowTablet.addReplica(new com.starrocks.catalog.Replica(shadowReplicaId, backendId,
                    com.starrocks.catalog.Replica.ReplicaState.NORMAL, 1, 0), false);
        }
        TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partitionId, shadowIndexId, TStorageMedium.HDD);
        shadowIndex.addTablet(shadowTablet, shadowTabletMeta);

        if (addShadowIndexToTable) {
            PhysicalPartition physicalPartition = table.getPartition(partitionId).getDefaultPhysicalPartition();
            physicalPartition.createRollupIndex(shadowIndex);
            table.setIndexMeta(shadowIndexId, shadowIndexName(), table.getBaseSchema(),
                    1, 1, (short) 1, TStorageType.COLUMN, table.getKeysType());
        }

        return new TableContext(db, table, partitionId, physicalPartitionId, baseIndexId, baseTabletId,
                shadowIndexId, shadowTabletId, shadowIndex, backendId, replicaId, shadowReplicaId, 1, 1);
    }

    private static String shadowIndexName() {
        return SchemaChangeHandler.SHADOW_NAME_PREFIX + TABLE_NAME;
    }

    private static OlapTable createHashOlapTable(long dbId, long tableId, long indexId, long tabletId,
                                                 long backendId, long replicaId, long partitionId,
                                                 long physicalPartitionId) {
        List<Column> columns = Lists.newArrayList();
        Column col1 = new Column("k1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v1", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        tablet.addReplica(new com.starrocks.catalog.Replica(replicaId, backendId,
                com.starrocks.catalog.Replica.ReplicaState.NORMAL, 1, 0), false);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, TABLE_NAME, columns, com.starrocks.sql.ast.KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        olapTable.setIndexMeta(indexId, TABLE_NAME, columns, 1, 1, (short) 1,
                TStorageType.COLUMN, com.starrocks.sql.ast.KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(indexId);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));

        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tabletId, tabletMeta);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(tabletId, tablet.getReplicaByBackendId(backendId));
        return olapTable;
    }

    private static class TableContext {
        private final Database db;
        private final OlapTable table;
        private final long partitionId;
        private final long physicalPartitionId;
        private final long baseIndexId;
        private final long baseTabletId;
        private final long shadowIndexId;
        private final long shadowTabletId;
        private final MaterializedIndex shadowIndex;
        private final long backendId;
        private final long replicaId;
        private final long shadowReplicaId;
        private final int schemaVersion;
        private final int schemaHash;
        private final String shadowIndexName;

        private TableContext(Database db, OlapTable table, long partitionId, long physicalPartitionId,
                             long baseIndexId, long baseTabletId, long shadowIndexId, long shadowTabletId,
                             MaterializedIndex shadowIndex, long backendId, long replicaId, long shadowReplicaId,
                             int schemaVersion, int schemaHash) {
            this.db = db;
            this.table = table;
            this.partitionId = partitionId;
            this.physicalPartitionId = physicalPartitionId;
            this.baseIndexId = baseIndexId;
            this.baseTabletId = baseTabletId;
            this.shadowIndexId = shadowIndexId;
            this.shadowTabletId = shadowTabletId;
            this.shadowIndex = shadowIndex;
            this.backendId = backendId;
            this.replicaId = replicaId;
            this.shadowReplicaId = shadowReplicaId;
            this.schemaVersion = schemaVersion;
            this.schemaHash = schemaHash;
            this.shadowIndexName = shadowIndexName();
        }
    }

    private static class TestSchemaChangeJobV2 extends SchemaChangeJobV2 {
        TestSchemaChangeJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
            super(jobId, dbId, tableId, tableName, timeoutMs);
        }

        @Override
        protected boolean checkTableStable(Database db) {
            return true;
        }
    }
}
