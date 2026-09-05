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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class LakeTableSchemaChangeJobEditLogTest {
    private static final String DB_NAME = "test_lake_schema_change_editlog";
    private static final String TABLE_NAME = "tbl";

    private Database db;
    private OlapTable table;
    private long dbId;
    private long tableId;
    private long partitionId;
    private long physicalPartitionId;
    private long tabletId;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        dbId = GlobalStateMgr.getCurrentState().getNextId();
        tableId = GlobalStateMgr.getCurrentState().getNextId();
        partitionId = GlobalStateMgr.getCurrentState().getNextId();
        physicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        tabletId = GlobalStateMgr.getCurrentState().getNextId();

        db = new Database(dbId, DB_NAME);
        metastore.unprotectCreateDb(db);
        table = createHashOlapTable(dbId, tableId, TABLE_NAME, partitionId, physicalPartitionId, tabletId);
        db.registerTableUnlocked(table);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testRunFinishedRewritingJobEditLog() throws Exception {
        TestLakeTableSchemaChangeJob job = new TestLakeTableSchemaChangeJob(
                GlobalStateMgr.getCurrentState().getNextId(), dbId, tableId, TABLE_NAME, Config.alter_table_timeout_second);
        job.setJobState(AlterJobV2.JobState.FINISHED_REWRITING);

        job.runFinishedRewritingJob();

        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        AlterJobV2 loggedJob = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ALTER_JOB_V2);
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, loggedJob.getJobState());
        Assertions.assertEquals(job.getJobId(), loggedJob.getJobId());
    }

    @Test
    public void testRunFinishedRewritingJobEditLogException() {
        TestLakeTableSchemaChangeJob job = new TestLakeTableSchemaChangeJob(
                GlobalStateMgr.getCurrentState().getNextId(), dbId, tableId, TABLE_NAME, Config.alter_table_timeout_second);
        job.setJobState(AlterJobV2.JobState.FINISHED_REWRITING);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterJob(any(AlterJobV2.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, job::runFinishedRewritingJob);
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testCancelImplEditLog() throws Exception {
        TestLakeTableSchemaChangeJob job = new TestLakeTableSchemaChangeJob(
                GlobalStateMgr.getCurrentState().getNextId(), dbId, tableId, TABLE_NAME, Config.alter_table_timeout_second);
        job.setJobState(AlterJobV2.JobState.RUNNING);

        Assertions.assertTrue(job.cancelImpl("cancel"));
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());

        AlterJobV2 loggedJob = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ALTER_JOB_V2);
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, loggedJob.getJobState());
        Assertions.assertEquals(job.getJobId(), loggedJob.getJobId());
    }

    @Test
    public void testCancelImplEditLogException() {
        TestLakeTableSchemaChangeJob job = new TestLakeTableSchemaChangeJob(
                GlobalStateMgr.getCurrentState().getNextId(), dbId, tableId, TABLE_NAME, Config.alter_table_timeout_second);
        job.setJobState(AlterJobV2.JobState.RUNNING);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterJob(any(AlterJobV2.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> job.cancelImpl("cancel"));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    private static OlapTable createHashOlapTable(long dbId, long tableId, String tableName, long partitionId,
                                                 long physicalPartitionId, long tabletId) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("k1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("k2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(1, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(1L, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, 1L, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(partitionId, physicalPartitionId, tableName, baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        olapTable.setIndexMeta(1L, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(1L);
        olapTable.setDefaultDistributionInfo(distributionInfo);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        return olapTable;
    }

    private static class TestLakeTableSchemaChangeJob extends LakeTableSchemaChangeJob {
        TestLakeTableSchemaChangeJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
            super(jobId, dbId, tableId, tableName, timeoutMs);
        }

        @Override
        boolean readyToPublishVersion() {
            return true;
        }

        @Override
        protected boolean publishVersion() {
            return true;
        }

        @Override
        List<MaterializedIndex> visualiseShadowIndex(OlapTable table) {
            return new ArrayList<>();
        }

        @Override
        void removeShadowIndex(OlapTable table) {
        }
    }
}
