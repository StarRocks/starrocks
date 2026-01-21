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

package com.starrocks.load;

import com.google.common.collect.Lists;
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
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.common.DmlException;
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

public class InsertOverwriteJobRunnerEditLogTest {
    private static final String DB_NAME = "test_insert_overwrite_editlog";
    private static final String TABLE_NAME = "t1";

    private static final long INDEX_ID = 41007L;

    private Database db;
    private OlapTable table;
    private long oldPartitionRetentionSecs;
    private long dbId;
    private long tableId;
    private long sourcePartitionId;
    private long sourcePhysicalPartitionId;
    private long tempPartitionId;
    private long tempPhysicalPartitionId;
    private long sourceTabletId;
    private long tempTabletId;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        oldPartitionRetentionSecs = Config.partition_recycle_retention_period_secs;
        Config.partition_recycle_retention_period_secs = 0;
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        dbId = GlobalStateMgr.getCurrentState().getNextId();
        tableId = GlobalStateMgr.getCurrentState().getNextId();
        sourcePartitionId = GlobalStateMgr.getCurrentState().getNextId();
        sourcePhysicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        tempPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        tempPhysicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        sourceTabletId = GlobalStateMgr.getCurrentState().getNextId();
        tempTabletId = GlobalStateMgr.getCurrentState().getNextId();

        db = new Database(dbId, DB_NAME);
        metastore.unprotectCreateDb(db);
        table = createHashOlapTable(dbId, tableId, TABLE_NAME, sourcePartitionId, sourcePhysicalPartitionId,
                sourceTabletId);
        db.registerTableUnlocked(table);
        addTempPartition(dbId, table, sourcePartitionId, tempPartitionId, tempPhysicalPartitionId, tempTabletId,
                TABLE_NAME + "_tmp");
    }

    @AfterEach
    public void tearDown() {
        Config.partition_recycle_retention_period_secs = oldPartitionRetentionSecs;
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testDoCommitAndReplayCommitWithEditLog() throws Exception {
        InsertOverwriteJob job = new InsertOverwriteJob(2002L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));

        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
        runner.doCommit();

        Partition replaced = table.getPartition(TABLE_NAME);
        Assertions.assertNotNull(replaced);
        Assertions.assertEquals(tempPartitionId, replaced.getId());
        Assertions.assertFalse(table.existTempPartitions());

        resetTableForReplay();

        InsertOverwriteStateChangeInfo info = (InsertOverwriteStateChangeInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE);
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_SUCCESS, info.getToState());
        Assertions.assertEquals(Lists.newArrayList(sourcePartitionId), info.getSourcePartitionIds());
        Assertions.assertEquals(Lists.newArrayList(tempPartitionId), info.getTmpPartitionIds());

        runner.replayStateChange(info);

        Partition replayed = table.getPartition(TABLE_NAME);
        Assertions.assertNotNull(replayed);
        Assertions.assertEquals(tempPartitionId, replayed.getId());
        Assertions.assertFalse(table.existTempPartitions());
    }

    @Test
    public void testGcAndReplayGCWithEditLog() throws Exception {
        InsertOverwriteJob job = new InsertOverwriteJob(2004L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));

        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
        runner.gc();
        Assertions.assertFalse(table.existTempPartitions());

        addTempPartition(dbId, table, sourcePartitionId, tempPartitionId, tempPhysicalPartitionId, tempTabletId,
                TABLE_NAME + "_tmp");

        InsertOverwriteStateChangeInfo info = (InsertOverwriteStateChangeInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE);
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_FAILED, info.getToState());
        Assertions.assertEquals(Lists.newArrayList(tempPartitionId), info.getTmpPartitionIds());
        runner.replayStateChange(info);

        Assertions.assertFalse(table.existTempPartitions());
    }

    @Test
    public void testDoCommitEditLogException() {
        InsertOverwriteJob job = new InsertOverwriteJob(2005L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logInsertOverwriteStateChange(any(InsertOverwriteStateChangeInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
            DmlException exception = Assertions.assertThrows(DmlException.class, runner::doCommit);
            Assertions.assertTrue(exception.getMessage().contains("replace partitions failed"));

            Partition sourcePartition = table.getPartition(TABLE_NAME);
            Assertions.assertNotNull(sourcePartition);
            Assertions.assertEquals(sourcePartitionId, sourcePartition.getId());
            Assertions.assertTrue(table.existTempPartitions());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testGcEditLogException() {
        InsertOverwriteJob job = new InsertOverwriteJob(2006L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_FAILED);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logInsertOverwriteStateChange(any(InsertOverwriteStateChangeInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
            Assertions.assertDoesNotThrow(runner::gc);
            Assertions.assertTrue(table.existTempPartitions());
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

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(partitionId, physicalPartitionId, tableName, baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.setDefaultDistributionInfo(distributionInfo);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        return olapTable;
    }

    private static void addTempPartition(long dbId, OlapTable table, long sourcePartitionId, long partitionId,
                                         long physicalPartitionId, long tabletId, String partitionName) {
        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        TabletMeta tabletMeta = new TabletMeta(dbId, table.getId(), partitionId, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        Partition partition = new Partition(partitionId, physicalPartitionId, partitionName, baseIndex,
                distributionInfo);

        table.addTempPartition(partition);
        table.getPartitionInfo().addPartition(partitionId,
                table.getPartitionInfo().getDataProperty(sourcePartitionId),
                table.getPartitionInfo().getReplicationNum(sourcePartitionId));
    }

    private void resetTableForReplay() {
        GlobalStateMgr.getCurrentState().getRecycleBin().removePartitionFromRecycleBin(sourcePartitionId);
        GlobalStateMgr.getCurrentState().getRecycleBin().removePartitionFromRecycleBin(tempPartitionId);
        table.dropPartition(dbId, TABLE_NAME, true);
        GlobalStateMgr.getCurrentState().getRecycleBin().removePartitionFromRecycleBin(tempPartitionId);

        Partition sourcePartition = buildPartition(dbId, table, sourcePartitionId, sourcePhysicalPartitionId,
                sourceTabletId, TABLE_NAME);
        table.addPartition(sourcePartition);
        table.getPartitionInfo().addPartition(sourcePartitionId,
                com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY, (short) 1);

        addTempPartition(dbId, table, sourcePartitionId, tempPartitionId, tempPhysicalPartitionId, tempTabletId,
                TABLE_NAME + "_tmp");
    }

    private static Partition buildPartition(long dbId, OlapTable table, long partitionId, long physicalPartitionId,
                                            long tabletId, String partitionName) {
        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        TabletMeta tabletMeta = new TabletMeta(dbId, table.getId(), partitionId, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        return new Partition(partitionId, physicalPartitionId, partitionName, baseIndex, distributionInfo);
    }
}
