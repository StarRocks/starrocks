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

package com.starrocks.replication;

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
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.ReplicationJobLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.system.Backend;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ReplicationEditLogTest {
    private static final String DB_NAME = "test_replication_editlog";
    private static final String TABLE_NAME = "test_table";
    private static final long DB_ID = 30001L;
    private static final long TABLE_ID = 30002L;
    private static final long PARTITION_ID = 30003L;
    private static final long PHYSICAL_PARTITION_ID = 30004L;
    private static final long INDEX_ID = 30005L;
    private static final long TABLET_ID = 30006L;
    private static final long REPLICA_ID = 30007L;
    private static final long BACKEND_ID = 30008L;

    private Database db;
    private OlapTable table;
    private OlapTable srcTable;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        if (GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(BACKEND_ID) == null) {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .addBackend(new Backend(BACKEND_ID, "127.0.0.1", 9050));
        }
        table = createHashOlapTable(TABLE_ID, TABLE_NAME, 1);
        db.registerTableUnlocked(table);
        srcTable = DeepCopy.copyWithGson(table, OlapTable.class);

        Partition partition = table.getPartitions().iterator().next();
        Partition srcPartition = srcTable.getPartitions().iterator().next();
        partition.getDefaultPhysicalPartition().updateVersionForRestore(10);
        srcPartition.getDefaultPhysicalPartition().updateVersionForRestore(100);
        partition.getDefaultPhysicalPartition().setDataVersion(8);
        partition.getDefaultPhysicalPartition().setNextDataVersion(9);
        srcPartition.getDefaultPhysicalPartition().setDataVersion(98);
        srcPartition.getDefaultPhysicalPartition().setNextDataVersion(99);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static OlapTable createHashOlapTable(long tableId, String tableName, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("key1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("key2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        tablet.addReplica(new com.starrocks.catalog.Replica(REPLICA_ID, BACKEND_ID,
                com.starrocks.catalog.Replica.ReplicaState.NORMAL, 1, 0), false);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, tableName, baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        return olapTable;
    }

    @Test
    public void testPersistStateChangeNormalCase() throws Exception {
        ReplicationJob job = new ReplicationJob("test_job", "test_token", db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());

        Assertions.assertEquals(ReplicationJobState.INITIALIZING, job.getState());
        job.persistStateChange(ReplicationJobState.SNAPSHOTING);
        Assertions.assertEquals(ReplicationJobState.SNAPSHOTING, job.getState());

        ReplicationJobLog replayLog = (ReplicationJobLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_REPLICATION_JOB);
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals("test_job", replayLog.getReplicationJob().getJobId());
        Assertions.assertEquals(ReplicationJobState.SNAPSHOTING, replayLog.getReplicationJob().getState());
    }

    @Test
    public void testPersistStateChangeEditLogException() {
        ReplicationJob job = new ReplicationJob("test_job_exception", "test_token", db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());

        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logReplicationJob(any(ReplicationJob.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        Assertions.assertEquals(ReplicationJobState.INITIALIZING, job.getState());
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> job.persistStateChange(ReplicationJobState.SNAPSHOTING));
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        Assertions.assertEquals(ReplicationJobState.INITIALIZING, job.getState());
    }

    @Test
    public void testClearExpiredJobsNormalCase() throws Exception {
        ReplicationMgr replicationMgr = new ReplicationMgr();

        ReplicationJob committedJob = new ReplicationJob("committed_job", "test_token", db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
        Deencapsulation.setField(committedJob, "state", ReplicationJobState.COMMITTED);
        Deencapsulation.setField(committedJob, "finishedTimeMs", System.currentTimeMillis());
        replicationMgr.replayReplicationJob(committedJob);

        ReplicationJob abortedJob = new ReplicationJob("aborted_job", "test_token", db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
        Deencapsulation.setField(abortedJob, "state", ReplicationJobState.ABORTED);
        Deencapsulation.setField(abortedJob, "finishedTimeMs", System.currentTimeMillis());
        replicationMgr.replayReplicationJob(abortedJob);

        int old = Config.history_job_keep_max_second;
        Config.history_job_keep_max_second = -1;
        try {
            Assertions.assertTrue(committedJob.isExpired());
            Assertions.assertTrue(abortedJob.isExpired());

            replicationMgr.clearExpiredJobs();

            Assertions.assertTrue(replicationMgr.getCommittedJobs().isEmpty());
            Assertions.assertTrue(replicationMgr.getAbortedJobs().isEmpty());

            ReplicationJobLog firstLog = (ReplicationJobLog) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_DELETE_REPLICATION_JOB);
            ReplicationJobLog secondLog = (ReplicationJobLog) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_DELETE_REPLICATION_JOB);

            Set<String> jobIds = new HashSet<>();
            jobIds.add(firstLog.getReplicationJob().getJobId());
            jobIds.add(secondLog.getReplicationJob().getJobId());
            Assertions.assertEquals(Set.of("committed_job", "aborted_job"), jobIds);
        } finally {
            Config.history_job_keep_max_second = old;
        }
    }

    @Test
    public void testClearExpiredJobsEditLogException() {
        ReplicationMgr replicationMgr = new ReplicationMgr();
        ReplicationJob committedJob = new ReplicationJob("committed_job_exception", "test_token", db.getId(), table,
                srcTable, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
        Deencapsulation.setField(committedJob, "state", ReplicationJobState.COMMITTED);
        Deencapsulation.setField(committedJob, "finishedTimeMs", System.currentTimeMillis());
        replicationMgr.replayReplicationJob(committedJob);

        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDeleteReplicationJob(any(ReplicationJob.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        int old = Config.history_job_keep_max_second;
        Config.history_job_keep_max_second = -1;
        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, replicationMgr::clearExpiredJobs);
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(1, replicationMgr.getCommittedJobs().size());
        } finally {
            Config.history_job_keep_max_second = old;
        }
    }
}
