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

package com.starrocks.clone;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.system.Backend;
import com.starrocks.task.CloneTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TabletSchedulerEditLogTest {
    private static final String DB_NAME = "test_tablet_scheduler_editlog";
    private static final long DB_ID = 60001L;
    private static final long TABLE_ID = 60002L;
    private static final long PARTITION_ID = 60004L;
    private static final long PHYSICAL_PARTITION_ID = 60005L;
    private static final long INDEX_ID = 60006L;
    private static final long TABLET_ID = 60007L;
    private static final long BACKEND_ID = 60008L;
    private static final long BACKEND_ID_2 = 60010L;
    private static final long REPLICA_ID = 60009L;
    private static final long REPLICA_ID_2 = 60011L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        // Create database and table directly (no mincluster)
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        
        // Add backend
        Backend backend = new Backend(BACKEND_ID, "127.0.0.1", 9050);
        backend.setAlive(true);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend);
        Backend backend2 = new Backend(BACKEND_ID_2, "127.0.0.2", 9050);
        backend2.setAlive(true);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend2);
        
        // Set config to allow force delete
        Config.tablet_sched_always_force_decommission_replica = true;
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
        Config.tablet_sched_always_force_decommission_replica = false;
    }

    private static OlapTable createOlapTable(long tableId, String tableName) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(3, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        // Add replica to tablet
        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(2, 2, 2);
        tablet.addReplica(replica);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, 
                com.starrocks.sql.ast.KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, 
                com.starrocks.thrift.TStorageType.COLUMN, com.starrocks.sql.ast.KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new com.starrocks.catalog.TableProperty(new java.util.HashMap<>()));
        return olapTable;
    }

    private static TabletSchedCtx createRunningTabletCtx(LocalTablet tablet, long visibleVersion, int schemaHash) {
        TabletSchedCtx tabletCtx = new TabletSchedCtx(
                TabletSchedCtx.Type.REPAIR,
                DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TABLET_ID,
                System.currentTimeMillis(),
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
        tabletCtx.setTablet(tablet);
        tabletCtx.setSchemaHash(schemaHash);
        tabletCtx.setStorageMedium(TStorageMedium.HDD);
        tabletCtx.setVersionInfo(visibleVersion, visibleVersion, 0L, 0L);
        tabletCtx.setDest(BACKEND_ID_2, 200L);
        tabletCtx.setState(TabletSchedCtx.State.RUNNING);
        return tabletCtx;
    }

    private static CloneTask createCloneTask(long visibleVersion, int schemaHash) {
        CloneTask cloneTask = new CloneTask(BACKEND_ID_2, "127.0.0.2",
                DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TABLET_ID,
                schemaHash,
                List.of(new TBackend("127.0.0.1", 9050, 9051)),
                TStorageMedium.HDD,
                visibleVersion,
                600);
        cloneTask.setPathHash(100L, 200L);
        return cloneTask;
    }

    private static TFinishTaskRequest createSuccessFinishTaskRequest(long visibleVersion, int schemaHash) {
        TFinishTaskRequest request = new TFinishTaskRequest();
        request.setTask_status(new TStatus(TStatusCode.OK));
        TTabletInfo tabletInfo = new TTabletInfo();
        tabletInfo.setTablet_id(TABLET_ID);
        tabletInfo.setSchema_hash(schemaHash);
        tabletInfo.setVersion(visibleVersion);
        tabletInfo.setMax_readable_version(visibleVersion);
        tabletInfo.setMin_readable_version(visibleVersion);
        tabletInfo.setData_size(100L);
        tabletInfo.setRow_count(200L);
        tabletInfo.setPath_hash(200L);
        request.setFinish_tablet_infos(List.of(tabletInfo));
        return request;
    }

    @Test
    public void testFinishCloneTaskLogAddReplicaNormalCase() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_add_replica");
        db.registerTableUnlocked(table);

        table.getPartitionInfo().setReplicationNum(PARTITION_ID, (short) 3);
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica cloneReplica = new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.CLONE);
        cloneReplica.updateVersionInfo(1, 1, 1);
        tablet.addReplica(cloneReplica);

        long visibleVersion = partition.getDefaultPhysicalPartition().getVisibleVersion();
        int schemaHash = table.getSchemaHashByIndexMetaId(INDEX_ID);
        TabletSchedCtx tabletCtx = createRunningTabletCtx(tablet, visibleVersion, schemaHash);
        CloneTask cloneTask = createCloneTask(visibleVersion, schemaHash);
        TFinishTaskRequest request = createSuccessFinishTaskRequest(visibleVersion, schemaHash);

        tabletCtx.finishCloneTask(cloneTask, request, new TabletSchedulerStat());
        Assertions.assertEquals(TabletSchedCtx.State.FINISHED, tabletCtx.getState());
        Assertions.assertEquals(Replica.ReplicaState.NORMAL, cloneReplica.getState());

        ReplicaPersistInfo replayInfo = (ReplicaPersistInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_REPLICA_V2);
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(BACKEND_ID_2, replayInfo.getBackendId());

        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createOlapTable(TABLE_ID, "test_add_replica");
        followerDb.registerTableUnlocked(followerTable);
        Partition followerPartition = followerTable.getPartition(PARTITION_ID);
        LocalTablet followerTablet = (LocalTablet) followerPartition.getDefaultPhysicalPartition().getIndex(INDEX_ID)
                .getTablet(TABLET_ID);
        Assertions.assertNull(followerTablet.getReplicaByBackendId(BACKEND_ID_2));

        followerMetastore.replayAddReplica(replayInfo);
        Assertions.assertNotNull(followerTablet.getReplicaByBackendId(BACKEND_ID_2));
    }

    @Test
    public void testFinishCloneTaskLogAddReplicaEditLogException() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_add_replica_error");
        db.registerTableUnlocked(table);

        table.getPartitionInfo().setReplicationNum(PARTITION_ID, (short) 3);
        Partition partition = table.getPartition(PARTITION_ID);
        MaterializedIndex index = partition.getDefaultPhysicalPartition().getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica cloneReplica = new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.CLONE);
        cloneReplica.updateVersionInfo(1, 1, 1);
        tablet.addReplica(cloneReplica);

        long visibleVersion = partition.getDefaultPhysicalPartition().getVisibleVersion();
        int schemaHash = table.getSchemaHashByIndexMetaId(INDEX_ID);
        TabletSchedCtx tabletCtx = createRunningTabletCtx(tablet, visibleVersion, schemaHash);
        CloneTask cloneTask = createCloneTask(visibleVersion, schemaHash);
        TFinishTaskRequest request = createSuccessFinishTaskRequest(visibleVersion, schemaHash);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddReplica(any(ReplicaPersistInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> tabletCtx.finishCloneTask(cloneTask, request, new TabletSchedulerStat()));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(TabletSchedCtx.State.RUNNING, tabletCtx.getState());
            Assertions.assertEquals(Replica.ReplicaState.CLONE, cloneReplica.getState());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testFinishCloneTaskLogUpdateReplicaNormalCase() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_update_replica");
        db.registerTableUnlocked(table);

        table.getPartitionInfo().setReplicationNum(PARTITION_ID, (short) 3);
        Partition partition = table.getPartition(PARTITION_ID);
        MaterializedIndex index = partition.getDefaultPhysicalPartition().getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica replica = new Replica(REPLICA_ID_2, BACKEND_ID_2, 1, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(1, 1, 1);
        tablet.addReplica(replica);

        long visibleVersion = Math.max(2L, partition.getDefaultPhysicalPartition().getVisibleVersion());
        int schemaHash = table.getSchemaHashByIndexMetaId(INDEX_ID);
        TabletSchedCtx tabletCtx = createRunningTabletCtx(tablet, visibleVersion, schemaHash);
        CloneTask cloneTask = createCloneTask(visibleVersion, schemaHash);
        TFinishTaskRequest request = createSuccessFinishTaskRequest(visibleVersion, schemaHash);

        tabletCtx.finishCloneTask(cloneTask, request, new TabletSchedulerStat());
        Assertions.assertEquals(TabletSchedCtx.State.FINISHED, tabletCtx.getState());

        ReplicaPersistInfo replayInfo = (ReplicaPersistInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_REPLICA_V2);
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(BACKEND_ID_2, replayInfo.getBackendId());

        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createOlapTable(TABLE_ID, "test_update_replica");
        followerDb.registerTableUnlocked(followerTable);
        Partition followerPartition = followerTable.getPartition(PARTITION_ID);
        LocalTablet followerTablet = (LocalTablet) followerPartition.getDefaultPhysicalPartition().getIndex(INDEX_ID)
                .getTablet(TABLET_ID);
        Replica followerReplica = new Replica(REPLICA_ID_2, BACKEND_ID_2, 1, Replica.ReplicaState.NORMAL);
        followerReplica.updateVersionInfo(1, 1, 1);
        followerTablet.addReplica(followerReplica);

        followerMetastore.replayUpdateReplica(replayInfo);
        Assertions.assertEquals(replayInfo.getVersion(), followerReplica.getVersion());
        Assertions.assertFalse(followerReplica.isBad());
    }

    @Test
    public void testFinishCloneTaskLogUpdateReplicaEditLogException() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_update_replica_error");
        db.registerTableUnlocked(table);

        table.getPartitionInfo().setReplicationNum(PARTITION_ID, (short) 3);
        Partition partition = table.getPartition(PARTITION_ID);
        MaterializedIndex index = partition.getDefaultPhysicalPartition().getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica replica = new Replica(REPLICA_ID_2, BACKEND_ID_2, 1, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(1, 1, 1);
        tablet.addReplica(replica);

        long visibleVersion = Math.max(2L, partition.getDefaultPhysicalPartition().getVisibleVersion());
        int schemaHash = table.getSchemaHashByIndexMetaId(INDEX_ID);
        TabletSchedCtx tabletCtx = createRunningTabletCtx(tablet, visibleVersion, schemaHash);
        CloneTask cloneTask = createCloneTask(visibleVersion, schemaHash);
        TFinishTaskRequest request = createSuccessFinishTaskRequest(visibleVersion, schemaHash);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateReplica(any(ReplicaPersistInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> tabletCtx.finishCloneTask(cloneTask, request, new TabletSchedulerStat()));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(TabletSchedCtx.State.RUNNING, tabletCtx.getState());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testDeleteReplicaInternalNormalCase() throws Exception {
        // 1. Create table with tablet and replica
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_table");
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        // 2. Create TabletSchedCtx
        TabletSchedCtx tabletCtx = new TabletSchedCtx(
                TabletSchedCtx.Type.REPAIR,
                DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TABLET_ID,
                System.currentTimeMillis(),
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
        
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        tabletCtx.setTablet(tablet);

        // Verify initial state - tablet has replica
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID));

        // 3. Create TabletScheduler
        TabletScheduler tabletScheduler = new TabletScheduler(new TabletSchedulerStat());

        // 4. Execute deleteReplicaInternal with force = true
        tabletScheduler.deleteReplicaInternal(tabletCtx, replica, "test reason", true);

        // 5. Verify master state - replica should be deleted from tablet
        Assertions.assertNull(tablet.getReplicaByBackendId(BACKEND_ID));

        // 6. Test follower replay
        ReplicaPersistInfo replayInfo = (ReplicaPersistInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DELETE_REPLICA_V2);

        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getTableId());
        Assertions.assertEquals(PHYSICAL_PARTITION_ID, replayInfo.getPartitionId());
        Assertions.assertEquals(INDEX_ID, replayInfo.getIndexId());
        Assertions.assertEquals(TABLET_ID, replayInfo.getTabletId());
        Assertions.assertEquals(BACKEND_ID, replayInfo.getBackendId());

        // Create follower metastore and the same id objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);

        // Create table with same ID
        OlapTable followerTable = createOlapTable(TABLE_ID, "test_table");
        followerDb.registerTableUnlocked(followerTable);

        // Add tablet to inverted index for follower
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Replica followerReplica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, followerReplica);

        // Verify follower initial state
        Partition followerPartition = followerTable.getPartition(PARTITION_ID);
        PhysicalPartition followerPhysicalPartition = followerPartition.getDefaultPhysicalPartition();
        MaterializedIndex followerIndex = followerPhysicalPartition.getIndex(INDEX_ID);
        LocalTablet followerTablet = (LocalTablet) followerIndex.getTablet(TABLET_ID);
        Assertions.assertNotNull(followerTablet.getReplicaByBackendId(BACKEND_ID));

        // Replay the operation
        followerMetastore.replayDeleteReplica(replayInfo);

        // 7. Verify follower state
        followerTablet = (LocalTablet) followerIndex.getTablet(TABLET_ID);
        Assertions.assertNull(followerTablet.getReplicaByBackendId(BACKEND_ID),
                "Replica should be deleted after replay");
        
        // Verify all properties match
        Assertions.assertEquals(DB_ID, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getTableId());
        Assertions.assertEquals(PHYSICAL_PARTITION_ID, replayInfo.getPartitionId());
        Assertions.assertEquals(INDEX_ID, replayInfo.getIndexId());
        Assertions.assertEquals(TABLET_ID, replayInfo.getTabletId());
        Assertions.assertEquals(BACKEND_ID, replayInfo.getBackendId());
    }

    @Test
    public void testDeleteReplicaInternalEditLogException() throws Exception {
        // 1. Create table with tablet and replica
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_table");
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        // 2. Create TabletSchedCtx
        TabletSchedCtx tabletCtx = new TabletSchedCtx(
                TabletSchedCtx.Type.REPAIR,
                DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TABLET_ID,
                System.currentTimeMillis(),
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
        
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        tabletCtx.setTablet(tablet);

        // Verify initial state
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID));

        // 3. Mock EditLog.logDeleteReplica to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDeleteReplica(any(ReplicaPersistInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Create TabletScheduler
        TabletScheduler tabletScheduler = new TabletScheduler(new TabletSchedulerStat());

        // 5. Execute deleteReplicaInternal and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            tabletScheduler.deleteReplicaInternal(tabletCtx, replica, "test reason", true);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 6. Verify replica may or may not be deleted (deleteReplica is called in WALApplier)
        // The replica deletion happens in the WALApplier callback, so if EditLog fails,
        // the callback won't be executed, and replica should still exist
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID),
                "Replica should still exist if EditLog write failed");
    }
}
