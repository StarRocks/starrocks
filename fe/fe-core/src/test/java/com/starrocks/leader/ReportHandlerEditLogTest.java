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

package com.starrocks.leader;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
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
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.BatchDeleteReplicaInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ReportHandlerEditLogTest {
    private static final String DB_NAME = "test_report_handler_editlog";
    private static final long DB_ID = 70001L;
    private static final long TABLE_ID = 70002L;
    private static final long PARTITION_ID = 70004L;
    private static final long PHYSICAL_PARTITION_ID = 70005L;
    private static final long INDEX_ID = 70006L;
    private static final long TABLET_ID = 70007L;
    private static final long BACKEND_ID = 70008L;
    private static final long BACKEND_ID_2 = 70011L;
    private static final long REPLICA_ID = 70009L;
    private static final long REPLICA_ID_2 = 70012L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        // Create database and table directly (no mincluster)
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        
        // Add backends with empty disks (so replica pathHash won't match any disk)
        // This ensures deleteFromMeta will process the tablet
        Backend backend = new Backend(BACKEND_ID, "127.0.0.1", 9050);
        backend.setAlive(true);
        backend.setDisks(ImmutableMap.of()); // Empty disks
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend);
        
        Backend backend2 = new Backend(BACKEND_ID_2, "127.0.0.2", 9050);
        backend2.setAlive(true);
        backend2.setDisks(ImmutableMap.of()); // Empty disks
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend2);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
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
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
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

    @Test
    public void testDeleteFromMetaDeleteReplica() throws Exception {
        // Test deleteFromMeta with multiple replicas - should delete replica
        // 1. Create table with tablet and multiple replicas
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_table");
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        
        // Get tablet reference
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);

        // Add first replica to tablet and inverted index
        Replica replica1 = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica1.updateVersionInfo(2, 2, 2);
        replica1.setDeferReplicaDeleteToNextReport(false); // Allow immediate deletion
        tablet.addReplica(replica1);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica1);
        
        // Add second replica to make it multi-replica tablet
        Replica replica2 = new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL);
        replica2.updateVersionInfo(2, 2, 2);
        tablet.addReplica(replica2);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica2);

        // Verify initial state - tablet has 2 replicas
        Assertions.assertEquals(2, tablet.getImmutableReplicas().size());
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID));
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID_2));

        // 2. Construct tabletDeleteFromMeta - tablet should be deleted from meta
        ListMultimap<Long, Long> tabletDeleteFromMeta = ArrayListMultimap.create();
        tabletDeleteFromMeta.put(DB_ID, TABLET_ID);
        
        // 3. Call deleteFromMeta directly
        // Since disks are empty, diskInfo will be null, so version check won't execute
        long backendReportVersion = 1L;
        ReportHandler.deleteFromMeta(tabletDeleteFromMeta, BACKEND_ID, backendReportVersion);

        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID_2),
                "Other replica should still exist");
        Assertions.assertEquals(1, tablet.getImmutableReplicas().size());

        // 4. Verify EditLog was written and deletion happened
        BatchDeleteReplicaInfo replayInfo = (BatchDeleteReplicaInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BATCH_DELETE_REPLICA);

        Assertions.assertNotNull(replayInfo, "EditLog should be written");
        Assertions.assertEquals(BACKEND_ID, replayInfo.getBackendId());
        Assertions.assertTrue(replayInfo.getTablets().contains(TABLET_ID),
                "Tablet should be in delete list");

        // 5. Test follower replay - create a new LocalMetastore and construct same table/tablet
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);

        // Create table with same ID
        OlapTable followerTable = createOlapTable(TABLE_ID, "test_table");
        followerDb.registerTableUnlocked(followerTable);

        // Add tablet to inverted index for follower
        TabletMeta followerTabletMeta = new TabletMeta(DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, followerTabletMeta);

        // Get follower tablet reference
        Partition followerPartition = followerTable.getPartition(PARTITION_ID);
        PhysicalPartition followerPhysicalPartition = followerPartition.getDefaultPhysicalPartition();
        MaterializedIndex followerIndex = followerPhysicalPartition.getIndex(INDEX_ID);
        LocalTablet followerTablet = (LocalTablet) followerIndex.getTablet(TABLET_ID);

        // Add replicas to follower tablet (same as leader)
        Replica followerReplica1 = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        followerReplica1.updateVersionInfo(2, 2, 2);
        followerTablet.addReplica(followerReplica1);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, followerReplica1);

        Replica followerReplica2 = new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL);
        followerReplica2.updateVersionInfo(2, 2, 2);
        followerTablet.addReplica(followerReplica2);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, followerReplica2);

        // Verify follower initial state - tablet has 2 replicas
        Assertions.assertEquals(2, followerTablet.getImmutableReplicas().size());
        Assertions.assertNotNull(followerTablet.getReplicaByBackendId(BACKEND_ID),
                "Follower replica1 should exist before replay");
        Assertions.assertNotNull(followerTablet.getReplicaByBackendId(BACKEND_ID_2),
                "Follower replica2 should exist before replay");

        // Replay the operation
        followerMetastore.replayBatchDeleteReplica(replayInfo);

        // 6. Verify follower state - replica1 should be deleted, replica2 should still exist
        followerTablet = (LocalTablet) followerIndex.getTablet(TABLET_ID);
        Assertions.assertNull(followerTablet.getReplicaByBackendId(BACKEND_ID),
                "Follower replica1 should be deleted after replay");
        Assertions.assertNotNull(followerTablet.getReplicaByBackendId(BACKEND_ID_2),
                "Follower replica2 should still exist after replay");
        Assertions.assertEquals(1, followerTablet.getImmutableReplicas().size(),
                "Follower tablet should have only 1 replica after replay");
    }

    @Test
    public void testDeleteFromMetaSingleReplicaSetBad() throws Exception {
        // Test deleteFromMeta with single replica - should setBad instead of delete
        // 1. Create table with tablet and single replica
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_table");
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(2, 2, 2);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        // Get tablet reference
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);

        // Verify initial state - tablet has only 1 replica
        Assertions.assertEquals(1, tablet.getImmutableReplicas().size());
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID));
        Assertions.assertFalse(tablet.getReplicaByBackendId(BACKEND_ID).isBad());

        // 2. Construct tabletDeleteFromMeta - tablet should be deleted from meta
        ListMultimap<Long, Long> tabletDeleteFromMeta = ArrayListMultimap.create();
        tabletDeleteFromMeta.put(DB_ID, TABLET_ID);
        
        // 3. Call deleteFromMeta directly
        // Since disks are empty, diskInfo will be null, so version check won't execute
        long backendReportVersion = 1L;
        ReportHandler.deleteFromMeta(tabletDeleteFromMeta, BACKEND_ID, backendReportVersion);

        // 5. Verify replica is setBad (not deleted) since tablet has only one replica
        Replica replicaAfter = tablet.getReplicaByBackendId(BACKEND_ID);
        Assertions.assertNotNull(replicaAfter, "Replica should not be deleted when tablet has only one replica");
        Assertions.assertTrue(replicaAfter.isBad(), "Replica should be setBad when tablet has only one replica");
        Assertions.assertEquals(1, tablet.getImmutableReplicas().size());

        // 6. Verify EditLog was written (BackendTabletsInfo for setBad)
        BackendTabletsInfo replayInfo = (BackendTabletsInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_TABLETS_INFO_V2);
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(BACKEND_ID, replayInfo.getBackendId());
        Assertions.assertTrue(replayInfo.isBad());
        Assertions.assertTrue(replayInfo.getReplicaPersistInfos().stream()
                .anyMatch(info -> info.getTabletId() == TABLET_ID && info.getReplicaId() == REPLICA_ID));

        // 7. Test follower replay - create a new LocalMetastore and construct same table/tablet
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);

        // Create table with same ID
        OlapTable followerTable = createOlapTable(TABLE_ID, "test_table");
        followerDb.registerTableUnlocked(followerTable);

        // Add tablet to inverted index for follower
        TabletMeta followerTabletMeta = new TabletMeta(DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, followerTabletMeta);

        // Get follower tablet reference
        Partition followerPartition = followerTable.getPartition(PARTITION_ID);
        PhysicalPartition followerPhysicalPartition = followerPartition.getDefaultPhysicalPartition();
        MaterializedIndex followerIndex = followerPhysicalPartition.getIndex(INDEX_ID);
        LocalTablet followerTablet = (LocalTablet) followerIndex.getTablet(TABLET_ID);

        // Add replica to follower tablet (same as leader - single replica)
        Replica followerReplica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        followerReplica.updateVersionInfo(2, 2, 2);
        followerTablet.addReplica(followerReplica);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, followerReplica);

        // Verify follower initial state - tablet has only 1 replica and replica is not bad
        Assertions.assertEquals(1, followerTablet.getImmutableReplicas().size());
        Assertions.assertNotNull(followerTablet.getReplicaByBackendId(BACKEND_ID),
                "Follower replica should exist before replay");
        Assertions.assertFalse(followerTablet.getReplicaByBackendId(BACKEND_ID).isBad(),
                "Follower replica should not be bad before replay");

        // Replay the operation
        followerMetastore.replayBackendTabletsInfo(replayInfo);

        // 8. Verify follower state - replica should be setBad (not deleted) since tablet has only one replica
        followerTablet = (LocalTablet) followerIndex.getTablet(TABLET_ID);
        Replica followerReplicaAfter = followerTablet.getReplicaByBackendId(BACKEND_ID);
        Assertions.assertNotNull(followerReplicaAfter,
                "Follower replica should not be deleted when tablet has only one replica");
        Assertions.assertTrue(followerReplicaAfter.isBad(),
                "Follower replica should be setBad after replay");
        Assertions.assertEquals(1, followerTablet.getImmutableReplicas().size(),
                "Follower tablet should still have 1 replica after replay");
    }

    @Test
    public void testHandleRecoverTabletNormalCase() throws Exception {
        // Test handleRecoverTablet by constructing scenario where needRecover returns true
        // 1. Create table with tablet and replica
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_table");
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(5, 5, 5); // Set lastReportVersion to 5
        replica.setLastReportVersion(5);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        // Get tablet reference
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica tabletReplica = tablet.getReplicaByBackendId(BACKEND_ID);

        // Verify initial state
        Assertions.assertNotNull(tabletReplica);
        Assertions.assertFalse(tabletReplica.isBad());

        // 2. Construct tabletRecoveryMap - tablet needs recovery
        ListMultimap<Long, Long> tabletRecoveryMap = ArrayListMultimap.create();
        tabletRecoveryMap.put(DB_ID, TABLET_ID);
        
        // 3. Construct backendTablets for handleRecoverTablet
        Map<Long, TTablet> backendTablets = new HashMap<>();
        TTablet backendTablet = new TTablet();
        TTabletInfo tabletInfo = new TTabletInfo();
        tabletInfo.setTablet_id(TABLET_ID);
        tabletInfo.setSchema_hash(0);
        tabletInfo.setVersion(3); // Version less than lastReportVersion (5) to trigger needRecover
        tabletInfo.setUsed(false); // Bad tablet
        tabletInfo.setPartition_id(PHYSICAL_PARTITION_ID);
        backendTablet.addToTablet_infos(tabletInfo);
        backendTablets.put(TABLET_ID, backendTablet);

        // 4. Call handleRecoverTablet directly
        ReportHandler.handleRecoverTablet(tabletRecoveryMap, backendTablets, BACKEND_ID);

        // 5. Verify replica is setBad
        Assertions.assertTrue(tabletReplica.isBad(), "Replica should be setBad after handleRecoverTablet");

        // 6. Verify EditLog was written
        BackendTabletsInfo replayInfo = (BackendTabletsInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_TABLETS_INFO_V2);
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(BACKEND_ID, replayInfo.getBackendId());
        Assertions.assertTrue(replayInfo.isBad());
        Assertions.assertTrue(replayInfo.getReplicaPersistInfos().stream()
                .anyMatch(info -> info.getTabletId() == TABLET_ID && info.getReplicaId() == REPLICA_ID));

        // 7. Test follower replay - create a new LocalMetastore and construct same table/tablet
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);

        // Create table with same ID
        OlapTable followerTable = createOlapTable(TABLE_ID, "test_table");
        followerDb.registerTableUnlocked(followerTable);

        // Add tablet to inverted index for follower
        TabletMeta followerTabletMeta = new TabletMeta(DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, followerTabletMeta);

        // Get follower tablet reference
        Partition followerPartition = followerTable.getPartition(PARTITION_ID);
        PhysicalPartition followerPhysicalPartition = followerPartition.getDefaultPhysicalPartition();
        MaterializedIndex followerIndex = followerPhysicalPartition.getIndex(INDEX_ID);
        LocalTablet followerTablet = (LocalTablet) followerIndex.getTablet(TABLET_ID);

        // Add replica to follower tablet (same as leader)
        Replica followerReplica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        followerReplica.updateVersionInfo(5, 5, 5); // Set lastReportVersion to 5, same as leader
        followerReplica.setLastReportVersion(5);
        followerTablet.addReplica(followerReplica);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, followerReplica);

        // Verify follower initial state - replica is not bad
        Assertions.assertNotNull(followerTablet.getReplicaByBackendId(BACKEND_ID),
                "Follower replica should exist before replay");
        Assertions.assertFalse(followerTablet.getReplicaByBackendId(BACKEND_ID).isBad(),
                "Follower replica should not be bad before replay");

        // Replay the operation
        followerMetastore.replayBackendTabletsInfo(replayInfo);

        // 8. Verify follower state - replica should be setBad after replay
        followerTablet = (LocalTablet) followerIndex.getTablet(TABLET_ID);
        Replica followerReplicaAfter = followerTablet.getReplicaByBackendId(BACKEND_ID);
        Assertions.assertNotNull(followerReplicaAfter,
                "Follower replica should still exist after replay");
        Assertions.assertTrue(followerReplicaAfter.isBad(),
                "Follower replica should be setBad after replay");
    }

    @Test
    public void testDeleteFromMetaDeleteReplicaEditLogException() throws Exception {
        // Test deleteFromMeta with EditLog exception - should throw exception and not delete replica
        // 1. Create table with tablet and multiple replicas
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_table");
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        
        // Get tablet reference
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        
        // Add first replica to tablet and inverted index
        Replica replica1 = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica1.updateVersionInfo(2, 2, 2);
        replica1.setDeferReplicaDeleteToNextReport(false); // Allow immediate deletion
        tablet.addReplica(replica1);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica1);
        
        // Add second replica to make it multi-replica tablet
        Replica replica2 = new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL);
        replica2.updateVersionInfo(2, 2, 2);
        tablet.addReplica(replica2);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica2);

        // Verify initial state - tablet has 2 replicas
        Assertions.assertEquals(2, tablet.getImmutableReplicas().size());
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID));
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID_2));

        // 2. Mock EditLog.logBatchDeleteReplica to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logBatchDeleteReplica(any(BatchDeleteReplicaInfo.class), any());

        // Temporarily set spy EditLog
        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Construct tabletDeleteFromMeta - tablet should be deleted from meta
        ListMultimap<Long, Long> tabletDeleteFromMeta = ArrayListMultimap.create();
        tabletDeleteFromMeta.put(DB_ID, TABLET_ID);
        
        // 4. Call deleteFromMeta and expect exception
        long backendReportVersion = 1L;
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            ReportHandler.deleteFromMeta(tabletDeleteFromMeta, BACKEND_ID, backendReportVersion);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 5. Restore original EditLog
        GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);

        // 6. Verify replica is NOT deleted (because EditLog failed, WALApplier won't execute)
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID),
                "Replica should not be deleted when EditLog write fails");
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID_2),
                "Other replica should still exist");
        Assertions.assertEquals(2, tablet.getImmutableReplicas().size(),
                "Tablet should still have 2 replicas when EditLog write fails");
    }

    @Test
    public void testDeleteFromMetaSingleReplicaSetBadEditLogException() throws Exception {
        // Test deleteFromMeta with EditLog exception - should throw exception and not setBad replica
        // 1. Create table with tablet and single replica
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_table");
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(2, 2, 2);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        // Get tablet reference
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);

        // Verify initial state - tablet has only 1 replica and replica is not bad
        Assertions.assertEquals(1, tablet.getImmutableReplicas().size());
        Assertions.assertNotNull(tablet.getReplicaByBackendId(BACKEND_ID));
        Assertions.assertFalse(tablet.getReplicaByBackendId(BACKEND_ID).isBad());

        // 2. Mock EditLog.logBackendTabletsInfo to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logBackendTabletsInfo(any(BackendTabletsInfo.class), any());

        // Temporarily set spy EditLog
        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Construct tabletDeleteFromMeta - tablet should be deleted from meta
        ListMultimap<Long, Long> tabletDeleteFromMeta = ArrayListMultimap.create();
        tabletDeleteFromMeta.put(DB_ID, TABLET_ID);
        
        // 4. Call deleteFromMeta and expect exception
        long backendReportVersion = 1L;
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            ReportHandler.deleteFromMeta(tabletDeleteFromMeta, BACKEND_ID, backendReportVersion);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 5. Restore original EditLog
        GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);

        // 6. Verify replica is NOT setBad (because EditLog failed, WALApplier won't execute)
        Replica replicaAfter = tablet.getReplicaByBackendId(BACKEND_ID);
        Assertions.assertNotNull(replicaAfter, "Replica should not be deleted when tablet has only one replica");
        Assertions.assertFalse(replicaAfter.isBad(), "Replica should not be setBad when EditLog write fails");
        Assertions.assertEquals(1, tablet.getImmutableReplicas().size());
    }

    @Test
    public void testHandleRecoverTabletNormalCaseEditLogException() throws Exception {
        // Test handleRecoverTablet with EditLog exception - should throw exception and not setBad replica
        // 1. Create table with tablet and replica
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_table");
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(5, 5, 5); // Set lastReportVersion to 5
        replica.setLastReportVersion(5);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        // Get tablet reference
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica tabletReplica = tablet.getReplicaByBackendId(BACKEND_ID);

        // Verify initial state
        Assertions.assertNotNull(tabletReplica);
        Assertions.assertFalse(tabletReplica.isBad());

        // 2. Mock EditLog.logBackendTabletsInfo to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logBackendTabletsInfo(any(BackendTabletsInfo.class), any());
        
        // Temporarily set spy EditLog
        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Construct tabletRecoveryMap - tablet needs recovery
        ListMultimap<Long, Long> tabletRecoveryMap = ArrayListMultimap.create();
        tabletRecoveryMap.put(DB_ID, TABLET_ID);
        
        // 4. Construct backendTablets for handleRecoverTablet
        Map<Long, TTablet> backendTablets = new HashMap<>();
        TTablet backendTablet = new TTablet();
        TTabletInfo tabletInfo = new TTabletInfo();
        tabletInfo.setTablet_id(TABLET_ID);
        tabletInfo.setSchema_hash(0);
        tabletInfo.setVersion(3); // Version less than lastReportVersion (5) to trigger needRecover
        tabletInfo.setUsed(false); // Bad tablet
        tabletInfo.setPartition_id(PHYSICAL_PARTITION_ID);
        backendTablet.addToTablet_infos(tabletInfo);
        backendTablets.put(TABLET_ID, backendTablet);

        // 5. Call handleRecoverTablet and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            ReportHandler.handleRecoverTablet(tabletRecoveryMap, backendTablets, BACKEND_ID);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 6. Restore original EditLog
        GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);

        // 7. Verify replica is NOT setBad (because EditLog failed, WALApplier won't execute)
        Assertions.assertFalse(tabletReplica.isBad(), "Replica should not be setBad when EditLog write fails");
    }
}
