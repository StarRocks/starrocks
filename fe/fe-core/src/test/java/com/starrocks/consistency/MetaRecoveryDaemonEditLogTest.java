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

package com.starrocks.consistency;

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
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class MetaRecoveryDaemonEditLogTest {
    private static final String DB_NAME = "test_meta_recovery_editlog";
    private static final long DB_ID = 50001L;
    private static final long TABLE_ID = 50002L;
    private static final long PARTITION_ID = 50004L;
    private static final long PHYSICAL_PARTITION_ID = 50005L;
    private static final long INDEX_ID = 50006L;
    private static final long TABLET_ID = 50007L;
    private static final long BACKEND_ID = 50008L;
    private static final long REPLICA_ID = 50009L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        // Create database and table directly (no mincluster)
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
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
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        // Add replica to tablet
        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(2, 2, 2);
        tablet.addReplica(replica);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        physicalPartition.setVisibleVersion(1, System.currentTimeMillis());
        physicalPartition.setNextVersion(2);

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
    public void testRecoverNormalCase() throws Exception {
        // 1. Create table with partition
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, "test_table");
        db.registerTableUnlocked(table);

        // Verify initial state - partition has visible version 1
        PhysicalPartition physicalPartition = table.getPartition(PARTITION_ID).getDefaultPhysicalPartition();
        long initialVisibleVersion = physicalPartition.getVisibleVersion();
        Assertions.assertEquals(1L, initialVisibleVersion);

        // 2. Create PartitionVersionRecoveryInfo directly to test EditLog
        long recoverVersion = 2L;
        List<PartitionVersionRecoveryInfo.PartitionVersion> partitionVersions = new ArrayList<>();
        partitionVersions.add(new PartitionVersionRecoveryInfo.PartitionVersion(
                DB_ID, TABLE_ID, PHYSICAL_PARTITION_ID, recoverVersion));
        PartitionVersionRecoveryInfo recoveryInfo = 
                new PartitionVersionRecoveryInfo(partitionVersions, System.currentTimeMillis());

        // 3. Execute recoverPartitionVersion and log EditLog
        MetaRecoveryDaemon recoveryDaemon = new MetaRecoveryDaemon();
        // Log EditLog
        GlobalStateMgr.getCurrentState().getEditLog()
                .logRecoverPartitionVersion(recoveryInfo, wal -> recoveryDaemon.recoverPartitionVersion(recoveryInfo));

        // 4. Verify master state - partition version should be recovered
        physicalPartition = table.getPartition(PARTITION_ID).getDefaultPhysicalPartition();
        Assertions.assertEquals(recoverVersion, physicalPartition.getVisibleVersion(),
                "Partition visible version should be updated to recovery version");

        // 5. Test follower replay
        PartitionVersionRecoveryInfo replayInfo = (PartitionVersionRecoveryInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RECOVER_PARTITION_VERSION);

        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertNotNull(replayInfo.getPartitionVersions());
        Assertions.assertEquals(1, replayInfo.getPartitionVersions().size());
        Assertions.assertEquals(DB_ID, replayInfo.getPartitionVersions().get(0).getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getPartitionVersions().get(0).getTableId());
        Assertions.assertEquals(PHYSICAL_PARTITION_ID, replayInfo.getPartitionVersions().get(0).getPartitionId());
        Assertions.assertEquals(recoverVersion, replayInfo.getPartitionVersions().get(0).getVersion());
        Assertions.assertTrue(replayInfo.getRecoverTime() > 0);

        // Reset the table to initial state for replay test
        // First, reset the master table's version back to initial state
        physicalPartition = table.getPartition(PARTITION_ID).getDefaultPhysicalPartition();
        physicalPartition.setVisibleVersion(1L, System.currentTimeMillis());
        physicalPartition.setNextVersion(2L);

        // Replay the operation using the same GlobalStateMgr (simulating follower replay)
        MetaRecoveryDaemon followerRecoveryDaemon = new MetaRecoveryDaemon();
        followerRecoveryDaemon.recoverPartitionVersion(replayInfo);

        // 6. Verify follower state
        physicalPartition = table.getPartition(PARTITION_ID).getDefaultPhysicalPartition();
        // After replay, the visible version should be set to the recovery version
        Assertions.assertEquals(recoverVersion, physicalPartition.getVisibleVersion(),
                "Partition visible version should match recovery version after replay");
        
        // Verify all properties match in replayInfo
        PartitionVersionRecoveryInfo.PartitionVersion masterPv = recoveryInfo.getPartitionVersions().get(0);
        PartitionVersionRecoveryInfo.PartitionVersion followerPv = replayInfo.getPartitionVersions().get(0);
        Assertions.assertEquals(masterPv.getDbId(), followerPv.getDbId());
        Assertions.assertEquals(masterPv.getTableId(), followerPv.getTableId());
        Assertions.assertEquals(masterPv.getPartitionId(), followerPv.getPartitionId());
        Assertions.assertEquals(masterPv.getVersion(), followerPv.getVersion());
        
        // Verify recoverTime is set (may differ slightly due to timing)
        Assertions.assertTrue(replayInfo.getRecoverTime() > 0);
    }
}

