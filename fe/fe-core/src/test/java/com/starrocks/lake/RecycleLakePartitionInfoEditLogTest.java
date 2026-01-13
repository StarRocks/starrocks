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

package com.starrocks.lake;

import com.google.common.collect.Range;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.persist.DisablePartitionRecoveryInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class RecycleLakePartitionInfoEditLogTest {
    private static final String DB_NAME = "test_recycle_lake_partition_editlog";
    private static final long DB_ID = 40001L;
    private static final long TABLE_ID = 40002L;
    private static final long PARTITION_ID = 40004L;
    private static final long PHYSICAL_PARTITION_ID = 40005L;
    private static final long INDEX_ID = 40006L;
    private static final long TABLET_ID = 40007L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        // Create database directly (no mincluster)
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        
        // Initialize default warehouse
        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static Partition createPartition(long partitionId, long physicalPartitionId, String partitionName) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", IntegerType.BIGINT));

        DistributionInfo distributionInfo = new HashDistributionInfo(3, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, partitionId, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(partitionId, physicalPartitionId, partitionName, baseIndex, distributionInfo);
        return partition;
    }

    @Test
    public void testRecycleLakeListPartitionInfoDeleteNormalCase() throws Exception {
        // 1. Create partition
        Partition partition = createPartition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1");

        // 2. Create RecycleLakeListPartitionInfo with recoverable = true
        RecycleLakeListPartitionInfo partitionInfo = new RecycleLakeListPartitionInfo(
                DB_ID, TABLE_ID, partition,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        partitionInfo.setRecoverable(true);

        // Verify initial state
        Assertions.assertTrue(partitionInfo.isRecoverable());

        // 3. Mock LakeTableHelper.removePartitionDirectory to return true
        // Mock LakeTableHelper using MockUp
        new MockUp<LakeTableHelper>() {
            @Mock
            public boolean removePartitionDirectory(Partition partition, ComputeResource computeResource) {
                return true;
            }
            
            @Mock
            public void deleteShardGroupMeta(Partition partition) {
                // Do nothing
            }
        };

        // 4. Execute delete
        boolean result = partitionInfo.delete();
        // 5. Verify master state
        Assertions.assertTrue(result);
        Assertions.assertFalse(partitionInfo.isRecoverable(), "Partition should not be recoverable after delete");

        // 6. Test follower replay
        DisablePartitionRecoveryInfo replayInfo = (DisablePartitionRecoveryInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DISABLE_PARTITION_RECOVERY);

        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(PARTITION_ID, replayInfo.getPartitionId());

        CatalogRecycleBin followerRecycleBin = new CatalogRecycleBin();
        // 2. Create RecycleLakeListPartitionInfo with recoverable = true
        RecycleLakeListPartitionInfo followerPartitionInfo = new RecycleLakeListPartitionInfo(
                DB_ID, TABLE_ID, partition,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        followerPartitionInfo.setRecoverable(true);
        followerRecycleBin.recyclePartition(followerPartitionInfo);

        followerRecycleBin.replayDisablePartitionRecovery(replayInfo.getPartitionId());
        Assertions.assertFalse(followerPartitionInfo.isRecoverable());
    }

    @Test
    public void testRecycleLakeRangePartitionInfoDeleteNormalCase() throws Exception {
        // 1. Create partition
        Partition partition = createPartition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1");

        // 2. Create RecycleLakeRangePartitionInfo with recoverable = true
        Range<PartitionKey> range = Range.all();
        RecycleLakeRangePartitionInfo partitionInfo = new RecycleLakeRangePartitionInfo(
                DB_ID, TABLE_ID, partition, range,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        partitionInfo.setRecoverable(true);

        // Verify initial state
        Assertions.assertTrue(partitionInfo.isRecoverable());

        // 3. Mock LakeTableHelper.removePartitionDirectory to return true
        new MockUp<LakeTableHelper>() {
            @Mock
            public boolean removePartitionDirectory(Partition partition, ComputeResource computeResource) {
                return true;
            }
            
            @Mock
            public void deleteShardGroupMeta(Partition partition) {
                // Do nothing
            }
        };

        // 4. Execute delete
        boolean result = partitionInfo.delete();
        // 5. Verify master state
        Assertions.assertTrue(result);
        Assertions.assertFalse(partitionInfo.isRecoverable(), "Partition should not be recoverable after delete");

        // 6. Test follower replay
        DisablePartitionRecoveryInfo replayInfo = (DisablePartitionRecoveryInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DISABLE_PARTITION_RECOVERY);

        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(PARTITION_ID, replayInfo.getPartitionId());

        CatalogRecycleBin followerRecycleBin = new CatalogRecycleBin();
        // 2. Create RecycleLakeListPartitionInfo with recoverable = true
        RecycleLakeRangePartitionInfo followerPartitionInfo = new RecycleLakeRangePartitionInfo(
                DB_ID, TABLE_ID, partition, range,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        followerPartitionInfo.setRecoverable(true);
        followerRecycleBin.recyclePartition(followerPartitionInfo);

        followerRecycleBin.replayDisablePartitionRecovery(replayInfo.getPartitionId());
        Assertions.assertFalse(followerPartitionInfo.isRecoverable());
    }

    @Test
    public void testRecycleLakeUnPartitionInfoDeleteNormalCase() throws Exception {
        // 1. Create partition
        Partition partition = createPartition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1");

        // 2. Create RecycleLakeUnPartitionInfo with recoverable = true
        RecycleLakeUnPartitionInfo partitionInfo = new RecycleLakeUnPartitionInfo(
                DB_ID, TABLE_ID, partition,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        partitionInfo.setRecoverable(true);

        // Verify initial state
        Assertions.assertTrue(partitionInfo.isRecoverable());

        // 3. Mock LakeTableHelper.removePartitionDirectory to return true
        new MockUp<LakeTableHelper>() {
            @Mock
            public boolean removePartitionDirectory(Partition partition, ComputeResource computeResource) {
                return true;
            }
            
            @Mock
            public void deleteShardGroupMeta(Partition partition) {
                // Do nothing
            }
        };

        // 4. Execute delete
        boolean result = partitionInfo.delete();

        // 5. Verify master state
        Assertions.assertTrue(result);
        Assertions.assertFalse(partitionInfo.isRecoverable(), "Partition should not be recoverable after delete");

        // 6. Test follower replay
        DisablePartitionRecoveryInfo replayInfo = (DisablePartitionRecoveryInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DISABLE_PARTITION_RECOVERY);

        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(PARTITION_ID, replayInfo.getPartitionId());

        CatalogRecycleBin followerRecycleBin = new CatalogRecycleBin();
        // 2. Create RecycleLakeListPartitionInfo with recoverable = true
        RecycleLakeUnPartitionInfo followerPartitionInfo = new RecycleLakeUnPartitionInfo(
                DB_ID, TABLE_ID, partition,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        followerPartitionInfo.setRecoverable(true);
        followerRecycleBin.recyclePartition(followerPartitionInfo);

        followerRecycleBin.replayDisablePartitionRecovery(replayInfo.getPartitionId());
        Assertions.assertFalse(followerPartitionInfo.isRecoverable());
    }

    @Test
    public void testRecycleLakeListPartitionInfoDeleteEditLogException() throws Exception {
        // 1. Create partition
        Partition partition = createPartition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1");

        // 2. Create RecycleLakeListPartitionInfo with recoverable = true
        RecycleLakeListPartitionInfo partitionInfo = new RecycleLakeListPartitionInfo(
                DB_ID, TABLE_ID, partition,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        partitionInfo.setRecoverable(true);

        // Verify initial state
        Assertions.assertTrue(partitionInfo.isRecoverable());

        // 3. Mock EditLog.logDisablePartitionRecovery to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDisablePartitionRecovery(any(Long.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute delete and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            partitionInfo.delete();
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 5. Verify partitionInfo state remains unchanged after exception
        Assertions.assertTrue(partitionInfo.isRecoverable(), "Partition should remain recoverable after exception");
    }
}
