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

package com.starrocks.lake.restore;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.backup.BackupHandler;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

/**
 * Regression tests for RestoreHandler focusing on ID reset and task planning.
 */
public class TableSnapshotRestoreHandlerTest {
    private RestoreHandler handler;
    private GlobalStateMgr globalStateMgr;
    private BackupHandler backupHandler;
    private MockStarOSAgent starOSAgent;
    private TabletInvertedIndex tabletInvertedIndex;
    private SnapshotRestoreJob submittedJob;

    @BeforeEach
    public void setUp() {
        handler = new RestoreHandler();
        globalStateMgr = GlobalStateMgr.getCurrentState();
        backupHandler = globalStateMgr.getBackupHandler();
        starOSAgent = new MockStarOSAgent();
        tabletInvertedIndex = globalStateMgr.getTabletInvertedIndex();

        globalStateMgr.setStarOSAgent(starOSAgent);

        GlobalStateMgr local = globalStateMgr;
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return local;
            }
        };

        new MockUp<BackupHandler>() {
            @Mock
            public void submitTableSnapshotRestoreJob(SnapshotRestoreJob job) throws AlreadyExistsException {
                submittedJob = job;
            }
        };

        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);
    }

    @Test
    public void testResetIdsForRestoreReassignsTabletIds() throws Exception {
        LakeTable table = createLakeTable();
        Database targetDb = new Database(22L, "target_db");
        long originalTableId = table.getId();

        RestoreHandler.RestoreIdMappings mappings = handler.resetIdsForRestore(targetDb, table);

        Assertions.assertNotEquals(originalTableId, table.getId());
        Assertions.assertEquals(2, mappings.tabletIdMapping.size());
    }

    @Test
    public void testResetIdsForRestoreUpdatesPhysicalPartitionMappings() throws Exception {
        LakeTable table = createLakeTable();
        Partition originalPartition = table.getPartitions().iterator().next();
        PhysicalPartition originalPhysicalPartition = originalPartition.getDefaultPhysicalPartition();
        long originalPartitionId = originalPartition.getId();
        long originalPhysicalPartitionId = originalPhysicalPartition.getId();

        handler.resetIdsForRestore(new Database(44L, "target_db"), table);

        Partition updatedPartition = table.getPartitions().iterator().next();
        PhysicalPartition updatedPhysicalPartition = updatedPartition.getDefaultPhysicalPartition();

        Assertions.assertNotEquals(originalPartitionId, updatedPartition.getId());
        Assertions.assertNotEquals(originalPhysicalPartitionId, updatedPhysicalPartition.getId());
        Assertions.assertEquals(updatedPartition.getId(), updatedPhysicalPartition.getParentId());
        Assertions.assertEquals(updatedPartition.getId(),
                table.getPhysicalPartitionIdToPartitionId().get(updatedPhysicalPartition.getId()));
        Assertions.assertFalse(table.getPhysicalPartitionIdToPartitionId().containsKey(originalPhysicalPartitionId));

        // Check materialized index
        Assertions.assertEquals(
                table.getBaseIndexMetaId(), (long) Deencapsulation.getField(updatedPhysicalPartition, "baseIndexMetaId"));
        MaterializedIndex baseIndex = updatedPhysicalPartition.getLatestBaseIndex();
        Assertions.assertNotNull(baseIndex);
        Assertions.assertEquals(baseIndex, updatedPhysicalPartition.getLatestIndex(table.getBaseIndexMetaId()));
        Assertions.assertEquals(baseIndex, updatedPhysicalPartition.getIndex(baseIndex.getId()));
    }

    private LakeTable createLakeTable() {
        List<Column> columns = Lists.newArrayList(new Column("c0", IntegerType.BIGINT, true));
        LakeTable table = new LakeTable(100L, "lake_table", columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(2));

        TableProperty property = new TableProperty(Maps.newHashMap());
        FilePathInfo pathInfo = FilePathInfo.newBuilder().build();
        FileCacheInfo cacheInfo = FileCacheInfo.newBuilder().build();
        property.setStorageInfo(new StorageInfo(pathInfo, cacheInfo));
        table.setTableProperty(property);

        long indexId = 200L;
        MaterializedIndex baseIndex = new MaterializedIndex(indexId);
        baseIndex.setState(IndexState.NORMAL);
        table.setBaseIndexMetaId(indexId);
        table.setIndexMeta(indexId, table.getName(), columns, 0, 0,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);

        long physicalPartitionId = 301L;
        Partition partition = new Partition(300L, physicalPartitionId, "p1", baseIndex,
                new RandomDistributionInfo(2));
        table.addPartition(partition);

        long[] tabletIds = new long[] {4001L, 4002L};
        for (long tabletId : tabletIds) {
            LakeTablet tablet = new LakeTablet(tabletId);
            TabletMeta meta = new TabletMeta(10L, table.getId(), physicalPartitionId, indexId,
                    TStorageMedium.HDD, true);
            baseIndex.addTablet(tablet, meta, false);
        }

        return table;
    }

    private static class MockStarOSAgent extends StarOSAgent {
        private long shardGroupId = 5000L;
        private long shardId = 100000L;

        @Override
        public FilePathInfo allocateFilePath(long dbId, long tableId) {
            return FilePathInfo.newBuilder().build();
        }

        @Override
        public long createShardGroup(long dbId, long tableId, long partitionId, long indexId) {
            return shardGroupId++;
        }

        @Override
        public List<Long> createShards(int numShards, FilePathInfo pathInfo, FileCacheInfo cacheInfo, long groupId,
                                       List<Long> matchShardIds, Map<String, String> properties,
                                       ComputeResource computeResource) throws DdlException {
            List<Long> ids = Lists.newArrayList();
            for (int i = 0; i < numShards; i++) {
                ids.add(shardId++);
            }
            return ids;
        }
    }
}
