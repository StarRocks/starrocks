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
import com.starrocks.backup.Status;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.StarRocksException;
import com.starrocks.epack.warehouse.WarehouseManagerEPack;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.CreateDbInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.task.TabletRestoreAgentTask;
import com.starrocks.task.TabletTaskExecutor;
import com.starrocks.thrift.TRestoreTabletInfo;
import com.starrocks.thrift.TRestoreTabletRequest;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class LakeRestoreJobTest {

    @Test
    public void testRunFailsWhenExistingFailedTask() {
        RestoreTask.TabletRestoreEntry entry = new RestoreTask.TabletRestoreEntry(1L, 2L, 3L);
        RestoreTask task = new RestoreTask(10L, 5L, Lists.newArrayList(entry));
        task.setTaskState(RestoreTask.PartitionTaskState.FAILED);

        SnapshotRestoreJob job = createJob(Lists.newArrayList(task));
        job.run();

        Assertions.assertEquals(SnapshotRestoreJob.State.FAILED, job.getState());
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, job.getStatus().getErrCode());
        Assertions.assertEquals("Tablet restore job failed due to failed subtasks", job.getErrorMessage());
        Assertions.assertTrue(job.getFinishedTimeMs() > 0);
    }

    @Test
    public void testBuildRestoreRequestIncludesSchemaAndVersion() throws Exception {
        long targetSchemaId = 11L;
        long sourceTabletId = 12L;
        long targetTabletId = 13L;
        long sourceVisibleVersion = 77L;
        RestoreTask.TabletRestoreEntry entry =
                new RestoreTask.TabletRestoreEntry(targetSchemaId, sourceTabletId, targetTabletId);
        RestoreTask task = new RestoreTask(20L, sourceVisibleVersion, Lists.newArrayList(entry));
        SnapshotRestoreJob job = createJob(Lists.newArrayList(task));

        Method method = SnapshotRestoreJob.class.getDeclaredMethod("buildRestoreRequest", RestoreTask.class);
        method.setAccessible(true);
        TRestoreTabletRequest request = (TRestoreTabletRequest) method.invoke(job, task);

        Assertions.assertEquals(sourceVisibleVersion, request.getSource_visible_version());
        Assertions.assertEquals(1, request.getTablet_infosSize());
        TRestoreTabletInfo tabletInfo = request.getTablet_infos().get(0);
        Assertions.assertEquals(sourceTabletId, tabletInfo.getSource_tablet_id());
        Assertions.assertEquals(targetTabletId, tabletInfo.getTarget_tablet_id());
        Assertions.assertEquals(targetSchemaId, tabletInfo.getTarget_schema_id());
    }

    @Test
    public void testRunTransitionsFromPendingToFinished() {
        RestoreTask.TabletRestoreEntry entry = new RestoreTask.TabletRestoreEntry(101L, 102L, 103L);
        RestoreTask task = new RestoreTask(201L, 301L, Lists.newArrayList(entry));
        SnapshotRestoreJob job = createJob(Lists.newArrayList(task));

        new MockUp<WarehouseManager>() {
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return 1L;
            }
        };
        new MockUp<WarehouseManagerEPack>() {
            @Mock
            public boolean warehouseExists(long warehouseId) {
                return true;
            }

            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return 1L;
            }
        };
        new MockUp<TabletTaskExecutor>() {
            @Mock
            public boolean sendTabletRestoreTask(TabletRestoreAgentTask task) {
                return true;
            }
        };
        new MockUp<SnapshotRestoreJob>() {
            @Mock
            protected void registerRestoredTable() {
                // Skip catalog registration in unit test.
            }
        };

        job.run();
        Assertions.assertEquals(SnapshotRestoreJob.State.RUNNING, job.getState());
        Assertions.assertEquals(RestoreTask.PartitionTaskState.RUNNING, task.getTaskState());

        job.onPartitionRestoreFinished(task.getTargetPhysicalPartitionId(), true, null);
        Assertions.assertEquals(SnapshotRestoreJob.State.FINISHED, job.getState());
        Assertions.assertEquals(RestoreTask.PartitionTaskState.SUCCESS, task.getTaskState());
        Assertions.assertTrue(job.getFinishedTimeMs() > 0);
    }

    @Test
    public void testRunFailsWhenDispatchingTask() {
        RestoreTask.TabletRestoreEntry entry = new RestoreTask.TabletRestoreEntry(201L, 202L, 203L);
        RestoreTask task = new RestoreTask(301L, 401L, Lists.newArrayList(entry));
        SnapshotRestoreJob job = createJob(Lists.newArrayList(task));

        new MockUp<WarehouseManagerEPack>() {
            @Mock
            public boolean warehouseExists(long warehouseId) {
                return true;
            }

            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return 1L;
            }
        };
        new MockUp<TabletTaskExecutor>() {
            @Mock
            public boolean sendTabletRestoreTask(TabletRestoreAgentTask task) {
                return false;
            }
        };

        job.run();
        Assertions.assertEquals(SnapshotRestoreJob.State.FAILED, job.getState());
        Assertions.assertEquals(RestoreTask.PartitionTaskState.FAILED, task.getTaskState());
        Assertions.assertTrue(job.getErrorMessage().contains("Failed to dispatch tablet restore task"));
    }

    @Test
    public void testRegisterRestoredTableRegistersTableAndTablets() throws Exception {
        long dbId = 9001L;
        long tableId = 9002L;
        long partitionId = 9003L;
        long physicalPartitionId = 9004L;
        long indexId = 9005L;
        long[] tabletIds = new long[] {9006L, 9007L};

        LakeTable table = createLakeTableForRegister(tableId, partitionId, physicalPartitionId, indexId, tabletIds);
        GlobalStateMgr local = GlobalStateMgr.getCurrentState();
        local.getLocalMetastore().replayCreateDb(new CreateDbInfo(dbId, "target_db"));

        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logCreateTable(any());
        local.setEditLog(editLog);

        SnapshotRestoreJob job = new SnapshotRestoreJob(1L, "job_register", dbId, tableId, Lists.newArrayList(), table);
        job.setGlobalStateMgr(local);

        job.registerRestoredTable();

        com.starrocks.catalog.Database targetDb = local.getLocalMetastore().getDb(dbId);
        Assertions.assertNotNull(targetDb.getTable(tableId));
        Arrays.stream(tabletIds).forEach(id -> {
            TabletMeta meta = local.getTabletInvertedIndex().getTabletMeta(id);
            Assertions.assertNotNull(meta);
            Assertions.assertEquals(dbId, meta.getDbId());
            Assertions.assertEquals(tableId, meta.getTableId());
        });
    }

    @Test
    public void testRegisterRestoredTableFailsWhenDatabaseMissing() {
        LakeTable table = createLakeTableForRegister(9102L, 9103L, 9104L, 9105L, new long[] {9106L});
        GlobalStateMgr local = GlobalStateMgr.getCurrentState();
        SnapshotRestoreJob job = new SnapshotRestoreJob(1L, "job_register_missing_db", 9999L, table.getId(),
                Lists.newArrayList(), table);
        job.setGlobalStateMgr(local);

        Assertions.assertThrows(StarRocksException.class, job::registerRestoredTable);
    }

    private SnapshotRestoreJob createJob(List<RestoreTask> tasks) {
        List<Column> columns = Lists.newArrayList(new Column("c0", IntegerType.BIGINT, true));
        LakeTable table = new LakeTable(1000L, "pending_table", columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(1));
        return new SnapshotRestoreJob(1L, "job_1", 1L, 2L, tasks, table);
    }

    private LakeTable createLakeTableForRegister(long tableId, long partitionId, long physicalPartitionId,
                                                 long indexId, long[] tabletIds) {
        List<Column> columns = Lists.newArrayList(new Column("c0", IntegerType.BIGINT, true));
        LakeTable table = new LakeTable(tableId, "restore_table", columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(1));

        TableProperty property = new TableProperty(Maps.newHashMap());
        FilePathInfo pathInfo = FilePathInfo.newBuilder().build();
        FileCacheInfo cacheInfo = FileCacheInfo.newBuilder().build();
        property.setStorageInfo(new StorageInfo(pathInfo, cacheInfo));
        table.setTableProperty(property);

        MaterializedIndex baseIndex = new MaterializedIndex(indexId);
        baseIndex.setState(IndexState.NORMAL);
        table.setBaseIndexMetaId(indexId);
        table.setIndexMeta(indexId, table.getName(), columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);

        Partition partition = new Partition(partitionId, physicalPartitionId, "p_reg", baseIndex,
                new RandomDistributionInfo(1));
        table.addPartition(partition);

        for (long tabletId : tabletIds) {
            LakeTablet tablet = new LakeTablet(tabletId);
            TabletMeta meta = new TabletMeta(1L, tableId, physicalPartitionId, indexId, TStorageMedium.HDD, true);
            baseIndex.addTablet(tablet, meta, false);
        }
        return table;
    }
}
