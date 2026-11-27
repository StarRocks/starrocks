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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.RecyclePartitionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.server.GlobalStateMgr;
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

public class RecycleLakePartitionInfoDropCacheTest {
    private static final long DB_ID = 41001L;
    private static final long TABLE_ID = 41002L;
    private static final long INDEX_ID = 41003L;

    private final List<Boolean> dropCacheArgs = new ArrayList<>();
    private boolean directoryRemoved = false;
    private long nextId = 41100L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .unprotectCreateDb(new Database(DB_ID, "test_recycle_lake_partition_drop_cache"));
        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        new MockUp<LakeTableHelper>() {
            @Mock
            public boolean removePartitionDirectory(Partition partition, ComputeResource computeResource,
                                                    boolean dropCache, boolean forceRemoveSharedDirectory) {
                dropCacheArgs.add(dropCache);
                return directoryRemoved;
            }

            @Mock
            public void deleteShardGroupMeta(Partition partition) {
            }
        };
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private Partition createPartition() {
        long partitionId = nextId++;
        long physicalPartitionId = nextId++;
        long tabletId = nextId++;
        Column key = new Column("k", IntegerType.BIGINT);
        key.setIsKey(true);
        DistributionInfo distributionInfo = new HashDistributionInfo(1, List.of(key));
        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        baseIndex.addTablet(new LocalTablet(tabletId),
                new TabletMeta(DB_ID, TABLE_ID, partitionId, INDEX_ID, TStorageMedium.HDD));
        return new Partition(partitionId, physicalPartitionId, "p" + partitionId, baseIndex, distributionInfo);
    }

    private List<RecyclePartitionInfo> createPartitionInfos(DataCacheInfo dataCacheInfo) {
        List<RecyclePartitionInfo> infos = List.of(
                new RecycleLakeRangePartitionInfo(DB_ID, TABLE_ID, createPartition(), Range.all(),
                        DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, dataCacheInfo),
                new RecycleLakeListPartitionInfo(DB_ID, TABLE_ID, createPartition(),
                        DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, dataCacheInfo),
                new RecycleLakeUnPartitionInfo(DB_ID, TABLE_ID, createPartition(),
                        DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, dataCacheInfo));
        for (RecyclePartitionInfo info : infos) {
            info.setRecoverable(false);
        }
        return infos;
    }

    // Only the first attempt to erase a partition asks the compute nodes to drop the cache; the retries of a failed
    // erase only remove the directory.
    @Test
    public void testDropCacheOnlyOnFirstEraseAttempt() {
        for (RecyclePartitionInfo info : createPartitionInfos(new DataCacheInfo(true, false))) {
            dropCacheArgs.clear();
            directoryRemoved = false;
            Assertions.assertFalse(info.delete());
            Assertions.assertFalse(info.delete());
            directoryRemoved = true;
            Assertions.assertTrue(info.delete());
            Assertions.assertEquals(List.of(true, false, false), dropCacheArgs, info.getClass().getSimpleName());
        }
    }
}
