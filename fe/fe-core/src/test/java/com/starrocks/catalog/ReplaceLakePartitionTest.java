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
package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.staros.client.StarClientException;
import com.staros.proto.FilePathInfo;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Test;

public class ReplaceLakePartitionTest {
    long dbId = 9010;
    long tableId = 9011;
    long partitionId = 9012;
    long indexId = 9013;
    long[] tabletId = {9014, 90015};
    long tempPartitionId = 9020;
    long nextTxnId = 20000;
    String partitionName = "p0";
    String tempPartitionName = "temp_" + partitionName;

    LakeTable tbl = null;
    private final ShardInfo shardInfo;

    @Mocked
    private EditLog editLog;

    @Mocked
    private StarOSAgent starOSAgent;

    @Mocked
    private WarehouseManager warehouseManager;

    public ReplaceLakePartitionTest() {
        shardInfo = ShardInfo.newBuilder().setFilePath(FilePathInfo.newBuilder().setFullPath("oss://1/2")).build();
        warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();
        editLog = new EditLog(null);
    }

    LakeTable buildLakeTableWithTempPartition(PartitionType partitionType) {
        MaterializedIndex index = new MaterializedIndex(indexId);
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long id : tabletId) {
            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, 0, 0, TStorageMedium.HDD, true);
            invertedIndex.addTablet(id, tabletMeta);
            index.addTablet(new LakeTablet(id), tabletMeta);
        }
        Partition partition = new Partition(partitionId, partitionName, index, null);
        Partition tempPartition = new Partition(tempPartitionId, tempPartitionName, index, null);

        PartitionInfo partitionInfo = new PartitionInfo(partitionType);
        partitionInfo.setReplicationNum(partitionId, (short) 1);
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, false));

        LakeTable table = new LakeTable(
                tableId, "t0",
                Lists.newArrayList(new Column("c0", Type.BIGINT)),
                KeysType.DUP_KEYS, partitionInfo, null);
        table.addPartition(partition);
        table.addTempPartition(tempPartition);
        return table;
    }

    @Test
    public void testUnPartitionedLakeTableReplacePartition() {
        LakeTable tbl = buildLakeTableWithTempPartition(PartitionType.UNPARTITIONED);
        tbl.replacePartition(partitionName, tempPartitionName);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                GlobalStateMgr.getCurrentState().getEditLog();
                minTimes = 0;
                result = editLog;

                editLog.logErasePartition(anyLong);
                minTimes = 0;
                result = new Delegate() {
                    public void logErasePartition(Long partitionId) {
                    }
                };
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public ShardInfo getShardInfo(long shardId, long workerGroupId) throws StarClientException {
                return shardInfo;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getBackgroundWarehouse() {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };
        GlobalStateMgr.getCurrentState().getRecycleBin().erasePartition(Long.MAX_VALUE);
    }
}
