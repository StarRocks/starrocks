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

import com.staros.client.StarClientException;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.RecycleUnPartitionInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Warehouse;

// This class is simply used as the Interface to trigger the background task
// in CatalogRecycleBin
public class RecycleLakeUnPartitionInfo extends RecycleUnPartitionInfo {
    public RecycleLakeUnPartitionInfo(long dbId, long tableId, Partition partition,
                                        DataProperty dataProperty, short replicationNum,
                                        boolean isInMemory, DataCacheInfo dataCacheInfo) {
        super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory, dataCacheInfo);
    }

    @Override
    public boolean delete() {
        if (isRecoverable()) {
            setRecoverable(false);
            GlobalStateMgr.getCurrentState().getEditLog().logDisablePartitionRecovery(partition.getId());
        }
        try {
            WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Warehouse warehouse = manager.getBackgroundWarehouse();
            if (LakeTableHelper.removePartitionDirectory(partition, warehouse.getId())) {
                GlobalStateMgr.getCurrentState().getLocalMetastore().onErasePartition(partition);
                LakeTableHelper.deleteShardGroupMeta(partition);
                return true;
            } else {
                return false;
            }
        } catch (StarClientException e) {
            return false;
        }
    }
}
