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
import com.staros.client.StarClientException;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RecycleRangePartitionInfo;
import com.starrocks.lake.Utils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Warehouse;

public class RecycleLakeRangePartitionInfo extends RecycleRangePartitionInfo  {
    public RecycleLakeRangePartitionInfo(long dbId, long tableId, Partition partition,
                                         Range<PartitionKey> range,
                                         DataProperty dataProperty, short replicationNum,
                                         boolean isInMemory, DataCacheInfo dataCacheInfo) {
        super(dbId, tableId, partition, range, dataProperty, replicationNum, isInMemory, dataCacheInfo);
    }

    @Override
    public boolean submitAndCheckAsyncDelete() {
        if (isRecoverable()) {
            setRecoverable(false);
            GlobalStateMgr.getCurrentState().getEditLog().logDisablePartitionRecovery(partition.getId());
        }

        try {
            if (asyncDeleteReturn.isEmpty()) {
                WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                Warehouse warehouse = manager.getBackgroundWarehouse();
                LakeTableHelper.submitAsyncRemovePartitionDirectory(partition, warehouse.getId(), asyncDeleteReturn);
            }
        } catch (StarClientException e) {
            // re-submit in the future if any exception is throw
            asyncDeleteReturn.clear();
            return false;
        }

        if (!Utils.checkAllAsyncTaskFinished(asyncDeleteReturn)) {
            return false;
        }

        if (Utils.checkAllAsyncTaskSuccessed(asyncDeleteReturn)) {
            GlobalStateMgr.getCurrentState().getLocalMetastore().onErasePartition(partition);
            return true;
        }

        // all finished but some tasks have failed, reset the futures and re-schedule
        asyncDeleteReturn.clear();
        return false;
    }
}
