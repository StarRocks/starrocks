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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.staros.client.StarClientException;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.lake.Utils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;

class LakeTableCleaner {
    private static final Logger LOG = LogManager.getLogger(LakeTableCleaner.class);

    // lake table or lake materialized view
    private final OlapTable table;
    private final List<CompletableFuture<Boolean>> asyncDeleteReturn;

    LakeTableCleaner(OlapTable table, List<CompletableFuture<Boolean>> asyncDeleteReturn) {
        this.table = table;
        this.asyncDeleteReturn = asyncDeleteReturn;
        Preconditions.checkState(this.asyncDeleteReturn != null);
    }

    private boolean submitAsyncCleanTable() {
        for (Partition partition : table.getAllPartitions()) {
            try {
                WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                Warehouse warehouse = manager.getBackgroundWarehouse();
                LakeTableHelper.submitAsyncRemovePartitionDirectory(partition, warehouse.getId(), asyncDeleteReturn);
            } catch (StarClientException e) {
                // re-submit in the future if any exception is throw
                asyncDeleteReturn.clear();
                LOG.warn("Fail to get shard info of partition {}: {}", partition.getId(), e.getMessage());
                return false;
            }
        }
        return true;
    }

    // Delete all data on remote storage in async mode. Successful deletion is *NOT* guaranteed.
    // If failed, caller should re-call submitAndCheckAsyncCleanTable to submit the deletion tasks again or check whether
    // all task are finished.
    public boolean submitAndCheckAsyncCleanTable() {
        if (asyncDeleteReturn.isEmpty() && !submitAsyncCleanTable()) {
            return false;
        }

        if (!Utils.checkAllAsyncTaskFinished(asyncDeleteReturn)) {
            return false;
        }

        if (Utils.checkAllAsyncTaskSuccessed(asyncDeleteReturn)) {
            return true;
        }

        // all finished but some tasks have failed, reset the futures and re-schedule
        asyncDeleteReturn.clear();
        return false;
    }

    // cleanTable in sync mode only for UT now
    @VisibleForTesting
    boolean cleanTable() {
        if (asyncDeleteReturn.isEmpty() && !submitAsyncCleanTable()) {
            return false;
        }

        while (!Utils.checkAllAsyncTaskFinished(asyncDeleteReturn)) {
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }

        boolean allSuccessed = Utils.checkAllAsyncTaskSuccessed(asyncDeleteReturn);
        asyncDeleteReturn.clear();
        return allSuccessed;
    }
}
