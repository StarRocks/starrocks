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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class LakeTableCleaner {
    private static final Logger LOG = LogManager.getLogger(LakeTableCleaner.class);

    // lake table or lake materialized view
    private final OlapTable table;

    LakeTableCleaner(OlapTable table) {
        this.table = table;
    }

    // Delete all data on remote storage. Successful deletion is *NOT* guaranteed.
    // If failed, manual removal of directories may be required by user.
    public boolean cleanTable() {
        boolean allRemoved = true;
        ComputeResource computeResource =
                GlobalStateMgr.getCurrentState().getWarehouseMgr().getBackgroundComputeResource(table.getId());
        for (Partition partition : table.getAllPartitions()) {
            try {
                boolean partitionResult = LakeTableHelper
                        .cleanSharedDataPartitionAndDeleteShardGroupMeta(partition, computeResource, false);
                allRemoved = allRemoved && partitionResult;
            } catch (StarClientException e) {
                LOG.warn("Fail to get shard info of partition {}: {}", partition.getId(), e.getMessage());
                allRemoved = false;
            }
        }
        return allRemoved;
    }
}
