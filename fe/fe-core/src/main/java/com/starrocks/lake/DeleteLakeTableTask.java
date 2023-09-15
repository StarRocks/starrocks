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

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.proto.DropTableResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

class DeleteLakeTableTask implements Runnable {
    private static final Logger LOG = LogManager.getLogger(DeleteLakeTableTask.class);

    // lake table or lake materialized view
    private final OlapTable table;

    DeleteLakeTableTask(OlapTable table) {
        this.table = table;
    }

    // Delete all data on remote storage. Successful deletion is *NOT* guaranteed.
    // If failed, manual removal of directories may be required by user.
    @Override
    public void run() {
        Map<String, LakeTablet> storagePathToTablet = findUniquePartitionDirectories();
        // TODO: If the remote storage is HDFS instead of object storage, after deleting
        //  all partition directories, an empty table directory may be left, which is the
        //  parent directory of the partition directories.
        for (Map.Entry<String, LakeTablet> entry : storagePathToTablet.entrySet()) {
            removePartitionDirectory(entry.getKey(), entry.getValue());
        }
    }

    // In some old versions, all partitions shared the same storage directory, unlike now where each partition has
    // its own separate directory. The storage directory of a partition is recorded in the ShardInfo of the tablet.
    // Here the storage directories are de-duplicated first to avoid concurrent deletes of the same directory when
    // multiple partitions share it.
    private Map<String, LakeTablet> findUniquePartitionDirectories() {
        Map<String, LakeTablet> storagePathToTablet = new HashMap<>();
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            LakeTablet anyTablet = getAnyTablet(partition);
            if (anyTablet == null) {
                continue;
            }

            try {
                String storagePath = anyTablet.getShardInfo().getFilePath().getFullPath();
                storagePathToTablet.putIfAbsent(storagePath, anyTablet);
            } catch (Exception e) {
                LOG.warn("Fail to get shard info of tablet {}: {}", anyTablet.getId(), e.getMessage());
                break;
            }
        }
        return storagePathToTablet;
    }

    private LakeTablet getAnyTablet(PhysicalPartition partition) {
        List<MaterializedIndex> allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
        for (MaterializedIndex materializedIndex : allIndices) {
            List<Tablet> tablets = materializedIndex.getTablets();
            if (!tablets.isEmpty()) {
                return (LakeTablet) tablets.get(0);
            }
        }
        return null;
    }

    private void removePartitionDirectory(String path, Tablet tablet) {
        DropTableRequest request = new DropTableRequest();
        request.tabletId = tablet.getId();
        ComputeNode node = Utils.chooseNode((LakeTablet) tablet);
        if (node == null) {
            LOG.warn("Fail to remove {}: no alive node", path);
            return;
        }
        TNetworkAddress address = new TNetworkAddress(node.getHost(), node.getBrpcPort());
        try {
            LakeService lakeService = BrpcProxy.getLakeService(address);
            Future<DropTableResponse> responseFuture = lakeService.dropTable(request);
            responseFuture.get();
            LOG.info("Executing removal of {} on node {}", path, node.getHost());
        } catch (Exception e) {
            LOG.warn("Fail to execute removal of {} on node {}: {}", path, node.getHost(), e.getMessage());
        }
    }
}