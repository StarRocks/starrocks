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

package com.starrocks.lake.snapshot;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnapshotInfoHelper {
    public static ClusterSnapshotInfo buildClusterSnapshotInfo(List<Database> dbs) {
        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        for (Database db : dbs) {
            dbInfos.put(db.getId(), SnapshotInfoHelper.buildDatabaseSnapshotInfo(db));
        }
        return new ClusterSnapshotInfo(dbInfos);
    }

    public static DatabaseSnapshotInfo buildDatabaseSnapshotInfo(Database db) {
        Map<Long, TableSnapshotInfo> tableInfos = new HashMap<>();
        for (Table table : db.getTables()) {
            if (table.isCloudNativeTableOrMaterializedView()) {
                tableInfos.put(table.getId(), SnapshotInfoHelper.buildTableSnapshotInfo((OlapTable) table));
            }
        }
        return new DatabaseSnapshotInfo(db.getId(), tableInfos);
    }

    public static TableSnapshotInfo buildTableSnapshotInfo(OlapTable table) {
        Map<Long, PartitionSnapshotInfo> partInfos = new HashMap<>();
        for (Partition partition : table.getPartitions()) {
            partInfos.put(partition.getId(), SnapshotInfoHelper.buildPartitionSnapshotInfo(partition));
        }
        return new TableSnapshotInfo(table.getId(), partInfos);
    }

    public static PartitionSnapshotInfo buildPartitionSnapshotInfo(Partition partition) {
        Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos = new HashMap<>();
        for (PhysicalPartition physicalPart : partition.getSubPartitions()) {
            physicalPartInfos.put(physicalPart.getId(), SnapshotInfoHelper.buildPhysicalPartitionSnapshotInfo(physicalPart));
        }
        return new PartitionSnapshotInfo(partition.getId(), physicalPartInfos);
    }

    public static PhysicalPartitionSnapshotInfo buildPhysicalPartitionSnapshotInfo(PhysicalPartition physicalPart) {
        Map<Long, MaterializedIndexSnapshotInfo> indexInfos = new HashMap<>();
        List<MaterializedIndex> indexes = physicalPart.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
        for (MaterializedIndex index : indexes) {
            indexInfos.put(index.getId(), SnapshotInfoHelper.buildMaterializedIndexSnapshotInfo(index));
        }
        return new PhysicalPartitionSnapshotInfo(
                physicalPart.getId(), physicalPart.getVisibleVersion(), indexInfos);
    }

    public static MaterializedIndexSnapshotInfo buildMaterializedIndexSnapshotInfo(MaterializedIndex index) {
        return new MaterializedIndexSnapshotInfo(index.getId(), index.getShardGroupId());
    }
}