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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;

public class ClusterSnapshotInfo {
    @SerializedName(value = "dbInfos")
    private final Map<Long, DatabaseSnapshotInfo> dbInfos;

    public ClusterSnapshotInfo(Map<Long, DatabaseSnapshotInfo> dbInfos) {
        this.dbInfos = dbInfos;
    }

    public boolean isEmpty() {
        return this.dbInfos.isEmpty();
    }

    public long getVersion(long dbId, long tableId, long partId, long physicalPartId) {
        PhysicalPartitionSnapshotInfo physicalPartInfo = getPhysicalPartitionInfo(dbId, tableId, partId, physicalPartId);
        if (physicalPartInfo == null) {
            return 0;
        }
        return physicalPartInfo.visibleVersion;
    }

    public List<Long> getCommittedVersionsAfterVisible(long dbId, long tableId, long partId, long physicalPartId) {
        List<Long> committedVersions = Lists.newArrayList();
        PhysicalPartitionSnapshotInfo physicalPartInfo = getPhysicalPartitionInfo(dbId, tableId, partId, physicalPartId);
        if (physicalPartInfo == null) {
            return committedVersions;
        }
        long visibleVersion = physicalPartInfo.visibleVersion;
        long committedVersion = physicalPartInfo.committedVersion;
        for (long version = visibleVersion + 1; version <= committedVersion; version++) {
            committedVersions.add(version);
        }
        return committedVersions;
    }

    public boolean containsDb(long dbId) {
        return getDbInfo(dbId) != null;
    }

    public boolean containsTable(long dbId, long tableId) {
        return getTableInfo(dbId, tableId) != null;
    }

    public boolean containsPartition(long dbId, long tableId, long partId) {
        return getPartitionInfo(dbId, tableId, partId) != null;
    }

    public boolean containsMaterializedIndex(long dbId, long tableId, long partId, long physicalPartId, long indexId) {
        return getIndexInfo(dbId, tableId, partId, physicalPartId, indexId) != null;
    }

    public boolean containsMaterializedIndex(long dbId, long tableId, long partId, long indexId) {
        PartitionSnapshotInfo partInfo = getPartitionInfo(dbId, tableId, partId);
        if (partInfo == null) {
            return false;
        }

        for (PhysicalPartitionSnapshotInfo physicalPartInfo : partInfo.physicalPartInfos.values()) {
            if (containsMaterializedIndex(dbId, tableId, partId, physicalPartInfo.physicalPartitionId, indexId)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsShardGroupId(long dbId, long tableId, long partId, long physicalPartId, long shardGroupId) {
        PhysicalPartitionSnapshotInfo physicalPartInfo = getPhysicalPartitionInfo(dbId, tableId, partId, physicalPartId);
        if (physicalPartInfo == null) {
            return false;
        }

        for (MaterializedIndexSnapshotInfo indexInfo : physicalPartInfo.indexInfos.values()) {
            if (indexInfo.shardGroupId == shardGroupId) {
                return true;
            }
        }
        return false;
    }

    public boolean containsShardGroupId(long dbId, long tableId, long partId, long shardGroupId) {
        PartitionSnapshotInfo partInfo = getPartitionInfo(dbId, tableId, partId);
        if (partInfo == null) {
            return false;
        }

        for (PhysicalPartitionSnapshotInfo physicalPartInfo : partInfo.physicalPartInfos.values()) {
            if (containsShardGroupId(dbId, tableId, partId, physicalPartInfo.physicalPartitionId, shardGroupId)) {
                return true;
            }
        }
        return false;
    }

    private DatabaseSnapshotInfo getDbInfo(long dbId) {
        return dbInfos.get(dbId);
    }

    private TableSnapshotInfo getTableInfo(long dbId, long tableId) {
        DatabaseSnapshotInfo dbInfo = getDbInfo(dbId);
        if (dbInfo == null) {
            return null;
        }
        return dbInfo.tableInfos.get(tableId);
    }

    private PartitionSnapshotInfo getPartitionInfo(long dbId, long tableId, long partId) {
        TableSnapshotInfo tableInfo = getTableInfo(dbId, tableId);
        if (tableInfo == null) {
            return null;
        }
        return tableInfo.partInfos.get(partId);
    }

    private PhysicalPartitionSnapshotInfo getPhysicalPartitionInfo(long dbId, long tableId, long partId, long physicalPartId) {
        PartitionSnapshotInfo partInfo = getPartitionInfo(dbId, tableId, partId);
        if (partInfo == null) {
            return null;
        }
        return partInfo.physicalPartInfos.get(physicalPartId);
    }

    private MaterializedIndexSnapshotInfo getIndexInfo(long dbId, long tableId, long partId, long physicalPartId, long indexId) {
        PhysicalPartitionSnapshotInfo physicalPartInfo = getPhysicalPartitionInfo(dbId, tableId, partId, physicalPartId);
        if (physicalPartInfo == null) {
            return null;
        }
        return physicalPartInfo.indexInfos.get(indexId);
    }
}