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

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.server.GlobalStateMgr.isCheckpointThread;

public class ClusterSnapshotInfo {
    // layer struct begin from db
    @SerializedName(value = "dbInfos")
    private Map<Long, DatabaseSnapshotInfo> dbInfos;

    public ClusterSnapshotInfo() {
        this.dbInfos = new HashMap<>();

        if (!isCheckpointThread()) {
            return;
        }

        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                continue;
            }
            dbInfos.put(dbId, new DatabaseSnapshotInfo(db));
        }
    }

    public boolean isEmpty() {
        return this.dbInfos.isEmpty();
    }

    public long getVersion(long dbId, long tableId, long partId, long physicalPartId) {
        PhysicalPartitionSnapshotInfo physicalPartInfo = getPhysicalPartitionInfo(dbId, tableId, partId, physicalPartId);
        if (physicalPartInfo == null) {
            return 0;
        }
        return physicalPartInfo.version;
    }

    public boolean isDbExisted(long dbId) {
        return getDbInfo(dbId) != null;
    }

    public boolean isTableExisted(long dbId, long tableId) {
        return getTableInfo(dbId, tableId) != null;
    }

    public boolean isPartitionExisted(long dbId, long tableId, long partId) {
        return getPartitionInfo(dbId, tableId, partId) != null;
    }

    public boolean isMaterializedIndexExisted(long dbId, long tableId, long partId, long physicalPartId, long indexId) {
        return getIndexInfo(dbId, tableId, partId, physicalPartId, indexId) != null;
    }

    public boolean isMaterializedIndexExisted(long dbId, long tableId, long partId, long indexId) {
        PartitionSnapshotInfo partInfo = getPartitionInfo(dbId, tableId, partId);
        if (partInfo == null) {
            return false;
        }

        for (PhysicalPartitionSnapshotInfo physicalPartInfo : partInfo.physicalPartInfos.values()) {
            if (isMaterializedIndexExisted(dbId, tableId, partId, physicalPartInfo.physicalPartitionId, indexId)) {
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

    private static class DatabaseSnapshotInfo {
        @SerializedName(value = "dbId")
        public long dbId;
        @SerializedName(value = "tableInfos")
        public Map<Long, TableSnapshotInfo> tableInfos;

        public DatabaseSnapshotInfo(Database db) {
            this.dbId = db.getId();
            this.tableInfos = new HashMap<>();

            for (Table table : db.getTables()) {
                if (table.isCloudNativeTableOrMaterializedView()) {
                    tableInfos.put(table.getId(), new TableSnapshotInfo((OlapTable) table));
                }
            }
        }
    }

    private static class TableSnapshotInfo {
        @SerializedName(value = "tableId")
        public long tableId;
        @SerializedName(value = "partInfos")
        public Map<Long, PartitionSnapshotInfo> partInfos;

        public TableSnapshotInfo(OlapTable table) {
            this.tableId = table.getId();
            this.partInfos = new HashMap<>();

            for (Partition partition : table.getPartitions()) {
                partInfos.put(partition.getId(), new PartitionSnapshotInfo(partition));
            }
        }
    }

    private static class PartitionSnapshotInfo {
        @SerializedName(value = "partitionId")
        public long partitionId;
        @SerializedName(value = "physicalPartInfos")
        public Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos;

        public PartitionSnapshotInfo(Partition partition) {
            this.partitionId = partition.getId();
            this.physicalPartInfos = new HashMap<>();

            for (PhysicalPartition physicalPart : partition.getSubPartitions()) {
                physicalPartInfos.put(physicalPart.getId(), new PhysicalPartitionSnapshotInfo(physicalPart));
            }
        }
    }

    private static class PhysicalPartitionSnapshotInfo {
        @SerializedName(value = "physicalPartitionId")
        public long physicalPartitionId;
        @SerializedName(value = "version")
        public long version;
        @SerializedName(value = "indexInfos")
        public Map<Long, MaterializedIndexSnapshotInfo> indexInfos;

        public PhysicalPartitionSnapshotInfo(PhysicalPartition physicalPart) {
            this.physicalPartitionId = physicalPart.getId();
            this.version = physicalPart.getVisibleVersion();
            this.indexInfos = new HashMap<>();

            List<MaterializedIndex> indexes = physicalPart.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            for (MaterializedIndex index : indexes) {
                indexInfos.put(index.getId(), new MaterializedIndexSnapshotInfo(index));
            }
        }
    }

    private static class MaterializedIndexSnapshotInfo {
        @SerializedName(value = "indexId")
        public long indexId;

        public MaterializedIndexSnapshotInfo(MaterializedIndex index) {
            this.indexId = index.getId();
        }
    }
}