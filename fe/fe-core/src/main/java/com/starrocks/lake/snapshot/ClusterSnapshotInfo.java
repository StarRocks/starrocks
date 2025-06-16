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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterSnapshotInfo {
    // tree struct begin from db
    @SerializedName(value = "dbInfos")
    private Map<Long, DatabaseSnapshotInfo> dbInfos;

    public ClusterSnapshotInfo() {
        this.dbInfos = new HashMap<>();
    }

    public void rebuildInfo(List<Database> dbs) {
        // always clear infos before rebuild
        dbInfos.clear();
        ClusterSnapshotInfoVisitor visitor = new ClusterSnapshotInfoVisitor();
        this.dbInfos = visitor.visit(dbs);
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

    private static class ClusterSnapshotInfoVisitor {
        public ClusterSnapshotInfoVisitor() {
        }

        public Map<Long, DatabaseSnapshotInfo> visit(List<Database> dbs) {
            Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
            for (Database db : dbs) {
                dbInfos.put(db.getId(), this.visit(db));
            }
            return dbInfos;
        }

        private DatabaseSnapshotInfo visit(Database db) {
            Map<Long, TableSnapshotInfo> tableInfos = new HashMap<>();
            for (Table table : db.getTables()) {
                if (table.isCloudNativeTableOrMaterializedView()) {
                    tableInfos.put(table.getId(), this.visit((OlapTable) table));
                }
            }
            return new DatabaseSnapshotInfo(db.getId(), tableInfos);
        }

        private TableSnapshotInfo visit(OlapTable table) {
            Map<Long, PartitionSnapshotInfo> partInfos = new HashMap<>();
            for (Partition partition : table.getPartitions()) {
                partInfos.put(partition.getId(), this.visit(partition));
            }
            return new TableSnapshotInfo(table.getId(), partInfos);
        }

        private PartitionSnapshotInfo visit(Partition partition) {
            Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos = new HashMap<>();
            for (PhysicalPartition physicalPart : partition.getSubPartitions()) {
                physicalPartInfos.put(physicalPart.getId(), this.visit(physicalPart));
            }
            return new PartitionSnapshotInfo(partition.getId(), physicalPartInfos);
        }

        private PhysicalPartitionSnapshotInfo visit(PhysicalPartition physicalPart) {
            Map<Long, MaterializedIndexSnapshotInfo> indexInfos = new HashMap<>();
            List<MaterializedIndex> indexes = physicalPart.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            for (MaterializedIndex index : indexes) {
                indexInfos.put(index.getId(), this.visit(index));
            }
            return new PhysicalPartitionSnapshotInfo(
                    physicalPart.getId(), physicalPart.getVisibleVersion(), indexInfos);
        }

        private MaterializedIndexSnapshotInfo visit(MaterializedIndex index) {
            return new MaterializedIndexSnapshotInfo(index.getId());
        }
    }

    private static class DatabaseSnapshotInfo {
        @SerializedName(value = "dbId")
        public long dbId;
        @SerializedName(value = "tableInfos")
        public Map<Long, TableSnapshotInfo> tableInfos;

        public DatabaseSnapshotInfo(long dbId, Map<Long, TableSnapshotInfo> tableInfos) {
            this.dbId = dbId;
            this.tableInfos = tableInfos;
        }
    }

    private static class TableSnapshotInfo {
        @SerializedName(value = "tableId")
        public long tableId;
        @SerializedName(value = "partInfos")
        public Map<Long, PartitionSnapshotInfo> partInfos;

        public TableSnapshotInfo(long tableId, Map<Long, PartitionSnapshotInfo> partInfos) {
            this.tableId = tableId;
            this.partInfos = partInfos;
        }
    }

    private static class PartitionSnapshotInfo {
        @SerializedName(value = "partitionId")
        public long partitionId;
        @SerializedName(value = "physicalPartInfos")
        public Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos;

        public PartitionSnapshotInfo(long partitionId, Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos) {
            this.partitionId = partitionId;
            this.physicalPartInfos = physicalPartInfos;
        }
    }

    private static class PhysicalPartitionSnapshotInfo {
        @SerializedName(value = "physicalPartitionId")
        public long physicalPartitionId;
        @SerializedName(value = "version")
        public long version;
        @SerializedName(value = "indexInfos")
        public Map<Long, MaterializedIndexSnapshotInfo> indexInfos;

        public PhysicalPartitionSnapshotInfo(long physicalPartId, long visibleVersion,
                                             Map<Long, MaterializedIndexSnapshotInfo> indexInfos) {
            this.physicalPartitionId = physicalPartId;
            this.version = visibleVersion;
            this.indexInfos = indexInfos;
        }
    }

    private static class MaterializedIndexSnapshotInfo {
        @SerializedName(value = "indexId")
        public long indexId;

        public MaterializedIndexSnapshotInfo(long indexId) {
            this.indexId = indexId;
        }
    }
}