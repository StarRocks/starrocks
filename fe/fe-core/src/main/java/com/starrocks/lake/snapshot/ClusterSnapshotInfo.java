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
import com.starrocks.thrift.TClusterSnapshotInfo;
import com.starrocks.thrift.TDatabaseSnapshotInfo;
import com.starrocks.thrift.TPartitionSnapshotInfo;
import com.starrocks.thrift.TPhysicalPartitionSnapshotInfo;
import com.starrocks.thrift.TTableSnapshotInfo;

import java.util.HashMap;
import java.util.Map;

public class ClusterSnapshotInfo {
    // layer struct begin from db
    @SerializedName(value = "dbInfos")
    private Map<Long, DatabaseSnapshotInfo> dbInfos;

    public ClusterSnapshotInfo() {
        this.dbInfos = new HashMap<>();
    }

    public ClusterSnapshotInfo(Map<Long, DatabaseSnapshotInfo> dbInfos) {
        this.dbInfos = dbInfos != null ? dbInfos : new HashMap<>();
    }

    public boolean isEmpty() {
        return this.dbInfos.isEmpty();
    }

    public void reset() {
        this.dbInfos.clear();
    }

    public TClusterSnapshotInfo toThrift() {
        TClusterSnapshotInfo tClusterSnapshotInfo = new TClusterSnapshotInfo();
        Map<Long, TDatabaseSnapshotInfo> thriftMap = new HashMap<>();
        for (Map.Entry<Long, DatabaseSnapshotInfo> entry : dbInfos.entrySet()) {
            thriftMap.put(entry.getKey(), entry.getValue().toThrift());
        }
        tClusterSnapshotInfo.setDb_infos(thriftMap);
        return tClusterSnapshotInfo;
    }

    public static ClusterSnapshotInfo fromThrift(TClusterSnapshotInfo tClusterSnapshotInfo) {
        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        for (Map.Entry<Long, TDatabaseSnapshotInfo> entry : tClusterSnapshotInfo.getDb_infos().entrySet()) {
            dbInfos.put(entry.getKey(), DatabaseSnapshotInfo.fromThrift(entry.getValue()));
        }
        return new ClusterSnapshotInfo(dbInfos);
    }

    public void putVersion(long dbId, long tableId, long partId, long physicalPartId, long version) {
        DatabaseSnapshotInfo dbInfo = dbInfos.computeIfAbsent(dbId, k -> new DatabaseSnapshotInfo(dbId));
        TableSnapshotInfo tableInfo = dbInfo.tableInfos.computeIfAbsent(tableId, k -> new TableSnapshotInfo(tableId));
        PartitionSnapshotInfo partInfo = tableInfo.partInfos.computeIfAbsent(partId, k -> new PartitionSnapshotInfo(partId));
        partInfo.physicalPartInfos.put(physicalPartId, new PhysicalPartitionSnapshotInfo(physicalPartId, version));
    }

    public long getVersion(long dbId, long tableId, long partId, long physicalPartId) {
        DatabaseSnapshotInfo dbInfo = dbInfos.get(dbId);
        if (dbInfo == null) {
            return 0;
        }
        TableSnapshotInfo tableInfo = dbInfo.tableInfos.get(tableId);
        if (tableInfo == null) {
            return 0;
        }
        PartitionSnapshotInfo partInfo = tableInfo.partInfos.get(partId);
        if (partInfo == null) {
            return 0;
        }
        PhysicalPartitionSnapshotInfo physicalPartInfo = partInfo.physicalPartInfos.get(physicalPartId);
        if (physicalPartInfo == null) {
            return 0;
        }
        return physicalPartInfo.version;
    }

    private static class DatabaseSnapshotInfo {
        @SerializedName(value = "dbId")
        public long dbId;
        @SerializedName(value = "tableInfos")
        public Map<Long, TableSnapshotInfo> tableInfos;

        public DatabaseSnapshotInfo(long dbId) {
            this.dbId = dbId;
            this.tableInfos = new HashMap<>();
        }

        public DatabaseSnapshotInfo(long dbId, Map<Long, TableSnapshotInfo> tableInfos) {
            this.dbId = dbId;
            this.tableInfos = tableInfos;
        }

        public TDatabaseSnapshotInfo toThrift() {
            TDatabaseSnapshotInfo tDatabaseSnapshotInfo = new TDatabaseSnapshotInfo();
            Map<Long, TTableSnapshotInfo> thriftMap = new HashMap<>();
            for (Map.Entry<Long, TableSnapshotInfo> entry : tableInfos.entrySet()) {
                thriftMap.put(entry.getKey(), entry.getValue().toThrift());
            }
            tDatabaseSnapshotInfo.setDb_id(dbId);
            tDatabaseSnapshotInfo.setTable_infos(thriftMap);
            return tDatabaseSnapshotInfo;
        }

        public static DatabaseSnapshotInfo fromThrift(TDatabaseSnapshotInfo tDatabaseSnapshotInfo) {
            Map<Long, TableSnapshotInfo> tableInfos = new HashMap<>();
            for (Map.Entry<Long, TTableSnapshotInfo> entry : tDatabaseSnapshotInfo.getTable_infos().entrySet()) {
                tableInfos.put(entry.getKey(), TableSnapshotInfo.fromThrift(entry.getValue()));
            }
            return new DatabaseSnapshotInfo(tDatabaseSnapshotInfo.getDb_id(), tableInfos);
        }
    }

    private static class TableSnapshotInfo {
        @SerializedName(value = "tableId")
        public long tableId;
        @SerializedName(value = "partInfos")
        public Map<Long, PartitionSnapshotInfo> partInfos;

        public TableSnapshotInfo(long tableId) {
            this.tableId = tableId;
            this.partInfos = new HashMap<>();
        }

        public TableSnapshotInfo(long tableId, Map<Long, PartitionSnapshotInfo> partInfos) {
            this.tableId = tableId;
            this.partInfos = partInfos;
        }

        public TTableSnapshotInfo toThrift() {
            TTableSnapshotInfo tTableSnapshotInfo = new TTableSnapshotInfo();
            Map<Long, TPartitionSnapshotInfo> thriftMap = new HashMap<>();
            for (Map.Entry<Long, PartitionSnapshotInfo> entry : partInfos.entrySet()) {
                thriftMap.put(entry.getKey(), entry.getValue().toThrift());
            }
            tTableSnapshotInfo.setTable_id(tableId);
            tTableSnapshotInfo.setPart_infos(thriftMap);
            return tTableSnapshotInfo;
        }

        public static TableSnapshotInfo fromThrift(TTableSnapshotInfo tTableSnapshotInfo) {
            Map<Long, PartitionSnapshotInfo> partInfos = new HashMap<>();
            for (Map.Entry<Long, TPartitionSnapshotInfo> entry : tTableSnapshotInfo.getPart_infos().entrySet()) {
                partInfos.put(entry.getKey(), PartitionSnapshotInfo.fromThrift(entry.getValue()));
            }
            return new TableSnapshotInfo(tTableSnapshotInfo.getTable_id(), partInfos);
        }
    }

    private static class PartitionSnapshotInfo {
        @SerializedName(value = "partitionId")
        public long partitionId;
        @SerializedName(value = "physicalPartInfos")
        public Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos;

        public PartitionSnapshotInfo(long partitionId) {
            this.partitionId = partitionId;
            this.physicalPartInfos = new HashMap<>();
        }

        public PartitionSnapshotInfo(long partitionId, Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos) {
            this.partitionId = partitionId;
            this.physicalPartInfos = physicalPartInfos;
        }

        public TPartitionSnapshotInfo toThrift() {
            TPartitionSnapshotInfo tPartitionSnapshotInfo = new TPartitionSnapshotInfo();
            Map<Long, TPhysicalPartitionSnapshotInfo> thriftMap = new HashMap<>();
            for (Map.Entry<Long, PhysicalPartitionSnapshotInfo> entry : physicalPartInfos.entrySet()) {
                thriftMap.put(entry.getKey(), entry.getValue().toThrift());
            }
            tPartitionSnapshotInfo.setPartition_id(partitionId);
            tPartitionSnapshotInfo.setPhysical_part_infos(thriftMap);
            return tPartitionSnapshotInfo;
        }

        public static PartitionSnapshotInfo fromThrift(TPartitionSnapshotInfo tPartitionSnapshotInfo) {
            Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos = new HashMap<>();
            for (Map.Entry<Long, TPhysicalPartitionSnapshotInfo> entry :
                    tPartitionSnapshotInfo.getPhysical_part_infos().entrySet()) {
                physicalPartInfos.put(entry.getKey(), PhysicalPartitionSnapshotInfo.fromThrift(entry.getValue()));
            }
            return new PartitionSnapshotInfo(tPartitionSnapshotInfo.getPartition_id(), physicalPartInfos);
        }
    }

    private static class PhysicalPartitionSnapshotInfo {
        @SerializedName(value = "physicalPartitionId")
        public long physicalPartitionId;
        @SerializedName(value = "version")
        public long version;

        public PhysicalPartitionSnapshotInfo(long physicalPartitionId, long version) {
            this.physicalPartitionId = physicalPartitionId;
            this.version = version;
        }

        public TPhysicalPartitionSnapshotInfo toThrift() {
            TPhysicalPartitionSnapshotInfo tPhysicalPartitionSnapshotInfo = new TPhysicalPartitionSnapshotInfo();
            tPhysicalPartitionSnapshotInfo.setPhysical_part_id(physicalPartitionId);
            tPhysicalPartitionSnapshotInfo.setVersion(version);
            return tPhysicalPartitionSnapshotInfo;
        }

        public static PhysicalPartitionSnapshotInfo fromThrift(TPhysicalPartitionSnapshotInfo tPhysicalPartitionSnapshotInfo) {
            return new PhysicalPartitionSnapshotInfo(tPhysicalPartitionSnapshotInfo.getPhysical_part_id(),
                                                     tPhysicalPartitionSnapshotInfo.getVersion());
        }
    }
}