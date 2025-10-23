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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/MetadataViewer.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.Replica.ReplicaStatus;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.ShowDataDistributionStmt;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MetadataViewer {

    public static List<List<String>> getTabletStatus(AdminShowReplicaStatusStmt stmt) throws DdlException {
        return getTabletStatus(stmt.getDbName(), stmt.getTblName(), stmt.getPartitions(),
                stmt.getStatusFilter(), stmt.getOp());
    }

    private static List<List<String>> getTabletStatus(String dbName, String tblName, List<String> partitions,
                                                      ReplicaStatus statusFilter, BinaryType op) throws DdlException {
        List<List<String>> result = Lists.newArrayList();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            Table tbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tblName);
            if (tbl == null || tbl.getType() != TableType.OLAP) {
                throw new DdlException("Table does not exist or is not OLAP table: " + tblName);
            }

            OlapTable olapTable = (OlapTable) tbl;

            if (partitions.isEmpty()) {
                partitions.addAll(olapTable.getPartitionNames());
            } else {
                // check partition
                for (String partName : partitions) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition does not exist: " + partName);
                    }
                }
            }

            for (String partName : partitions) {
                Partition partition = olapTable.getPartition(partName);
                short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {

                    long visibleVersion = physicalPartition.getVisibleVersion();

                    for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            int count = replicationNum;
                            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                --count;
                                List<String> row = Lists.newArrayList();

                                ReplicaStatus status = replica.computeReplicaStatus(infoService, visibleVersion, schemaHash);

                                if (filterReplica(status, statusFilter, op)) {
                                    continue;
                                }

                                row.add(String.valueOf(tabletId));
                                row.add(String.valueOf(replica.getId()));
                                row.add(String.valueOf(replica.getBackendId()));
                                row.add(String.valueOf(replica.getVersion()));
                                row.add(String.valueOf(replica.getLastFailedVersion()));
                                row.add(String.valueOf(replica.getLastSuccessVersion()));
                                row.add(String.valueOf(visibleVersion));
                                row.add(String.valueOf(Replica.DEPRECATED_PROP_SCHEMA_HASH));
                                row.add(String.valueOf(replica.getVersionCount()));
                                row.add(String.valueOf(replica.isBad()));
                                row.add(String.valueOf(replica.isSetBadForce()));
                                row.add(replica.getState().name());
                                row.add(status.name());
                                result.add(row);
                            }

                            if (filterReplica(ReplicaStatus.MISSING, statusFilter, op)) {
                                continue;
                            }

                            // get missing replicas
                            for (int i = 0; i < count; ++i) {
                                List<String> row = Lists.newArrayList();
                                row.add(String.valueOf(tabletId));
                                row.add("-1");
                                row.add("-1");
                                row.add("-1");
                                row.add("-1");
                                row.add("-1");
                                row.add("-1");
                                row.add("-1");
                                row.add(FeConstants.NULL_STRING);
                                row.add(FeConstants.NULL_STRING);
                                row.add(ReplicaStatus.MISSING.name());
                                result.add(row);
                            }
                        }
                    }
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        return result;
    }

    private static boolean filterReplica(ReplicaStatus status, ReplicaStatus statusFilter, BinaryType op) {
        if (statusFilter == null) {
            return false;
        }
        if (op == BinaryType.EQ) {
            return status != statusFilter;
        } else {
            return status == statusFilter;
        }
    }

    public static List<List<String>> getTabletDistribution(AdminShowReplicaDistributionStmt stmt) throws DdlException {
        return getTabletDistribution(stmt.getDbName(), stmt.getTblName(), stmt.getPartitionNames());
    }

    private static List<List<String>> getTabletDistribution(String dbName, String tblName,
                                                            PartitionNames partitionNames)
            throws DdlException {
        DecimalFormat df = new DecimalFormat("##.00 %");

        List<List<String>> result = Lists.newArrayList();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            Table tbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tblName);
            if (tbl == null || !tbl.isNativeTableOrMaterializedView()) {
                throw new DdlException("Table does not exist or is not native table: " + tblName);
            }

            OlapTable olapTable = (OlapTable) tbl;

            List<Long> partitionIds = Lists.newArrayList();
            if (partitionNames == null) {
                for (Partition partition : olapTable.getPartitions()) {
                    partitionIds.add(partition.getId());
                }
            } else {
                // check partition
                for (String partName : partitionNames.getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partName, partitionNames.isTemp());
                    if (partition == null) {
                        throw new DdlException("Partition does not exist: " + partName);
                    }
                    partitionIds.add(partition.getId());
                }
            }

            // backend id -> replica count
            Map<Long, Integer> countMap = Maps.newHashMap();
            // init map
            List<Long> beIds = getAllComputeNodeIds();
            for (long beId : beIds) {
                countMap.put(beId, 0);
            }

            int totalReplicaNum = 0;
            for (long partId : partitionIds) {
                Partition partition = olapTable.getPartition(partId);
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        for (Tablet tablet : index.getTablets()) {
                            for (long beId : tablet.getBackendIds()) {
                                if (!countMap.containsKey(beId)) {
                                    continue;
                                }
                                countMap.put(beId, countMap.get(beId) + 1);
                                totalReplicaNum++;
                            }
                        }
                    }
                }
            }

            // graph
            Collections.sort(beIds);
            for (Long beId : beIds) {
                List<String> row = Lists.newArrayList();
                row.add(String.valueOf(beId));
                row.add(String.valueOf(countMap.get(beId)));
                row.add(graph(countMap.get(beId), totalReplicaNum, beIds.size()));
                row.add(df.format((double) countMap.get(beId) / totalReplicaNum));
                result.add(row);
            }

        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        return result;
    }

    private static List<Long> getAllComputeNodeIds() throws DdlException {
        SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<Long> allComputeNodeIds = Lists.newArrayList();
        if (RunMode.isSharedDataMode()) {
            // check warehouse
            long warehouseId = ConnectContext.get().getCurrentWarehouseId();
            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            List<Long> computeNodeIs = warehouseManager.getAllComputeNodeIds(warehouseId);
            if (computeNodeIs.isEmpty()) {
                final Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
                throw new DdlException("no available compute nodes in warehouse " + warehouse.getName());
            }
            allComputeNodeIds.addAll(computeNodeIs);
        } else {
            allComputeNodeIds = infoService.getBackendIds(false);

        }
        return allComputeNodeIds;
    }

    public static List<List<String>> getDataDistribution(ShowDataDistributionStmt stmt) throws DdlException {
        return getDataDistribution(stmt.getDbName(), stmt.getTblName(), stmt.getPartitionNames());
    }

    public static List<List<String>> getDataDistribution(
            String dbName, String tblName, PartitionNames partitionNames) throws DdlException {

        DecimalFormat df = new DecimalFormat("00.00 %");
        List<List<String>> result = Lists.newArrayList();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            Table tbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tblName);
            if (tbl == null || !tbl.isNativeTableOrMaterializedView()) {
                throw new DdlException("Table does not exist or is not native table: " + tblName);
            }

            OlapTable olapTable = (OlapTable) tbl;

            List<Long> partitionIds = Lists.newArrayList();
            if (partitionNames == null) {
                for (Partition partition : olapTable.getPartitions()) {
                    partitionIds.add(partition.getId());
                }
            } else {
                for (String partitionName : partitionNames.getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partitionName, partitionNames.isTemp());
                    if (partition == null) {
                        throw new DdlException("Partition does not exist: " + partitionName);
                    }
                    partitionIds.add(partition.getId());
                }
            }

            Collections.sort(partitionIds);

            for (long partitionId : partitionIds) {
                Partition partition = olapTable.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }

                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        List<Tablet> tablets = index.getTablets();

                        List<Long> rowCountStatistics = Lists.newArrayListWithCapacity(tablets.size());
                        List<Long> dataSizeStatistics = Lists.newArrayListWithCapacity(tablets.size());

                        long totalRowCount = 0;
                        long totalDataSize = 0;
                        long version = physicalPartition.getVisibleVersion();
                        for (Tablet tablet : tablets) {
                            long rowCount = tablet.getRowCount(version);
                            long dataSize = tablet.getDataSize(true);
                            rowCountStatistics.add(rowCount);
                            dataSizeStatistics.add(dataSize);
                            totalRowCount += rowCount;
                            totalDataSize += dataSize;
                        }

                        for (int i = 0; i < tablets.size(); i++) {
                            List<String> row = Lists.newArrayList();
                            row.add(partition.getName());
                            row.add(String.valueOf(physicalPartition.getId()));
                            row.add(olapTable.getIndexNameById(index.getId()));
                            row.add(index.getVirtualBucketsByTabletId(tablets.get(i).getId()).toString());
                            row.add(String.valueOf(rowCountStatistics.get(i)));
                            row.add(totalRowCount == 0L ? "0.00 %"
                                    : df.format((double) rowCountStatistics.get(i) / totalRowCount));
                            row.add(String.valueOf(dataSizeStatistics.get(i)));
                            row.add(totalDataSize == 0L ? "0.00 %"
                                    : df.format((double) dataSizeStatistics.get(i) / totalDataSize));
                            result.add(row);
                        }
                    }
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        return result;
    }

    private static String graph(int num, int totalNum, int mod) {
        StringBuilder sb = new StringBuilder();
        int normalized = (int) Math.ceil(num * mod * 1.0 / totalNum);
        for (int i = 0; i < normalized; ++i) {
            sb.append(">");
        }
        return sb.toString();
    }
}
