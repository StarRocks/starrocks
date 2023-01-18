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

package com.starrocks.sql.optimizer.operator.stream;


import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.DdlException;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.load.Load;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNodeInfo;
import com.starrocks.thrift.TNodesInfo;
import com.starrocks.thrift.TOlapTableIndexSchema;
import com.starrocks.thrift.TOlapTableIndexTablets;
import com.starrocks.thrift.TOlapTableLocationParam;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTablePartitionParam;
import com.starrocks.thrift.TOlapTableRouteInfo;
import com.starrocks.thrift.TOlapTableSchemaParam;
import com.starrocks.thrift.TTabletLocation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Describe route info of an Olap Table
 */
public class OlapTableRouteInfo {

    private final int numReplicas;
    private final String tableName;
    private final TupleDescriptor tupleDescriptor;

    private final TOlapTableSchemaParam tableSchema;
    private final TOlapTablePartitionParam partition;
    private final TOlapTableLocationParam location;
    private final TNodesInfo nodes;
    private final OlapTable tableInfo;

    public OlapTableRouteInfo(OlapTable table, int numReplicas, TupleDescriptor tupleDescriptor,
                              TOlapTableSchemaParam tableSchema,
                              TOlapTablePartitionParam partition, TOlapTableLocationParam location, TNodesInfo nodes) {
        this.numReplicas = numReplicas;
        this.tupleDescriptor = tupleDescriptor;
        this.tableSchema = tableSchema;
        this.partition = partition;
        this.location = location;
        this.nodes = nodes;
        this.tableInfo = table;
        this.tableName = table.getName();
    }

    public void finalizeTupleDescriptor(DescriptorTable descriptorTable, TupleDescriptor tupleDesc) {
        tableSchema.tuple_desc = tupleDesc.toThrift();
        for (int i = 0; i < tupleDesc.getSlots().size(); i++) {
            SlotDescriptor slot = tupleDesc.getSlots().get(i);
            // Inherit column name from table descriptor
            Column column = tableInfo.getFullSchema().get(i);
            slot.setColumn(column);
            slot.setIsNullable(column.isAllowNull());
            tableSchema.addToSlot_descs(slot.toThrift());
        }
    }

    /**
     * Create a route info for all partitions
     */
    public static OlapTableRouteInfo create(long dbId, OlapTable olapTable,
                                            TupleDescriptor outputTupleDesc) throws UserException {
        List<Long> partitionIds =
                olapTable.getAllPartitions().stream().map(Partition::getId).collect(Collectors.toList());
        TOlapTableSchemaParam tableSchema = createTableSchema(dbId, olapTable, outputTupleDesc);
        TOlapTablePartitionParam partition = createPartitionParam(dbId, olapTable, partitionIds);
        TOlapTableLocationParam location = createLocation(olapTable, partitionIds);
        TNodesInfo nodes = createNodesInfo();
        return new OlapTableRouteInfo(olapTable, getNumReplicas(olapTable),
                outputTupleDesc, tableSchema, partition, location, nodes);
    }

    public TOlapTableRouteInfo toThrift() {
        TOlapTableRouteInfo res = new TOlapTableRouteInfo();
        res.setNum_replicas(numReplicas);
        res.setSchema(this.tableSchema);
        res.setPartition(this.partition);
        res.setLocation(this.location);
        res.setNum_replicas(this.numReplicas);
        res.setTable_name(this.tableName);
        res.setNodes_info(this.nodes);
        res.setKeys_type(this.tableInfo.getKeysType().toThrift());
        return res;
    }

    private static TOlapTableSchemaParam createTableSchema(long dbId, OlapTable table,
                                                           TupleDescriptor tupleDescriptor) {
        TOlapTableSchemaParam schemaParam = new TOlapTableSchemaParam();
        schemaParam.setDb_id(dbId);
        schemaParam.setTable_id(table.getId());

        // TODO: Optimize it later, how to get IMT's version.
        long version = table.getAllPartitions().stream().findFirst().get().getVisibleVersion();
        schemaParam.setVersion(version);

        // TODO: make it configurable.
        boolean isAddOpsColumn = true;
        // IMT Table only support one index for now.
        Preconditions.checkState(table.getIndexIdToMeta().size() == 1);
        for (Map.Entry<Long, MaterializedIndexMeta> pair : table.getIndexIdToMeta().entrySet()) {
            MaterializedIndexMeta indexMeta = pair.getValue();
            List<String> columns = Lists.newArrayList();
            columns.addAll(indexMeta.getSchema().stream().map(Column::getName).collect(Collectors.toList()));
            // TODO: support __op column?
            if (isAddOpsColumn && table.getKeysType() == KeysType.PRIMARY_KEYS) {
                columns.add(Load.LOAD_OP_COLUMN);
            }
            TOlapTableIndexSchema indexSchema = new TOlapTableIndexSchema(pair.getKey(), columns,
                    indexMeta.getSchemaHash());
            schemaParam.addToIndexes(indexSchema);
        }

        DescriptorTable descriptorTable = new DescriptorTable();
        TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();
        for (Column column : table.getFullSchema()) {
            SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
            slotDescriptor.setIsMaterialized(true);
            slotDescriptor.setType(column.getType());
            slotDescriptor.setColumn(column);
            slotDescriptor.setIsNullable(column.isAllowNull());
        }
        if (isAddOpsColumn) {
            SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
            Column opColumn = new Column(Load.LOAD_OP_COLUMN, ScalarType.TINYINT);
            slotDescriptor.setIsMaterialized(true);
            slotDescriptor.setType(opColumn.getType());
            slotDescriptor.setColumn(opColumn);
            slotDescriptor.setIsNullable(false);
        }
        schemaParam.tuple_desc = olapTuple.toThrift();
        for (SlotDescriptor slotDesc : olapTuple.getSlots()) {
            schemaParam.addToSlot_descs(slotDesc.toThrift());
        }
        return schemaParam;
    }

    private static TOlapTablePartitionParam createPartitionParam(long dbId, OlapTable table, List<Long> partitionIds)
            throws UserException {
        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        partitionParam.setDb_id(dbId);
        partitionParam.setTable_id(table.getId());

        // TODO: Optimize it later, how to get IMT's version.
        long version = table.getAllPartitions().stream().findFirst().get().getVisibleVersion();
        partitionParam.setVersion(version);

        PartitionType partType = table.getPartitionInfo().getType();
        if (partType != PartitionType.UNPARTITIONED) {
            throw new UserException("[MV] Create IMT State table failed: only support un-partitioned " +
                    "state table for now, input partition:" + partType);
        }

        // there is no partition columns for single partition
        Preconditions.checkArgument(table.getPartitions().size() == 1,
                "Number of table partitions is not 1 for unpartitioned table, partitionNum="
                        + table.getPartitions().size());
        Partition partition = null;
        if (partitionIds != null) {
            Preconditions.checkState(partitionIds.size() == 1,
                    "invalid partitionIds size:{}", partitionIds.size());
            partition = table.getPartition(partitionIds.get(0));
        } else {
            partition = table.getPartitions().iterator().next();
        }

        TOlapTablePartition tPartition = new TOlapTablePartition();
        tPartition.setId(partition.getId());
        // No lowerBound and upperBound for this range
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                    index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
            tPartition.setNum_buckets(index.getTablets().size());
        }
        partitionParam.addToPartitions(tPartition);
        partitionParam.setDistributed_columns(getDistColumns(partition.getDistributionInfo(), table));
        return partitionParam;
    }

    private static TOlapTableLocationParam createLocation(OlapTable table, List<Long> partitionIds) throws UserException {
        TOlapTableLocationParam locationParam = new TOlapTableLocationParam();
        // BE id -> path hash
        Multimap<Long, Long> allBePathsMap = HashMultimap.create();
        for (Long partitionId : partitionIds) {
            Partition partition = table.getPartition(partitionId);
            int quorum = table.getPartitionInfo().getQuorumNum(partition.getId(), table.writeQuorum());
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    Preconditions.checkState(!table.isLakeTable());
                    // we should ensure the replica backend is alive
                    // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                    LocalTablet localTablet = (LocalTablet) tablet;
                    Multimap<Replica, Long> bePathsMap =
                            localTablet.getNormalReplicaBackendPathMap(table.getClusterId());
                    if (bePathsMap.keySet().size() < quorum) {
                        throw new UserException(InternalErrorCode.REPLICA_FEW_ERR,
                                "Tablet lost replicas. Check if any backend is down or not. tablet_id: "
                                        + tablet.getId() + ", backends: " +
                                        Joiner.on(",").join(localTablet.getBackends()));
                    }

                    // replicas[0] will be the primary replica
                    List<Replica> replicas = Lists.newArrayList(bePathsMap.keySet());
                    Collections.shuffle(replicas);

                    locationParam
                            .addToTablets(
                                    new TTabletLocation(tablet.getId(), replicas.stream().map(Replica::getBackendId)
                                            .collect(Collectors.toList())));
                    for (Map.Entry<Replica, Long> entry : bePathsMap.entries()) {
                        allBePathsMap.put(entry.getKey().getBackendId(), entry.getValue());
                    }
                }
            }
        }

        // check if disk capacity reach limit
        // this is for load process, so use high water mark to check
        Status st = GlobalStateMgr.getCurrentSystemInfo().checkExceedDiskCapacityLimit(allBePathsMap, true);
        if (!st.ok()) {
            throw new DdlException(st.getErrorMsg());
        }
        return locationParam;
    }

    private static TNodesInfo createNodesInfo() {
        TNodesInfo nodesInfo = new TNodesInfo();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getClusterInfo();
        for (Long id : systemInfoService.getBackendIds(false)) {
            Backend backend = systemInfoService.getBackend(id);
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
        return nodesInfo;
    }

    private static List<String> getDistColumns(DistributionInfo distInfo, OlapTable table) throws UserException {
        List<String> distColumns = Lists.newArrayList();
        if (distInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distInfo;
            for (Column column : hashDistributionInfo.getDistributionColumns()) {
                distColumns.add(column.getName());
            }
        } else {
            throw new UserException("unsupported distributed type, type=" + distInfo.getType());
        }
        return distColumns;
    }

    private static int getNumReplicas(OlapTable table) {
        for (Partition partition : table.getPartitions()) {
            return table.getPartitionInfo().getReplicationNum(partition.getId());
        }
        Preconditions.checkState(false, "table has no partition: " + table.getName());
        return 1;
    }

    public String getTableName() {
        return this.tableName;
    }

    public OlapTable getOlapTable() {
        return tableInfo;
    }
}
