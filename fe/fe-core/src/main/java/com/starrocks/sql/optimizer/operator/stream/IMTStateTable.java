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
import com.starrocks.analysis.TableName;
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
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TIMTDescriptor;
import com.starrocks.thrift.TIMTType;
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
 * Intermediate materialized table
 */
public class IMTStateTable {
    private final TIMTType type;
    private final int numReplicas;
    private final String tableName;
    private final TOlapTableSchemaParam tableSchema;
    private final TOlapTablePartitionParam partition;
    private final TOlapTableLocationParam location;
    private final TNodesInfo nodes;
    private final OlapTable olapTable;

    public IMTStateTable(TIMTType type, OlapTable table, int numReplicas,
                         TOlapTableSchemaParam tableSchema,
                         TOlapTablePartitionParam partition, TOlapTableLocationParam location, TNodesInfo nodes) {
        this.type = type;
        this.numReplicas = numReplicas;
        this.tableSchema = tableSchema;
        this.partition = partition;
        this.location = location;
        this.nodes = nodes;
        this.olapTable = table;
        this.tableName = table.getName();
    }

    public static IMTStateTable fromTableName(long dbId, TableName tableName) throws UserException {
        Preconditions.checkState(tableName != null);
        OlapTable olapTable = (OlapTable) MetaUtils.getTable(tableName);

        List<Long> partitionIds =
                olapTable.getAllPartitions().stream().map(Partition::getId).collect(Collectors.toList());
        TOlapTableSchemaParam tableSchema = createTableSchema(dbId, olapTable);
        TOlapTablePartitionParam partition = createPartitionParam(dbId, olapTable, partitionIds);
        TOlapTableLocationParam location = createLocation(olapTable, partitionIds);
        TNodesInfo nodes = createNodesInfo();
        return new IMTStateTable(TIMTType.OLAP_TABLE, olapTable, getNumReplicas(olapTable),
                tableSchema, partition, location, nodes);
    }

    public TIMTDescriptor toThrift() {
        TIMTDescriptor desc = new TIMTDescriptor();
        desc.setImt_type(this.type);

        // route info
        TOlapTableRouteInfo routeInfo = new TOlapTableRouteInfo();
        routeInfo.setNum_replicas(numReplicas);
        routeInfo.setSchema(this.tableSchema);
        routeInfo.setPartition(this.partition);
        routeInfo.setLocation(this.location);
        routeInfo.setNum_replicas(this.numReplicas);
        routeInfo.setTable_name(this.tableName);
        routeInfo.setNodes_info(this.nodes);
        routeInfo.setKeys_type(this.olapTable.getKeysType().toThrift());
        // NOTE: must enable replicated storage for parallel tablet sink dop > 1
        routeInfo.setEnable_replicated_storage(true);
        desc.setOlap_table(routeInfo);
        return desc;
    }

    public OlapTable getOlapTable() {
        Preconditions.checkState(type == TIMTType.OLAP_TABLE);
        return olapTable;
    }
    private static TOlapTableSchemaParam createTableSchema(long dbId, OlapTable table) {
        TOlapTableSchemaParam schemaParam = new TOlapTableSchemaParam();
        schemaParam.setDb_id(dbId);
        schemaParam.setTable_id(table.getId());

        // NOTE: Use the newest IMT's version as its version.
        // For each epoch, will also fetch the newest version.
        long version = table.getAllPartitions().stream().findFirst().get().getVisibleVersion();
        schemaParam.setVersion(version);

        // IMT Table only support one index for now.
        Preconditions.checkState(table.getIndexIdToMeta().size() == 1);
        for (Map.Entry<Long, MaterializedIndexMeta> pair : table.getIndexIdToMeta().entrySet()) {
            MaterializedIndexMeta indexMeta = pair.getValue();
            List<String> columns = Lists.newArrayList();
            columns.addAll(indexMeta.getSchema().stream().map(Column::getName).collect(Collectors.toList()));
            if (table.getKeysType() == KeysType.PRIMARY_KEYS) {
                columns.add(Load.LOAD_OP_COLUMN);
            }
            TOlapTableIndexSchema indexSchema = new TOlapTableIndexSchema(pair.getKey(), columns,
                    indexMeta.getSchemaHash());
            schemaParam.addToIndexes(indexSchema);
        }

        // Normal columns
        DescriptorTable descriptorTable = new DescriptorTable();
        TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();
        for (Column column : table.getFullSchema()) {
            SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
            slotDescriptor.setIsMaterialized(true);
            slotDescriptor.setType(column.getType());
            slotDescriptor.setColumn(column);
            slotDescriptor.setIsNullable(column.isAllowNull());
        }
        // Add `__ops` columns' slot desc
        SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
        Column opColumn = new Column(Load.LOAD_OP_COLUMN, ScalarType.TINYINT);
        slotDescriptor.setIsMaterialized(true);
        slotDescriptor.setType(opColumn.getType());
        slotDescriptor.setColumn(opColumn);
        slotDescriptor.setIsNullable(false);
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

            // IMTStateTable should only have one based table, MV on MV is still not supported yet.
            Preconditions.checkState(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).size() == 1);
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

    @Override
    public String toString() {
        return String.format("IMTStateTable type=%s, table_name=%s", type.toString(), tableName);
    }
}