// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner.stream;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.DdlException;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.lake.LakeTablet;
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
    private final TOlapTableSchemaParam tableSchema;
    private final TOlapTablePartitionParam partition;
    private final TOlapTableLocationParam location;
    private final TNodesInfo nodes;
    private final OlapTable tableInfo;

    public OlapTableRouteInfo(OlapTable table, int numReplicas, TOlapTableSchemaParam tableSchema,
                              TOlapTablePartitionParam partition, TOlapTableLocationParam location, TNodesInfo nodes) {
        this.numReplicas = numReplicas;
        this.tableSchema = tableSchema;
        this.partition = partition;
        this.location = location;
        this.nodes = nodes;
        this.tableInfo = table;
        this.tableName = table.getName();
    }

    /**
     * Create a route info for all partitions
     */
    public static OlapTableRouteInfo create(long dbId, OlapTable olapTable) throws UserException {
        List<Long> partitionIds = olapTable.getAllPartitions().stream().map(Partition::getId).collect(Collectors.toList());
        return create(dbId, olapTable, partitionIds);
    }

    public static OlapTableRouteInfo create(long dbId, OlapTable olapTable, List<Long> partitionIds) throws UserException {
        TOlapTableSchemaParam tableSchema = createTableSchema(dbId, olapTable);
        TOlapTablePartitionParam partition = createPartitionParam(dbId, olapTable, partitionIds);
        TOlapTableLocationParam location = createLocation(olapTable, partitionIds);
        TNodesInfo nodes = createNodesInfo();
        return new OlapTableRouteInfo(olapTable, getNumReplicas(olapTable), tableSchema, partition, location, nodes);
    }

    public TOlapTableRouteInfo toThrift() {
        TOlapTableRouteInfo res = new TOlapTableRouteInfo();
        // TODO: db id
        // TODO: db name
        res.setSchema(this.tableSchema);
        res.setPartition(this.partition);
        res.setLocation(this.location);
        res.setNum_replicas(this.numReplicas);
        res.setTable_name(this.tableName);
        res.setNodes_info(this.nodes);
        res.setKeys_type(this.tableInfo.getKeysType().toThrift());
        return res;
    }

    private static TOlapTableSchemaParam createTableSchema(long dbId, OlapTable table) {
        TOlapTableSchemaParam schemaParam = new TOlapTableSchemaParam();
        schemaParam.setDb_id(dbId);
        schemaParam.setTable_id(table.getId());
        schemaParam.setVersion(0);

        // schemaParam.tuple_desc = tupleDescriptor.toThrift();
        // for (SlotDescriptor slotDesc : tupleDescriptor.getSlots()) {
        //     schemaParam.addToSlot_descs(slotDesc.toThrift());
        // }

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
        return schemaParam;
    }

    private static TOlapTablePartitionParam createPartitionParam(long dbId, OlapTable table, List<Long> partitionIds)
            throws UserException {
        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        partitionParam.setDb_id(dbId);
        partitionParam.setTable_id(table.getId());
        partitionParam.setVersion(0);

        PartitionType partType = table.getPartitionInfo().getType();
        switch (partType) {
            case RANGE: {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
                for (Column partCol : rangePartitionInfo.getPartitionColumns()) {
                    partitionParam.addToPartition_columns(partCol.getName());
                }

                int partColNum = rangePartitionInfo.getPartitionColumns().size();
                DistributionInfo selectedDistInfo = null;

                for (Long partitionId : partitionIds) {
                    Partition partition = table.getPartition(partitionId);
                    TOlapTablePartition tPartition = new TOlapTablePartition();
                    tPartition.setId(partition.getId());
                    Range<PartitionKey> range = rangePartitionInfo.getRange(partition.getId());
                    // set start keys
                    if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
                        for (int i = 0; i < partColNum; i++) {
                            tPartition.addToStart_keys(
                                    range.lowerEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
                        }
                    }
                    // set end keys
                    if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
                        for (int i = 0; i < partColNum; i++) {
                            tPartition.addToEnd_keys(
                                    range.upperEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
                        }
                    }

                    for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                                index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                        tPartition.setNum_buckets(index.getTablets().size());
                    }
                    partitionParam.addToPartitions(tPartition);

                    DistributionInfo distInfo = partition.getDistributionInfo();
                    if (selectedDistInfo == null) {
                        partitionParam.setDistributed_columns(getDistColumns(distInfo, table));
                        selectedDistInfo = distInfo;
                    } else {
                        if (selectedDistInfo.getType() != distInfo.getType()) {
                            throw new UserException("different distribute types in two different partitions, type1="
                                    + selectedDistInfo.getType() + ", type2=" + distInfo.getType());
                        }
                    }
                }
                if (rangePartitionInfo instanceof ExpressionRangePartitionInfo) {
                    ExpressionRangePartitionInfo exprPartitionInfo = (ExpressionRangePartitionInfo) rangePartitionInfo;
                    partitionParam.setPartition_exprs(Expr.treesToThrift(exprPartitionInfo.getPartitionExprs()));
                }
                break;
            }
            case UNPARTITIONED: {
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
                break;
            }
            default: {
                throw new UserException("unsupported partition for OlapTable, partition=" + partType);
            }
        }
        return partitionParam;
    }

    private static TOlapTableLocationParam createLocation(OlapTable table, List<Long> partitionIds) throws UserException {
        TOlapTableLocationParam locationParam = new TOlapTableLocationParam();
        // BE id -> path hash
        Multimap<Long, Long> allBePathsMap = HashMultimap.create();
        for (Long partitionId : partitionIds) {
            Partition partition = table.getPartition(partitionId);
            int quorum = table.getPartitionInfo().getQuorumNum(partition.getId());
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    if (table.isLakeTable()) {
                        locationParam.addToTablets(new TTabletLocation(
                                tablet.getId(), Lists.newArrayList(((LakeTablet) tablet).getPrimaryBackendId())));
                    } else {
                        // we should ensure the replica backend is alive
                        // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                        LocalTablet localTablet = (LocalTablet) tablet;
                        Multimap<Long, Long> bePathsMap =
                                localTablet.getNormalReplicaBackendPathMap(table.getClusterId());
                        if (bePathsMap.keySet().size() < quorum) {
                            throw new UserException(InternalErrorCode.REPLICA_FEW_ERR,
                                    "Tablet lost replicas. Check if any backend is down or not. tablet_id: "
                                            + tablet.getId() + ", backends: " +
                                            Joiner.on(",").join(localTablet.getBackends()));
                        }
                        // replicas[0] will be the primary replica
                        List<Long> replicas = Lists.newArrayList(bePathsMap.keySet());
                        Collections.shuffle(replicas);
                        locationParam
                                .addToTablets(
                                        new TTabletLocation(tablet.getId(), replicas));
                        allBePathsMap.putAll(bePathsMap);
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
        return 0;
    }
}
