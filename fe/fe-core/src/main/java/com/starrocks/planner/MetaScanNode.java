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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.common.Pair;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TMetaScanNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;

public class MetaScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(MetaScanNode.class);
    private final Map<Integer, Pair<String, Column>> columnIdToColumns;
    private final OlapTable olapTable;
    private final List<Column> tableSchema;
    private final List<String> selectPartitionNames;
    private final List<TScanRangeLocations> result = Lists.newArrayList();
    private long selectedIndexId = -1;

    public MetaScanNode(PlanNodeId id, TupleDescriptor desc, OlapTable olapTable,
                        Map<Integer, Pair<String, Column>> aggColumnIdToColumns, List<String> selectPartitionNames,
                        long selectedIndexId, ComputeResource computeResource) {
        super(id, desc, "MetaScan");
        this.olapTable = olapTable;
        this.tableSchema = olapTable.getBaseSchema();
        this.columnIdToColumns = aggColumnIdToColumns;
        this.selectPartitionNames = selectPartitionNames;
        this.selectedIndexId = selectedIndexId;
        this.computeResource = computeResource;
    }

    public void computeRangeLocations(ComputeResource computeResource) {
        Collection<PhysicalPartition> partitions;
        if (selectPartitionNames.isEmpty()) {
            partitions = olapTable.getPhysicalPartitions();
        } else {
            partitions = selectPartitionNames.stream().map(name -> {
                Partition partition = olapTable.getPartition(name, false);
                if (partition != null) {
                    return partition;
                }
                return olapTable.getPartition(name, true);
            }).map(Partition::getSubPartitions).flatMap(Collection::stream).collect(Collectors.toList());
        }
        for (PhysicalPartition partition : partitions) {
            MaterializedIndex index = partition.getBaseIndex();
            int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
            List<Tablet> tablets = index.getTablets();

            long visibleVersion = partition.getVisibleVersion();
            String visibleVersionStr = String.valueOf(visibleVersion);

            for (Tablet tablet : tablets) {
                long tabletId = tablet.getId();
                TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

                TInternalScanRange internalRange = new TInternalScanRange();
                internalRange.setDb_name("");
                internalRange.setSchema_hash(String.valueOf(schemaHash));
                internalRange.setVersion(visibleVersionStr);
                internalRange.setVersion_hash("0");
                internalRange.setTablet_id(tabletId);

                // random shuffle List && only collect one copy
                List<Replica> allQueryableReplicas = Lists.newArrayList();
                if (RunMode.isSharedDataMode()) {
                    tablet.getQueryableReplicas(allQueryableReplicas, Collections.emptyList(),
                            visibleVersion, -1, schemaHash, computeResource);
                } else {
                    tablet.getQueryableReplicas(allQueryableReplicas, Collections.emptyList(),
                            visibleVersion, -1, schemaHash);
                }

                if (allQueryableReplicas.isEmpty()) {
                    LOG.error("no queryable replica found in tablet {}. visible version {}",
                            tabletId, visibleVersion);
                    if (LOG.isDebugEnabled()) {
                        if (olapTable.isCloudNativeTableOrMaterializedView()) {
                            LOG.debug("tablet: {}, shard: {}, backends: {}", tabletId,
                                    ((LakeTablet) tablet).getShardId(),
                                    tablet.getBackendIds());
                        } else {
                            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                LOG.debug("tablet {}, replica: {}", tabletId, replica.toString());
                            }
                        }
                    }
                    throw new StarRocksPlannerException("Failed to get scan range, no queryable replica found " +
                            "in tablet: " + tabletId, INTERNAL_ERROR);
                }

                Collections.shuffle(allQueryableReplicas);
                boolean tabletIsNull = true;
                for (Replica replica : allQueryableReplicas) {
                    ComputeNode node =
                            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                                    .getBackendOrComputeNode(replica.getBackendId());
                    if (node == null) {
                        LOG.debug("replica {} not exists", replica.getBackendId());
                        continue;
                    }
                    String ip = node.getHost();
                    int port = node.getBePort();
                    TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(ip, port));
                    scanRangeLocation.setBackend_id(replica.getBackendId());
                    scanRangeLocations.addToLocations(scanRangeLocation);
                    internalRange.addToHosts(new TNetworkAddress(ip, port));
                    tabletIsNull = false;
                }
                if (tabletIsNull) {
                    throw new StarRocksPlannerException(tabletId + "have no alive replicas", INTERNAL_ERROR);
                }
                TScanRange scanRange = new TScanRange();
                scanRange.setInternal_scan_range(internalRange);
                scanRangeLocations.setScan_range(scanRange);

                result.add(scanRangeLocations);
            }
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        if (olapTable.isCloudNativeTableOrMaterializedView()) {
            msg.node_type = TPlanNodeType.LAKE_META_SCAN_NODE;
        } else {
            msg.node_type = TPlanNodeType.META_SCAN_NODE;
        }
        msg.meta_scan_node = new TMetaScanNode();
        Map<Integer, String> columnIdToNames = buildColumnIdToNames(columnIdToColumns);
        msg.meta_scan_node.setId_to_names(columnIdToNames);
        msg.meta_scan_node.setLow_cardinality_threshold(CacheDictManager.LOW_CARDINALITY_THRESHOLD);
        List<TColumn> columnsDesc = Lists.newArrayList();
        for (Column column : tableSchema) {
            TColumn tColumn = column.toThrift();
            tColumn.setColumn_name(column.getColumnId().getId());
            columnsDesc.add(tColumn);
        }
        msg.meta_scan_node.setColumns(columnsDesc);
        if (selectedIndexId != -1) {
            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(selectedIndexId);
            if (indexMeta != null) {
                long schemaId = indexMeta.getSchemaId();
                msg.meta_scan_node.setSchema_id(schemaId);
            }
        }

        if (CollectionUtils.isNotEmpty(columnAccessPaths)) {
            msg.meta_scan_node.setColumn_access_paths(columnAccessPathToThrift());
        }
    }

    private Map<Integer, String> buildColumnIdToNames(Map<Integer, Pair<String, Column>> aggColumnIdToColumns) {
        Map<Integer, String> result = Maps.newHashMap();
        for (Map.Entry<Integer, Pair<String, Column>> entry : aggColumnIdToColumns.entrySet()) {
            String aggFuncName = entry.getValue().first;
            String columnName = entry.getValue().second.getColumnId().getId();
            result.put(entry.getKey(), aggFuncName + "_" + columnName);
        }
        return result;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("Table: ").append(olapTable.getName()).append("\n");
        for (Map.Entry<Integer, Pair<String, Column>> entry : columnIdToColumns.entrySet()) {
            output.append(prefix);
            String aggFuncName = entry.getValue().first;
            String columnName = entry.getValue().second.getName();
            output.append("<id ").
                    append(entry.getKey()).
                    append("> : ").
                    append(aggFuncName).
                    append("_").
                    append(columnName).
                    append("\n");
        }
        if (!selectPartitionNames.isEmpty()) {
            output.append(prefix).append("Partitions: ").append(selectPartitionNames).append("\n");
        }

        if (detailLevel == TExplainLevel.VERBOSE) {
            output.append(explainColumnAccessPath(prefix));
        }
        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    public boolean isRunningAsConnectorOperator() {
        return false;
    }
}
