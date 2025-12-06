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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TCacheStatsScanNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;

public class CacheStatsScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(CacheStatsScanNode.class);

    private final OlapTable olapTable;
    private final Map<Integer, String> columnIdToNames;
    private final List<TScanRangeLocations> result = Lists.newArrayList();

    public CacheStatsScanNode(PlanNodeId id, TupleDescriptor desc, OlapTable olapTable,
                              Map<Integer, String> columnIdToNames) {
        super(id, desc, "CacheStatsScan");
        this.olapTable = olapTable;
        this.columnIdToNames = columnIdToNames;
    }

    public void computeRangeLocations(ComputeResource computeResource) {
        StringWriter sw = new StringWriter();
        new Throwable("").printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        LOG.info("{}", stackTrace);
        Collection<PhysicalPartition> partitions = olapTable.getPhysicalPartitions();

        for (PhysicalPartition partition : partitions) {
            MaterializedIndex index = partition.getBaseIndex();
            int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
            List<Tablet> tablets = index.getTablets();
            long visibleVersion = partition.getVisibleVersion();

            for (Tablet tablet : tablets) {
                long tabletId = tablet.getId();
                TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

                TInternalScanRange internalRange = new TInternalScanRange();
                internalRange.setDb_name("");
                internalRange.setVersion(String.valueOf(visibleVersion));
                internalRange.setTablet_id(tabletId);

                List<Replica> allQueryableReplicas = Lists.newArrayList();
                tablet.getQueryableReplicas(allQueryableReplicas, Collections.emptyList(),
                        visibleVersion, -1, schemaHash, computeResource);
                if (allQueryableReplicas.isEmpty()) {
                    LOG.error("no queryable replica found in tablet {}. visible version {}",
                            tabletId, visibleVersion);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("tablet: {}, shard: {}, backends: {}", tabletId,
                                ((LakeTablet) tablet).getShardId(),
                                tablet.getBackendIds());
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
                    throw new StarRocksPlannerException(tabletId + " have no alive replicas", INTERNAL_ERROR);
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
        msg.node_type = TPlanNodeType.LAKE_CACHE_STATS_SCAN_NODE;

        TCacheStatsScanNode scanNode = new TCacheStatsScanNode();
        scanNode.setTuple_id(desc.getId().asInt());
        scanNode.setId_to_names(columnIdToNames);
        scanNode.setTable_id(olapTable.getId());
        scanNode.setTable_name(olapTable.getName());

        msg.cache_stats_scan_node = scanNode;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("Table: ").append(olapTable.getName()).append("\n");
        output.append(prefix).append("CacheStats Query\n");
        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
