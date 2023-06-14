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
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TMetaScanNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;

public class MetaScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(MetaScanNode.class);
    private final Map<Integer, String> columnIdToNames;
    private final OlapTable olapTable;
    private final List<TScanRangeLocations> result = Lists.newArrayList();

    public MetaScanNode(PlanNodeId id, TupleDescriptor desc, OlapTable olapTable,
                        Map<Integer, String> columnIdToNames) {
        super(id, desc, "MetaScan");
        this.olapTable = olapTable;
        this.columnIdToNames = columnIdToNames;
    }

    public void computeRangeLocations() {
        Collection<Partition> partitions = olapTable.getPartitions();
        for (Partition partition : partitions) {
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
                tablet.getQueryableReplicas(allQueryableReplicas, Collections.emptyList(),
                        visibleVersion, -1, schemaHash);
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
                    ComputeNode node = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(replica.getBackendId());
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
        msg.meta_scan_node.setId_to_names(columnIdToNames);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("Table: ").append(olapTable.getName()).append("\n");
        for (Map.Entry<Integer, String> kv : columnIdToNames.entrySet()) {
            output.append(prefix);
            output.append("<id ").
                    append(kv.getKey()).
                    append("> : ").
                    append(kv.getValue()).
                    append("\n");
        }
        return output.toString();
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
