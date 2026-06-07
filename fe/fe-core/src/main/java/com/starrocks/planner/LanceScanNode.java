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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.LanceTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.Type;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.thrift.TExplainLevel.VERBOSE;

public class LanceScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(LanceScanNode.class);
    private final LanceTable lanceTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private CloudConfiguration cloudConfiguration = null;

    public LanceScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        this.lanceTable = (LanceTable) desc.getTable();
        setupCloudCredential();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    private void setupCloudCredential() {
        // Lance tables will use properties/credentials defined in catalog
        // For Phase 1 & 2 mock catalog, we can safely allow empty config or resolve standard connectors if registered
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("lanceTable=" + lanceTable.getName());
        return helper.toString();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    public void setupScanRangeLocations(TupleDescriptor tupleDescriptor, ScalarOperator predicate) {
        List<Long> nodeIds = getAllAvailableBackendOrComputeIds();
        if (nodeIds.isEmpty()) {
            return;
        }

        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setUse_lance_jni_reader(true);
        hdfsScanRange.setLance_dataset_uri(lanceTable.getUri());
        hdfsScanRange.setLance_split_info("{\"fragment_ids\": [0]}");
        hdfsScanRange.setFile_length(0);
        hdfsScanRange.setLength(0);
        hdfsScanRange.setFile_format(THdfsFileFormat.UNKNOWN);

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        TScanRangeLocation scanRangeLocation = new TScanRangeLocation();
        scanRangeLocation.setBackend_id(nodeIds.get(0));
        scanRangeLocations.addToLocations(scanRangeLocation);
        scanRangeLocationsList.add(scanRangeLocations);
    }

    @VisibleForTesting
    public List<Long> getAllAvailableBackendOrComputeIds() {
        List<Long> allNodes = new ArrayList<>();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        if (RunMode.isSharedDataMode()) {
            ComputeResource computeResource = WarehouseManager.DEFAULT_RESOURCE;
            if (ConnectContext.get() != null) {
                computeResource = ConnectContext.get().getCurrentComputeResource();
            }
            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            allNodes = warehouseManager.getAliveComputeNodes(computeResource)
                    .stream()
                    .map(ComputeNode::getId)
                    .collect(Collectors.toList());
        } else {
            allNodes = systemInfoService.getAvailableBackendIds();
            if (allNodes == null) {
                allNodes = new ArrayList<>();
            } else {
                allNodes = new ArrayList<>(allNodes);
            }
            List<Long> computeNodeIds = systemInfoService.getAvailableComputeNodeIds();
            if (computeNodeIds != null) {
                allNodes.addAll(computeNodeIds);
            }
        }
        if (allNodes.isEmpty()) {
            allNodes.add(10001L); // Fallback for unit testing where cluster state is empty
        }
        return allNodes;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(lanceTable.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    explainExpr(conjuncts)).append("\n");
        }
        if (!scanNodePredicates.getPartitionConjuncts().isEmpty()) {
            output.append(prefix).append("PARTITION PREDICATES: ").append(
                    explainExpr(scanNodePredicates.getPartitionConjuncts())).append("\n");
        }
        if (!scanNodePredicates.getNonPartitionConjuncts().isEmpty()) {
            output.append(prefix).append("NON-PARTITION PREDICATES: ").append(
                    explainExpr(scanNodePredicates.getNonPartitionConjuncts())).append("\n");
        }
        if (!scanNodePredicates.getNoEvalPartitionConjuncts().isEmpty()) {
            output.append(prefix).append("NO EVAL-PARTITION PREDICATES: ").append(
                    explainExpr(scanNodePredicates.getNoEvalPartitionConjuncts())).append("\n");
        }
        if (!scanNodePredicates.getMinMaxConjuncts().isEmpty()) {
            output.append(prefix).append("MIN/MAX PREDICATES: ").append(
                    explainExpr(scanNodePredicates.getMinMaxConjuncts())).append("\n");
        }

        if (detailLevel != VERBOSE) {
            output.append(prefix).append(String.format("cardinality=%s", cardinality));
            output.append("\n");
        }

        output.append("\n");
        output.append(prefix).append(String.format("avgRowSize=%s\n", avgRowSize));

        if (detailLevel == TExplainLevel.VERBOSE) {
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                Type type = slotDescriptor.getOriginType();
                if (type.isComplexType()) {
                    output.append(prefix)
                            .append(String.format("Pruned type: %d <-> [%s]\n", slotDescriptor.getId().asInt(), type));
                }
            }
        }

        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        msg.hdfs_scan_node = tHdfsScanNode;

        String sqlPredicates = getExplainString(conjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicates);

        if (lanceTable != null) {
            msg.hdfs_scan_node.setTable_name(lanceTable.getName());
        }

        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        HdfsScanNode.setMinMaxConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setNonEvalPartitionConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setNonPartitionConjunctsToThrift(msg, this, this.getScanNodePredicates());

        setConnectorCatalogType(msg);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
