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
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TConnectorScanNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
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

    public LanceTable getLanceTable() {
        return lanceTable;
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    private void setupCloudCredential() {
        String catalog = lanceTable.getCatalogName();
        if (catalog == null) {
            return;
        }
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
        Preconditions.checkState(connector != null, "Missing Lance catalog connector");
        cloudConfiguration = connector.getMetadata().getCloudConfiguration();
        Preconditions.checkState(cloudConfiguration != null, "Missing Lance catalog cloud configuration");
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

    public void setupScanRangeLocations() {
        scanRangeLocationsList.clear();
        List<Long> nodeIds = getAllAvailableBackendOrComputeIds();
        Preconditions.checkState(!nodeIds.isEmpty(), "No alive backend or compute node for Lance scan");

        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setUse_lance_jni_reader(true);
        hdfsScanRange.setFull_path(lanceTable.getUri());
        // The reader takes its dataset URI from TLanceTable; full_path identifies the range to the scheduler.
        // A single range scans the entire dataset, including every fragment.
        // Leave split_info unset until fragment enumeration and reader splitting are implemented.
        hdfsScanRange.setFile_length(0);
        hdfsScanRange.setLength(0);
        hdfsScanRange.setFile_format(THdfsFileFormat.LANCE);

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        // Remote datasets have no local replica. The connector scheduler chooses an available worker.
        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
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
        return allNodes;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(lanceTable.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        appendExplainPredicates(output, prefix);
        appendExplainStatistics(output, prefix, detailLevel);
        if (detailLevel == TExplainLevel.VERBOSE) {
            appendExplainPrunedTypes(output, prefix);
        }

        return output.toString();
    }

    private void appendExplainPredicates(StringBuilder output, String prefix) {
        appendExplainPredicate(output, prefix, "PREDICATES", conjuncts);
        appendExplainPredicate(output, prefix, "PARTITION PREDICATES", scanNodePredicates.getPartitionConjuncts());
        appendExplainPredicate(output, prefix, "NON-PARTITION PREDICATES", scanNodePredicates.getNonPartitionConjuncts());
        appendExplainPredicate(output, prefix, "NO EVAL-PARTITION PREDICATES", scanNodePredicates.getNoEvalPartitionConjuncts());
        appendExplainPredicate(output, prefix, "MIN/MAX PREDICATES", scanNodePredicates.getMinMaxConjuncts());
    }

    private void appendExplainPredicate(StringBuilder output, String prefix, String label, List<Expr> predicates) {
        if (!predicates.isEmpty()) {
            // Preserve session-controlled EXPLAIN desensitization.
            output.append(prefix).append(label).append(": ").append(explainExpr(predicates)).append("\n");
        }
    }

    private void appendExplainStatistics(StringBuilder output, String prefix, TExplainLevel detailLevel) {
        if (detailLevel != TExplainLevel.VERBOSE) {
            output.append(prefix).append(String.format("cardinality=%s\n", cardinality));
        }
        output.append("\n");
        output.append(prefix).append(String.format("avgRowSize=%s\n", avgRowSize));
    }

    private void appendExplainPrunedTypes(StringBuilder output, String prefix) {
        for (SlotDescriptor slot : desc.getSlots()) {
            Type type = slot.getOriginType();
            if (type.isComplexType()) {
                output.append(prefix).append(String.format("Pruned type: %d <-> [%s]\n", slot.getId().asInt(), type));
            }
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        msg.hdfs_scan_node = tHdfsScanNode;

        TConnectorScanNode connectorScanNode = new TConnectorScanNode();
        connectorScanNode.setConnector_name("lance");
        msg.connector_scan_node = connectorScanNode;

        String sqlPredicates = getExplainString(conjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicates);

        if (lanceTable != null) {
            msg.hdfs_scan_node.setTable_name(lanceTable.getName());
        }

        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        // PlanNode.treeToThrift is the single source of scan predicates. Lance evaluates them
        // on decoded chunks and does not use Hive partition or min/max predicate channels.

        setConnectorCatalogType(msg);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
