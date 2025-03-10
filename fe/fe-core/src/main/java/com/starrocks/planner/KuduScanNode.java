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
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.kudu.KuduRemoteFileDesc;
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
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.warehouse.Warehouse;
import org.apache.kudu.client.KuduScanToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.thrift.TExplainLevel.VERBOSE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class KuduScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(KuduScanNode.class);
    private final KuduTable kuduTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private CloudConfiguration cloudConfiguration = null;

    public KuduScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        this.kuduTable = (KuduTable) desc.getTable();
        setupCloudCredential();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    private void setupCloudCredential() {
        String catalog = kuduTable.getCatalogName();
        if (catalog == null) {
            return;
        }
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
        Preconditions.checkState(connector != null,
                String.format("connector of catalog %s should not be null", catalog));
        cloudConfiguration = connector.getMetadata().getCloudConfiguration();
        Preconditions.checkState(cloudConfiguration != null,
                String.format("cloudConfiguration of catalog %s should not be null", catalog));
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("kuduTable=" + kuduTable.getName());
        return helper.toString();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    public void setupScanRangeLocations(TupleDescriptor tupleDescriptor, ScalarOperator predicate) {
        List<String> fieldNames =
                tupleDescriptor.getSlots().stream().map(s -> s.getColumn().getName()).collect(Collectors.toList());
        GetRemoteFilesParams params =
                GetRemoteFilesParams.newBuilder().setPredicate(predicate).setFieldNames(fieldNames).build();
        List<RemoteFileInfo> fileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(kuduTable, params);
        KuduRemoteFileDesc remoteFileDesc = (KuduRemoteFileDesc) fileInfos.get(0).getFiles().get(0);
        List<KuduScanToken> tokens = remoteFileDesc.getKuduScanTokens();
        if (tokens.isEmpty()) {
            LOG.warn("There is no tokens on {}.{} and predicate: [{}]",
                    kuduTable.getCatalogDBName(), kuduTable.getCatalogTableName(), predicate);
            return;
        }
        List<Long> nodeIds = getAllAvailableBackendOrComputeIds();
        for (int i = 0; i < tokens.size(); i++) {
            KuduScanToken token = tokens.get(i);
            addScanRangeLocations(token, kuduTable.getMasterAddresses(), nodeIds.get(i % nodeIds.size()));
        }
    }

    private void addScanRangeLocations(KuduScanToken token, String masterAddresses, long nodeId) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setUse_kudu_jni_reader(true);
        hdfsScanRange.setKudu_master(masterAddresses);
        try {
            String kuduScanToken = new String(BASE64_ENCODER.encode(token.serialize()), UTF_8);
            hdfsScanRange.setKudu_scan_token(kuduScanToken);
            // just skip HiveDataSource::open
            hdfsScanRange.setFile_length(kuduScanToken.length());
            hdfsScanRange.setLength(kuduScanToken.length());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        TScanRangeLocation scanRangeLocation = new TScanRangeLocation();
        scanRangeLocation.setBackend_id(nodeId);
        scanRangeLocations.addToLocations(scanRangeLocation);
        scanRangeLocationsList.add(scanRangeLocations);
    }

    @VisibleForTesting
    public List<Long> getAllAvailableBackendOrComputeIds() {
        List<Long> allNodes;
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        if (RunMode.isSharedDataMode()) {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            String warehouseName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
            if (ConnectContext.get() != null) {
                warehouseName = ConnectContext.get().getCurrentWarehouseName();
            }
            Warehouse warehouse = warehouseManager.getWarehouse(warehouseName);
            allNodes = warehouseManager.getAliveComputeNodes(warehouse.getId()).stream().map(ComputeNode::getId)
                    .collect(Collectors.toList());
        } else {
            allNodes = systemInfoService.getAvailableBackendIds();
            assert allNodes != null;
            allNodes.addAll(systemInfoService.getAvailableComputeNodeIds());
        }
        return allNodes;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(kuduTable.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (!scanNodePredicates.getPartitionConjuncts().isEmpty()) {
            output.append(prefix).append("PARTITION PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getPartitionConjuncts())).append("\n");
        }
        if (!scanNodePredicates.getNonPartitionConjuncts().isEmpty()) {
            output.append(prefix).append("NON-PARTITION PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getNonPartitionConjuncts())).append("\n");
        }
        if (!scanNodePredicates.getNoEvalPartitionConjuncts().isEmpty()) {
            output.append(prefix).append("NO EVAL-PARTITION PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getNoEvalPartitionConjuncts())).append("\n");
        }
        if (!scanNodePredicates.getMinMaxConjuncts().isEmpty()) {
            output.append(prefix).append("MIN/MAX PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getMinMaxConjuncts())).append("\n");
        }

        // TODO: support it in verbose
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

        if (kuduTable != null) {
            msg.hdfs_scan_node.setTable_name(kuduTable.getName());
        }

        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        HdfsScanNode.setMinMaxConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setNonEvalPartitionConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setNonPartitionConjunctsToThrift(msg, this, this.getScanNodePredicates());
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    private static final Base64.Encoder BASE64_ENCODER =
            Base64.getUrlEncoder().withoutPadding();
}
