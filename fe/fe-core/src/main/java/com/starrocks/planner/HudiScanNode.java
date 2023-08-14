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

import com.google.common.base.MoreObjects;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.Connector;
import com.starrocks.connector.RemoteScanRangeLocations;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.List;

public class HudiScanNode extends ScanNode {
    private final RemoteScanRangeLocations scanRangeLocations = new RemoteScanRangeLocations();

    private final HudiTable hudiTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private CloudConfiguration cloudConfiguration = null;

    private DescriptorTable descTbl;

    public HudiScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        this.hudiTable = (HudiTable) desc.getTable();
        setupCloudCredential();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    public HudiTable getHudiTable() {
        return hudiTable;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("hudiTable=" + hudiTable.getName());
        return helper.toString();
    }

    public void setupScanRangeLocations(DescriptorTable descTbl) {
        this.descTbl = descTbl;
        scanRangeLocations.setup(descTbl, hudiTable, scanNodePredicates);
    }

    private void setupCloudCredential() {
        String catalog = hudiTable.getCatalogName();
        if (catalog == null) {
            return;
        }
        Connector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
        if (connector != null) {
            cloudConfiguration = connector.getCloudConfiguration();
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations.getScanRangeLocations(descTbl, hudiTable, scanNodePredicates);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(hudiTable.getName()).append("\n");

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
        if (!scanNodePredicates.getMinMaxConjuncts().isEmpty()) {
            output.append(prefix).append("MIN/MAX PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getMinMaxConjuncts())).append("\n");
        }

        output.append(prefix).append(
                String.format("partitions=%s/%s", scanNodePredicates.getSelectedPartitionIds().size(),
                        scanNodePredicates.getIdToPartitionKey().size()));
        output.append("\n");

        output.append(prefix).append(String.format("cardinality=%s", cardinality));
        output.append("\n");

        output.append(prefix).append(String.format("avgRowSize=%s", avgRowSize));
        output.append("\n");

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
    public int getNumInstances() {
        return scanRangeLocations.getScanRangeLocationsSize();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        msg.hdfs_scan_node = tHdfsScanNode;

        List<Expr> noEvalPartitionConjuncts = scanNodePredicates.getNoEvalPartitionConjuncts();
        String partitionSqlPredicate = getExplainString(noEvalPartitionConjuncts);
        for (Expr expr : noEvalPartitionConjuncts) {
            msg.hdfs_scan_node.addToPartition_conjuncts(expr.treeToThrift());
        }
        msg.hdfs_scan_node.setPartition_sql_predicates(partitionSqlPredicate);

        // put non-partition conjuncts into conjuncts
        if (msg.isSetConjuncts()) {
            msg.conjuncts.clear();
        }

        List<Expr> nonPartitionConjuncts = scanNodePredicates.getNonPartitionConjuncts();
        for (Expr expr : nonPartitionConjuncts) {
            msg.addToConjuncts(expr.treeToThrift());
        }
        String sqlPredicate = getExplainString(nonPartitionConjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicate);

        List<Expr> minMaxConjuncts = scanNodePredicates.getMinMaxConjuncts();
        if (!minMaxConjuncts.isEmpty()) {
            String minMaxSqlPredicate = getExplainString(minMaxConjuncts);
            for (Expr expr : minMaxConjuncts) {
                msg.hdfs_scan_node.addToMin_max_conjuncts(expr.treeToThrift());
            }
            msg.hdfs_scan_node.setMin_max_tuple_id(scanNodePredicates.getMinMaxTuple().getId().asInt());
            msg.hdfs_scan_node.setMin_max_sql_predicates(minMaxSqlPredicate);
        }

        if (hudiTable != null) {
            msg.hdfs_scan_node.setHive_column_names(hudiTable.getDataColumnNames());
            msg.hdfs_scan_node.setTable_name(hudiTable.getName());
        }

        if (cloudConfiguration != null) {
            TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
            cloudConfiguration.toThrift(tCloudConfiguration);
            msg.hdfs_scan_node.setCloud_configuration(tCloudConfiguration);
        }

        msg.hdfs_scan_node.setCan_use_any_column(canUseAnyColumn);
        msg.hdfs_scan_node.setCan_use_min_max_count_opt(canUseMinMaxCountOpt);
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
