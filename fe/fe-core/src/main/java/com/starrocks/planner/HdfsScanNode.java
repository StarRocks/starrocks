// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.HiveTable;
import com.starrocks.common.UserException;
import com.starrocks.external.RemoteScanRangeLocations;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.List;

import static com.starrocks.thrift.TExplainLevel.VERBOSE;

/**
 * Scan node for HDFS files, like hive table.
 * <p>
 * The class is responsible for
 * 1. Partition pruning: filter out irrelevant partitions based on the conjuncts
 * and table partition schema.
 * 2. Min-max pruning: creates an additional list of conjuncts that are used to
 * prune a row group if any fail the row group's min-max parquet::Statistics.
 * 3. Get scan range locations.
 * 4. Compute stats, like cardinality, avgRowSize and numNodes.
 * <p>
 * TODO: Dictionary pruning
 */
public class HdfsScanNode extends ScanNode {
    private RemoteScanRangeLocations scanRangeLocations = new RemoteScanRangeLocations();

    private HiveTable hiveTable = null;
    private HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();

    public HdfsScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        hiveTable = (HiveTable) desc.getTable();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    public HiveTable getHiveTable() {
        return hiveTable;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("hiveTable=" + hiveTable.getName());
        return helper.toString();
    }

    public void setupScanRangeLocations(DescriptorTable descTbl) throws UserException {
        scanRangeLocations.setupScanRangeLocations(descTbl, hiveTable, scanNodePredicates);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations.getScanRangeLocations(maxScanRangeLength);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(hiveTable.getName()).append("\n");

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

        output.append(prefix).append(
                String.format("partitions=%s/%s", scanNodePredicates.getSelectedPartitionIds().size(),
                        scanNodePredicates.getIdToPartitionKey().size()));
        output.append("\n");

        // TODO: support it in verbose
        if (detailLevel != VERBOSE) {
            output.append(prefix).append(String.format("cardinality=%s", cardinality));
            output.append("\n");
        }

        output.append(prefix).append(String.format("avgRowSize=%s", avgRowSize));
        output.append("\n");

        output.append(prefix).append(String.format("numNodes=%s", numNodes));
        output.append("\n");

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

        if (hiveTable != null) {
            msg.hdfs_scan_node.setHive_column_names(hiveTable.getDataColumnNames());
            msg.hdfs_scan_node.setTable_name(hiveTable.getName());
        }
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
    }
}
