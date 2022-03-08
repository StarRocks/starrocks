// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.UserException;
import com.starrocks.external.RemoteScanRangeLocations;
import com.starrocks.external.hive.HdfsFileBlockDesc;
import com.starrocks.external.hive.HdfsFileDesc;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class HudiScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(HudiScanNode.class);

    private RemoteScanRangeLocations scanRangeLocations = new RemoteScanRangeLocations();

    private HudiTable hudiTable;

    // partitionColumnName -> (LiteralExpr -> partition ids)
    // no null partitions in this map, used by ListPartitionPruner
    private final Map<String, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
    // Store partitions with null partition values separately, used by ListPartitionPruner
    // partitionColumnName -> null partitionIds
    private final Map<String, Set<Long>> columnToNullPartitions = Maps.newHashMap();
    // id -> partition key
    private Map<Long, PartitionKey> idToPartitionKey = Maps.newHashMap();
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();

    // partitionConjuncts contains partition filters.
    private final List<Expr> partitionConjuncts = Lists.newArrayList();
    // After partition pruner prune, conjuncts that are not evaled will be send to backend.
    private final List<Expr> noEvalPartitionConjuncts = Lists.newArrayList();
    // nonPartitionConjuncts contains non-partition filters, and will be sent to backend.
    private final List<Expr> nonPartitionConjuncts = Lists.newArrayList();

    // List of conjuncts for min/max values that are used to skip data when scanning Parquet files.
    private final List<Expr> minMaxConjuncts = new ArrayList<>();
    private TupleDescriptor minMaxTuple;

    public HudiScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        hudiTable = (HudiTable) desc.getTable();
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("hudiTable=" + hudiTable.getName());
        return helper.toString();
    }

    public void setSelectedPartitionIds(Collection<Long> selectedPartitionIds) {
        this.selectedPartitionIds = selectedPartitionIds;
    }

    public void setIdToPartitionKey(Map<Long, PartitionKey> idToPartitionKey) {
        this.idToPartitionKey = idToPartitionKey;
    }

    public List<Expr> getNonPartitionConjuncts() {
        return nonPartitionConjuncts;
    }

    public List<Expr> getNoEvalPartitionConjuncts() {
        return noEvalPartitionConjuncts;
    }

    public List<Expr> getMinMaxConjuncts() {
        return minMaxConjuncts;
    }

    public void setMinMaxTuple(TupleDescriptor tuple) {
        minMaxTuple = tuple;
    }

    public void getScanRangeLocations(DescriptorTable descTbl) throws UserException {
        if (selectedPartitionIds.isEmpty()) {
            return;
        }

        long start = System.currentTimeMillis();
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        List<DescriptorTable.ReferencedPartitionInfo> partitionInfos = Lists.newArrayList();
        for (long partitionId : selectedPartitionIds) {
            PartitionKey partitionKey = idToPartitionKey.get(partitionId);
            partitionKeys.add(partitionKey);
            partitionInfos.add(new DescriptorTable.ReferencedPartitionInfo(partitionId, partitionKey));
        }
        List<HivePartition> hudiPartitions = hudiTable.getPartitions(partitionKeys);

        for (int i = 0; i < hudiPartitions.size(); i++) {
            descTbl.addReferencedPartitions(hudiTable, partitionInfos.get(i));
            for (HdfsFileDesc fileDesc : hudiPartitions.get(i).getFiles()) {
                for (HdfsFileBlockDesc blockDesc : fileDesc.getBlockDescs()) {
                    scanRangeLocations.addScanRangeLocations(partitionInfos.get(i).getId(), fileDesc, blockDesc,
                            hudiPartitions.get(i).getFormat());
                    LOG.debug("Add scan range success. partition: {}, file: {}, block: {}-{}",
                            hudiPartitions.get(i).getFullPath(), fileDesc.getFileName(), blockDesc.getOffset(),
                            blockDesc.getLength());
                }
            }
        }
        LOG.debug("Get {} scan range locations cost: {} ms",
                scanRangeLocations.getScanRangeLocationsSize(), (System.currentTimeMillis() - start));
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations.getScanRangeLocations(maxScanRangeLength);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(hudiTable.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (!partitionConjuncts.isEmpty()) {
            output.append(prefix).append("PARTITION PREDICATES: ").append(
                    getExplainString(partitionConjuncts)).append("\n");
        }
        if (!nonPartitionConjuncts.isEmpty()) {
            output.append(prefix).append("NON-PARTITION PREDICATES: ").append(
                    getExplainString(nonPartitionConjuncts)).append("\n");
        }

        output.append(prefix).append(
                String.format("partitions=%s/%s", selectedPartitionIds.size(), idToPartitionKey.size()));
        output.append("\n");

        output.append(prefix).append(String.format("cardinality=%s", cardinality));
        output.append("\n");

        output.append(prefix).append(String.format("avgRowSize=%s", avgRowSize));
        output.append("\n");

        output.append(prefix).append(String.format("numNodes=%s", numNodes));
        output.append("\n");

        return output.toString();
    }

    @Override
    protected String getNodeVerboseExplain(String prefix) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(hudiTable.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (!partitionConjuncts.isEmpty()) {
            output.append(prefix).append("PARTITION PREDICATES: ").append(
                    getExplainString(partitionConjuncts)).append("\n");
        }
        if (!nonPartitionConjuncts.isEmpty()) {
            output.append(prefix).append("NON-PARTITION PREDICATES: ").append(
                    getExplainString(nonPartitionConjuncts)).append("\n");
        }

        output.append(prefix).append(
                String.format("partitions=%s/%s", selectedPartitionIds.size(), idToPartitionKey.size()));
        output.append("\n");

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

        String partitionSqlPredicate = getExplainString(noEvalPartitionConjuncts);
        for (Expr expr : noEvalPartitionConjuncts) {
            msg.hdfs_scan_node.addToPartition_conjuncts(expr.treeToThrift());
        }
        msg.hdfs_scan_node.setPartition_sql_predicates(partitionSqlPredicate);

        // put non-partition conjuncts into conjuncts
        if (msg.isSetConjuncts()) {
            msg.conjuncts.clear();
        }
        for (Expr expr : nonPartitionConjuncts) {
            msg.addToConjuncts(expr.treeToThrift());
        }
        String sqlPredicate = getExplainString(nonPartitionConjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicate);

        if (!minMaxConjuncts.isEmpty()) {
            String minMaxSqlPredicate = getExplainString(minMaxConjuncts);
            for (Expr expr : minMaxConjuncts) {
                msg.hdfs_scan_node.addToMin_max_conjuncts(expr.treeToThrift());
            }
            msg.hdfs_scan_node.setMin_max_tuple_id(minMaxTuple.getId().asInt());
            msg.hdfs_scan_node.setMin_max_sql_predicates(minMaxSqlPredicate);
        }

        if (hudiTable != null) {
            msg.hdfs_scan_node.setHive_column_names(hudiTable.getDataColumnNames());
            msg.hdfs_scan_node.setTable_name(hudiTable.getName());
        }
    }
}