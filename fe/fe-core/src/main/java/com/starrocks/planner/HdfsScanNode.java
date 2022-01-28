// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.external.PredicateUtils;
import com.starrocks.external.hive.HdfsFileBlockDesc;
import com.starrocks.external.hive.HdfsFileDesc;
import com.starrocks.external.hive.HdfsFileFormat;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.qe.ConnectContext;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class HdfsScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(HdfsScanNode.class);

    private List<TScanRangeLocations> result = new ArrayList<>();

    private HiveTable hiveTable = null;
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

    private final HashMultimap<String, Long> hostToBeId = HashMultimap.create();
    private final Set<Long> localBackendIds = Sets.newHashSet();
    private long totalBytes = 0;

    private boolean isFinalized = false;

    public HdfsScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        hiveTable = (HiveTable) desc.getTable();
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("hiveTable=" + hiveTable.getName());
        return helper.toString();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        getAliveBackends();
        initPartitionInfo();
        preProcessConjuncts();
        computePartitionInfo();
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }

        LOG.debug("HdfsScanNode finalize. Tuple: {}", desc);
        try {
            getScanRangeLocations(analyzer.getDescTbl());
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }

        // Min max tuple must be computed after analyzer.materializeSlots()
        DescriptorTable descTbl = analyzer.getDescTbl();
        minMaxTuple = descTbl.createTupleDescriptor();
        PredicateUtils.computeMinMaxTupleAndConjuncts(analyzer, minMaxTuple, minMaxConjuncts, nonPartitionConjuncts);

        computeStats(analyzer);
        isFinalized = true;
    }

    private void getAliveBackends() throws UserException {
        for (Backend be : Catalog.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAlive()) {
                hostToBeId.put(be.getHost(), be.getId());
            }
        }
        if (hostToBeId.isEmpty()) {
            throw new UserException("Backend not found. Check if any backend is down or not");
        }
    }

    private void initPartitionInfo() throws DdlException {
        List<Column> partitionColumns = hiveTable.getPartitionColumns();
        for (Column column : partitionColumns) {
            String columnName = column.getName();
            columnToPartitionValuesMap.put(columnName, new TreeMap<>());
            columnToNullPartitions.put(columnName, Sets.newHashSet());
        }

        // no partition column table:
        // 1. partitionColumns is empty
        // 2. partitionKeys size = 1
        // 3. key.getKeys() is empty
        Map<PartitionKey, Long> partitionKeys = hiveTable.getPartitionKeys();
        for (Map.Entry<PartitionKey, Long> entry : partitionKeys.entrySet()) {
            PartitionKey key = entry.getKey();
            long partitionId = entry.getValue();
            List<LiteralExpr> literals = key.getKeys();
            for (int i = 0; i < literals.size(); i++) {
                String columnName = partitionColumns.get(i).getName();
                LiteralExpr literal = literals.get(i);
                if (Expr.IS_NULL_LITERAL.apply(literal)) {
                    columnToNullPartitions.get(columnName).add(partitionId);
                    continue;
                }

                Set<Long> partitions = columnToPartitionValuesMap.get(columnName).get(literal);
                if (partitions == null) {
                    partitions = Sets.newHashSet();
                    columnToPartitionValuesMap.get(columnName).put(literal, partitions);
                }
                partitions.add(partitionId);
            }
            idToPartitionKey.put(partitionId, key);
        }
        LOG.debug("table: {}, partition values map: {}, null partition map: {}",
                hiveTable.getName(), columnToPartitionValuesMap, columnToNullPartitions);
    }

    private void preProcessConjuncts() {
        List<SlotId> partitionSlotIds = Lists.newArrayList();
        for (String columnName : columnToPartitionValuesMap.keySet()) {
            SlotDescriptor slotDesc = desc.getColumnSlot(columnName);
            if (slotDesc == null) {
                continue;
            }
            partitionSlotIds.add(slotDesc.getId());
        }

        for (Expr expr : conjuncts) {
            if (expr.isBound(partitionSlotIds)) {
                partitionConjuncts.add(expr);
            } else {
                nonPartitionConjuncts.add(expr);
            }
        }
    }

    private void computePartitionInfo() throws AnalysisException {
        long start = System.currentTimeMillis();
        PartitionPruner partitionPruner = new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                partitionConjuncts, desc);
        selectedPartitionIds = partitionPruner.prune();
        if (selectedPartitionIds == null) {
            selectedPartitionIds = idToPartitionKey.keySet();
        }
        LOG.debug("partition prune cost: {} ms, partitions: {}",
                (System.currentTimeMillis() - start), selectedPartitionIds);

        noEvalPartitionConjuncts.addAll(((ListPartitionPruner) partitionPruner).getNoEvalConjuncts());
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
        List<HivePartition> hivePartitions = hiveTable.getPartitions(partitionKeys);

        for (int i = 0; i < hivePartitions.size(); i++) {
            descTbl.addReferencedPartitions(hiveTable, partitionInfos.get(i));
            for (HdfsFileDesc fileDesc : hivePartitions.get(i).getFiles()) {
                totalBytes += fileDesc.getLength();
                for (HdfsFileBlockDesc blockDesc : fileDesc.getBlockDescs()) {
                    addScanRangeLocations(partitionInfos.get(i).getId(), fileDesc, blockDesc,
                            hivePartitions.get(i).getFormat());
                    LOG.debug("add scan range success. partition: {}, file: {}, block: {}-{}",
                            hivePartitions.get(i).getFullPath(), fileDesc.getFileName(), blockDesc.getOffset(),
                            blockDesc.getLength());
                }
            }
        }
        LOG.debug("get {} scan range locations cost: {} ms", result.size(), (System.currentTimeMillis() - start));
    }

    private void addScanRangeLocations(long partitionId, HdfsFileDesc fileDesc, HdfsFileBlockDesc blockDesc,
                                       HdfsFileFormat fileFormat) {
        // NOTE: Config.hive_max_split_size should be extracted to a local variable,
        // because it may be changed before calling 'splitScanRangeLocations'
        // and after needSplit has been calculated.
        long splitSize = Config.hive_max_split_size;
        boolean needSplit = fileDesc.isSplittable() && blockDesc.getLength() > splitSize;
        if (needSplit) {
            splitScanRangeLocations(partitionId, fileDesc, blockDesc, fileFormat, splitSize);
        } else {
            createScanRangeLocationsForSplit(partitionId, fileDesc, blockDesc,
                    fileFormat, blockDesc.getOffset(), blockDesc.getLength());
        }
    }

    private void splitScanRangeLocations(long partitionId,
                                         HdfsFileDesc fileDesc,
                                         HdfsFileBlockDesc blockDesc,
                                         HdfsFileFormat fileFormat,
                                         long splitSize) {
        long remainingBytes = blockDesc.getLength();
        long length = blockDesc.getLength();
        long offset = blockDesc.getOffset();
        do {
            if (remainingBytes <= splitSize) {
                createScanRangeLocationsForSplit(partitionId, fileDesc,
                        blockDesc, fileFormat, offset + length - remainingBytes,
                        remainingBytes);
                remainingBytes = 0;
            } else if (remainingBytes <= 2 * splitSize) {
                long mid = (remainingBytes + 1) / 2;
                createScanRangeLocationsForSplit(partitionId, fileDesc,
                        blockDesc, fileFormat, offset + length - remainingBytes, mid);
                createScanRangeLocationsForSplit(partitionId, fileDesc,
                        blockDesc, fileFormat, offset + length - remainingBytes + mid,
                        remainingBytes - mid);
                remainingBytes = 0;
            } else {
                createScanRangeLocationsForSplit(partitionId, fileDesc,
                        blockDesc, fileFormat, offset + length - remainingBytes,
                        splitSize);
                remainingBytes -= splitSize;
            }
        } while (remainingBytes > 0);
    }

    private void createScanRangeLocationsForSplit(long partitionId,
                                                  HdfsFileDesc fileDesc,
                                                  HdfsFileBlockDesc blockDesc,
                                                  HdfsFileFormat fileFormat,
                                                  long offset, long length) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path(fileDesc.getFileName());
        hdfsScanRange.setOffset(offset);
        hdfsScanRange.setLength(length);
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileDesc.getLength());
        hdfsScanRange.setFile_format(fileFormat.toThrift());
        hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        for (long hostId : blockDesc.getReplicaHostIds()) {
            String host = blockDesc.getDataNodeIp(hostId);
            TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(host, -1));
            scanRangeLocations.addToLocations(scanRangeLocation);
            if (hostToBeId.containsKey(host)) {
                localBackendIds.addAll(hostToBeId.get(host));
            }
        }

        result.add(scanRangeLocations);
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        Set<Long> scanBackendIds = Sets.newHashSet();
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isForceScheduleLocal()) {
            // force_schedule_local variable set
            scanBackendIds.addAll(localBackendIds);
        } else {
            // all alive be will be used to process scan ranges default
            scanBackendIds.addAll(hostToBeId.values());
        }

        computeCardinality();
        if (cardinality > 0) {
            avgRowSize = totalBytes / (float) cardinality;
            if (hasLimit()) {
                cardinality = Math.min(cardinality, limit);
            }

            numNodes = Math.min(scanBackendIds.size(), result.size());
        }
        // even current node scan has no data, at least one backend will be assigned when the fragment actually execute
        numNodes = numNodes <= 0 ? 1 : numNodes;
        // when node scan has no data, cardinality should be 0 instead of a invalid value after computeStats()
        cardinality = cardinality == -1 ? 0 : cardinality;
    }

    /**
     * 1. compute based on table stats and partition file total bytes to be scanned
     * 2. get from partition row num stats if table stats is missing
     */
    public void computeCardinality() {
        cardinality = hiveTable.getExtrapolatedRowCount(totalBytes);
        LOG.debug("get cardinality from table stats: {}", cardinality);
        if (cardinality == -1) {
            List<PartitionKey> partitions = Lists.newArrayList();
            for (long partitionId : selectedPartitionIds) {
                partitions.add(idToPartitionKey.get(partitionId));
            }
            cardinality = hiveTable.getPartitionStatsRowCount(partitions);
            LOG.debug("get cardinality from partition stats: {}", cardinality);
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(hiveTable.getName()).append("\n");

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

        output.append(prefix).append("TABLE: ").append(hiveTable.getName()).append("\n");

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
        return result.size();
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

        if (hiveTable != null) {
            msg.hdfs_scan_node.setHive_column_names(hiveTable.getDataColumnNames());
            msg.hdfs_scan_node.setTable_name(hiveTable.getName());
        }
    }

}
