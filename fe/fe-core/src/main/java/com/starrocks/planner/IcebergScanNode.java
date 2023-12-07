// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.connector.PredicateUtils;
import com.starrocks.connector.iceberg.IcebergConnector;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TIcebergDeleteFile;
import com.starrocks.thrift.TIcebergFileContent;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class IcebergScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    private IcebergTable srIcebergTable; // table definition in starRocks

    private HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();

    private List<TScanRangeLocations> result = new ArrayList<>();

    // Scalar operators converted to Iceberg Expression.
    private Expression icebergPredicate = null;

    private Set<String> equalityDeleteColumns = new HashSet<>();

    private final HashMultimap<String, Long> hostToBeId = HashMultimap.create();
    private long totalBytes = 0;

    private boolean isFinalized = false;
    private CloudConfiguration cloudConfiguration = null;

    private final AtomicLong partitionIdGen = new AtomicLong(0L);

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        srIcebergTable = (IcebergTable) desc.getTable();
        setupCloudCredential();
    }

    private void setupCloudCredential() {
        String catalogName = srIcebergTable.getCatalog();
        if (catalogName == null) {
            return;
        }
        IcebergConnector connector = (IcebergConnector) GlobalStateMgr.getCurrentState().getConnectorMgr().
                getConnector(catalogName);
        if (connector != null) {
            cloudConfiguration = connector.getCloudConfiguration();
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        getAliveBackends();
    }

    private void getAliveBackends() throws UserException {
        ImmutableCollection<ComputeNode> computeNodes =
                ImmutableList.copyOf(GlobalStateMgr.getCurrentSystemInfo().getComputeNodes());

        for (ComputeNode computeNode : computeNodes) {
            if (computeNode.isAlive()) {
                hostToBeId.put(computeNode.getHost(), computeNode.getId());
            }
        }
        if (hostToBeId.isEmpty()) {
            throw new UserException("Backend not found. Check if any backend is down or not");
        }
    }

    /**
     * Extracts predicates from conjuncts that can be pushed down to Iceberg.
     * <p>
     * Since Iceberg will filter data files by metadata instead of scan data files,
     * we pushdown all predicates to Iceberg to get the minimum data files to scan.
     * Here are three cases for predicate pushdown:
     * 1.The column is not part of any Iceberg partition expression
     * 2.The column is part of all partition keys without any transformation (i.e. IDENTITY)
     * 3.The column is part of all partition keys with transformation (i.e. MONTH/DAY/HOUR)
     * We can use case 1 and 3 to filter data files, but also need to evaluate it in the
     * scan, for case 2 we don't need to evaluate it in the scan. So we evaluate all
     * predicates in the scan to keep consistency. More details about Iceberg scanning,
     * please refer: https://iceberg.apache.org/spec/#scan-planning
     */
    public void preProcessIcebergPredicate(List<ScalarOperator> operators) {
        Types.StructType schema = srIcebergTable.getIcebergTable().schema().asStruct();
        ScalarOperatorToIcebergExpr.IcebergContext icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(schema);
        icebergPredicate = new ScalarOperatorToIcebergExpr().convert(operators, icebergContext);
    }

    /**
     * Append columns need to read for equality delete files
     */
    public void appendEqualityColumns(PhysicalIcebergScanOperator node,
                                      ColumnRefFactory columnRefFactory,
                                      ExecPlan context) {
        Table referenceTable = node.getTable();
        Set<String> scanNodeColumns = node.getColRefToColumnMetaMap().values().stream()
                .map(Column::getName).collect(Collectors.toSet());
        Set<String> appendEqualityColumns = equalityDeleteColumns.stream()
                .filter(name -> !scanNodeColumns.contains(name)).collect(Collectors.toSet());
        Map<String, Column> nameToColumns = referenceTable.getFullSchema().stream()
                .collect(Collectors.toMap(Column::getName, item -> item));
        for (String eqName : appendEqualityColumns) {
            if (nameToColumns.containsKey(eqName)) {
                Column column = nameToColumns.get(eqName);
                Field field;
                TableName tableName = desc.getRef().getName();
                if (referenceTable.getFullSchema().contains(column)) {
                    field = new Field(column.getName(), column.getType(), tableName,
                            new SlotRef(tableName, column.getName(), column.getName()), true);
                } else {
                    field = new Field(column.getName(), column.getType(), tableName,
                            new SlotRef(tableName, column.getName(), column.getName()), false);
                }
                ColumnRefOperator columnRef = columnRefFactory.create(field.getName(),
                        field.getType(), column.isAllowNull());
                SlotDescriptor slotDescriptor = context.getDescTbl().addSlotDescriptor(desc, new SlotId(columnRef.getId()));
                slotDescriptor.setColumn(column);
                slotDescriptor.setIsNullable(column.isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                context.getColRefToExpr().put(columnRef, new SlotRef(columnRef.toString(), slotDescriptor));
            }
        }
    }

    @Override
    public void finalizeStats(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }

        LOG.debug("IcebergScanNode finalize. Tuple: {}", desc);
        try {
            setupScanRangeLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }

        // Min max tuple must be computed after analyzer.materializeSlots()
        PredicateUtils.computeMinMaxTupleAndConjuncts(analyzer, scanNodePredicates.getMinMaxTuple(),
                scanNodePredicates.getMinMaxConjuncts(), conjuncts);

        computeStats(analyzer);
        isFinalized = true;
    }

    private long nextPartitionId() {
        return partitionIdGen.getAndIncrement();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    public void setupScanRangeLocations() throws UserException {
        Optional<Snapshot> snapshot = IcebergUtil.getCurrentTableSnapshot(
                srIcebergTable.getIcebergTable());
        if (!snapshot.isPresent()) {
            LOG.info(String.format("Table %s has no snapshot!", srIcebergTable.getTable()));
            return;
        }
        // partition -> partitionId
        Map<StructLike, Long> partitionMap = Maps.newHashMap();

        for (CombinedScanTask combinedScanTask : IcebergUtil.getTableScan(
                srIcebergTable.getIcebergTable(), snapshot.get(), icebergPredicate).planTasks()) {
            for (FileScanTask task : combinedScanTask.files()) {
                DataFile file = task.file();
                LOG.debug("Scan with file " + file.path() + ", file record count " + file.recordCount());
                if (file.fileSizeInBytes() == 0) {
                    continue;
                }

                StructLike partition = task.file().partition();
                if (!partitionMap.containsKey(partition)) {
                    long partitionId = nextPartitionId();
                    partitionMap.put(partition, partitionId);
                }

                TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

                THdfsScanRange hdfsScanRange = new THdfsScanRange();
                hdfsScanRange.setFull_path(file.path().toString());
                hdfsScanRange.setOffset(task.start());
                hdfsScanRange.setLength(task.length());
                // For iceberg table we do not need partition id
                hdfsScanRange.setPartition_id(-1);
                hdfsScanRange.setFile_length(file.fileSizeInBytes());
                hdfsScanRange.setModification_time(0);
                hdfsScanRange.setFile_format(IcebergUtil.getHdfsFileFormat(file.format()).toThrift());

                hdfsScanRange.setDelete_files(task.deletes().stream().map(source -> {
                    TIcebergDeleteFile target = new TIcebergDeleteFile();
                    target.setFull_path(source.path().toString());
                    target.setFile_content(
                            source.content() == FileContent.EQUALITY_DELETES ? TIcebergFileContent.EQUALITY_DELETES : TIcebergFileContent.POSITION_DELETES);
                    target.setLength(source.fileSizeInBytes());

                    if (source.content() == FileContent.EQUALITY_DELETES) {
                        source.equalityFieldIds().forEach(fieldId -> {
                            equalityDeleteColumns.add(srIcebergTable.getIcebergTable().schema().findColumnName(fieldId));
                        });
                    }

                    return target;
                }).collect(Collectors.toList()));
                TScanRange scanRange = new TScanRange();
                scanRange.setHdfs_scan_range(hdfsScanRange);
                scanRangeLocations.setScan_range(scanRange);

                // TODO: get hdfs block location information for scheduling, use iceberg meta cache
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
                scanRangeLocations.addToLocations(scanRangeLocation);

                result.add(scanRangeLocations);
            }
        }

        scanNodePredicates.setSelectedPartitionIds(partitionMap.values());
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.add("icebergTable=", srIcebergTable.getName());
        return helper.toString();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(srIcebergTable.getTable()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
        }
        if (!scanNodePredicates.getMinMaxConjuncts().isEmpty()) {
            output.append(prefix).append("MIN/MAX PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getMinMaxConjuncts())).append("\n");
        }

        output.append(prefix).append(String.format("cardinality=%s", cardinality));
        output.append("\n");

        output.append(prefix).append(String.format("avgRowSize=%s", avgRowSize));
        output.append("\n");

        output.append(prefix).append(String.format("numNodes=%s", numNodes));
        output.append("\n");

        if (detailLevel == TExplainLevel.VERBOSE) {
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                Type type = slotDescriptor.getOriginType();
                if (type.isComplexType()) {
                    output.append(prefix).append(String.format("Pruned type: %d <-> [%s]\n", slotDescriptor.getId().asInt(), type));
                }
            }
        }

        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return result.size();
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        if (cardinality > 0) {
            avgRowSize = totalBytes / (float) cardinality;
            if (hasLimit()) {
                cardinality = Math.min(cardinality, limit);
            }

            numNodes = Math.min(hostToBeId.size(), result.size());
        }
        // even current node scan has no data, at least one backend will be assigned when the fragment actually execute
        numNodes = numNodes <= 0 ? 1 : numNodes;
        // when node scan has no data, cardinality should be 0 instead of a invalid value after computeStats()
        cardinality = cardinality == -1 ? 0 : cardinality;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        msg.hdfs_scan_node = tHdfsScanNode;

        String sqlPredicates = getExplainString(conjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicates);

        List<Expr> minMaxConjuncts = scanNodePredicates.getMinMaxConjuncts();
        if (!minMaxConjuncts.isEmpty()) {
            String minMaxSqlPredicate = getExplainString(minMaxConjuncts);
            for (Expr expr : minMaxConjuncts) {
                msg.hdfs_scan_node.addToMin_max_conjuncts(expr.treeToThrift());
            }
            msg.hdfs_scan_node.setMin_max_tuple_id(scanNodePredicates.getMinMaxTuple().getId().asInt());
            msg.hdfs_scan_node.setMin_max_sql_predicates(minMaxSqlPredicate);
        }

        if (srIcebergTable != null) {
            msg.hdfs_scan_node.setTable_name(srIcebergTable.getTable());
        }

        if (cloudConfiguration != null) {
            TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
            cloudConfiguration.toThrift(tCloudConfiguration);
            msg.hdfs_scan_node.setCloud_configuration(tCloudConfiguration);
        }
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
    }
}
