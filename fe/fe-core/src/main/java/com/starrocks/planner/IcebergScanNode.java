// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.external.PredicateUtils;
import com.starrocks.external.iceberg.ExpressionConverter;
import com.starrocks.external.iceberg.IcebergUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.fs.hdfs.HdfsFsManager.FS_S3A_ACCESS_KEY;
import static com.starrocks.fs.hdfs.HdfsFsManager.FS_S3A_CONNECTION_SSL_ENABLED;
import static com.starrocks.fs.hdfs.HdfsFsManager.FS_S3A_ENDPOINT;
import static com.starrocks.fs.hdfs.HdfsFsManager.FS_S3A_SECRET_KEY;

public class IcebergScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    private IcebergTable srIcebergTable; // table definition in starRocks

    private HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();

    private List<TScanRangeLocations> result = new ArrayList<>();
    private final THdfsProperties tHdfsProperties = new THdfsProperties();

    // Exprs in icebergConjuncts converted to Iceberg Expression.
    private List<Expression> icebergPredicates = null;

    private final HashMultimap<String, Long> hostToBeId = HashMultimap.create();
    private long totalBytes = 0;

    private boolean isFinalized = false;

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        srIcebergTable = (IcebergTable) desc.getTable();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        getAliveBackends();
        preProcessConjuncts();
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

    public void setTHdfsProperties(Map<String, String> properties) {
        if (properties.containsKey(FS_S3A_ACCESS_KEY)) {
            this.tHdfsProperties.setAccess_key(properties.get(FS_S3A_ACCESS_KEY));
        }
        if (properties.containsKey(FS_S3A_SECRET_KEY)) {
            this.tHdfsProperties.setSecret_key(properties.get(FS_S3A_SECRET_KEY));
        }
        if (properties.containsKey(FS_S3A_ENDPOINT)) {
            this.tHdfsProperties.setEnd_point(properties.get(FS_S3A_ENDPOINT));
        }
        if (properties.containsKey(FS_S3A_CONNECTION_SSL_ENABLED)) {
            this.tHdfsProperties.setSsl_enable(Boolean.parseBoolean(properties.get(FS_S3A_CONNECTION_SSL_ENABLED)));
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
    private void preProcessConjuncts() {
        List<Expression> expressions = new ArrayList<>(conjuncts.size());
        ExpressionConverter convertor = new ExpressionConverter();
        for (Expr expr : conjuncts) {
            Expression filterExpr = convertor.convert(expr);
            if (filterExpr != null) {
                try {
                    Binder.bind(srIcebergTable.getIcebergTable().schema().asStruct(), filterExpr, false);
                    expressions.add(filterExpr);
                } catch (ValidationException e) {
                    LOG.debug("binding to the table schema failed, cannot be pushed down expression: {}",
                            expr.toSql());
                }
            }
        }
        LOG.debug("Number of predicates pushed down / Total number of predicates: {}/{}",
                expressions.size(), conjuncts.size());
        icebergPredicates = expressions;
    }

    @Override
    public void finalizeStats(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }

        LOG.debug("IcebergScanNode finalize. Tuple: {}", desc);
        try {
            getScanRangeLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }

        // Min max tuple must be computed after analyzer.materializeSlots()
        PredicateUtils.computeMinMaxTupleAndConjuncts(analyzer, scanNodePredicates.getMinMaxTuple(),
                scanNodePredicates.getMinMaxConjuncts(), conjuncts);

        computeStats(analyzer);
        isFinalized = true;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    public void getScanRangeLocations() throws UserException {
        Optional<Snapshot> snapshot = IcebergUtil.getCurrentTableSnapshot(
                srIcebergTable.getIcebergTable());
        if (!snapshot.isPresent()) {
            LOG.info(String.format("Table %s has no snapshot!", srIcebergTable.getTable()));
            return;
        }
        preProcessConjuncts();
        for (CombinedScanTask combinedScanTask : IcebergUtil.getTableScan(
                srIcebergTable.getIcebergTable(), snapshot.get(),
                icebergPredicates).planTasks()) {
            for (FileScanTask task : combinedScanTask.files()) {
                DataFile file = task.file();
                LOG.debug("Scan with file " + file.path() + ", file record count " + file.recordCount());
                if (file.fileSizeInBytes() == 0) {
                    continue;
                }

                TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

                THdfsScanRange hdfsScanRange = new THdfsScanRange();
                hdfsScanRange.setFull_path(file.path().toString());
                hdfsScanRange.setOffset(task.start());
                hdfsScanRange.setLength(task.length());
                // For iceberg table we do not need partition id
                hdfsScanRange.setPartition_id(-1);
                hdfsScanRange.setFile_length(file.fileSizeInBytes());
                hdfsScanRange.setFile_format(IcebergUtil.getHdfsFileFormat(file.format()).toThrift());
                TScanRange scanRange = new TScanRange();
                scanRange.setHdfs_scan_range(hdfsScanRange);
                scanRangeLocations.setScan_range(scanRange);

                // TODO: get hdfs block location information for scheduling, use iceberg meta cache
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
                scanRangeLocations.addToLocations(scanRangeLocation);

                result.add(scanRangeLocations);
            }
        }
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
        msg.hdfs_scan_node.setHdfs_properties(tHdfsProperties);
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
    }
}
