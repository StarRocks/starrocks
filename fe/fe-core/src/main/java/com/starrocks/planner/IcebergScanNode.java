// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.starrocks.analysis.*;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.external.PredicateUtils;
import com.starrocks.external.iceberg.IcebergUtil;
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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static com.starrocks.external.iceberg.ExpressionConverter.toIcebergExpression;

public class IcebergScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    private IcebergTable srIcebergTable; // table definition in starRocks

    private List<TScanRangeLocations> result = new ArrayList<>();

    // Exprs in icebergConjuncts converted to UnboundPredicate.
    private final List<UnboundPredicate> icebergPredicates = new ArrayList<>();

    // List of conjuncts for min/max values that are used to skip data when scanning Parquet files.
    private final List<Expr> minMaxConjuncts = new ArrayList<>();
    private TupleDescriptor minMaxTuple;

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
        for (Backend be : Catalog.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAlive()) {
                hostToBeId.put(be.getHost(), be.getId());
            }
        }
        if (hostToBeId.isEmpty()) {
            throw new UserException("Backend not found. Check if any backend is down or not");
        }
    }

    /**
     * Extracts predicates from conjuncts that can be pushed down to Iceberg.
     *
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
        for (Expr expr : conjuncts) {
            UnboundPredicate unboundPredicate = toIcebergExpression(expr);
            if (unboundPredicate != null) {
                icebergPredicates.add(unboundPredicate);
            }
        }
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
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
        DescriptorTable descTbl = analyzer.getDescTbl();
        minMaxTuple = descTbl.createTupleDescriptor();
        PredicateUtils.computeMinMaxTupleAndConjuncts(analyzer, minMaxTuple, minMaxConjuncts, conjuncts);

        computeStats(analyzer);
        isFinalized = true;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    public void getScanRangeLocations() throws UserException {
        Optional<Snapshot> snapshot = IcebergUtil.getCurrentTableSnapshot(
                srIcebergTable.getIcebergTable(), true);
        if (!snapshot.isPresent()) {
            LOG.info(String.format("Table %s has no snapshot!", srIcebergTable.getTable()));
            return;
        }
        preProcessConjuncts();
        for (FileScanTask task : IcebergUtil.getTableScan(
                srIcebergTable.getIcebergTable(), snapshot.get(),
                icebergPredicates, true).planFiles()) {
            DataFile file = task.file();
            LOG.debug("Scan with file " + file.path() + ", file record count " + file.recordCount());

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

        output.append(prefix).append("TABLE: ").append(srIcebergTable.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
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
    protected String getNodeVerboseExplain(String prefix) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(srIcebergTable.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
        }

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

        if (!minMaxConjuncts.isEmpty()) {
            for (Expr expr : minMaxConjuncts) {
                msg.hdfs_scan_node.addToMin_max_conjuncts(expr.treeToThrift());
            }
            msg.hdfs_scan_node.setMin_max_tuple_id(minMaxTuple.getId().asInt());
        }
    }

    public List<Expr> getMinMaxConjuncts() {
        return minMaxConjuncts;
    }

    public void setMinMaxTuple(TupleDescriptor tuple) {
        minMaxTuple = tuple;
    }
}
