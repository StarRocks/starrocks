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
import com.google.common.collect.Maps;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.delta.DeltaLakeConnector;
import com.starrocks.connector.delta.DeltaUtils;
import com.starrocks.connector.delta.ExpressionConverter;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.expressions.And;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.thrift.TExplainLevel.VERBOSE;

public class DeltaLakeScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeScanNode.class);
    private final AtomicLong partitionIdGen = new AtomicLong(0L);
    private DeltaLakeTable deltaLakeTable;
    private HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private Optional<Expression> deltaLakePredicates = Optional.empty();
    private CloudConfiguration cloudConfiguration = null;

    public DeltaLakeScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        deltaLakeTable = (DeltaLakeTable) desc.getTable();
        setupCloudCredential();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    public DeltaLakeTable getDeltaLakeTable() {
        return deltaLakeTable;
    }

    private void setupCloudCredential() {
        String catalog = deltaLakeTable.getCatalogName();
        if (catalog == null) {
            return;
        }
        DeltaLakeConnector connector = (DeltaLakeConnector) GlobalStateMgr.getCurrentState().getConnectorMgr().
                getConnector(catalog);
        if (connector != null) {
            cloudConfiguration = connector.getCloudConfiguration();
        }
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("deltaLakeTable=" + deltaLakeTable.getName());
        return helper.toString();
    }

    private void preProcessConjuncts(StructType tableSchema) {
        List<Expression> expressions = new ArrayList<>(conjuncts.size());
        ExpressionConverter convertor = new ExpressionConverter(tableSchema);
        for (Expr expr : conjuncts) {
            Expression filterExpr = convertor.convert(expr);
            if (filterExpr != null) {
                try {
                    expressions.add(filterExpr);
                } catch (Exception e) {
                    LOG.debug("binding to the table schema failed, cannot be pushed down expression: {}", expr.toSql());
                }
            }
        }

        LOG.debug("Number of predicates pushed down / Total number of predicates: {}/{}",
                expressions.size(), conjuncts.size());
        deltaLakePredicates = expressions.stream().reduce(And::new);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    public void setupScanRangeLocations(DescriptorTable descTbl) throws AnalysisException {
        DeltaLog deltaLog = deltaLakeTable.getDeltaLog();
        if (!deltaLog.tableExists()) {
            return;
        }
        // use current snapshot now
        Snapshot snapshot = deltaLog.snapshot();
        preProcessConjuncts(snapshot.getMetadata().getSchema());
        List<String> partitionColumnNames = snapshot.getMetadata().getPartitionColumns();
        // PartitionKey -> partition id
        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();

        DeltaScan scan = deltaLakePredicates.isPresent() ? snapshot.scan(deltaLakePredicates.get()) : snapshot.scan();

        for (CloseableIterator<AddFile> it = scan.getFiles(); it.hasNext(); ) {
            AddFile file = it.next();
            Map<String, String> partitionValueMap = file.getPartitionValues();
            List<String> partitionValues = partitionColumnNames.stream().map(partitionValueMap::get).collect(
                    Collectors.toList());

            Metadata metadata = snapshot.getMetadata();
            PartitionKey partitionKey =
                    PartitionUtil.createPartitionKey(partitionValues, deltaLakeTable.getPartitionColumns(),
                            deltaLakeTable.getType());
            addPartitionLocations(partitionKeys, partitionKey, descTbl, file, metadata);
        }

        scanNodePredicates.setSelectedPartitionIds(partitionKeys.values());
    }

    private void addPartitionLocations(Map<PartitionKey, Long> partitionKeys, PartitionKey partitionKey,
                                       DescriptorTable descTbl, AddFile file, Metadata metadata) {
        long partitionId = -1;
        if (!partitionKeys.containsKey(partitionKey)) {
            partitionId = nextPartitionId();
            String tableLocation = deltaLakeTable.getTableLocation();
            Path filePath = new Path(tableLocation, file.getPath());

            DescriptorTable.ReferencedPartitionInfo referencedPartitionInfo =
                    new DescriptorTable.ReferencedPartitionInfo(partitionId, partitionKey,
                            filePath.getParent().toString());
            descTbl.addReferencedPartitions(deltaLakeTable, referencedPartitionInfo);
            partitionKeys.put(partitionKey, partitionId);
        } else {
            partitionId = partitionKeys.get(partitionKey);
        }
        addScanRangeLocations(file, partitionId, metadata);

    }

    private void addScanRangeLocations(AddFile file, Long partitionId, Metadata metadata) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();

        hdfsScanRange.setRelative_path(new Path(file.getPath()).getName());
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(file.getSize());
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(file.getSize());
        hdfsScanRange.setFile_format(DeltaUtils.getRemoteFileFormat(metadata.getFormat().getProvider()).toThrift());
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);

        scanRangeLocationsList.add(scanRangeLocations);
    }

    private long nextPartitionId() {
        return partitionIdGen.getAndIncrement();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(deltaLakeTable.getName()).append("\n");

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

        List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                deltaLakeTable.getCatalogName(), deltaLakeTable.getDbName(), deltaLakeTable.getTableName());

        output.append(prefix).append(
                String.format("partitions=%s/%s", scanNodePredicates.getSelectedPartitionIds().size(),
                        partitionNames.size() == 0 ? 1 : partitionNames.size()));
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
        return scanRangeLocationsList.size();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        msg.hdfs_scan_node = tHdfsScanNode;

        List<Expr> minMaxConjuncts = scanNodePredicates.getMinMaxConjuncts();
        if (!minMaxConjuncts.isEmpty()) {
            String minMaxSqlPredicate = getExplainString(minMaxConjuncts);
            for (Expr expr : minMaxConjuncts) {
                msg.hdfs_scan_node.addToMin_max_conjuncts(expr.treeToThrift());
            }
            msg.hdfs_scan_node.setMin_max_tuple_id(scanNodePredicates.getMinMaxTuple().getId().asInt());
            msg.hdfs_scan_node.setMin_max_sql_predicates(minMaxSqlPredicate);
        }

        if (deltaLakeTable != null) {
            msg.hdfs_scan_node.setTable_name(deltaLakeTable.getName());
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
