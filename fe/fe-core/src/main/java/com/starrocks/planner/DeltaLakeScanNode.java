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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.delta.DeltaUtils;
import com.starrocks.connector.delta.ExpressionConverter;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

public class DeltaLakeScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeScanNode.class);
    private final AtomicLong partitionIdGen = new AtomicLong(0L);
    private DeltaLakeTable deltaLakeTable;
    private HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private Optional<Predicate> deltaLakePredicates = Optional.empty();
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
        helper.addValue("deltaLakeTable=" + deltaLakeTable.getName());
        return helper.toString();
    }

    private void preProcessConjuncts(StructType tableSchema) {
        List<Predicate> expressions = new ArrayList<>(conjuncts.size());
        ExpressionConverter convertor = new ExpressionConverter(tableSchema);
        for (Expr expr : conjuncts) {
            try {
                // convert expr to delta expression, some expr can not convert and will throw exception
                Predicate filterExpr = convertor.convert(expr);
                if (filterExpr != null) {
                    expressions.add(filterExpr);
                }
            } catch (Exception e) {
                LOG.warn("Failed to convert expr: {}", expr.debugString(), e);
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
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "DeltaLake.getScanFiles")) {
            setupScanRangeLocationsImpl(descTbl);
        }
    }

    public void setupScanRangeLocationsImpl(DescriptorTable descTbl) throws AnalysisException {
        Metadata deltaMetadata = deltaLakeTable.getDeltaMetadata();
        DeltaUtils.checkTableFeatureSupported(((SnapshotImpl) deltaLakeTable.getDeltaSnapshot()).getProtocol(),
                deltaMetadata);

        preProcessConjuncts(deltaMetadata.getSchema());
        List<String> partitionColumnNames = deltaLakeTable.getPartitionColumnNames();
        // PartitionKey -> partition id
        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();

        Engine deltaEngine = deltaLakeTable.getDeltaEngine();
        ScanBuilder scanBuilder = deltaLakeTable.getDeltaSnapshot().getScanBuilder(deltaEngine);
        Scan scan = deltaLakePredicates.isPresent() ?
                scanBuilder.withFilter(deltaEngine, deltaLakePredicates.get()).build() :
                scanBuilder.build();

        try (CloseableIterator<FilteredColumnarBatch> scanFilesAsBatches = scan.getScanFiles(deltaEngine)) {
            while (scanFilesAsBatches.hasNext()) {
                FilteredColumnarBatch scanFileBatch = scanFilesAsBatches.next();

                try (CloseableIterator<Row> scanFileRows = scanFileBatch.getRows()) {
                    while (scanFileRows.hasNext()) {
                        Row scanFileRow = scanFileRows.next();
                        DeletionVectorDescriptor dv = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFileRow);
                        if (dv != null) {
                            ErrorReport.reportValidateException(ErrorCode.ERR_BAD_TABLE_ERROR, ErrorType.UNSUPPORTED,
                                    "Delta table feature [deletion vectors] is not supported");
                        }
                        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
                        Map<String, String> partitionValueMap = InternalScanFileUtils.getPartitionValues(scanFileRow);
                        List<String> partitionValues =
                                partitionColumnNames.stream().map(partitionValueMap::get).collect(
                                        Collectors.toList());
                        PartitionKey partitionKey =
                                PartitionUtil.createPartitionKey(partitionValues, deltaLakeTable.getPartitionColumns(),
                                        deltaLakeTable.getType());
                        addPartitionLocations(partitionKeys, partitionKey, descTbl, fileStatus, deltaMetadata);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to get delta lake scan files", e);
            throw new StarRocksConnectorException("Failed to get delta lake scan files", e);
        }

        scanNodePredicates.setSelectedPartitionIds(partitionKeys.values());
    }

    private void addPartitionLocations(Map<PartitionKey, Long> partitionKeys, PartitionKey partitionKey,
                                       DescriptorTable descTbl, FileStatus fileStatus, Metadata metadata) {
        long partitionId = -1;
        if (!partitionKeys.containsKey(partitionKey)) {
            partitionId = nextPartitionId();
            Path filePath = new Path(URLDecoder.decode(fileStatus.getPath(), StandardCharsets.UTF_8));

            DescriptorTable.ReferencedPartitionInfo referencedPartitionInfo =
                    new DescriptorTable.ReferencedPartitionInfo(partitionId, partitionKey,
                            filePath.getParent().toString());
            descTbl.addReferencedPartitions(deltaLakeTable, referencedPartitionInfo);
            partitionKeys.put(partitionKey, partitionId);
        } else {
            partitionId = partitionKeys.get(partitionKey);
        }
        addScanRangeLocations(fileStatus, partitionId, metadata);

    }

    private void addScanRangeLocations(FileStatus fileStatus, Long partitionId, Metadata metadata) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();

        hdfsScanRange.setRelative_path(new Path(fileStatus.getPath()).getName());
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(fileStatus.getSize());
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileStatus.getSize());
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

            List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    deltaLakeTable.getCatalogName(), deltaLakeTable.getDbName(), deltaLakeTable.getTableName());

            output.append(prefix).append(
                    String.format("partitions=%s/%s", scanNodePredicates.getSelectedPartitionIds().size(),
                            partitionNames.size() == 0 ? 1 : partitionNames.size()));
            output.append("\n");
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

        if (deltaLakeTable != null) {
            msg.hdfs_scan_node.setTable_name(deltaLakeTable.getName());
        }

        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        HdfsScanNode.setMinMaxConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setPartitionConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
