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
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.UserException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.delta.DeltaLakeRemoteFileDesc;
import com.starrocks.connector.delta.DeltaUtils;
import com.starrocks.connector.delta.FileScanTask;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
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
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

public class DeltaLakeScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeScanNode.class);
    private final AtomicLong partitionIdGen = new AtomicLong(0L);
    private final DeltaLakeTable deltaLakeTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private CloudConfiguration cloudConfiguration = null;
    private ScalarOperator predicate = null;

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

    public void preProcessDeltaLakePredicate(ScalarOperator predicate) {
        this.predicate = predicate;
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

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    public void setupScanRangeLocations(DescriptorTable descTbl, List<String> fieldNames) throws UserException {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "DeltaLake.getScanFiles")) {
            setupScanRangeLocationsImpl(descTbl, fieldNames);
        }
    }

    public void setupScanRangeLocationsImpl(DescriptorTable descTbl, List<String> fieldNames) throws UserException {
        Metadata deltaMetadata = deltaLakeTable.getDeltaMetadata();
        SnapshotImpl snapshot = (SnapshotImpl) deltaLakeTable.getDeltaSnapshot();

        String catalogName = deltaLakeTable.getCatalogName();
        DeltaUtils.checkProtocolAndMetadata(snapshot.getProtocol(), snapshot.getMetadata());

        Engine engine = deltaLakeTable.getDeltaEngine();
        long snapshotId = snapshot.getVersion(engine);
        String dbName = deltaLakeTable.getDbName();
        String tableName = deltaLakeTable.getTableName();
        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();

        List<RemoteFileInfo> splits = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                catalogName, deltaLakeTable, null, snapshotId, predicate, fieldNames, -1);
        if (splits.isEmpty()) {
            LOG.warn("There is no scan tasks after planFiles on {}.{} and predicate: [{}]", dbName, tableName, predicate);
            return;
        }

        DeltaLakeRemoteFileDesc remoteFileDesc = (DeltaLakeRemoteFileDesc) splits.get(0).getFiles().get(0);
        if (remoteFileDesc == null) {
            LOG.warn("There is no scan tasks after planFiles on {}.{} and predicate: [{}]", dbName, tableName, predicate);
            return;
        }

        List<FileScanTask> splitsInfo = remoteFileDesc.getDeltaLakeScanTasks();
        for (FileScanTask split : splitsInfo) {
            List<String> partitionValues = new ArrayList<>();
            deltaLakeTable.getPartitionColumnNames().forEach(column -> {
                partitionValues.add(split.getPartitionValues().get(column));
            });
            PartitionKey partitionKey = PartitionUtil.createPartitionKey(partitionValues,
                    deltaLakeTable.getPartitionColumns(), deltaLakeTable);
            addPartitionLocations(partitionKeys, partitionKey, descTbl, split.getFileStatus(), deltaMetadata);
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
            HdfsScanNode.appendDataCacheOptionsInExplain(output, prefix, dataCacheOptions);

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
        HdfsScanNode.setDataCacheOptionsToThrift(tHdfsScanNode, dataCacheOptions);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
