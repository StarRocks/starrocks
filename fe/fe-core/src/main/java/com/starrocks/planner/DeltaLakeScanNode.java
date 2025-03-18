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
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.RemoteFilesSampleStrategy;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.delta.DeltaConnectorScanRangeSource;
import com.starrocks.connector.delta.DeltaUtils;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

public class DeltaLakeScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeScanNode.class);
    private final DeltaLakeTable deltaLakeTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private CloudConfiguration cloudConfiguration = null;
    private DeltaConnectorScanRangeSource scanRangeSource = null;
    private int selectedPartitionCount = -1;

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

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        if (maxScanRangeLength == 0) {
            return scanRangeSource.getAllOutputs();
        }
        return scanRangeSource.getOutputs((int) maxScanRangeLength);
    }

    @Override
    public boolean hasMoreScanRanges() {
        return scanRangeSource.hasMoreOutput();
    }

    public void setupScanRangeSource(ScalarOperator predicate, List<String> fieldNames, boolean enableIncrementalScanRanges)
            throws StarRocksException {
        SnapshotImpl snapshot = (SnapshotImpl) deltaLakeTable.getDeltaSnapshot();
        DeltaUtils.checkProtocolAndMetadata(snapshot.getProtocol(), snapshot.getMetadata());
        Engine engine = deltaLakeTable.getDeltaEngine();
        long snapshotId = snapshot.getVersion(engine);

        GetRemoteFilesParams params =
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TableVersionRange.withEnd(Optional.of(snapshotId)))
                        .setPredicate(predicate).setFieldNames(fieldNames).build();
        RemoteFileInfoSource remoteFileInfoSource = null;
        if (enableIncrementalScanRanges) {
            remoteFileInfoSource = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFilesAsync(deltaLakeTable, params);
        } else {
            remoteFileInfoSource = new RemoteFileInfoDefaultSource(
                    GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(deltaLakeTable, params));
        }
        scanRangeSource = new DeltaConnectorScanRangeSource(deltaLakeTable, remoteFileInfoSource);
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
                    deltaLakeTable.getCatalogName(), deltaLakeTable.getCatalogDBName(), deltaLakeTable.getCatalogTableName(),
                    ConnectorMetadatRequestContext.DEFAULT);

            if (selectedPartitionCount == -1) {
                if (scanRangeSource != null) {
                    // we have to consume all scan ranges to know how many partition been selected.
                    while (scanRangeSource.hasMoreOutput()) {
                        scanRangeSource.getOutputs(1000);
                    }
                    selectedPartitionCount = scanRangeSource.selectedPartitionCount();
                } else {
                    selectedPartitionCount = 0;
                }
            }

            output.append(prefix).append(
                    String.format("partitions=%s/%s", selectedPartitionCount,
                            partitionNames.size() == 0 ? 1 : partitionNames.size()));
            output.append("\n");
        }

        return output.toString();
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

    @Override
    public void setScanSampleStrategy(RemoteFilesSampleStrategy strategy) {
        scanRangeSource.setSampleStrategy(strategy);
    }
}
