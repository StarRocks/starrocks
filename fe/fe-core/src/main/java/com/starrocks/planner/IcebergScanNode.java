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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.RemoteFilesSampleStrategy;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.iceberg.IcebergConnectorScanRangeSource;
import com.starrocks.connector.iceberg.IcebergGetRemoteFilesParams;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergRemoteSourceTrigger;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.connector.iceberg.QueueIcebergRemoteFileInfoSource;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class IcebergScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    protected final IcebergTable icebergTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private ScalarOperator icebergJobPlanningPredicate = null;
    private CloudConfiguration cloudConfiguration = null;
    protected Optional<Long> snapshotId;
    private IcebergConnectorScanRangeSource scanRangeSource = null;
    private final IcebergTableMORParams tableFullMORParams;
    private final IcebergMORParams morParams;
    private int selectedPartitionCount = -1;

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                           IcebergTableMORParams tableFullMORParams, IcebergMORParams morParams) {
        super(id, desc, planNodeName);
        this.icebergTable = (IcebergTable) desc.getTable();
        this.tableFullMORParams = tableFullMORParams;
        this.morParams = morParams;
        setupCloudCredential();
    }

    @Override
    public boolean hasMoreScanRanges() {
        if (scanRangeSource == null) {
            return false;
        }

        return scanRangeSource.hasMoreOutput();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        if (snapshotId.isEmpty() || scanRangeSource == null) {
            return List.of();
        }

        if (maxScanRangeLength == 0) {
            return scanRangeSource.getAllOutputs();
        }
        return scanRangeSource.getOutputs((int) maxScanRangeLength);
    }

    public void setupScanRangeLocations(boolean enableIncrementalScanRanges) throws StarRocksException {
        Preconditions.checkNotNull(snapshotId, "snapshot id is null");
        if (snapshotId.isEmpty()) {
            LOG.warn(String.format("Table %s has no snapshot!", icebergTable.getCatalogTableName()));
            return;
        }

        GetRemoteFilesParams params =
                IcebergGetRemoteFilesParams.newBuilder()
                        .setAllParams(tableFullMORParams)
                        .setParams(morParams)
                        .setTableVersionRange(TableVersionRange.withEnd(snapshotId))
                        .setPredicate(icebergJobPlanningPredicate)
                        .build();

        RemoteFileInfoSource remoteFileInfoSource;
        if (enableIncrementalScanRanges) {
            remoteFileInfoSource = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFilesAsync(icebergTable, params);
        } else {
            List<RemoteFileInfo> splits = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(icebergTable, params);
            if (splits.isEmpty()) {
                LOG.warn("There is no scan tasks after planFies on {}.{} and predicate: [{}]",
                        icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), icebergJobPlanningPredicate);
                return;
            }
            remoteFileInfoSource = new RemoteFileInfoDefaultSource(splits);
            if (morParams != IcebergMORParams.EMPTY) {
                boolean needToCheckEqualityIds = tableFullMORParams.size() != 3;
                IcebergRemoteSourceTrigger trigger = new IcebergRemoteSourceTrigger(
                        remoteFileInfoSource, morParams, needToCheckEqualityIds);
                Deque<RemoteFileInfo> remoteFileInfoDeque = trigger.getQueue(morParams);
                remoteFileInfoSource = new QueueIcebergRemoteFileInfoSource(trigger, remoteFileInfoDeque);
            }
        }

        scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable, remoteFileInfoSource, morParams, desc);
    }

    private void setupCloudCredential() {
        String catalogName = icebergTable.getCatalogName();
        if (catalogName == null) {
            return;
        }

        // Hard coding here
        // Try to get tabular signed temporary credential
        CloudConfiguration vendedCredentialsCloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForVendedCredentials(icebergTable.getNativeTable().io().properties());
        if (vendedCredentialsCloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            // If we get CloudConfiguration succeed from iceberg FileIO's properties, we just using it.
            cloudConfiguration = vendedCredentialsCloudConfiguration;
        } else {
            CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
            Preconditions.checkState(connector != null,
                    String.format("connector of catalog %s should not be null", catalogName));
            cloudConfiguration = connector.getMetadata().getCloudConfiguration();
            Preconditions.checkState(cloudConfiguration != null,
                    String.format("cloudConfiguration of catalog %s should not be null", catalogName));
        }
    }

    public void setCloudConfiguration(CloudConfiguration cloudConfiguration) {
        this.cloudConfiguration = cloudConfiguration;
    }

    public void preProcessIcebergPredicate(ScalarOperator predicate) {
        this.icebergJobPlanningPredicate = predicate;
    }

    // for unit tests
    public ScalarOperator getIcebergJobPlanningPredicate() {
        return icebergJobPlanningPredicate;
    }

    // for unit tests
    public List<Integer> getExtendedColumnSlotIds() {
        return scanRangeSource.getExtendedColumnSlotIds();
    }

    public Set<String> getSeenEqualityDeleteFiles() {
        return scanRangeSource.getSeenEqDeleteFiles();
    }

    // for unit tests
    public IcebergMORParams getMORParams() {
        return morParams;
    }

    // for unit tests
    public IcebergTableMORParams getTableFullMORParams() {
        return tableFullMORParams;
    }

    public void setSnapshotId(Optional<Long> snapshotId) {
        this.snapshotId = snapshotId;
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.add("icebergTable=", icebergTable.getName());
        return helper.toString();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ")
                .append(icebergTable.getCatalogDBName())
                .append(".")
                .append(icebergTable.getName())
                .append("\n");

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

        if (detailLevel == TExplainLevel.VERBOSE) {
            HdfsScanNode.appendDataCacheOptionsInExplain(output, prefix, dataCacheOptions);

            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                Type type = slotDescriptor.getOriginType();
                if (type.isComplexType()) {
                    output.append(prefix)
                            .append(String.format("Pruned type: %d <-> [%s]\n", slotDescriptor.getId().asInt(), type));
                }
            }
        }

        if (detailLevel == TExplainLevel.VERBOSE && !isResourceMappingCatalog(icebergTable.getCatalogName())) {
            ConnectorMetadatRequestContext requestContext = new ConnectorMetadatRequestContext();
            requestContext.setTableVersionRange(TableVersionRange.withEnd(snapshotId));
            List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    icebergTable.getCatalogName(), icebergTable.getCatalogDBName(),
                    icebergTable.getCatalogTableName(), requestContext);

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
                            partitionNames.isEmpty() ? 1 : partitionNames.size()));
            output.append("\n");
        }

        if (morParams.getScanTaskType() == IcebergMORParams.ScanTaskType.EQ_DELETE) {
            List<String> identifierColumnNames = morParams.getEqualityIds().stream()
                    .map(id -> icebergTable.getNativeTable().schema().findColumnName(id))
                    .collect(Collectors.toList());
            output.append(prefix).append("Iceberg identifier columns: ").append(identifierColumnNames);
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

        String sqlPredicates = getExplainString(conjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicates);
        if (scanRangeSource != null) {
            msg.hdfs_scan_node.setExtended_slot_ids(scanRangeSource.getExtendedColumnSlotIds());
        }
        msg.hdfs_scan_node.setTable_name(icebergTable.getName());
        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        HdfsScanNode.setMinMaxConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setDataCacheOptionsToThrift(tHdfsScanNode, dataCacheOptions);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    protected boolean supportTopNRuntimeFilter() {
        return true;
    }

    @Override
    public void setScanSampleStrategy(RemoteFilesSampleStrategy strategy) {
        scanRangeSource.setSampleStrategy(strategy);
    }
}
