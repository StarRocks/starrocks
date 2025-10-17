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
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.FlussTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.fluss.FlussRemoteFileDesc;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.paimon.source.PaimonLakeSource;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.fluss.FlussMetadata.RT_TABLE_SPLITTER;
import static com.starrocks.thrift.TExplainLevel.VERBOSE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class FlussScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(FlussScanNode.class);
    private final AtomicLong partitionIdGen = new AtomicLong(0L);
    private final FlussTable flussTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private CloudConfiguration cloudConfiguration = null;

    public FlussScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        this.flussTable = (FlussTable) desc.getTable();
        setupCloudCredential();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    public FlussTable getFlussTable() {
        return flussTable;
    }

    private void setupCloudCredential() {
        String catalog = flussTable.getCatalogName();
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
        helper.addValue("flussTable=" + flussTable.getName());
        return helper.toString();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    public void setupScanRangeLocations(TupleDescriptor tupleDescriptor, ScalarOperator predicate, long limit) throws IOException {
        List<String> fieldNames =
                tupleDescriptor.getSlots().stream().map(s -> s.getColumn().getName()).collect(Collectors.toList());
        List<RemoteFileInfo> fileInfos;
        try (Timer ignored = Tracers.watchScope(EXTERNAL, flussTable.getTableName() + ".getFlussRemoteFileInfos")) {
            fileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                    flussTable.getCatalogName(), flussTable, null, -1, predicate, fieldNames, limit);
        }

        FlussRemoteFileDesc remoteFileDesc = (FlussRemoteFileDesc) fileInfos.get(0).getFiles().get(0);
        List<SourceSplitBase> splits = remoteFileDesc.getFlussSplitsInfo();

        if (splits.isEmpty()) {
            LOG.warn("There is no fluss splits on {}.{} and predicate: [{}]",
                    flussTable.getDbName(), flussTable.getTableName(), predicate);
            return;
        }

        Map<String, Long> selectedPartitions = Maps.newHashMap();
        for (int i = 0; i < splits.size(); i++) {
            SourceSplitBase split = splits.get(i);
            if (split instanceof HybridSnapshotLogSplit) {
                boolean lakeEnabled = this.flussTable.getTableInfo().getTableConfig().isDataLakeEnabled();
                throw new IOException("HybridSnapshotLogSplit is not supported, table detail: " +
                        flussTable.getTableInfo() + "isDataLakeEnabled: " + lakeEnabled);
            }
            addSplitScanRangeLocations(split, i);
            String partitionValue = split.getPartitionName();
            if (!selectedPartitions.containsKey(partitionValue)) {
                selectedPartitions.put(partitionValue, nextPartitionId());
            }
        }
        scanNodePredicates.setSelectedPartitionIds(selectedPartitions.values());
    }

    public void addSplitScanRangeLocations(SourceSplitBase split, int loop) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setUse_fluss_jni_reader(true);
        hdfsScanRange.setFluss_split_info(encodeSplitToString(split));
        hdfsScanRange.setFile_length(1);
        hdfsScanRange.setLength(1);
        hdfsScanRange.setFile_format(THdfsFileFormat.UNKNOWN);
        hdfsScanRange.setRelative_path(String.valueOf(split.hashCode()));

        if (flussTable.getTableNamePrefix().equals(RT_TABLE_SPLITTER) && loop == 0) {
            hdfsScanRange.setRecord_count(this.flussTable.getRTCount());
            hdfsScanRange.setIs_first_split(true);
        }

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

        output.append(prefix).append("TABLE: ").append(flussTable.getName()).append("\n");

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

        // TODO: support it in verbose
        if (detailLevel != VERBOSE) {
            output.append(prefix).append(String.format("cardinality=%s", cardinality));
            output.append("\n");
        }

        output.append(prefix).append(String.format("avgRowSize=%s\n", avgRowSize));

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
                    flussTable.getCatalogName(), flussTable.getDbName(), flussTable.getTableName());
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

        String sqlPredicates = getExplainString(conjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicates);
        msg.hdfs_scan_node.setTable_name(flussTable.getName());

        LOG.debug(cloudConfiguration.toConfString());
        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, this.cloudConfiguration);
        HdfsScanNode.setNonEvalPartitionConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setMinMaxConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setNonPartitionConjunctsToThrift(msg, this, this.getScanNodePredicates());
        HdfsScanNode.setDataCacheOptionsToThrift(tHdfsScanNode, dataCacheOptions);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    private static final Base64.Encoder BASE64_ENCODER =
            Base64.getUrlEncoder().withoutPadding();

    public static String encodeSplitToString(SourceSplitBase t) {
        try {
            LakeSource lakeSource = new PaimonLakeSource(null, null);
            SourceSplitSerializer serializer = new SourceSplitSerializer(lakeSource);
            byte[] bytes = serializer.serialize(t);
            return new String(BASE64_ENCODER.encode(bytes), UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
