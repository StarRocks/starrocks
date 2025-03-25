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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.paimon.PaimonRemoteFileDesc;
import com.starrocks.connector.paimon.PaimonSplitsInfo;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPaimonDeletionFile;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.InstantiationUtil;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.thrift.TExplainLevel.VERBOSE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PaimonScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(PaimonScanNode.class);
    private final AtomicLong partitionIdGen = new AtomicLong(0L);
    private final PaimonTable paimonTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private CloudConfiguration cloudConfiguration = null;

    public PaimonScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        this.paimonTable = (PaimonTable) desc.getTable();
        setupCloudCredential();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    public PaimonTable getPaimonTable() {
        return paimonTable;
    }

    private void setupCloudCredential() {
        String catalog = paimonTable.getCatalogName();
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
        helper.addValue("paimonTable=" + paimonTable.getName());
        return helper.toString();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    public long getEstimatedLength(long rowCount, TupleDescriptor tupleDescriptor) {
        List<Column> dataColumns = tupleDescriptor.getSlots().stream().map(s -> s.getColumn())
                .collect(Collectors.toList());
        long rowSize = dataColumns.stream().mapToInt(column -> column.getType().getTypeSize()).sum();

        return rowCount * rowSize;
    }

    public void setupScanRangeLocations(TupleDescriptor tupleDescriptor, ScalarOperator predicate, long limit) {
        List<String> fieldNames =
                tupleDescriptor.getSlots().stream().map(s -> s.getColumn().getName()).collect(Collectors.toList());
        GetRemoteFilesParams params =
                GetRemoteFilesParams.newBuilder().setPredicate(predicate).setFieldNames(fieldNames).setLimit(limit)
                        .build();
        List<RemoteFileInfo> fileInfos;
        try (Timer ignored = Tracers.watchScope(EXTERNAL, paimonTable.getCatalogTableName() + ".getPaimonRemoteFileInfos")) {
            fileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(paimonTable, params);
        }

        PaimonRemoteFileDesc remoteFileDesc = (PaimonRemoteFileDesc) fileInfos.get(0).getFiles().get(0);
        PaimonSplitsInfo splitsInfo = remoteFileDesc.getPaimonSplitsInfo();
        String predicateInfo = encodeObjectToString(splitsInfo.getPredicate());
        List<Split> splits = splitsInfo.getPaimonSplits();

        if (splits.isEmpty()) {
            LOG.warn("There is no paimon splits on {}.{} and predicate: [{}]",
                    paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName(), predicate);
            return;
        }

        boolean forceJNIReader = ConnectContext.get().getSessionVariable().getPaimonForceJNIReader();
        Map<BinaryRow, Long> selectedPartitions = Maps.newHashMap();
        for (Split split : splits) {
            if (split instanceof DataSplit) {
                DataSplit dataSplit = (DataSplit) split;
                Optional<List<RawFile>> optionalRawFiles = dataSplit.convertToRawFiles();
                if (!forceJNIReader && optionalRawFiles.isPresent()) {
                    List<RawFile> rawFiles = optionalRawFiles.get();
                    boolean validFormat = rawFiles.stream().allMatch(p -> fromType(p.format()) != THdfsFileFormat.UNKNOWN);
                    if (validFormat) {
                        Optional<List<DeletionFile>> deletionFiles = dataSplit.deletionFiles();
                        for (int i = 0; i < rawFiles.size(); i++) {
                            if (deletionFiles.isPresent()) {
                                splitRawFileScanRangeLocations(rawFiles.get(i), deletionFiles.get().get(i));
                            } else {
                                splitRawFileScanRangeLocations(rawFiles.get(i), null);
                            }
                        }
                    } else {
                        long totalFileLength = getTotalFileLength(dataSplit);
                        addSplitScanRangeLocations(dataSplit, predicateInfo, totalFileLength);
                    }
                } else {
                    long totalFileLength = getTotalFileLength(dataSplit);
                    addSplitScanRangeLocations(dataSplit, predicateInfo, totalFileLength);
                }
                BinaryRow partitionValue = dataSplit.partition();
                if (!selectedPartitions.containsKey(partitionValue)) {
                    selectedPartitions.put(partitionValue, nextPartitionId());
                }
            } else {
                // paimon system table
                long length = getEstimatedLength(split.rowCount(), tupleDescriptor);
                addSplitScanRangeLocations(split, predicateInfo, length);
            }

        }
        scanNodePredicates.setSelectedPartitionIds(selectedPartitions.values());
        traceJniMetrics();
        traceDeletionVectorMetrics();
    }

    private void traceJniMetrics() {
        int totalReaderCount = 0, jniReaderCount = 0;
        long totalReaderLength = 0, jniReaderLength = 0;

        for (TScanRangeLocations rangeLocation : scanRangeLocationsList) {
            THdfsScanRange hdfsScanRange = rangeLocation.getScan_range().getHdfs_scan_range();
            if (hdfsScanRange.use_paimon_jni_reader) {
                jniReaderCount++;
                jniReaderLength += hdfsScanRange.length;
            }
            totalReaderCount++;
            totalReaderLength += hdfsScanRange.length;
        }

        String prefix = "Paimon.metadata.reader." + paimonTable.getCatalogTableName() + ".";
        Tracers.record(EXTERNAL, prefix + "nativeReaderReadNum", String.valueOf(totalReaderCount - jniReaderCount));
        Tracers.record(EXTERNAL, prefix + "nativeReaderReadBytes", (totalReaderLength - jniReaderLength) + " B");
        Tracers.record(EXTERNAL, prefix + "jniReaderReadNum", String.valueOf(jniReaderCount));
        Tracers.record(EXTERNAL, prefix + "jniReaderReadBytes", jniReaderLength + " B");
    }

    private void traceDeletionVectorMetrics() {
        String prefix = "Paimon.metadata.deletionVector." + paimonTable.getCatalogTableName() + ".";
        int deletionVectorCount = 0;
        long deletionVectorReaderScanRange = 0;
        for (TScanRangeLocations rangeLocation : scanRangeLocationsList) {
            THdfsScanRange hdfsScanRange = rangeLocation.getScan_range().getHdfs_scan_range();
            if (hdfsScanRange.getPaimon_deletion_file() != null) {
                deletionVectorCount++;
                deletionVectorReaderScanRange += hdfsScanRange.length - hdfsScanRange.offset;
            }
        }
        Tracers.record(EXTERNAL, prefix + "count", String.valueOf(deletionVectorCount));
        Tracers.record(EXTERNAL, prefix + "readBytes", String.valueOf(deletionVectorReaderScanRange));
    }

    private THdfsFileFormat fromType(String type) {
        THdfsFileFormat tHdfsFileFormat;
        switch (type) {
            case "orc":
                tHdfsFileFormat = THdfsFileFormat.ORC;
                break;
            case "parquet":
                tHdfsFileFormat = THdfsFileFormat.PARQUET;
                break;
            default:
                tHdfsFileFormat = THdfsFileFormat.UNKNOWN;
        }
        return tHdfsFileFormat;
    }

    public void splitRawFileScanRangeLocations(RawFile rawFile, @Nullable DeletionFile deletionFile) {
        SessionVariable sv = SessionVariable.DEFAULT_SESSION_VARIABLE;
        long splitSize = sv.getConnectorMaxSplitSize();
        long totalSize = rawFile.length();
        long offset = rawFile.offset();
        boolean needSplit = totalSize > splitSize;
        if (needSplit) {
            splitScanRangeLocations(rawFile, offset, totalSize, splitSize, deletionFile);
        } else {
            addRawFileScanRangeLocations(rawFile, deletionFile);
        }
    }

    public void splitScanRangeLocations(RawFile rawFile,
                                        long offset,
                                        long length,
                                        long splitSize,
                                        @Nullable DeletionFile deletionFile) {
        long remainingBytes = length;
        do {
            if (remainingBytes < 2 * splitSize) {
                addRawFileScanRangeLocations(rawFile, offset + length - remainingBytes, remainingBytes, deletionFile);
                remainingBytes = 0;
            } else {
                addRawFileScanRangeLocations(rawFile, offset + length - remainingBytes, splitSize, deletionFile);
                remainingBytes -= splitSize;
            }
        } while (remainingBytes > 0);
    }

    private void addRawFileScanRangeLocations(RawFile rawFile, @Nullable DeletionFile deletionFile) {
        addRawFileScanRangeLocations(rawFile, rawFile.offset(), rawFile.length(), deletionFile);
    }

    private void addRawFileScanRangeLocations(RawFile rawFile,
                                              long offset,
                                              long length,
                                              @Nullable DeletionFile deletionFile) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setUse_paimon_jni_reader(false);
        hdfsScanRange.setFull_path(rawFile.path());
        hdfsScanRange.setOffset(offset);
        hdfsScanRange.setFile_length(rawFile.length());
        hdfsScanRange.setLength(length);
        hdfsScanRange.setFile_format(fromType(rawFile.format()));

        if (null != deletionFile) {
            TPaimonDeletionFile paimonDeletionFile = new TPaimonDeletionFile();
            paimonDeletionFile.setPath(deletionFile.path());
            paimonDeletionFile.setOffset(deletionFile.offset());
            paimonDeletionFile.setLength(deletionFile.length());
            hdfsScanRange.setPaimon_deletion_file(paimonDeletionFile);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);

        scanRangeLocationsList.add(scanRangeLocations);
    }

    public void addSplitScanRangeLocations(Split split, String predicateInfo, long totalFileLength) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setUse_paimon_jni_reader(true);
        hdfsScanRange.setPaimon_split_info(encodeObjectToString(split));
        hdfsScanRange.setPaimon_predicate_info(predicateInfo);
        hdfsScanRange.setFile_length(totalFileLength);
        hdfsScanRange.setLength(totalFileLength);
        hdfsScanRange.setFile_format(THdfsFileFormat.UNKNOWN);
        // Only uses for hasher in HDFSBackendSelector to select BE
        if (split instanceof DataSplit) {
            DataSplit dataSplit = (DataSplit) split;
            hdfsScanRange.setRelative_path(String.valueOf(dataSplit.hashCode()));
        }
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);

        scanRangeLocationsList.add(scanRangeLocations);
    }

    long getTotalFileLength(DataSplit split) {
        return split.dataFiles().stream().map(DataFileMeta::fileSize).reduce(0L, Long::sum);
    }

    private long nextPartitionId() {
        return partitionIdGen.getAndIncrement();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(paimonTable.getName()).append("\n");

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
                paimonTable.getCatalogName(), paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName(),
                ConnectorMetadatRequestContext.DEFAULT);

        output.append(prefix).append(
                String.format("partitions=%s/%s", scanNodePredicates.getSelectedPartitionIds().size(),
                        partitionNames.size() == 0 ? 1 : partitionNames.size()));
        output.append("\n");

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

        if (paimonTable != null) {
            msg.hdfs_scan_node.setTable_name(paimonTable.getName());
        }

        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
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
            java.util.Base64.getUrlEncoder().withoutPadding();

    public static <T> String encodeObjectToString(T t) {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(t);
            return new String(BASE64_ENCODER.encode(bytes), UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
