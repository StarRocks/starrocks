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
package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorScanRangeSource;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.datacache.DataCacheExprRewriter;
import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.datacache.DataCacheOptions;
import com.starrocks.datacache.DataCacheRule;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TDataCacheOptions;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.hive.HiveMetadata.useMetadataCache;

public class HiveConnectorScanRangeSource extends ConnectorScanRangeSource {
    private static final Logger LOG = LogManager.getLogger(HiveConnectorScanRangeSource.class);

    protected DescriptorTable descriptorTable;
    protected Table table;
    protected HDFSScanNodePredicates scanNodePredicates;
    private RemoteFileInfoSource remoteFileInfoSource;
    private boolean forceScheduleLocal = false;
    private Iterator<TScanRangeLocations> iterator;
    private RemoteFileInfo buffer;
    private boolean hasMoreOutput = true;
    private boolean backendSplitFile = true;
    private long backendSplitCount = 0;

    public HiveConnectorScanRangeSource(DescriptorTable descriptorTable, Table table, HDFSScanNodePredicates scanNodePredicates) {
        this.descriptorTable = descriptorTable;
        this.table = table;
        this.scanNodePredicates = scanNodePredicates;
        forceScheduleLocal = false;
        if (ConnectContext.get() != null) {
            // ConnectContext sometimes will be nullptr, we need to cover it up
            forceScheduleLocal = ConnectContext.get().getSessionVariable().getForceScheduleLocal();
        }
    }

    public void setup() {
        Collection<Long> selectedPartitionIds = scanNodePredicates.getSelectedPartitionIds();
        if (selectedPartitionIds.isEmpty()) {
            return;
        }

        for (long partitionId : selectedPartitionIds) {
            PartitionKey partitionKey = scanNodePredicates.getIdToPartitionKey().get(partitionId);
            DescriptorTable.ReferencedPartitionInfo partitionInfo =
                    new DescriptorTable.ReferencedPartitionInfo(partitionId, partitionKey);
            descriptorTable.addReferencedPartitions(table, partitionInfo);
        }
    }

    public static class PartitionAttachment {
        public long partitionId = 0;
        public DataCacheOptions dataCacheOptions;
    }

    protected void initRemoteFileInfoSource() {
        List<PartitionAttachment> partitionAttachments = Lists.newArrayList();

        // get partitions keys;
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        for (long partitionId : scanNodePredicates.getSelectedPartitionIds()) {
            PartitionKey partitionKey = scanNodePredicates.getIdToPartitionKey().get(partitionId);
            partitionKeys.add(partitionKey);
            PartitionAttachment attachment = new PartitionAttachment();
            attachment.partitionId = partitionId;
            partitionAttachments.add(attachment);
        }

        // get data options by keys;
        Optional<List<DataCacheOptions>> dataCacheOptionsList = generateDataCacheOptions(table, partitionKeys);
        if (dataCacheOptionsList.isPresent()) {
            List<DataCacheOptions> options = dataCacheOptionsList.get();
            for (int i = 0; i < partitionKeys.size(); i++) {
                partitionAttachments.get(i).dataCacheOptions = options.get(i);
            }
        }

        GetRemoteFilesParams params =
                GetRemoteFilesParams.newBuilder().setPartitionKeys(partitionKeys)
                        .setPartitionAttachments(partitionAttachments)
                        .setUseCache(useMetadataCache())
                        .build();
        remoteFileInfoSource = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFilesAsync(table, params);
    }

    private Optional<List<DataCacheOptions>> generateDataCacheOptions(Table table,
                                                                      final List<PartitionKey> partitionKeys) {
        QualifiedName qualifiedName = QualifiedName.of(ImmutableList.of(table.getCatalogName(),
                table.getCatalogDBName(), table.getCatalogTableName()));
        List<String> partitionColumnNames = table.getPartitionColumnNames();

        if (!ConnectContext.get().getSessionVariable().isEnableScanDataCache()) {
            return Optional.empty();
        }

        Optional<DataCacheRule> dataCacheRule = DataCacheMgr.getInstance().getCacheRule(qualifiedName);
        if (!dataCacheRule.isPresent()) {
            return Optional.empty();
        }

        List<DataCacheOptions> dataCacheOptions = new ArrayList<>(partitionKeys.size());
        Expr predicates = dataCacheRule.get().getPredicates();
        if (predicates == null) {
            for (int i = 0; i < partitionKeys.size(); i++) {
                dataCacheOptions.add(DataCacheOptions.DataCacheOptionsBuilder.builder()
                        .setPriority(dataCacheRule.get().getPriority()).build());
            }
        } else {
            // evaluate partition predicates
            for (PartitionKey partitionKey : partitionKeys) {
                // key is ColumnName, value is Expr(Literal)
                Map<String, Expr> mapping = new HashMap<>(partitionColumnNames.size());
                Preconditions.checkArgument(partitionColumnNames.size() == partitionKey.getKeys().size(),
                        "PartitionColumnName size must equal with PartitionKey keys' size.");
                for (int i = 0; i < partitionKey.getKeys().size(); i++) {
                    mapping.put(partitionColumnNames.get(i), partitionKey.getKeys().get(i));
                }
                // Must clone expr first, avoid change original expr
                Expr clonedExpr = predicates.clone();
                Expr rewritedExpr = DataCacheExprRewriter.rewrite(clonedExpr, mapping);
                ScalarOperator op = SqlToScalarOperatorTranslator.translate(rewritedExpr);
                ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
                op = scalarRewriter.rewrite(op, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                if (op.isConstantTrue()) {
                    // matched partition predicates
                    dataCacheOptions.add(DataCacheOptions.DataCacheOptionsBuilder.builder()
                            .setPriority(dataCacheRule.get().getPriority()).build());
                } else {
                    // not matched, add null DataCacheOption
                    dataCacheOptions.add(null);
                    if (!op.isConstantRef()) {
                        LOG.warn(String.format("ConstFolding failed for expr: %s, rewrite scalarOperator is %s",
                                rewritedExpr.toMySql(), op.debugString()));
                    }
                }
            }
        }
        return Optional.of(dataCacheOptions);
    }

    class ScanRangeIterator implements Iterator<TScanRangeLocations> {
        RemoteFileInfo remoteFileInfo;
        List<RemoteFileDesc> files;
        int fileIndex = 0;
        List<TScanRangeLocations> buffer = new ArrayList<>();

        public ScanRangeIterator(RemoteFileInfo remoteFileInfo) {
            this.remoteFileInfo = remoteFileInfo;
            this.files = remoteFileInfo.getFiles();
        }

        @Override
        public boolean hasNext() {
            tryPopulateBuffer();
            return fileIndex < files.size() || !buffer.isEmpty();
        }

        @Override
        public TScanRangeLocations next() {
            return buffer.remove(buffer.size() - 1);
        }

        private void tryPopulateBuffer() {
            while (buffer.isEmpty() && fileIndex < files.size()) {
                splitFile(files.get(fileIndex));
                fileIndex += 1;
            }
        }

        private void splitFile(RemoteFileDesc fileDesc) {
            if (forceScheduleLocal) {
                for (RemoteFileBlockDesc blockDesc : fileDesc.getBlockDescs()) {
                    addScanRangeLocations(fileDesc, Optional.of(blockDesc));
                    LOG.debug("Add scan range success. partition: {}, file: {}, block: {}-{}",
                            remoteFileInfo.getFullPath(), fileDesc.getFileName(), blockDesc.getOffset(),
                            blockDesc.getLength());
                }
            } else {
                addScanRangeLocations(fileDesc, Optional.empty());
                LOG.debug("Add scan range success. partition: {}, file: {}, range: {}-{}",
                        remoteFileInfo.getFullPath(), fileDesc.getFileName(), 0, fileDesc.getLength());
            }
        }

        private void addScanRangeLocations(RemoteFileDesc fileDesc, Optional<RemoteFileBlockDesc> blockDesc) {
            SessionVariable sv = SessionVariable.DEFAULT_SESSION_VARIABLE;
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null) {
                sv = connectContext.getSessionVariable();
            }

            long totalSize = fileDesc.getLength();
            long offset = 0;
            if (blockDesc.isPresent()) {
                // If blockDesc existed, we will split according block desc
                RemoteFileBlockDesc block = blockDesc.get();
                totalSize = block.getLength();
                offset = block.getOffset();
            }

            // assume we can not split at all.
            long splitSize = getSplitSize(fileDesc, totalSize, sv);
            boolean needSplit = (totalSize > splitSize);
            if (needSplit) {
                splitScanRangeLocations(fileDesc, blockDesc, offset, totalSize, splitSize);
            } else {
                createScanRangeLocationsForSplit(fileDesc, blockDesc, offset, totalSize);
            }
        }

        private long getSplitSize(RemoteFileDesc fileDesc, long totalSize, SessionVariable sv) {
            long splitSize = totalSize;
            if (fileDesc.isSplittable()) {
                // if splittable, then use max split size.
                splitSize = sv.getConnectorMaxSplitSize();
                if (backendSplitFile && sv.isEnableConnectorSplitIoTasks() && remoteFileInfo.getFormat().isBackendSplittable()) {
                    // if BE can split, use a higher threshold.
                    splitSize = sv.getConnectorHugeFileSize();
                }
            }
            return splitSize;
        }

        private void splitScanRangeLocations(RemoteFileDesc fileDesc, Optional<RemoteFileBlockDesc> blockDesc,
                                             long offset, long length, long splitSize) {
            long remainingBytes = length;
            do {
                if (remainingBytes < 2 * splitSize) {
                    createScanRangeLocationsForSplit(fileDesc,
                            blockDesc, offset + length - remainingBytes,
                            remainingBytes);
                    remainingBytes = 0;
                } else {
                    createScanRangeLocationsForSplit(fileDesc,
                            blockDesc, offset + length - remainingBytes,
                            splitSize);
                    remainingBytes -= splitSize;
                }
            } while (remainingBytes > 0);
        }

        private void createScanRangeLocationsForSplit(RemoteFileDesc fileDesc, Optional<RemoteFileBlockDesc> blockDesc,
                                                      long offset, long length) {
            PartitionAttachment attachment = (PartitionAttachment) remoteFileInfo.getAttachment();
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

            THdfsScanRange hdfsScanRange = new THdfsScanRange();
            hdfsScanRange.setRelative_path(fileDesc.getFileName());
            hdfsScanRange.setOffset(offset);
            hdfsScanRange.setLength(length);
            hdfsScanRange.setPartition_id(attachment.partitionId);
            hdfsScanRange.setFile_length(fileDesc.getLength());
            hdfsScanRange.setModification_time(fileDesc.getModificationTime());

            RemoteFileInputFormat fileFormat = remoteFileInfo.getFormat();
            hdfsScanRange.setFile_format(fileFormat.toThrift());
            if (fileFormat.isTextFormat()) {
                hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
            }

            if (attachment.dataCacheOptions != null) {
                TDataCacheOptions tDataCacheOptions = new TDataCacheOptions();
                tDataCacheOptions.setPriority(attachment.dataCacheOptions.getPriority());
                hdfsScanRange.setDatacache_options(tDataCacheOptions);
            }

            TScanRange scanRange = new TScanRange();
            scanRange.setHdfs_scan_range(hdfsScanRange);
            scanRangeLocations.setScan_range(scanRange);

            if (blockDesc.isPresent()) {
                if (blockDesc.get().getReplicaHostIds().length == 0) {
                    String message = String.format("hdfs file block has no host. file = %s/%s",
                            remoteFileInfo.getFullPath(), fileDesc.getFileName());
                    throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
                }

                for (long hostId : blockDesc.get().getReplicaHostIds()) {
                    String host = blockDesc.get().getDataNodeIp(hostId);
                    TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(host, -1));
                    scanRangeLocations.addToLocations(scanRangeLocation);
                }
            } else {
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
                scanRangeLocations.addToLocations(scanRangeLocation);
            }
            buffer.add(scanRangeLocations);
        }

    }

    public Iterator<TScanRangeLocations> createScanRangeIterator(RemoteFileInfo remoteFileInfo) {
        return new ScanRangeIterator(remoteFileInfo);
    }

    private void updateBackendSplitFile(RemoteFileInfo partition) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return;
        }
        long hugeFileSize = connectContext.getSessionVariable().getConnectorHugeFileSize();
        for (RemoteFileDesc fileDesc : partition.getFiles()) {
            if (fileDesc.isSplittable()) {
                backendSplitCount += (fileDesc.getLength() + hugeFileSize - 1) / hugeFileSize;
            } else {
                backendSplitCount += 1;
            }
        }
        // if splits is small comparing to nodes, then better not let backend do split.
        int nodes = connectContext.getAliveComputeNumber() + connectContext.getAliveBackendNumber();
        backendSplitFile = (backendSplitCount > 2 * nodes);
    }

    private void updateIterator() {
        if (!hasMoreOutput) {
            return;
        }
        if (remoteFileInfoSource == null) {
            initRemoteFileInfoSource();
        }
        while ((iterator == null || !iterator.hasNext())) {
            do {
                if (!remoteFileInfoSource.hasMoreOutput()) {
                    hasMoreOutput = false;
                    return;
                } else {
                    buffer = remoteFileInfoSource.getOutput();
                }
            } while (buffer == null || buffer.getFiles() == null || buffer.getFiles().isEmpty());
            updateBackendSplitFile(buffer);
            iterator = createScanRangeIterator(buffer);
        }
    }

    @Override
    public List<TScanRangeLocations> getSourceOutputs(int maxSize) {
        List<TScanRangeLocations> res = new ArrayList<>();
        updateIterator();
        while (hasMoreOutput) {
            while (res.size() < maxSize && iterator.hasNext()) {
                res.add(iterator.next());
            }
            if (res.size() >= maxSize) {
                break;
            }
            updateIterator();
        }
        // Previously, the order of the scan range was from front to back, which would cause some probing sql to
        // encounter very bad cases (scan ranges that meet the predicate conditions are in the later partitions),
        // making BE have to scan more data to find rows that meet the conditions.
        // So shuffle scan ranges can naturally disrupt the scan ranges' order to avoid very bad cases.
        Collections.shuffle(res);
        return res;
    }

    @Override
    public boolean sourceHasMoreOutput() {
        updateIterator();
        return hasMoreOutput;
    }
}
