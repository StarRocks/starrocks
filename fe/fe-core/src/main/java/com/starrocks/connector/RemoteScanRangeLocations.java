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

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.hudi.HudiRemoteFileDesc;
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
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RemoteScanRangeLocations {
    private static final Logger LOG = LogManager.getLogger(RemoteScanRangeLocations.class);

    private final List<TScanRangeLocations> result = new ArrayList<>();
    private final List<DescriptorTable.ReferencedPartitionInfo> partitionInfos = new ArrayList<>();
    private boolean forceScheduleLocal = false;
    private boolean canBackendSplitFile = false;

    public void setup(DescriptorTable descTbl, Table table, HDFSScanNodePredicates scanNodePredicates) {
        Collection<Long> selectedPartitionIds = scanNodePredicates.getSelectedPartitionIds();
        if (selectedPartitionIds.isEmpty()) {
            return;
        }

        for (long partitionId : selectedPartitionIds) {
            PartitionKey partitionKey = scanNodePredicates.getIdToPartitionKey().get(partitionId);
            DescriptorTable.ReferencedPartitionInfo partitionInfo =
                    new DescriptorTable.ReferencedPartitionInfo(partitionId, partitionKey);
            partitionInfos.add(partitionInfo);
            descTbl.addReferencedPartitions(table, partitionInfo);
        }

        forceScheduleLocal = false;
        if (ConnectContext.get() != null) {
            // ConnectContext sometimes will be nullptr, we need to cover it up
            forceScheduleLocal = ConnectContext.get().getSessionVariable().getForceScheduleLocal();
        }
    }

    private void addScanRangeLocations(long partitionId, RemoteFileInfo partition, RemoteFileDesc fileDesc,
                                       Optional<RemoteFileBlockDesc> blockDesc, DataCacheOptions dataCacheOptions) {
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
        long splitSize = totalSize;
        if (fileDesc.isSplittable()) {
            // if splittable, then use max split size.
            splitSize = sv.getConnectorMaxSplitSize();
            if (canBackendSplitFile && sv.isEnableConnectorSplitIoTasks() && partition.getFormat().isBackendSplittable()) {
                // if BE can split, use a higher threshold.
                splitSize = sv.getConnectorHugeFileSize();
            }
        }
        boolean needSplit = (totalSize > splitSize);
        if (needSplit) {
            splitScanRangeLocations(partitionId, partition, fileDesc, blockDesc, offset, totalSize, splitSize,
                    dataCacheOptions);
        } else {
            createScanRangeLocationsForSplit(partitionId, partition, fileDesc, blockDesc, offset, totalSize,
                    dataCacheOptions);
        }
    }

    private void splitScanRangeLocations(long partitionId, RemoteFileInfo partition,
                                         RemoteFileDesc fileDesc,
                                         Optional<RemoteFileBlockDesc> blockDesc,
                                         long offset, long length, long splitSize, DataCacheOptions dataCacheOptions) {
        long remainingBytes = length;
        do {
            if (remainingBytes < 2 * splitSize) {
                createScanRangeLocationsForSplit(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes,
                        remainingBytes, dataCacheOptions);
                remainingBytes = 0;
            } else {
                createScanRangeLocationsForSplit(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes,
                        splitSize, dataCacheOptions);
                remainingBytes -= splitSize;
            }
        } while (remainingBytes > 0);
    }

    private void createScanRangeLocationsForSplit(long partitionId, RemoteFileInfo partition,
                                                  RemoteFileDesc fileDesc,
                                                  Optional<RemoteFileBlockDesc> blockDesc,
                                                  long offset, long length, DataCacheOptions dataCacheOptions) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path(fileDesc.getFileName());
        hdfsScanRange.setOffset(offset);
        hdfsScanRange.setLength(length);
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileDesc.getLength());
        hdfsScanRange.setModification_time(fileDesc.getModificationTime());
        hdfsScanRange.setFile_format(partition.getFormat().toThrift());
        if (isTextFormat(hdfsScanRange.getFile_format())) {
            hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
        }

        if (dataCacheOptions != null) {
            TDataCacheOptions tDataCacheOptions = new TDataCacheOptions();
            tDataCacheOptions.setPriority(dataCacheOptions.getPriority());
            hdfsScanRange.setDatacache_options(tDataCacheOptions);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        if (blockDesc.isPresent()) {
            if (blockDesc.get().getReplicaHostIds().length == 0) {
                String message = String.format("hdfs file block has no host. file = %s/%s",
                        partition.getFullPath(), fileDesc.getFileName());
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

        result.add(scanRangeLocations);
    }

    public static boolean isTextFormat(THdfsFileFormat format) {
        return format == THdfsFileFormat.TEXT || format == THdfsFileFormat.LZO_TEXT;
    }

    private void createHudiScanRangeLocations(long partitionId,
                                              RemoteFileInfo partition,
                                              HudiRemoteFileDesc fileDesc,
                                              boolean useJNIReader, DataCacheOptions dataCacheOptions) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path(fileDesc.getFileName());
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(fileDesc.getLength());
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileDesc.getLength());
        hdfsScanRange.setFile_format(partition.getFormat().toThrift());
        if (isTextFormat(hdfsScanRange.getFile_format())) {
            hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
        }
        for (String log : fileDesc.getHudiDeltaLogs()) {
            hdfsScanRange.addToHudi_logs(log);
        }
        hdfsScanRange.setUse_hudi_jni_reader(useJNIReader);
        if (dataCacheOptions != null) {
            TDataCacheOptions tDataCacheOptions = new TDataCacheOptions();
            tDataCacheOptions.setPriority(dataCacheOptions.getPriority());
            hdfsScanRange.setDatacache_options(tDataCacheOptions);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        // TODO: get block info
        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);

        result.add(scanRangeLocations);
    }

    private Optional<List<DataCacheOptions>> generateDataCacheOptions(final QualifiedName qualifiedName,
                                                                      final List<String> partitionColumnNames,
                                                                      final List<PartitionKey> partitionKeys) {
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

    private void updateCanBackendSplitFile(List<RemoteFileInfo> partitions) {
        canBackendSplitFile = true;
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return;
        }
        // if we let backend do split file work, how many splits we will get.
        int splits = getSplitsIfBackendSplitFile(partitions, connectContext);
        // if splits is small comparing to nodes, then better not let backend do split.
        int nodes = connectContext.getAliveComputeNumber() + connectContext.getAliveBackendNumber();
        if ((nodes * 2) >= splits) {
            canBackendSplitFile = false;
        }
    }

    private static int getSplitsIfBackendSplitFile(List<RemoteFileInfo> partitions, ConnectContext connectContext) {
        SessionVariable sv = connectContext.getSessionVariable();
        int splits = 0;
        long splitSize = sv.getConnectorHugeFileSize();
        for (int i = 0; i < partitions.size(); i++) {
            for (RemoteFileDesc fileDesc : partitions.get(i).getFiles()) {
                if (fileDesc.isSplittable()) {
                    splits += (fileDesc.getLength() + splitSize - 1) / splitSize;
                } else {
                    splits += 1;
                }
            }
        }
        return splits;
    }

    public List<TScanRangeLocations> getScanRangeLocations(DescriptorTable descTbl, Table table,
                                                           HDFSScanNodePredicates scanNodePredicates) {
        result.clear();
        HiveMetaStoreTable hiveMetaStoreTable = (HiveMetaStoreTable) table;

        long start = System.currentTimeMillis();
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        for (long partitionId : scanNodePredicates.getSelectedPartitionIds()) {
            PartitionKey partitionKey = scanNodePredicates.getIdToPartitionKey().get(partitionId);
            partitionKeys.add(partitionKey);
        }
        String catalogName = hiveMetaStoreTable.getCatalogName();
        QualifiedName qualifiedName = QualifiedName.of(ImmutableList.of(catalogName,
                hiveMetaStoreTable.getDbName(), hiveMetaStoreTable.getTableName()));
        Optional<List<DataCacheOptions>> dataCacheOptionsList = generateDataCacheOptions(qualifiedName,
                hiveMetaStoreTable.getPartitionColumnNames(), partitionKeys);

        List<RemoteFileInfo> partitions;

        try {
            partitions = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(catalogName, table, partitionKeys);
        } catch (Exception e) {
            LOG.error("Failed to get remote files", e);
            throw e;
        }

        updateCanBackendSplitFile(partitions);

        if (table instanceof HiveTable) {
            for (int i = 0; i < partitions.size(); i++) {
                DataCacheOptions dataCacheOptions = null;
                if (dataCacheOptionsList.isPresent()) {
                    dataCacheOptions = dataCacheOptionsList.get().get(i);
                }
                for (RemoteFileDesc fileDesc : partitions.get(i).getFiles()) {
                    if (fileDesc.getLength() == 0) {
                        continue;
                    }
                    if (forceScheduleLocal) {
                        for (RemoteFileBlockDesc blockDesc : fileDesc.getBlockDescs()) {
                            addScanRangeLocations(partitionInfos.get(i).getId(), partitions.get(i), fileDesc,
                                    Optional.of(blockDesc),
                                    dataCacheOptions);
                            LOG.debug("Add scan range success. partition: {}, file: {}, block: {}-{}",
                                    partitions.get(i).getFullPath(), fileDesc.getFileName(), blockDesc.getOffset(),
                                    blockDesc.getLength());
                        }
                    } else {
                        addScanRangeLocations(partitionInfos.get(i).getId(), partitions.get(i), fileDesc,
                                Optional.empty(),
                                dataCacheOptions);
                        LOG.debug("Add scan range success. partition: {}, file: {}, range: {}-{}",
                                partitions.get(i).getFullPath(), fileDesc.getFileName(), 0, fileDesc.getLength());
                    }
                }
            }
        } else if (table instanceof HudiTable) {
            HudiTable hudiTable = (HudiTable) table;
            String tableInputFormat = hudiTable.getHudiInputFormat();
            boolean morTable = hudiTable.getTableType() == HoodieTableType.MERGE_ON_READ;
            boolean readOptimized = tableInputFormat.equals(HudiTable.MOR_RO_INPUT_FORMAT)
                    || tableInputFormat.equals(HudiTable.MOR_RO_INPUT_FORMAT_LEGACY);
            boolean snapshot = tableInputFormat.equals(HudiTable.MOR_RT_INPUT_FORMAT)
                    || tableInputFormat.equals(HudiTable.MOR_RT_INPUT_FORMAT_LEGACY);
            boolean forceJNIReader = ConnectContext.get().getSessionVariable().getHudiMORForceJNIReader();
            for (int i = 0; i < partitions.size(); i++) {
                DataCacheOptions dataCacheOptions = null;
                if (dataCacheOptionsList.isPresent()) {
                    dataCacheOptions = dataCacheOptionsList.get().get(i);
                }
                descTbl.addReferencedPartitions(table, partitionInfos.get(i));
                for (RemoteFileDesc fileDesc : partitions.get(i).getFiles()) {
                    HudiRemoteFileDesc hudiFiledesc = (HudiRemoteFileDesc) fileDesc;
                    if (fileDesc.getLength() == -1 && hudiFiledesc.getHudiDeltaLogs().isEmpty()) {
                        String message = "Error: get a empty hudi fileSlice";
                        throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
                    }
                    // ignore the scan range when read optimized mode and file slices contain logs only
                    if (morTable && readOptimized && fileDesc.getLength() == -1 && fileDesc.getFileName().isEmpty()) {
                        continue;
                    }
                    boolean useJNIReader =
                            forceJNIReader || (morTable && snapshot && !hudiFiledesc.getHudiDeltaLogs().isEmpty());
                    createHudiScanRangeLocations(partitionInfos.get(i).getId(), partitions.get(i), hudiFiledesc,
                            useJNIReader, dataCacheOptions);
                }
            }
        } else {
            String message = "Only Hive/Hudi table is supported.";
            throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
        }

        // Previously, the order of the scan range was from front to back, which would cause some probing sql to
        // encounter very bad cases (scan ranges that meet the predicate conditions are in the later partitions),
        // making BE have to scan more data to find rows that meet the conditions.
        // So shuffle scan ranges can naturally disrupt the scan ranges' order to avoid very bad cases.
        Collections.shuffle(result);

        LOG.debug("Get {} scan range locations cost: {} ms",
                getScanRangeLocationsSize(), (System.currentTimeMillis() - start));
        return result;
    }

    public int getScanRangeLocationsSize() {
        return result.size();
    }
}
