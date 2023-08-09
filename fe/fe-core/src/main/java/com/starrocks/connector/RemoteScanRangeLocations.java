// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
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
import java.util.List;

public class RemoteScanRangeLocations {
    private static final Logger LOG = LogManager.getLogger(RemoteScanRangeLocations.class);

    private final List<TScanRangeLocations> result = new ArrayList<>();

    private void addScanRangeLocations(long partitionId, RemoteFileInfo partition, RemoteFileDesc fileDesc,
                                       RemoteFileBlockDesc blockDesc) {
        // NOTE: Config.hive_max_split_size should be extracted to a local variable,
        // because it may be changed before calling 'splitScanRangeLocations'
        // and after needSplit has been calculated.
        long splitSize = Config.hive_max_split_size;
        boolean needSplit = fileDesc.isSplittable() && blockDesc.getLength() > splitSize;
        if (needSplit) {
            splitScanRangeLocations(partitionId, partition, fileDesc, blockDesc, splitSize);
        } else {
            createScanRangeLocationsForSplit(partitionId, partition, fileDesc, blockDesc, blockDesc.getOffset(),
                    blockDesc.getLength());
        }
    }

    private void splitScanRangeLocations(long partitionId, RemoteFileInfo partition,
                                         RemoteFileDesc fileDesc,
                                         RemoteFileBlockDesc blockDesc,
                                         long splitSize) {
        long remainingBytes = blockDesc.getLength();
        long length = blockDesc.getLength();
        long offset = blockDesc.getOffset();
        do {
            if (remainingBytes <= splitSize) {
                createScanRangeLocationsForSplit(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes,
                        remainingBytes);
                remainingBytes = 0;
            } else if (remainingBytes <= 2 * splitSize) {
                long mid = (remainingBytes + 1) / 2;
                createScanRangeLocationsForSplit(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes, mid);
                createScanRangeLocationsForSplit(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes + mid,
                        remainingBytes - mid);
                remainingBytes = 0;
            } else {
                createScanRangeLocationsForSplit(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes,
                        splitSize);
                remainingBytes -= splitSize;
            }
        } while (remainingBytes > 0);
    }

    private void createScanRangeLocationsForSplit(long partitionId, RemoteFileInfo partition,
                                                  RemoteFileDesc fileDesc,
                                                  RemoteFileBlockDesc blockDesc,
                                                  long offset, long length) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path(fileDesc.getFileName());
        hdfsScanRange.setOffset(offset);
        hdfsScanRange.setLength(length);
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileDesc.getLength());
        hdfsScanRange.setModification_time(fileDesc.getModificationTime());
        hdfsScanRange.setFile_format(partition.getFormat().toThrift());
        hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        if (blockDesc.getReplicaHostIds().length == 0) {
            String message = String.format("hdfs file block has no host. file = %s/%s",
                    partition.getFullPath(), fileDesc.getFileName());
            throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
        }

        for (long hostId : blockDesc.getReplicaHostIds()) {
            String host = blockDesc.getDataNodeIp(hostId);
            TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(host, -1));
            scanRangeLocations.addToLocations(scanRangeLocation);
        }

        result.add(scanRangeLocations);
    }

    private void createHudiScanRangeLocations(long partitionId,
                                              RemoteFileInfo partition,
                                              RemoteFileDesc fileDesc,
                                              boolean useJNIReader) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path(fileDesc.getFileName());
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(fileDesc.getLength());
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileDesc.getLength());
        hdfsScanRange.setFile_format(partition.getFormat().toThrift());
        hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
        for (String log : fileDesc.getHudiDeltaLogs()) {
            hdfsScanRange.addToHudi_logs(log);
        }
        hdfsScanRange.setUse_hudi_jni_reader(useJNIReader);
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        // TODO: get block info
        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);

        result.add(scanRangeLocations);
    }

    public void setupScanRangeLocations(DescriptorTable descTbl, Table table,
                                        HDFSScanNodePredicates scanNodePredicates) {
        HiveMetaStoreTable hiveMetaStoreTable = (HiveMetaStoreTable) table;
        Collection<Long> selectedPartitionIds = scanNodePredicates.getSelectedPartitionIds();
        if (selectedPartitionIds.isEmpty()) {
            return;
        }

        long start = System.currentTimeMillis();
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        List<DescriptorTable.ReferencedPartitionInfo> partitionInfos = Lists.newArrayList();
        for (long partitionId : selectedPartitionIds) {
            PartitionKey partitionKey = scanNodePredicates.getIdToPartitionKey().get(partitionId);
            partitionKeys.add(partitionKey);
            partitionInfos.add(new DescriptorTable.ReferencedPartitionInfo(partitionId, partitionKey));
        }
        String catalogName = hiveMetaStoreTable.getCatalogName();
        List<RemoteFileInfo> partitions = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getRemoteFileInfos(catalogName, table, partitionKeys);

        if (table instanceof HiveTable) {
            Preconditions.checkState(partitions.size() == partitionKeys.size());
            for (int i = 0; i < partitions.size(); i++) {
                descTbl.addReferencedPartitions(table, partitionInfos.get(i));
                for (RemoteFileDesc fileDesc : partitions.get(i).getFiles()) {
                    if (fileDesc.getLength() == 0) {
                        continue;
                    }
                    for (RemoteFileBlockDesc blockDesc : fileDesc.getBlockDescs()) {
                        addScanRangeLocations(partitionInfos.get(i).getId(), partitions.get(i), fileDesc, blockDesc);
                        LOG.debug("Add scan range success. partition: {}, file: {}, block: {}-{}",
                                partitions.get(i).getFullPath(), fileDesc.getFileName(), blockDesc.getOffset(),
                                blockDesc.getLength());
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
                descTbl.addReferencedPartitions(table, partitionInfos.get(i));
                for (RemoteFileDesc fileDesc : partitions.get(i).getFiles()) {
                    if (fileDesc.getLength() == -1 && fileDesc.getHudiDeltaLogs().isEmpty()) {
                        String message = "Error: get a empty hudi fileSlice";
                        throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
                    }
                    // ignore the scan range when read optimized mode and file slices contain logs only
                    if (morTable && readOptimized && fileDesc.getLength() == -1 && fileDesc.getFileName().equals("")) {
                        continue;
                    }
                    boolean useJNIReader =
                            forceJNIReader || (morTable && snapshot && !fileDesc.getHudiDeltaLogs().isEmpty());
                    createHudiScanRangeLocations(partitionInfos.get(i).getId(), partitions.get(i), fileDesc,
                            useJNIReader);
                }
            }
        } else {
            String message = "Only Hive/Hudi table is supported.";
            throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
        }

        LOG.debug("Get {} scan range locations cost: {} ms",
                getScanRangeLocationsSize(), (System.currentTimeMillis() - start));
    }

    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    public int getScanRangeLocationsSize() {
        return result.size();
    }
}
