// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.external.hive.HdfsFileBlockDesc;
import com.starrocks.external.hive.HdfsFileDesc;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.qe.ConnectContext;
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
import java.util.Map;
import java.util.function.UnaryOperator;

public class RemoteScanRangeLocations {
    private static final Logger LOG = LogManager.getLogger(RemoteScanRangeLocations.class);
    public static final String PSEUDO_BLOCK_HOST = "PseudoBlockHost";
    private final List<TScanRangeLocations> result = new ArrayList<>();

    private void addScanRangeLocations(long partitionId, HivePartition partition, HdfsFileDesc fileDesc,
                                       HdfsFileBlockDesc blockDesc, UnaryOperator<THdfsScanRange> fn) {
        // NOTE: Config.hive_max_split_size should be extracted to a local variable,
        // because it may be changed before calling 'splitScanRangeLocations'
        // and after needSplit has been calculated.
        long splitSize = Config.hive_max_split_size;
        splitScanRangeLocations(partitionId, partition, fileDesc, blockDesc, splitSize, fn);
    }

    private void splitScanRangeLocations(long partitionId, HivePartition partition,
                                         HdfsFileDesc fileDesc,
                                         HdfsFileBlockDesc blockDesc,
                                         long splitSize,
                                         UnaryOperator<THdfsScanRange> fn) {
        long remainingBytes = blockDesc.getLength();
        long length = blockDesc.getLength();
        long offset = blockDesc.getOffset();
        do {
            if (remainingBytes <= splitSize) {
                addAtomScanRangeLocations(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes,
                        remainingBytes, fn);
                remainingBytes = 0;
            } else if (remainingBytes <= 2 * splitSize) {
                long mid = (remainingBytes + 1) / 2;
                addAtomScanRangeLocations(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes, mid, fn);
                addAtomScanRangeLocations(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes + mid,
                        remainingBytes - mid, fn);
                remainingBytes = 0;
            } else {
                addAtomScanRangeLocations(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes,
                        splitSize, fn);
                remainingBytes -= splitSize;
            }
        } while (remainingBytes > 0);
    }

    private void addAtomScanRangeLocations(long partitionId, HivePartition partition,
                                           HdfsFileDesc fileDesc,
                                           HdfsFileBlockDesc blockDesc,
                                           long offset, long length,
                                           UnaryOperator<THdfsScanRange> fn) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path(fileDesc.getFileName());
        hdfsScanRange.setOffset(offset);
        hdfsScanRange.setLength(length);
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileDesc.getLength());
        hdfsScanRange.setFile_format(partition.getFormat().toThrift());
        hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        if (blockDesc == null) {
            TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(PSEUDO_BLOCK_HOST, -1));
            scanRangeLocations.addToLocations(scanRangeLocation);
        } else {
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
        }
        if (fn != null) {
            fn.apply(hdfsScanRange);
        }
        result.add(scanRangeLocations);
    }

    public void addHiveHdfsFiles(long partitionId, HivePartition partition, ImmutableList<HdfsFileDesc> files) {
        for (HdfsFileDesc fileDesc : files) {
            if (fileDesc.getLength() == 0) {
                continue;
            }
            if (!fileDesc.isSplittable()) {
                addAtomScanRangeLocations(partitionId, partition, fileDesc, null, 0, fileDesc.getLength(), null);
                LOG.debug("Add unsplittable scan range success. partition: {}, file: {}, block: {}-{}",
                        partition.getFullPath(), fileDesc.getFileName(), 0, fileDesc.getLength());
            } else {
                for (HdfsFileBlockDesc blockDesc : fileDesc.getBlockDescs()) {
                    addScanRangeLocations(partitionId, partition, fileDesc, blockDesc, null);
                    LOG.debug("Add splittable scan range success. partition: {}, file: {}, block: {}-{}",
                            partition.getFullPath(), fileDesc.getFileName(), blockDesc.getOffset(),
                            blockDesc.getLength());
                }
            }
        }
    }

    public void addHudiHdfsFiles(long partitionId, HivePartition partition, ImmutableList<HdfsFileDesc> files,
                                 boolean morTable, boolean readOptimized, boolean snapshot) {
        for (HdfsFileDesc fileDesc : files) {
            if (fileDesc.isInvalidFileLength() && fileDesc.getHudiDeltaLogs().isEmpty()) {
                String message = "Empty hudi fileSlice";
                throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
            }
            // ignore the scan range when read optimized mode and file slices contain logs only
            if (morTable && readOptimized && fileDesc.isInvalidFileName() && fileDesc.isInvalidFileLength()) {
                continue;
            }
            // unsplittable.
            boolean useJNIReader = (morTable && snapshot && !fileDesc.getHudiDeltaLogs().isEmpty());
            addAtomScanRangeLocations(partitionId, partition, fileDesc,
                    null, 0, fileDesc.getLength(), x -> {
                        x.setUse_hudi_jni_reader(useJNIReader);
                        return x;
                    });
            LOG.debug("Add unsplittable scan range success. partition: {}, file: {}, block: {}-{}",
                    partition.getFullPath(), fileDesc.getFileName(), 0, fileDesc.getLength());
        }
    }

    public void setupScanRangeLocations(DescriptorTable descTbl, Table table,
                                        HDFSScanNodePredicates scanNodePredicates) throws UserException {
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
        List<HivePartition> partitions = hiveMetaStoreTable.getPartitions(partitionKeys);

        if (table instanceof HiveTable) {
            Preconditions.checkState(partitions.size() == partitionKeys.size());
            Map<PartitionKey, HivePartition> partitionMap = Maps.newHashMap();
            for (int index = 0; index < partitions.size(); ++index) {
                partitionMap.put(partitionKeys.get(index), partitions.get(index));
            }
            ConnectContext.get().getDumpInfo().getHMSTable(hiveMetaStoreTable.getResourceName(),
                    hiveMetaStoreTable.getDbName(), hiveMetaStoreTable.getTableName()).addPartitions(partitionMap);

            for (int i = 0; i < partitions.size(); i++) {
                descTbl.addReferencedPartitions(table, partitionInfos.get(i));
                long partitionId = partitionInfos.get(i).getId();
                HivePartition partition = partitions.get(i);
                addHiveHdfsFiles(partitionId, partition, partition.getFiles());
            }

        } else if (table instanceof HudiTable) {
            HudiTable hudiTable = (HudiTable) table;
            String tableInputFormat = hudiTable.getHudiInputFormat();
            boolean morTable = hudiTable.getTableType() == HoodieTableType.MERGE_ON_READ;
            boolean readOptimized = tableInputFormat.equals(HudiTable.MOR_RO_INPUT_FORMAT)
                    || tableInputFormat.equals(HudiTable.MOR_RO_INPUT_FORMAT_LEGACY);
            boolean snapshot = tableInputFormat.equals(HudiTable.MOR_RT_INPUT_FORMAT)
                    || tableInputFormat.equals(HudiTable.MOR_RT_INPUT_FORMAT_LEGACY);
            for (int i = 0; i < partitions.size(); i++) {
                descTbl.addReferencedPartitions(table, partitionInfos.get(i));
                long partitionId = partitionInfos.get(i).getId();
                HivePartition partition = partitions.get(i);
                addHudiHdfsFiles(partitionId, partition, partition.getFiles(), morTable, readOptimized, snapshot);
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
