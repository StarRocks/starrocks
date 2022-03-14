// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external;

import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.external.hive.HdfsFileBlockDesc;
import com.starrocks.external.hive.HdfsFileDesc;
import com.starrocks.external.hive.HdfsFileFormat;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RemoteScanRangeLocations {
    private static final Logger LOG = LogManager.getLogger(RemoteScanRangeLocations.class);

    private final List<TScanRangeLocations> result = new ArrayList<>();

    private void addScanRangeLocations(long partitionId, HdfsFileDesc fileDesc, HdfsFileBlockDesc blockDesc,
                               HdfsFileFormat fileFormat) {
        // NOTE: Config.hive_max_split_size should be extracted to a local variable,
        // because it may be changed before calling 'splitScanRangeLocations'
        // and after needSplit has been calculated.
        long splitSize = Config.hive_max_split_size;
        boolean needSplit = fileDesc.isSplittable() && blockDesc.getLength() > splitSize;
        if (needSplit) {
            splitScanRangeLocations(partitionId, fileDesc, blockDesc, fileFormat, splitSize);
        } else {
            createScanRangeLocationsForSplit(partitionId, fileDesc, blockDesc,
                    fileFormat, blockDesc.getOffset(), blockDesc.getLength());
        }
    }

    private void splitScanRangeLocations(long partitionId,
                                         HdfsFileDesc fileDesc,
                                         HdfsFileBlockDesc blockDesc,
                                         HdfsFileFormat fileFormat,
                                         long splitSize) {
        long remainingBytes = blockDesc.getLength();
        long length = blockDesc.getLength();
        long offset = blockDesc.getOffset();
        do {
            if (remainingBytes <= splitSize) {
                createScanRangeLocationsForSplit(partitionId, fileDesc,
                        blockDesc, fileFormat, offset + length - remainingBytes,
                        remainingBytes);
                remainingBytes = 0;
            } else if (remainingBytes <= 2 * splitSize) {
                long mid = (remainingBytes + 1) / 2;
                createScanRangeLocationsForSplit(partitionId, fileDesc,
                        blockDesc, fileFormat, offset + length - remainingBytes, mid);
                createScanRangeLocationsForSplit(partitionId, fileDesc,
                        blockDesc, fileFormat, offset + length - remainingBytes + mid,
                        remainingBytes - mid);
                remainingBytes = 0;
            } else {
                createScanRangeLocationsForSplit(partitionId, fileDesc,
                        blockDesc, fileFormat, offset + length - remainingBytes,
                        splitSize);
                remainingBytes -= splitSize;
            }
        } while (remainingBytes > 0);
    }

    private void createScanRangeLocationsForSplit(long partitionId,
                                                  HdfsFileDesc fileDesc,
                                                  HdfsFileBlockDesc blockDesc,
                                                  HdfsFileFormat fileFormat,
                                                  long offset, long length) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path(fileDesc.getFileName());
        hdfsScanRange.setOffset(offset);
        hdfsScanRange.setLength(length);
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileDesc.getLength());
        hdfsScanRange.setFile_format(fileFormat.toThrift());
        hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        for (long hostId : blockDesc.getReplicaHostIds()) {
            String host = blockDesc.getDataNodeIp(hostId);
            TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(host, -1));
            scanRangeLocations.addToLocations(scanRangeLocation);
        }

        result.add(scanRangeLocations);
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

        for (int i = 0; i < partitions.size(); i++) {
            descTbl.addReferencedPartitions(table, partitionInfos.get(i));
            for (HdfsFileDesc fileDesc : partitions.get(i).getFiles()) {
                for (HdfsFileBlockDesc blockDesc : fileDesc.getBlockDescs()) {
                    addScanRangeLocations(partitionInfos.get(i).getId(), fileDesc, blockDesc,
                            partitions.get(i).getFormat());
                    LOG.debug("Add scan range success. partition: {}, file: {}, block: {}-{}",
                            partitions.get(i).getFullPath(), fileDesc.getFileName(), blockDesc.getOffset(),
                            blockDesc.getLength());
                }
            }
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
