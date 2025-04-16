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
package com.starrocks.connector.delta;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ConnectorScanRangeSource;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.thrift.TDeletionVectorDescriptor;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

public class DeltaConnectorScanRangeSource extends ConnectorScanRangeSource {
    private static final Logger LOG = LogManager.getLogger(DeltaConnectorScanRangeSource.class);
    private DeltaLakeTable table;
    private RemoteFileInfoSource remoteFileInfoSource;
    private RemoteFileInputFormat remoteFileInputFormat;
    private final AtomicLong partitionIdGen = new AtomicLong(0L);
    private Map<PartitionKey, Long> partitionKeys = new HashMap<>();
    private Map<Long, DescriptorTable.ReferencedPartitionInfo> referencedPartitions = new HashMap<>();

    public DeltaConnectorScanRangeSource(DeltaLakeTable table, RemoteFileInfoSource remoteFileInfoSource) {
        this.table = table;
        this.remoteFileInfoSource = remoteFileInfoSource;
        this.remoteFileInputFormat = DeltaUtils.getRemoteFileFormat(table.getDeltaMetadata().getFormat().getProvider());
    }

    private long addPartition(FileScanTask fileScanTask) throws AnalysisException {
        List<String> partitionValues = new ArrayList<>();
        table.getPartitionColumnNames().forEach(column -> {
            partitionValues.add(fileScanTask.getPartitionValues().get(column));
        });
        PartitionKey partitionKey = PartitionUtil.createPartitionKey(partitionValues,
                table.getPartitionColumns(), table);
        if (partitionKeys.containsKey(partitionKey)) {
            return partitionKeys.get(partitionKey);
        }

        long partitionId = partitionIdGen.getAndIncrement();
        FileStatus fileStatus = fileScanTask.getFileStatus();
        Path filePath = new Path(URLDecoder.decode(fileStatus.getPath(), StandardCharsets.UTF_8));
        DescriptorTable.ReferencedPartitionInfo referencedPartitionInfo =
                new DescriptorTable.ReferencedPartitionInfo(partitionId, partitionKey,
                        filePath.getParent().toString());
        partitionKeys.put(partitionKey, partitionId);
        referencedPartitions.put(partitionId, referencedPartitionInfo);
        return partitionId;
    }

    private TScanRangeLocations toScanRange(FileScanTask fileScanTask) {
        FileStatus fileStatus = fileScanTask.getFileStatus();

        long partitionId = -1;
        try {
            partitionId = addPartition(fileScanTask);
        } catch (AnalysisException e) {
            throw new StarRocksConnectorException("add scan range partition failed", e);
        }
        DescriptorTable.ReferencedPartitionInfo referencedPartitionInfo = referencedPartitions.get(partitionId);
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        if (fileStatus.getPath().contains(table.getTableLocation())) {
            hdfsScanRange.setRelative_path(URLDecoder.decode("/" + Paths.get(table.getTableLocation()).
                    relativize(Paths.get(fileStatus.getPath())), StandardCharsets.UTF_8));
        } else {
            hdfsScanRange.setFull_path(URLDecoder.decode(fileStatus.getPath(), StandardCharsets.UTF_8));
        }
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(fileStatus.getSize());
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileStatus.getSize());
        hdfsScanRange.setFile_format(remoteFileInputFormat.toThrift());
        hdfsScanRange.setPartition_value(table.toHdfsPartition(referencedPartitionInfo));
        hdfsScanRange.setTable_id(table.getId());
        // serialize dv
        if (fileScanTask.getDv() != null) {
            TDeletionVectorDescriptor dv = new TDeletionVectorDescriptor();
            dv.setStorageType(fileScanTask.getDv().getStorageType());
            dv.setPathOrInlineDv(fileScanTask.getDv().getPathOrInlineDv());
            dv.setOffset(fileScanTask.getDv().getOffset().orElse(0));
            dv.setSizeInBytes(fileScanTask.getDv().getSizeInBytes());
            dv.setCardinality(fileScanTask.getDv().getCardinality());
            hdfsScanRange.setDeletion_vector_descriptor(dv);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);
        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);
        return scanRangeLocations;
    }

    @Override
    public List<TScanRangeLocations> getSourceOutputs(int maxSize) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "DeltaLake.getScanFiles")) {
            List<TScanRangeLocations> res = new ArrayList<>();
            while (hasMoreOutput() && res.size() < maxSize) {
                DeltaRemoteFileInfo remoteFileInfo = (DeltaRemoteFileInfo) remoteFileInfoSource.getOutput();
                res.add(toScanRange(remoteFileInfo.getFileScanTask()));
            }
            return res;
        }
    }

    @Override
    public boolean sourceHasMoreOutput() {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "DeltaLake.getScanFiles")) {
            return remoteFileInfoSource.hasMoreOutput();
        }
    }

    public int selectedPartitionCount() {
        return partitionKeys.size();
    }
}
