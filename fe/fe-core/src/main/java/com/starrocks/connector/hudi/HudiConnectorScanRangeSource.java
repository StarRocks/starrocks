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
package com.starrocks.connector.hudi;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.hive.HiveConnectorScanRangeSource;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TDataCacheOptions;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HudiConnectorScanRangeSource extends HiveConnectorScanRangeSource {
    private static final Logger LOG = LogManager.getLogger(HudiConnectorScanRangeSource.class);
    private boolean morTable;
    private boolean readOptimized;
    private boolean snapshot;
    private boolean forceJNIReader = false;

    public HudiConnectorScanRangeSource(DescriptorTable descriptorTable, Table table, HDFSScanNodePredicates scanNodePredicates) {
        super(descriptorTable, table, scanNodePredicates);
    }

    @Override
    protected void initRemoteFileInfoSource() {
        super.initRemoteFileInfoSource();
        HudiTable hudiTable = (HudiTable) table;
        String tableInputFormat = hudiTable.getHudiInputFormat();
        morTable = hudiTable.getTableType() == HoodieTableType.MERGE_ON_READ;
        readOptimized = tableInputFormat.equals(HudiTable.MOR_RO_INPUT_FORMAT)
                || tableInputFormat.equals(HudiTable.MOR_RO_INPUT_FORMAT_LEGACY);
        snapshot = tableInputFormat.equals(HudiTable.MOR_RT_INPUT_FORMAT)
                || tableInputFormat.equals(HudiTable.MOR_RT_INPUT_FORMAT_LEGACY);
        if (ConnectContext.get() != null) {
            forceJNIReader = ConnectContext.get().getSessionVariable().getHudiMORForceJNIReader();
        }
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
            HudiRemoteFileDesc hudiFiledesc = (HudiRemoteFileDesc) fileDesc;
            if (fileDesc.getLength() == -1 && hudiFiledesc.getHudiDeltaLogs().isEmpty()) {
                String message = "Error: get a empty hudi fileSlice";
                throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
            }
            // ignore the scan range when read optimized mode and file slices contain logs only
            if (morTable && readOptimized && fileDesc.getLength() == -1 && fileDesc.getFileName().isEmpty()) {
                return;
            }
            createHudiScanRangeLocations(hudiFiledesc);
        }

        private void createHudiScanRangeLocations(HudiRemoteFileDesc fileDesc) {
            boolean useJNIReader =
                    forceJNIReader || (morTable && snapshot && !fileDesc.getHudiDeltaLogs().isEmpty());
            PartitionAttachment attachment = (PartitionAttachment) remoteFileInfo.getAttachment();
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

            THdfsScanRange hdfsScanRange = new THdfsScanRange();
            hdfsScanRange.setRelative_path(fileDesc.getFileName());
            hdfsScanRange.setOffset(0);
            hdfsScanRange.setLength(fileDesc.getLength());
            hdfsScanRange.setPartition_id(attachment.partitionId);
            hdfsScanRange.setFile_length(fileDesc.getLength());

            RemoteFileInputFormat fileFormat = remoteFileInfo.getFormat();
            hdfsScanRange.setFile_format(fileFormat.toThrift());
            if (fileFormat.isTextFormat()) {
                hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
            }
            for (String log : fileDesc.getHudiDeltaLogs()) {
                hdfsScanRange.addToHudi_logs(log);
            }
            hdfsScanRange.setUse_hudi_jni_reader(useJNIReader);
            if (attachment.dataCacheOptions != null) {
                TDataCacheOptions tDataCacheOptions = new TDataCacheOptions();
                tDataCacheOptions.setPriority(attachment.dataCacheOptions.getPriority());
                hdfsScanRange.setDatacache_options(tDataCacheOptions);
            }

            TScanRange scanRange = new TScanRange();
            scanRange.setHdfs_scan_range(hdfsScanRange);
            scanRangeLocations.setScan_range(scanRange);

            // TODO: get block info
            TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
            scanRangeLocations.addToLocations(scanRangeLocation);

            buffer.add(scanRangeLocations);
        }
    }

    @Override
    public Iterator<TScanRangeLocations> createScanRangeIterator(RemoteFileInfo remoteFileInfo) {
        return new ScanRangeIterator(remoteFileInfo);
    }
}
