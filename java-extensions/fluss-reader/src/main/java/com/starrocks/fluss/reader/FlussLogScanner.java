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

package com.starrocks.fluss.reader;

import com.starrocks.jni.connector.ColumnValue;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.starrocks.fluss.reader.FlussSplitScanner.DEFAULT_POLL_TIMEOUT;

public class FlussLogScanner extends ConnectorScannerProxy  {
    private static final Logger LOG = LogManager.getLogger(FlussLogScanner.class);

    private final int fetchSize;
    private final String[] requiredFields;
    private final LogSplit logSplit;
    private final Table flussTable;
    private final String timeZone;

    private LogScanner logScanner;
    private Iterator<ScanRecord> iterator;
    private long stoppingOffset;
    private TableBucket tableBucket;
    private boolean needStop = false;

    public FlussLogScanner(int fetchSize, String[] requiredFields, SourceSplitBase split, Table flussTable, String timeZone) {
        this.fetchSize = fetchSize;
        this.requiredFields = requiredFields;
        this.logSplit = (LogSplit) split;
        this.flussTable = flussTable;
        this.timeZone = timeZone;
    }

    private void initReader() {
        this.logScanner = this.flussTable.newScan().project(Arrays.asList(this.requiredFields)).createLogScanner();
        long startingOffset = this.logSplit.getStartingOffset();
        this.stoppingOffset = this.logSplit.getStoppingOffset().orElse(Long.MAX_VALUE);
        this.tableBucket = this.logSplit.getTableBucket();
        Long partitionId = this.tableBucket.getPartitionId();
        int bucket = this.tableBucket.getBucket();
        if (partitionId != null) {
            this.logScanner.subscribe(partitionId, bucket, startingOffset);
        } else {
            this.logScanner.subscribe(bucket, startingOffset);
        }
    }

    @Override
    public void openProxy(FlussSplitScanner parent) {
        initReader();
        parent.initOffHeapTableWriter(parent.getRequiredTypes(), requiredFields, fetchSize);
    }

    @Override
    public int getNextProxy(FlussSplitScanner parent) {
        if (this.needStop) {
            return 0;
        }
        if (this.iterator == null || !iterator.hasNext()) {
            ScanRecords scanRecords = this.logScanner.poll(DEFAULT_POLL_TIMEOUT);
            List<ScanRecord> bucketScanRecords = new ArrayList<>(scanRecords.records(this.tableBucket));
            if (!bucketScanRecords.isEmpty()) {
                ScanRecord lastRecord = bucketScanRecords.get(bucketScanRecords.size() - 1);
                if (lastRecord.logOffset() >= stoppingOffset) {
                    for (int i = bucketScanRecords.size() - 1; i >= 0; i--) {
                        if (bucketScanRecords.get(i).logOffset() >= stoppingOffset) {
                            bucketScanRecords.remove(i);
                        }
                    }
                }
                if (lastRecord.logOffset() == stoppingOffset - 1) {
                    this.needStop = true;
                }
                this.iterator = bucketScanRecords.iterator();
            } else {
                this.needStop = true;
                return 0;
            }
        }
        int numRows = 0;
        while (iterator.hasNext() && numRows < fetchSize) {
            InternalRow row = iterator.next().getRow();
            if (row == null) {
                break;
            }
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = parent.getFlussFieldGetters()[i].getFieldOrNull(row);
                if (fieldData == null) {
                    parent.appendData(i, null);
                } else {
                    ColumnValue fieldValue = new FlussColumnValue(fieldData, parent.getLogicalTypes()[i], timeZone);
                    parent.appendData(i, fieldValue);
                }
            }
            numRows++;
        }
        return numRows;
    }

    @Override
    public void closeProxy(FlussSplitScanner parent) {
        try {
            if (logScanner != null) {
                logScanner.close();
            }
            this.flussTable.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
