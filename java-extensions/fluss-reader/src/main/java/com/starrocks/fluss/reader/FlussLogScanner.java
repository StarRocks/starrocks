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

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;

public class FlussLogScanner extends ConnectorScannerProxy {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    private final Table table;
    private final LogSplit logSplit;
    private final int[] projectedFields;
    private LogScanner logScanner;
    private Iterator<ScanRecord> currentBatch;
    private boolean finished = false;
    private final long stoppingOffset;
    private TableBucket tableBucket;

    public FlussLogScanner(Table table, LogSplit logSplit, int[] projectedFields) {
        this.table = table;
        this.logSplit = logSplit;
        this.projectedFields = projectedFields;
        this.stoppingOffset = logSplit.getStoppingOffset().orElseThrow(
                () -> new IllegalArgumentException("Stopping offset is required for Fluss log split: " + logSplit));
    }

    @Override
    void open() throws IOException {
        tableBucket = logSplit.getTableBucket();
        if (stoppingOffset <= 0 || logSplit.getStartingOffset() >= stoppingOffset) {
            finished = true;
            return;
        }

        logScanner = table.newScan().project(projectedFields).createLogScanner();
        if (tableBucket.getPartitionId() != null) {
            logScanner.subscribe(tableBucket.getPartitionId(),
                    tableBucket.getBucket(), logSplit.getStartingOffset());
        } else {
            logScanner.subscribe(tableBucket.getBucket(), logSplit.getStartingOffset());
        }
    }

    @Override
    boolean hasNext() throws IOException {
        while (true) {
            if (currentBatch != null && currentBatch.hasNext()) {
                return true;
            }
            if (finished) {
                return false;
            }

            ScanRecords records = logScanner.poll(POLL_TIMEOUT);
            List<ScanRecord> bucketRecords = records.records(tableBucket);
            if (bucketRecords.isEmpty()) {
                // An empty poll may be a timeout; keep polling until the stopping offset is reached.
                continue;
            }
            currentBatch = trimToStoppingOffset(bucketRecords).iterator();
        }
    }

    @Override
    InternalRow getNext() throws IOException {
        ScanRecord record = currentBatch.next();
        return record.getRow();
    }

    List<ScanRecord> trimToStoppingOffset(List<ScanRecord> records) {
        ScanRecord lastRecord = records.get(records.size() - 1);
        if (lastRecord.logOffset() < stoppingOffset - 1) {
            return records;
        }

        finished = true;
        if (lastRecord.logOffset() < stoppingOffset) {
            return records;
        }

        int endIndex = records.size();
        while (endIndex > 0 && records.get(endIndex - 1).logOffset() >= stoppingOffset) {
            endIndex--;
        }
        return records.subList(0, endIndex);
    }

    @Override
    void close() throws IOException {
        if (logScanner != null) {
            try {
                logScanner.close();
            } catch (Exception e) {
                throw new IOException("Failed to close log scanner", e);
            }
        }
    }
}
