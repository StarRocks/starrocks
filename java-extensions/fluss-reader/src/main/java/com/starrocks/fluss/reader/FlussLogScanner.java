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

public class FlussLogScanner extends ConnectorScannerProxy {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    private final Table table;
    private final LogSplit logSplit;
    private final int[] projectedFields;
    private LogScanner logScanner;
    private Iterator<ScanRecord> currentBatch;
    private boolean finished = false;
    private final long stoppingOffset;

    public FlussLogScanner(Table table, LogSplit logSplit, int[] projectedFields) {
        this.table = table;
        this.logSplit = logSplit;
        this.projectedFields = projectedFields;
        this.stoppingOffset = logSplit.getStoppingOffset().orElse(LogScanner.NO_STOPPING_OFFSET);
    }

    @Override
    void open() throws IOException {
        logScanner = table.newScan().project(projectedFields).createLogScanner();
        TableBucket tableBucket = logSplit.getTableBucket();
        if (tableBucket.getPartitionId() != null) {
            logScanner.subscribe(tableBucket.getPartitionId(),
                    tableBucket.getBucket(), logSplit.getStartingOffset());
        } else {
            logScanner.subscribe(tableBucket.getBucket(), logSplit.getStartingOffset());
        }
    }

    @Override
    boolean hasNext() throws IOException {
        if (finished) {
            return false;
        }
        if (currentBatch != null && currentBatch.hasNext()) {
            return true;
        }
        ScanRecords records = logScanner.poll(POLL_TIMEOUT);
        if (records.isEmpty()) {
            finished = true;
            return false;
        }
        currentBatch = records.iterator();
        return currentBatch.hasNext();
    }

    @Override
    InternalRow getNext() throws IOException {
        ScanRecord record = currentBatch.next();
        if (stoppingOffset != LogScanner.NO_STOPPING_OFFSET
                && record.logOffset() >= stoppingOffset - 1) {
            finished = true;
        }
        return record.getRow();
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
