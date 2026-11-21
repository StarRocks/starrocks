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

import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.time.Duration;

/** Reads one split through a Fluss {@link BatchScanner}, which returns null once the split is read. */
public class FlussBatchScanner extends ConnectorScannerProxy {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    private final BatchScanner batchScanner;
    private CloseableIterator<InternalRow> currentBatch;
    private boolean finished = false;

    public FlussBatchScanner(BatchScanner batchScanner) {
        this.batchScanner = batchScanner;
    }

    @Override
    void open() {
        // The Fluss scanner is ready to read once constructed.
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

            // An empty batch is transient; only null marks the end of input.
            currentBatch = batchScanner.pollBatch(POLL_TIMEOUT);
            if (currentBatch == null) {
                finished = true;
                return false;
            }
        }
    }

    @Override
    InternalRow getNext() throws IOException {
        return currentBatch.next();
    }

    @Override
    void close() throws IOException {
        batchScanner.close();
    }
}
