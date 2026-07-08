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

import org.apache.fluss.flink.lake.reader.LakeSnapshotScanner;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.time.Duration;

public class FlussSnapshotScanner extends ConnectorScannerProxy {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    private final LakeSource<LakeSplit> lakeSource;
    private final LakeSnapshotSplit lakeSnapshotSplit;
    private LakeSnapshotScanner batchScanner;
    private CloseableIterator<InternalRow> currentBatch;
    private boolean finished = false;

    public FlussSnapshotScanner(LakeSource<LakeSplit> lakeSource,
                                LakeSnapshotSplit lakeSnapshotSplit) {
        this.lakeSource = lakeSource;
        this.lakeSnapshotSplit = lakeSnapshotSplit;
    }

    @Override
    void open() throws IOException {
        batchScanner = new LakeSnapshotScanner(lakeSource, lakeSnapshotSplit);
    }

    @Override
    boolean hasNext() throws IOException {
        if (finished) {
            return false;
        }
        if (currentBatch != null && currentBatch.hasNext()) {
            return true;
        }
        currentBatch = batchScanner.pollBatch(POLL_TIMEOUT);
        if (currentBatch == null) {
            finished = true;
            return false;
        }
        return currentBatch.hasNext();
    }

    @Override
    InternalRow getNext() throws IOException {
        return currentBatch.next();
    }

    @Override
    void close() throws IOException {
        if (batchScanner != null) {
            batchScanner.close();
        }
    }
}
