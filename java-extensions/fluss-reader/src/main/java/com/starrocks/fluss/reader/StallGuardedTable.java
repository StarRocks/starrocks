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

import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.TypedLogScanner;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.Predicate;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A table whose log scanners are {@link StallGuardedLogScanner}s, for Fluss code that creates its log scanner
 * itself.
 */
final class StallGuardedTable implements Table {
    private final Table delegate;
    private final String readDescription;
    private final Duration maxStall;

    StallGuardedTable(Table delegate, String readDescription, Duration maxStall) {
        this.delegate = delegate;
        this.readDescription = readDescription;
        this.maxStall = maxStall;
    }

    @Override
    public TableInfo getTableInfo() {
        return delegate.getTableInfo();
    }

    @Override
    public Scan newScan() {
        return new GuardedScan(delegate.newScan());
    }

    @Override
    public Lookup newLookup() {
        return delegate.newLookup();
    }

    @Override
    public Append newAppend() {
        return delegate.newAppend();
    }

    @Override
    public Upsert newUpsert() {
        return delegate.newUpsert();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    private final class GuardedScan implements Scan {
        private final Scan scan;

        private GuardedScan(Scan scan) {
            this.scan = scan;
        }

        @Override
        public Scan project(@Nullable int[] projectedColumns) {
            return new GuardedScan(scan.project(projectedColumns));
        }

        @Override
        public Scan project(List<String> projectedColumnNames) {
            return new GuardedScan(scan.project(projectedColumnNames));
        }

        @Override
        public Scan limit(int rowNumber) {
            return new GuardedScan(scan.limit(rowNumber));
        }

        @Override
        public Scan filter(@Nullable Predicate predicate) {
            return new GuardedScan(scan.filter(predicate));
        }

        @Override
        public LogScanner createLogScanner() {
            return new StallGuardedLogScanner(scan.createLogScanner(), readDescription, maxStall);
        }

        @Override
        public <T> TypedLogScanner<T> createTypedLogScanner(Class<T> pojoClass) {
            return scan.createTypedLogScanner(pojoClass);
        }

        @Override
        public BatchScanner createBatchScanner(TableBucket tableBucket) {
            return scan.createBatchScanner(tableBucket);
        }

        @Override
        public BatchScanner createBatchScanner(TableBucket tableBucket, long snapshotId) {
            return scan.createBatchScanner(tableBucket, snapshotId);
        }

        @Override
        public BatchScanner createBatchScanner() throws IOException {
            return scan.createBatchScanner();
        }
    }
}
