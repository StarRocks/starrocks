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

import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Fails a bounded log read that stops making progress.
 *
 * <p>Every offset the reader polls for exists: a split stops at an offset Fluss had already written when the query
 * was planned. Polls still come back empty when the Fluss client cannot fetch the log, for example a segment in
 * remote storage it has no file system for. The client retries that download without limit and reports no error,
 * so without this check the read would poll until the query timed out, and keep polling after it was cancelled.
 *
 * <p>Only time spent in polls that bring no progress counts toward the limit. The time between polls, while
 * StarRocks processes the rows already read or waits for downstream operators, does not.
 */
final class StallGuardedLogScanner implements LogScanner {
    // Well above a normal pause, such as a bucket leader moving or a remote segment downloading, and below the
    // default query timeout.
    static final Duration MAX_STALL = Duration.ofMinutes(2);

    private final LogScanner delegate;
    private final String readDescription;
    private final long maxStallNanos;
    private final LongSupplier nanoClock;
    private long stalledNanos;

    StallGuardedLogScanner(LogScanner delegate, String readDescription, Duration maxStall) {
        this(delegate, readDescription, maxStall, System::nanoTime);
    }

    StallGuardedLogScanner(LogScanner delegate, String readDescription, Duration maxStall, LongSupplier nanoClock) {
        this.delegate = delegate;
        this.readDescription = readDescription;
        this.maxStallNanos = maxStall.toNanos();
        this.nanoClock = nanoClock;
    }

    static String describeRead(TableBucket bucket, long startingOffset, long stoppingOffset) {
        return String.format("Fluss bucket %s from offset %d to %d", bucket, startingOffset, stoppingOffset);
    }

    @Override
    public ScanRecords poll(Duration timeout) {
        long start = nanoClock.getAsLong();
        ScanRecords records = delegate.poll(timeout);
        if (records.hasProgress()) {
            stalledNanos = 0;
            return records;
        }
        stalledNanos += nanoClock.getAsLong() - start;
        if (stalledNanos >= maxStallNanos) {
            throw new IllegalStateException(String.format(
                    "Read nothing from %s in %d seconds of polling. The Fluss client may be unable to fetch the " +
                            "log, for example a segment in remote storage that it has no file system for.",
                    readDescription, TimeUnit.NANOSECONDS.toSeconds(stalledNanos)));
        }
        return records;
    }

    @Override
    public void subscribe(int bucket, long offset) {
        delegate.subscribe(bucket, offset);
    }

    @Override
    public void subscribe(long partitionId, int bucket, long offset) {
        delegate.subscribe(partitionId, bucket, offset);
    }

    @Override
    public void unsubscribe(long partitionId, int bucket) {
        delegate.unsubscribe(partitionId, bucket);
    }

    @Override
    public void unsubscribe(int bucket) {
        delegate.unsubscribe(bucket);
    }

    @Override
    public void wakeup() {
        delegate.wakeup();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
