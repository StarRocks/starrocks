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

package com.starrocks.common.tvr;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;

import static java.util.Optional.ofNullable;

public class TvrDeltaStats {
    public static final TvrDeltaStats EMPTY = new TvrDeltaStats(0, 0);

    private final long addedRows;
    private final long addedFileSize;

    public TvrDeltaStats(long addedRows, long addedFileSize) {
        this.addedRows = addedRows;
        this.addedFileSize = addedFileSize;
    }

    public static TvrDeltaStats of(Long addedRows, Long addedFileSize) {
        long rows = ofNullable(addedRows).orElse(0L);
        long fileSize = ofNullable(addedFileSize).orElse(0L);
        return new TvrDeltaStats(rows, fileSize);
    }

    public static TvrDeltaStats of(Snapshot snapshot) {
        if (snapshot == null || snapshot.summary() == null) {
            return EMPTY;
        }
        Long addedRows = Long.parseLong(snapshot.summary()
                .getOrDefault(SnapshotSummary.ADDED_RECORDS_PROP, "0"));
        Long addedFileSize = Long.parseLong(snapshot.summary()
                .getOrDefault(SnapshotSummary.ADDED_FILE_SIZE_PROP, "0"));
        return of(addedRows, addedFileSize);
    }

    public long getAddedRows() {
        return addedRows;
    }

    public long getAddedFileSize() {
        return addedFileSize;
    }

    @Override
    public String toString() {
        return "Stats{" +
                "addedRows=" + addedRows +
                ", addedFileSize=" + addedFileSize +
                '}';
    }
}
