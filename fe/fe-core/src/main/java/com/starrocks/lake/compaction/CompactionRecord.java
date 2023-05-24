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

package com.starrocks.lake.compaction;

import java.util.Objects;
import java.util.Optional;

public class CompactionRecord {
    private final long txnId;
    private final long startTs;
    private final long commitTs;
    private final long finishTs;
    private final String partitionName;
    private final String errorMessage;

    private CompactionRecord(CompactionJob context, String errorMessage) {
        Objects.requireNonNull(context.getFullPartitionName());
        this.txnId = context.getTxnId();
        this.startTs = context.getStartTs();
        this.commitTs = context.getCommitTs();
        this.finishTs = context.getFinishTs();
        this.partitionName = context.getFullPartitionName();
        this.errorMessage = errorMessage;
    }

    static CompactionRecord build(CompactionJob context) {
        return new CompactionRecord(context, null);
    }

    static CompactionRecord build(CompactionJob context, String errorMessage) {
        return new CompactionRecord(context, errorMessage);
    }

    public long getTxnId() {
        return txnId;
    }

    public long getStartTs() {
        return startTs;
    }

    public Optional<Long> getCommitTs() {
        return commitTs > 0 ? Optional.of(commitTs) : Optional.empty();
    }

    public Optional<Long> getFinishTs() {
        return finishTs > 0 ? Optional.of(finishTs) : Optional.empty();
    }

    public String getPartitionName() {
        return partitionName;
    }

    public Optional<String> getErrorMessage() {
        return errorMessage != null ? Optional.of(errorMessage) : Optional.empty();
    }
}
