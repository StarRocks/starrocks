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

package com.starrocks.alter.reshard.presplit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;

import java.util.List;
import java.util.Objects;

/**
 * Input to a {@link Sampler}. Immutable. {@code sampleByteLimit} is a soft
 * limit — the sampler is allowed to stop short of reading the full input.
 *
 * <p>{@code partitionSourceColumns} are additional source-table columns the
 * sampler should project alongside the sort key when the target is
 * partitioned. They flow through to the per-row partition-source tuple in
 * {@link SampleSet#getPartitionSourceTuples()} so a downstream multi-partition
 * grouper can split the sample by source-partition before computing per-group
 * boundaries. Empty for unpartitioned targets (no projection extension, no
 * tuple population) — existing callers use the four-arg constructor and see
 * no behavior change.
 */
public final class SampleRequest {
    private final ScanContext scanContext;
    private final List<Column> sortKey;
    private final List<Column> partitionSourceColumns;
    private final long sampleByteLimit;
    private final long seed;
    // Wall-clock cap for the BE-side sampling sub-query, in seconds; 0 = no cap
    // (the BE applies its own default query_timeout). Set via withQueryTimeoutSeconds.
    private final int queryTimeoutSeconds;

    public SampleRequest(ScanContext scanContext, List<Column> sortKey,
                         List<Column> partitionSourceColumns, long sampleByteLimit, long seed) {
        this(scanContext, sortKey, partitionSourceColumns, sampleByteLimit, seed, /*queryTimeoutSeconds=*/ 0);
    }

    /**
     * Backwards-compatible constructor for unpartitioned-target callers; defaults
     * {@code partitionSourceColumns} to an empty list.
     */
    public SampleRequest(ScanContext scanContext, List<Column> sortKey, long sampleByteLimit, long seed) {
        this(scanContext, sortKey, ImmutableList.of(), sampleByteLimit, seed);
    }

    private SampleRequest(ScanContext scanContext, List<Column> sortKey, List<Column> partitionSourceColumns,
                          long sampleByteLimit, long seed, int queryTimeoutSeconds) {
        this.scanContext = Objects.requireNonNull(scanContext, "scanContext");
        Objects.requireNonNull(sortKey, "sortKey");
        Objects.requireNonNull(partitionSourceColumns, "partitionSourceColumns");
        Preconditions.checkArgument(!sortKey.isEmpty(), "sortKey must be non-empty");
        Preconditions.checkArgument(sampleByteLimit > 0,
                "sampleByteLimit must be positive, was %s", sampleByteLimit);
        Preconditions.checkArgument(queryTimeoutSeconds >= 0,
                "queryTimeoutSeconds must be non-negative, was %s", queryTimeoutSeconds);
        this.sortKey = ImmutableList.copyOf(sortKey);
        this.partitionSourceColumns = ImmutableList.copyOf(partitionSourceColumns);
        this.sampleByteLimit = sampleByteLimit;
        this.seed = seed;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    /**
     * Returns a copy with the sampling sub-query's wall-clock cap set to
     * {@code queryTimeoutSeconds}; all other fields carry through. Used by the
     * data-tier pipeline to bound the sample to the remaining pre-submit budget.
     */
    public SampleRequest withQueryTimeoutSeconds(int queryTimeoutSeconds) {
        return new SampleRequest(scanContext, sortKey, partitionSourceColumns,
                sampleByteLimit, seed, queryTimeoutSeconds);
    }

    public ScanContext getScanContext() {
        return scanContext;
    }

    public List<Column> getSortKey() {
        return sortKey;
    }

    public List<Column> getPartitionSourceColumns() {
        return partitionSourceColumns;
    }

    public long getSampleByteLimit() {
        return sampleByteLimit;
    }

    public long getSeed() {
        return seed;
    }

    public int getQueryTimeoutSeconds() {
        return queryTimeoutSeconds;
    }
}
