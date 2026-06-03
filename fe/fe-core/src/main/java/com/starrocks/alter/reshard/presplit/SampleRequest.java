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
 */
public final class SampleRequest {
    private final ScanContext scanContext;
    private final List<Column> sortKey;
    private final long sampleByteLimit;
    private final long seed;

    public SampleRequest(ScanContext scanContext, List<Column> sortKey, long sampleByteLimit, long seed) {
        this.scanContext = Objects.requireNonNull(scanContext, "scanContext");
        Objects.requireNonNull(sortKey, "sortKey");
        Preconditions.checkArgument(!sortKey.isEmpty(), "sortKey must be non-empty");
        Preconditions.checkArgument(sampleByteLimit > 0,
                "sampleByteLimit must be positive, was %s", sampleByteLimit);
        this.sortKey = ImmutableList.copyOf(sortKey);
        this.sampleByteLimit = sampleByteLimit;
        this.seed = seed;
    }

    public ScanContext getScanContext() {
        return scanContext;
    }

    public List<Column> getSortKey() {
        return sortKey;
    }

    public long getSampleByteLimit() {
        return sampleByteLimit;
    }

    public long getSeed() {
        return seed;
    }
}
