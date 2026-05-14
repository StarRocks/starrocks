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
import com.starrocks.catalog.Tuple;

/**
 * Per-row-group statistics for one Parquet (or ORC) row group on the
 * sort-key columns. Fields are final, but the {@link Tuple} value contents
 * are NOT deep-copied — the provider must pass stable Tuple instances
 * (Parquet footer projections are read-once, so this is a natural fit).
 *
 * <p>{@code minTuple} / {@code maxTuple} are nullable to model row groups
 * whose footer carries no statistics, or null-only statistics, or
 * truncated string statistics — all conditions that
 * {@link ParquetMetadataSampler} surfaces as
 * {@link Tier1UnavailableException}.
 */
public final class RowGroupStatistics {
    private final Tuple minTuple;
    private final Tuple maxTuple;
    private final long rowCount;
    private final boolean truncated;

    public RowGroupStatistics(Tuple minTuple, Tuple maxTuple, long rowCount, boolean truncated) {
        Preconditions.checkArgument(rowCount >= 0, "rowCount must be non-negative, was %s", rowCount);
        this.minTuple = minTuple;
        this.maxTuple = maxTuple;
        this.rowCount = rowCount;
        this.truncated = truncated;
    }

    public Tuple getMinTuple() {
        return minTuple;
    }

    public Tuple getMaxTuple() {
        return maxTuple;
    }

    public long getRowCount() {
        return rowCount;
    }

    /**
     * @return true when the source (e.g. Parquet) truncated this row group's
     *         string min/max for length, making the bounds unreliable for
     *         quantile selection. Triggers Tier 1 fallback.
     */
    public boolean isTruncated() {
        return truncated;
    }
}
