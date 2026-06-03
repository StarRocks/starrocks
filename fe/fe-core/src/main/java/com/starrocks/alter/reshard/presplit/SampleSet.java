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
import com.starrocks.catalog.Tuple;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Output of a {@link Sampler}. The tuple lists are defensively copied at
 * construction; {@link Tuple} value contents are NOT deep-copied, so callers
 * are responsible for providing stable {@code Tuple} instances (the production
 * {@link ReservoirSampler} does this when reading from an executor that may
 * reuse row buffers).
 *
 * <p>{@code tuples} carries one sort-key tuple per sampled row.
 * {@code partitionSourceTuples} carries the parallel per-row partition-source
 * tuple list: same size as {@code tuples} (1:1 by index) when populated, or
 * empty when the underlying request projected no partition-source columns.
 *
 * <p>{@link #EMPTY} represents a successful sample of zero rows — distinct
 * from "sampler failed", which the sampler signals by throwing.
 */
public final class SampleSet {
    public static final SampleSet EMPTY = new SampleSet(Collections.emptyList(), Estimates.ZERO);

    private final List<Tuple> tuples;
    private final List<Tuple> partitionSourceTuples;
    private final Estimates estimates;

    public SampleSet(List<Tuple> tuples, Estimates estimates) {
        this(tuples, Collections.emptyList(), estimates);
    }

    public SampleSet(List<Tuple> tuples, List<Tuple> partitionSourceTuples, Estimates estimates) {
        Objects.requireNonNull(tuples, "tuples");
        Objects.requireNonNull(partitionSourceTuples, "partitionSourceTuples");
        Preconditions.checkArgument(
                partitionSourceTuples.isEmpty() || partitionSourceTuples.size() == tuples.size(),
                "partitionSourceTuples must be either empty or the same size as tuples (%s vs %s)",
                partitionSourceTuples.size(), tuples.size());
        this.tuples = ImmutableList.copyOf(tuples);
        this.partitionSourceTuples = ImmutableList.copyOf(partitionSourceTuples);
        this.estimates = Objects.requireNonNull(estimates, "estimates");
    }

    public List<Tuple> getTuples() {
        return tuples;
    }

    /**
     * Parallel-by-index partition-source tuples. Empty when the originating
     * {@link SampleRequest} carried no {@code partitionSourceColumns}; otherwise
     * the same size as {@link #getTuples()}.
     */
    public List<Tuple> getPartitionSourceTuples() {
        return partitionSourceTuples;
    }

    public Estimates getEstimates() {
        return estimates;
    }

    public boolean isEmpty() {
        return tuples.isEmpty();
    }
}
