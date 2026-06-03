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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Tuple;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Output of a {@link Sampler}. The tuple list is defensively copied at
 * construction; {@link Tuple} value contents are NOT deep-copied, so callers
 * are responsible for providing stable {@code Tuple} instances (the production
 * {@link ReservoirSampler} does this when reading from an executor that may
 * reuse row buffers).
 *
 * <p>{@link #EMPTY} represents a successful sample of zero rows — distinct
 * from "sampler failed", which the sampler signals by throwing.
 */
public final class SampleSet {
    public static final SampleSet EMPTY = new SampleSet(Collections.emptyList(), Estimates.ZERO);

    private final List<Tuple> tuples;
    private final Estimates estimates;

    public SampleSet(List<Tuple> tuples, Estimates estimates) {
        this.tuples = ImmutableList.copyOf(Objects.requireNonNull(tuples, "tuples"));
        this.estimates = Objects.requireNonNull(estimates, "estimates");
    }

    public List<Tuple> getTuples() {
        return tuples;
    }

    public Estimates getEstimates() {
        return estimates;
    }

    public boolean isEmpty() {
        return tuples.isEmpty();
    }
}
