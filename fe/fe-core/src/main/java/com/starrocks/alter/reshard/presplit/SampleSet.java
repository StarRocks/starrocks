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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
 * <p>{@code secondaryIndexMetaIds} is the AUTHORITATIVE secondary index-id set,
 * taken from the originating {@link SampleRequest}'s specs in projection
 * order; it is present even for a zero-row sample, so downstream code checks
 * id-set membership against this list rather than inspecting rows.
 * {@code secondaryIndexTuples} carries the parallel per-row secondary-index
 * tuple list: empty when {@code secondaryIndexMetaIds} is empty, otherwise the
 * same size as {@code tuples} (including zero), with every present row's
 * {@link IndexTuple} id-set equal to {@code secondaryIndexMetaIds} (exactly
 * one per id, no duplicates, no unknown ids) — a short or malformed row is
 * rejected at construction rather than silently dropped downstream.
 *
 * <p>{@link #EMPTY} represents a successful sample of zero rows with no
 * secondary indexes — distinct from "sampler failed", which the sampler
 * signals by throwing.
 */
public final class SampleSet {
    public static final SampleSet EMPTY = new SampleSet(Collections.emptyList(), Estimates.ZERO);

    private final List<Tuple> tuples;
    private final List<Tuple> partitionSourceTuples;
    private final List<Long> secondaryIndexMetaIds;
    private final List<List<IndexTuple>> secondaryIndexTuples;
    private final Estimates estimates;

    public SampleSet(List<Tuple> tuples, Estimates estimates) {
        this(tuples, Collections.emptyList(), estimates);
    }

    public SampleSet(List<Tuple> tuples, List<Tuple> partitionSourceTuples, Estimates estimates) {
        this(tuples, partitionSourceTuples, Collections.emptyList(), Collections.emptyList(), estimates);
    }

    public SampleSet(List<Tuple> tuples, List<Tuple> partitionSourceTuples,
                     List<Long> secondaryIndexMetaIds, List<List<IndexTuple>> secondaryIndexTuples,
                     Estimates estimates) {
        Objects.requireNonNull(tuples, "tuples");
        Objects.requireNonNull(partitionSourceTuples, "partitionSourceTuples");
        Objects.requireNonNull(secondaryIndexMetaIds, "secondaryIndexMetaIds");
        Objects.requireNonNull(secondaryIndexTuples, "secondaryIndexTuples");
        Preconditions.checkArgument(
                partitionSourceTuples.isEmpty() || partitionSourceTuples.size() == tuples.size(),
                "partitionSourceTuples must be either empty or the same size as tuples (%s vs %s)",
                partitionSourceTuples.size(), tuples.size());
        if (secondaryIndexMetaIds.isEmpty()) {
            Preconditions.checkArgument(secondaryIndexTuples.isEmpty(),
                    "secondaryIndexTuples must be empty when secondaryIndexMetaIds is empty, had %s row(s)",
                    secondaryIndexTuples.size());
        } else {
            Preconditions.checkArgument(secondaryIndexTuples.size() == tuples.size(),
                    "secondaryIndexTuples must be the same size as tuples when secondaryIndexMetaIds is "
                            + "non-empty (%s vs %s)",
                    secondaryIndexTuples.size(), tuples.size());
            Set<Long> expectedIds = new HashSet<>(secondaryIndexMetaIds);
            for (int rowIndex = 0; rowIndex < secondaryIndexTuples.size(); rowIndex++) {
                List<IndexTuple> rowTuples = Objects.requireNonNull(
                        secondaryIndexTuples.get(rowIndex), "secondaryIndexTuples row " + rowIndex);
                Set<Long> rowIds = new HashSet<>();
                for (IndexTuple indexTuple : rowTuples) {
                    Objects.requireNonNull(indexTuple, "secondaryIndexTuples row " + rowIndex + " entry");
                    Preconditions.checkArgument(rowIds.add(indexTuple.indexMetaId()),
                            "secondaryIndexTuples row %s carries a duplicate index id %s",
                            rowIndex, indexTuple.indexMetaId());
                }
                Preconditions.checkArgument(rowIds.equals(expectedIds),
                        "secondaryIndexTuples row %s carries index ids %s but expected exactly %s",
                        rowIndex, rowIds, expectedIds);
            }
        }
        this.tuples = ImmutableList.copyOf(tuples);
        this.partitionSourceTuples = ImmutableList.copyOf(partitionSourceTuples);
        this.secondaryIndexMetaIds = ImmutableList.copyOf(secondaryIndexMetaIds);
        ImmutableList.Builder<List<IndexTuple>> secondaryIndexTuplesBuilder = ImmutableList.builder();
        for (List<IndexTuple> rowTuples : secondaryIndexTuples) {
            secondaryIndexTuplesBuilder.add(ImmutableList.copyOf(rowTuples));
        }
        this.secondaryIndexTuples = secondaryIndexTuplesBuilder.build();
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

    /**
     * Parallel-by-index secondary-index tuple lists, one inner list per sampled
     * row. Empty when {@link #getSecondaryIndexMetaIds()} is empty; otherwise
     * the same size as {@link #getTuples()} (including zero for a zero-row
     * sample).
     */
    public List<List<IndexTuple>> getSecondaryIndexTuples() {
        return secondaryIndexTuples;
    }

    /**
     * The authoritative secondary index-id set, taken from the originating
     * {@link SampleRequest}'s specs in projection order. Present even for a
     * zero-row sample — callers should check id-set membership against this
     * list rather than inspecting rows.
     */
    public List<Long> getSecondaryIndexMetaIds() {
        return secondaryIndexMetaIds;
    }

    public Estimates getEstimates() {
        return estimates;
    }

    public boolean isEmpty() {
        return tuples.isEmpty();
    }
}
