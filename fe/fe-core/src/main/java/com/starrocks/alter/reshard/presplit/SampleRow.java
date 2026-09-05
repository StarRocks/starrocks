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

import com.starrocks.catalog.Variant;

import java.util.List;
import java.util.Objects;

/**
 * One row of a data-tier sample. Carries the sort-key tuple
 * {@link BoundaryPlanner} consumes, plus an optional partition-source tuple a
 * downstream multi-partition grouper consumes, plus zero or more secondary-
 * index tuples a downstream multi-index grouper consumes. The
 * partition-source tuple is empty when the target is unpartitioned (the
 * executor projects only the sort key and the request's
 * {@code partitionSourceColumns} is empty), and is non-empty when the request
 * supplied partition-source columns. {@code secondaryIndexTuples} is empty
 * when the request carried no {@link SecondaryIndexSpec}s, and otherwise
 * carries one {@link IndexTuple} per spec, in the request's declared order.
 *
 * <p>Existing single-tuple callers use {@link #ofSortKey}; the two-tuple
 * constructor is reserved for callers that already know both the sort-key and
 * partition-source tuples but not secondary-index tuples (defaults them to
 * empty); the canonical three-tuple constructor is for the multi-index
 * data-tier executor.
 */
public record SampleRow(
        List<Variant> sortKeyTuple, List<Variant> partitionSourceTuple, List<IndexTuple> secondaryIndexTuples) {

    public SampleRow {
        Objects.requireNonNull(sortKeyTuple, "sortKeyTuple");
        Objects.requireNonNull(partitionSourceTuple, "partitionSourceTuple");
        Objects.requireNonNull(secondaryIndexTuples, "secondaryIndexTuples");
    }

    /**
     * Backwards-compatible constructor for callers that know the sort-key and
     * partition-source tuples but not secondary-index tuples; defaults
     * {@code secondaryIndexTuples} to an empty list.
     */
    public SampleRow(List<Variant> sortKeyTuple, List<Variant> partitionSourceTuple) {
        this(sortKeyTuple, partitionSourceTuple, List.of());
    }

    /**
     * Backwards-compatible factory for callers that only have a sort-key tuple;
     * defaults {@code partitionSourceTuple} and {@code secondaryIndexTuples} to
     * empty lists. Used by the unpartitioned-target executor path and by test
     * fixtures that pre-date partition-source / secondary-index projection.
     */
    public static SampleRow ofSortKey(List<Variant> sortKeyTuple) {
        return new SampleRow(sortKeyTuple, List.of(), List.of());
    }
}
