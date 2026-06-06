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
 * downstream multi-partition grouper consumes. The partition-source tuple is
 * empty when the target is unpartitioned (the executor projects only the sort
 * key and the request's {@code partitionSourceColumns} is empty), and is
 * non-empty when the request supplied partition-source columns.
 *
 * <p>Existing single-tuple callers use {@link #ofSortKey}; the record's full
 * constructor is reserved for callers that already know both tuples (the
 * partitioned-target data-tier executor).
 */
public record SampleRow(List<Variant> sortKeyTuple, List<Variant> partitionSourceTuple) {

    public SampleRow {
        Objects.requireNonNull(sortKeyTuple, "sortKeyTuple");
        Objects.requireNonNull(partitionSourceTuple, "partitionSourceTuple");
    }

    /**
     * Backwards-compatible factory for callers that only have a sort-key tuple;
     * defaults {@code partitionSourceTuple} to an empty list. Used by the
     * unpartitioned-target executor path and by test fixtures that pre-date
     * partition-source projection.
     */
    public static SampleRow ofSortKey(List<Variant> sortKeyTuple) {
        return new SampleRow(sortKeyTuple, List.of());
    }
}
