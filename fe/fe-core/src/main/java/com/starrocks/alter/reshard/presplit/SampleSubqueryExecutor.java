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
import com.starrocks.common.StarRocksException;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Pluggable backend for {@link ReservoirSampler}. Decouples FE-side
 * accumulation logic (row → tuple, byte-limit enforcement, SampleSet
 * assembly) from the actual sub-query execution.
 *
 * <p>Production-side implementations construct a FileScanNode (via the same
 * planner path the load uses), inject a sampling clause
 * ({@code TABLESAMPLE BERNOULLI(p)} or {@code WHERE rand_with_seed(seed) < p}),
 * and submit as a coordinator-driven sub-query. Test-side implementations
 * return canned rows from an in-memory list. Both share this contract:
 *
 * <ul>
 *   <li>{@code rows} streams sort-key tuples — each row is a
 *       {@code List<Variant>} whose order matches
 *       {@link SampleRequest#getSortKey}.</li>
 *   <li>{@code estimates} carry the executor's best guess of the FULL input
 *       size (not the sample size); used downstream for tablet-count selection.</li>
 *   <li>Exceptions thrown from {@link #execute} or from the row iterator
 *       cause the surrounding {@link Sampler#sample} call to throw; the
 *       coordinator then skips pre-split (the load itself proceeds).</li>
 * </ul>
 */
@FunctionalInterface
public interface SampleSubqueryExecutor {

    SampleExecution execute(SampleRequest request) throws StarRocksException;

    /** One run of a sampling sub-query. */
    record SampleExecution(Iterator<List<Variant>> rows, Estimates estimates) {
        public SampleExecution {
            Objects.requireNonNull(rows, "rows");
            Objects.requireNonNull(estimates, "estimates");
        }
    }
}
