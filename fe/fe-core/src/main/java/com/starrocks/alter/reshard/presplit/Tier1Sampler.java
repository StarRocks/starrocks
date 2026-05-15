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

import com.starrocks.common.StarRocksException;

/**
 * Tier 1 path-planner contract used by {@link DefaultPreSplitPipeline}.
 * Concrete production impl is {@link ParquetMetadataSampler#tryPlan}; tests
 * supply a lambda. Decoupled from the (final) {@code ParquetMetadataSampler}
 * class so tests don't need an inline Mockito mock-maker.
 */
@FunctionalInterface
public interface Tier1Sampler {

    /**
     * @throws Tier1UnavailableException when the caller should retry with Tier 2.
     * @throws StarRocksException any other failure — the pipeline maps it to
     *         {@link SkipReason#SAMPLE_FAILED}.
     */
    BoundaryPlannerResult tryPlan(SampleRequest request, int requestedTabletCount) throws StarRocksException;
}
