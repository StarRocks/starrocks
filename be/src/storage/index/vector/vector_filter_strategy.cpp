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

#include "storage/index/vector/vector_filter_strategy.h"

namespace starrocks {

AnnFilterStrategy resolve_ann_filter_strategy(const AnnFilterResolveInputs& in) {
    // --- Explicit user overrides. They may relax the selectivity gate (handled later, in PRE
    // execution), but must never bypass the soundness/completeness invariant (design doc §2). ---
    switch (in.user_choice) {
    case AnnFilterStrategy::POST:
        // User opts into the approximate path (may return < k). Only reachable when explicit.
        return AnnFilterStrategy::POST;
    case AnnFilterStrategy::BRUTE:
        return AnnFilterStrategy::BRUTE;
    case AnnFilterStrategy::PRE:
        // Explicit PRE still falls back to exact BRUTE when PRE cannot be proven sound+complete.
        return (!in.has_above_predicate && in.exact_possible && in.supports_filtered) ? AnnFilterStrategy::PRE
                                                                                      : AnnFilterStrategy::BRUTE;
    case AnnFilterStrategy::AUTO:
        break;
    }

    // --- AUTO ---
    if (!in.has_residual) {
        // No residual: this is a plain ANN top-k, exact + complete by construction.
        return AnnFilterStrategy::PRE;
    }
    if (in.has_above_predicate) {
        // A predicate is evaluated above the iterator -> a segment-level k-limit would under-return.
        return AnnFilterStrategy::BRUTE; // completeness
    }
    if (!in.supports_filtered) {
        // ANN reader cannot consume a candidate bitmap.
        return AnnFilterStrategy::BRUTE;
    }
    if (!in.prefilter_enabled) {
        // Kill-switch off: do not scan-prefilter an un-indexed residual.
        return AnnFilterStrategy::BRUTE;
    }
    if (!in.exact_possible) {
        // Residual cannot be evaluated into an exact bitmap in this segment.
        return AnnFilterStrategy::BRUTE; // exactness
    }
    // Exact + complete + reader-capable. The selectivity gate (PRE -> BRUTE for very selective
    // filters) is applied during PRE execution, not here (it needs the residual bitmap).
    return AnnFilterStrategy::PRE;
}

} // namespace starrocks
