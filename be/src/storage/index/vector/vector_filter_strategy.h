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

#pragma once

namespace starrocks {

// Vector-search-with-residual-filter execution strategy.
//
// AUTO is an *input* choice only (the user/session default). The resolver
// (resolve_ann_filter_strategy) always returns one of PRE/POST/BRUTE.
//
// Wire-compatible with TVectorSearchOptions.filter_strategy:
//   0 = AUTO, 1 = PRE, 2 = POST, 3 = BRUTE.
enum class AnnFilterStrategy { AUTO = 0, PRE = 1, POST = 2, BRUTE = 3 };

// Bitmap-free inputs to the strategy decision (design doc §4).
//
// The selectivity gate is intentionally NOT an input here: it needs the residual
// bitmap, so it is applied later, during PRE execution (which may downgrade to BRUTE).
struct AnnFilterResolveInputs {
    // User/session choice from TVectorSearchOptions.filter_strategy. AUTO follows the config.
    AnnFilterStrategy user_choice = AnnFilterStrategy::AUTO;
    // config::enable_vector_index_residual_prefilter. When false, AUTO never scan-prefilters
    // an un-indexed residual (kill-switch).
    bool prefilter_enabled = true;
    // !_opts.pred_tree.empty() -- there is a residual predicate to satisfy alongside the ANN.
    bool has_residual = false;
    // A predicate is evaluated *above* the segment iterator (OlapChunkSource not_push_down /
    // _non_pushdown_pred_tree). The iterator cannot fold it into the ANN candidate, so a
    // segment-level k-limit would under-return -> must BRUTE (completeness).
    bool has_above_predicate = false;
    // Every column referenced by _opts.pred_tree has a readable iterator in this segment, so the
    // whole tree can be evaluated into an exact bitmap (exactness precondition for PRE).
    bool exact_possible = true;
    // ann_reader->supports_efficient_filtered_search() -- the reader can consume an IdFilter.
    bool supports_filtered = true;
};

// Pure decision function (design doc §4). Runs per-segment at BE runtime, after FE has
// unconditionally rewritten to an ANN plan. Returns PRE/POST/BRUTE, never AUTO.
//
// Soundness/completeness invariant (design doc §2): PRE (segment-level k-limit) is only allowed
// when the residual can be folded into an exact + complete bitmap the ANN reader can consume;
// otherwise the query falls back to exact BRUTE. The selectivity downgrade (PRE -> BRUTE for very
// selective filters) happens later, in PRE execution, and is not modeled here.
AnnFilterStrategy resolve_ann_filter_strategy(const AnnFilterResolveInputs& in);

} // namespace starrocks
