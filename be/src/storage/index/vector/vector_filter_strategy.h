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
// Wire-compatible with TQueryOptions.ann_filter_strategy (set from the
// ann_filter_strategy session variable): 0 = AUTO, 1 = PRE, 2 = POST, 3 = BRUTE.
enum class AnnFilterStrategy { AUTO = 0, PRE = 1, POST = 2, BRUTE = 3 };

// Pure per-segment decision: how to serve an ANN top-k alongside its residual filter.
//
//   user_choice       the ann_filter_strategy session variable.
//   prefilter_allowed true iff the PRE path is sound and enabled for this segment (the caller
//                     derives it from the segment facts).
//
// AUTO and an explicit 'pre' map to PRE exactly when prefilter_allowed and to exact BRUTE
// otherwise: a user choice cannot buy out completeness, and the kill-switch binds 'pre' too.
// POST stays an explicit opt-in (approximate, may return < k). The selectivity gates run later,
// during PRE execution.
AnnFilterStrategy resolve_ann_filter_strategy(AnnFilterStrategy user_choice, bool prefilter_allowed);

} // namespace starrocks
