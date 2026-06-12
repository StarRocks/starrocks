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
//   prefilter_allowed PRE is executable for this segment: the residual (possibly empty) can be
//                     folded into the search as an exact candidate set. The caller
//                     (_init_ann_reader) computes this as
//                         no residual at all                                  (trivially sound)
//                         || (no predicate evaluated above the iterator      (completeness)
//                             && the reader supports filtered search         (capability)
//                             && enable_vector_index_residual_prefilter).    (kill-switch)
//
// AUTO and an explicit 'pre' both map to PRE exactly when prefilter_allowed, and to exact BRUTE
// otherwise: a user choice cannot buy out completeness, and the kill-switch binds an explicit
// 'pre' too (an emergency switch a session variable could bypass would not be an emergency
// switch). POST stays an explicit opt-in (approximate, may return < k); AUTO never chooses it.
// The selectivity gates (PRE -> exact rescan over the candidates) run later, during PRE
// execution, and are not modeled here.
AnnFilterStrategy resolve_ann_filter_strategy(AnnFilterStrategy user_choice, bool prefilter_allowed);

} // namespace starrocks
