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

AnnFilterStrategy resolve_ann_filter_strategy(AnnFilterStrategy user_choice, bool prefilter_allowed) {
    switch (user_choice) {
    case AnnFilterStrategy::POST:
        // Explicit opt-in to the approximate path (may return < k). Never chosen by AUTO.
        return AnnFilterStrategy::POST;
    case AnnFilterStrategy::BRUTE:
        // Explicit exact brute-force. Honored even when PRE would be sound -- including on a query
        // with no residual at all, where it is the ground-truth/recall-diagnostic tool.
        return AnnFilterStrategy::BRUTE;
    case AnnFilterStrategy::PRE:
    case AnnFilterStrategy::AUTO:
    default:
        // AUTO and an explicit 'pre' coincide: PRE exactly when it is sound + enabled (see
        // prefilter_allowed in the header), exact BRUTE otherwise. default: an out-of-range wire
        // value degrades to the same safe pair.
        return prefilter_allowed ? AnnFilterStrategy::PRE : AnnFilterStrategy::BRUTE;
    }
}

} // namespace starrocks
