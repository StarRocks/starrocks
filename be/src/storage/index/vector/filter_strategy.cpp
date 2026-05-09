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

#include "storage/index/vector/filter_strategy.h"

namespace starrocks {

std::unique_ptr<FilterStrategy> FilterStrategy::create(FilterStrategyType type) {
    switch (type) {
    case FilterStrategyType::PRE_FILTER:
        return std::make_unique<PreFilterStrategy>();
    case FilterStrategyType::POST_FILTER:
        return std::make_unique<PostFilterStrategy>();
    default:
        return std::make_unique<PreFilterStrategy>();
    }
}

// ============================================================
// PreFilterStrategy
// ============================================================

Status PreFilterStrategy::execute(VectorAnnIndex* index, const VectorQuery& query, ScalarIndexProvider* scalar_provider,
                                  const ScalarPredicate* predicate, VectorAnnResult* result) {
    if (!predicate || predicate->empty() || !scalar_provider) {
        return index->search(query, result);
    }

    ASSIGN_OR_RETURN(auto filter, scalar_provider->evaluate(*predicate));

    if (filter.empty()) {
        return index->search(query, result);
    }

    return index->filtered_search(query, filter, result);
}

// ============================================================
// PostFilterStrategy
// ============================================================

Status PostFilterStrategy::execute(VectorAnnIndex* index, const VectorQuery& query,
                                   ScalarIndexProvider* scalar_provider, const ScalarPredicate* predicate,
                                   VectorAnnResult* result) {
    if (!predicate || predicate->empty() || !scalar_provider) {
        return index->search(query, result);
    }

    // Evaluate predicate to get the allow-list for post-filtering.
    ASSIGN_OR_RETURN(auto filter, scalar_provider->evaluate(*predicate));

    if (filter.empty()) {
        return index->search(query, result);
    }

    // Search with oversampled top_k, then filter results.
    VectorQuery oversampled = query;
    oversampled.top_k = query.top_k * kDefaultOversampleFactor;

    VectorAnnResult raw;
    RETURN_IF_ERROR(index->search(oversampled, &raw));

    result->clear();
    result->reserve(query.top_k);
    for (int32_t i = 0; i < raw.result_count && result->result_count < query.top_k; ++i) {
        if (filter.contains(raw.row_ids[i])) {
            result->add(raw.row_ids[i], raw.scores[i]);
        }
    }
    return Status::OK();
}

} // namespace starrocks
