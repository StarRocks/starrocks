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

#include <memory>

#include "common/status.h"
#include "storage/index/vector/scalar_index_provider.h"
#include "storage/index/vector/vector_ann_index.h"

namespace starrocks {

enum class FilterStrategyType {
    PRE_FILTER,
    POST_FILTER,
    // Extension points (not yet implemented):
    // ITERATIVE,
    // ADAPTIVE,
};

// FilterStrategy orchestrates the interaction between scalar predicate
// evaluation and ANN vector search.
//
// Different strategies handle the pre-filter / post-filter / iterative
// trade-off differently. The Strategy is independent of index type and
// table type — it works with any VectorAnnIndex + ScalarIndexProvider.
class FilterStrategy {
public:
    virtual ~FilterStrategy() = default;

    // Execute vector search with optional scalar predicate filtering.
    //   index           - the ANN index to search
    //   query           - vector search parameters
    //   scalar_provider - provider for scalar predicate evaluation (may be nullptr)
    //   predicate       - the scalar predicate (may be nullptr)
    //   result          - output
    virtual Status execute(VectorAnnIndex* index, const VectorQuery& query, ScalarIndexProvider* scalar_provider,
                           const ScalarPredicate* predicate, VectorAnnResult* result) = 0;

    static std::unique_ptr<FilterStrategy> create(FilterStrategyType type);
};

// PreFilterStrategy: evaluate predicate first → bitmap → filtered_search.
// Best when the predicate is moderately selective (retains >10% of data)
// and the ANN index supports efficient in-search filtering.
class PreFilterStrategy final : public FilterStrategy {
public:
    Status execute(VectorAnnIndex* index, const VectorQuery& query, ScalarIndexProvider* scalar_provider,
                   const ScalarPredicate* predicate, VectorAnnResult* result) override;
};

// PostFilterStrategy: search first (oversampled) → filter results.
// Best when the predicate cannot be pushed to the index, or the
// predicate is very selective (retains <1% of data).
class PostFilterStrategy final : public FilterStrategy {
public:
    static constexpr int32_t kDefaultOversampleFactor = 3;

    Status execute(VectorAnnIndex* index, const VectorQuery& query, ScalarIndexProvider* scalar_provider,
                   const ScalarPredicate* predicate, VectorAnnResult* result) override;
};

} // namespace starrocks
