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
#include "storage/index/vector/vector_ann_index.h"

namespace starrocks {

enum class FilterStrategyType {
    PRE_FILTER,
    POST_FILTER,
    // Extension points (not yet implemented):
    // ITERATIVE,
    // ADAPTIVE,
};

// FilterStrategy orchestrates the interaction between a pre-built
// row filter and ANN vector search.
//
// Predicate evaluation happens at the call site (each table type
// translates its own predicate form into a RowIdFilter); the
// strategy is intentionally agnostic to predicate representation
// and table type.
class FilterStrategy {
public:
    virtual ~FilterStrategy() = default;

    // Execute vector search with an optional row filter.
    //   index  - the ANN index to search
    //   query  - vector search parameters
    //   filter - pre-built row filter (may be nullptr for no filtering)
    //   result - output
    virtual Status execute(VectorAnnIndex* index, const VectorQuery& query, const RowIdFilter* filter,
                           VectorAnnResult* result) = 0;

    static std::unique_ptr<FilterStrategy> create(FilterStrategyType type);
};

// PreFilterStrategy: feed the filter directly into the index's
// filtered_search path (the index skips non-matching rows during
// graph traversal). Best when the ANN index supports efficient
// in-search filtering.
class PreFilterStrategy final : public FilterStrategy {
public:
    Status execute(VectorAnnIndex* index, const VectorQuery& query, const RowIdFilter* filter,
                   VectorAnnResult* result) override;
};

// PostFilterStrategy: search oversampled, then drop non-matching
// rows from the result. Best when the index cannot push the filter
// down efficiently, or when the filter is very selective.
class PostFilterStrategy final : public FilterStrategy {
public:
    static constexpr int32_t kDefaultOversampleFactor = 3;

    Status execute(VectorAnnIndex* index, const VectorQuery& query, const RowIdFilter* filter,
                   VectorAnnResult* result) override;
};

} // namespace starrocks
