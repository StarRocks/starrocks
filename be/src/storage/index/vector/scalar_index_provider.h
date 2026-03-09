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

#include <any>
#include <string>

#include "common/statusor.h"
#include "storage/index/vector/vector_ann_index.h"

namespace starrocks {

// ScalarPredicate is a type-erased wrapper around the predicate
// representation used by different table types:
//   - OLAP: ColumnPredicate*
//   - Paimon: rapidjson::Value (JSON predicate node)
//   - Iceberg/Lance: expression tree
//
// The concrete ScalarIndexProvider knows how to unpack it.
class ScalarPredicate {
public:
    ScalarPredicate() = default;

    template <typename T>
    explicit ScalarPredicate(T&& pred) : _data(std::forward<T>(pred)) {}

    template <typename T>
    const T* get() const {
        return std::any_cast<T>(&_data);
    }

    bool empty() const { return !_data.has_value(); }

private:
    std::any _data;
};

// ScalarIndexProvider evaluates scalar predicates against an index
// (bitmap, bloom, BSI, Paimon global index, etc.) and returns a
// RowIdFilter representing the matching row set.
//
// Each table type provides its own implementation.
class ScalarIndexProvider {
public:
    virtual ~ScalarIndexProvider() = default;

    // Evaluate the predicate and return matching row IDs.
    virtual StatusOr<RowIdFilter> evaluate(const ScalarPredicate& predicate) = 0;

    // Estimate the fraction of rows matching the predicate (0.0 = none, 1.0 = all).
    // Used by AdaptiveStrategy to choose pre-filter vs post-filter.
    // Default: 0.5 (unknown).
    virtual float estimate_selectivity(const ScalarPredicate& predicate) const { return 0.5f; }
};

} // namespace starrocks
