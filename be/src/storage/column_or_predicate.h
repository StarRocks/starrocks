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

#include <vector>

#include "storage/column_predicate.h"

namespace starrocks {

class ColumnOrPredicate final : public ColumnPredicate {
public:
    explicit ColumnOrPredicate(const TypeInfoPtr& type_info, ColumnId cid) : ColumnPredicate(type_info, cid) {}

    template <typename Container>
    ColumnOrPredicate(const TypeInfoPtr& type_info, ColumnId cid, const Container& c)
            : ColumnPredicate(type_info, cid), _child(c.begin(), c.end()) {}

    // Does NOT take the ownership of |child|.
    void add_child(ColumnPredicate* child) { _child.emplace_back(child); }

    template <typename Iterator>
    void add_child(Iterator begin, Iterator end) {
        _child.insert(_child.end(), begin, end);
    }

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override;

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override;

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override;

    bool filter(const BloomFilter& bf) const override { return true; }

    bool zone_map_filter(const ZoneMapDetail& detail) const override;

    bool can_vectorized() const override { return false; }

    PredicateType type() const override { return PredicateType::kOr; }

    // Always return `NULL`.
    Datum value() const override { return {}; }

    // Always return an empty set.
    std::vector<Datum> values() const override { return std::vector<Datum>{}; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override;

    std::string debug_string() const override;

private:
    Status _evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const;

    // TODO: reorder child predicates based on their cost.
    std::vector<const ColumnPredicate*> _child;
    mutable std::vector<uint8_t> _buff;
};

} // namespace starrocks
