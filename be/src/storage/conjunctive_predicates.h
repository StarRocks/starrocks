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

#include <butil/containers/flat_map.h>

#include <vector>

#include "storage/column_predicate.h"

namespace starrocks {

class Chunk;

// ConjunctivePredicates represent a list of conjunctive predicates, e.g, (`c1=1` AND `c2=10`).
class ConjunctivePredicates {
public:
    ConjunctivePredicates() = default;
    ConjunctivePredicates(const std::initializer_list<const ColumnPredicate*>& preds);
    ~ConjunctivePredicates() = default;

    ConjunctivePredicates(const ConjunctivePredicates&) = default;
    ConjunctivePredicates(ConjunctivePredicates&&) = default;
    ConjunctivePredicates& operator=(const ConjunctivePredicates&) = default;
    ConjunctivePredicates& operator=(ConjunctivePredicates&&) = default;

    Status evaluate(const Chunk* chunk, uint8_t* selection) const;

    Status evaluate_or(const Chunk* chunk, uint8_t* selection) const;

    Status evaluate_and(const Chunk* chunk, uint8_t* selection) const;

    Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    Status evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    Status evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    // [thread-unsafe]
    // Does NOT take the ownership of |pred|.
    void add(const ColumnPredicate* pred);

    // [thread-unsafe]
    void add(const ConjunctivePredicates& rhs);

    size_t size() const { return _vec_preds.size() + _non_vec_preds.size(); }

    bool empty() const { return _vec_preds.empty() && _non_vec_preds.empty(); }

    template <typename Set>
    void get_column_ids(Set* result) const;

    template <typename Container>
    void predicates_of_column(ColumnId cid, Container* container) const;

    Status convert_to(ConjunctivePredicates* dst, const std::vector<LogicalType>& new_types,
                      ObjectPool* obj_pool) const {
        size_t num_vec_preds = _vec_preds.size();
        dst->_vec_preds.resize(num_vec_preds);
        for (size_t i = 0; i < num_vec_preds; ++i) {
            ColumnId cid = _vec_preds[i]->column_id();
            RETURN_IF_ERROR(_vec_preds[i]->convert_to(&dst->_vec_preds[i], get_type_info(new_types[cid]), obj_pool));
        }

        size_t num_non_vec_preds = _non_vec_preds.size();
        dst->_non_vec_preds.resize(num_non_vec_preds);
        for (size_t i = 0; i < num_non_vec_preds; ++i) {
            ColumnId cid = _non_vec_preds[i]->column_id();
            RETURN_IF_ERROR(
                    _non_vec_preds[i]->convert_to(&dst->_non_vec_preds[i], get_type_info(new_types[cid]), obj_pool));
        }
        return Status::OK();
    }

    std::string debug_string() const;

    std::vector<const ColumnPredicate*>& vec_preds() { return _vec_preds; }
    std::vector<const ColumnPredicate*>& non_vec_preds() { return _non_vec_preds; }

private:
    Status _evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    Status _evaluate_non_vec(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    std::vector<const ColumnPredicate*> _vec_preds;
    std::vector<const ColumnPredicate*> _non_vec_preds;
    mutable std::vector<uint16_t> _selected_idx;
};

inline ConjunctivePredicates::ConjunctivePredicates(const std::initializer_list<const ColumnPredicate*>& preds) {
    for (auto p : preds) {
        add(p);
    }
}

inline void ConjunctivePredicates::add(const ColumnPredicate* pred) {
    if (pred->can_vectorized()) {
        _vec_preds.emplace_back(pred);
    } else {
        _non_vec_preds.emplace_back(pred);
    }
}

inline void ConjunctivePredicates::add(const ConjunctivePredicates& rhs) {
    _vec_preds.insert(_vec_preds.end(), rhs._vec_preds.begin(), rhs._vec_preds.end());
    _non_vec_preds.insert(_non_vec_preds.end(), rhs._non_vec_preds.begin(), rhs._non_vec_preds.end());
}

template <typename Set>
inline void ConjunctivePredicates::get_column_ids(Set* result) const {
    for (const ColumnPredicate* pred : _vec_preds) {
        result->insert(pred->column_id());
    }
    for (const ColumnPredicate* pred : _non_vec_preds) {
        result->insert(pred->column_id());
    }
}

template <typename Container>
inline void ConjunctivePredicates::predicates_of_column(ColumnId cid, Container* container) const {
    for (const ColumnPredicate* pred : _vec_preds) {
        if (pred->column_id() == cid) {
            container->push_back(pred);
        }
    }
    for (const ColumnPredicate* pred : _non_vec_preds) {
        if (pred->column_id() == cid) {
            container->push_back(pred);
        }
    }
}

} // namespace starrocks
