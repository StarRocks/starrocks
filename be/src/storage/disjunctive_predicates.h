// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "storage/conjunctive_predicates.h"
#include "storage/vectorized_column_predicate.h"

namespace starrocks::vectorized {

class Chunk;
class ConjunctivePredicates;

// DisjunctivePredicates represent a list of disjunctive predicates, each of which is a
// `ConjunctivePredicate`.
//
// Difference with `ColumnOrPredicate`:
//  - `ColumnOrPredicate` is a type of `ColumnPredicate`, while `DisjunctivePredicates` is a
//    container of `ColumnPredicate`.
//  - `DisjunctivePredicates` can represent predicates of different columns, e.g,
//    `c1=100 or c2=200`, while `ColumnOrPredicate` can only represent a compound predicate of
//    the same column, e.g, `c1=100 or c2=200`.
//  - The type of child element of `ColumnOrPredicate` is `ColumnPredicate` while the element
//    type of `DisjunctivePredicates` is `ConjunctivePredicates`.
// TODO(zhuming): replace class with `ColumnOrPredicate`.
class DisjunctivePredicates {
public:
    DisjunctivePredicates() = default;
    ~DisjunctivePredicates() = default;

    Status evaluate(const Chunk* chunk, uint8_t* selection) const;

    Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    void add(const ConjunctivePredicates& pred) { _preds.emplace_back(pred); }
    void add(ConjunctivePredicates&& pred) { _preds.emplace_back(std::move(pred)); }

    size_t size() const { return _preds.size(); }

    bool empty() const { return _preds.empty(); }

    template <typename Set>
    void get_column_ids(Set* result) const {
        for (auto& pred : _preds) {
            pred.get_column_ids(result);
        }
    }

    const ConjunctivePredicates& operator[](size_t idx) const { return _preds[idx]; }
    ConjunctivePredicates& operator[](size_t idx) { return _preds[idx]; }

    Status convert_to(DisjunctivePredicates* dst, const std::vector<FieldType>& new_types, ObjectPool* obj_pool) const {
        int num_preds = _preds.size();
        dst->_preds.resize(num_preds);
        for (int i = 0; i < num_preds; ++i) {
            RETURN_IF_ERROR(_preds[i].convert_to(&dst->_preds[i], new_types, obj_pool));
        }
        return Status::OK();
    }

    std::vector<ConjunctivePredicates>& predicate_list() { return _preds; }

private:
    // TODO: reorder for better performance.
    std::vector<ConjunctivePredicates> _preds;
};

} // namespace starrocks::vectorized
