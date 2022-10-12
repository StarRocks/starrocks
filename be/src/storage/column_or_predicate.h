// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "storage/vectorized_column_predicate.h"

namespace starrocks::vectorized {

class ColumnOrPredicate : public ColumnPredicate {
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
    Datum value() const override { return Datum(); }

    // Always return an empty set.
    std::vector<Datum> values() const override { return std::vector<Datum>{}; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override;

private:
    Status _evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const;

    // TODO: reorder child predicates based on their cost.
    std::vector<const ColumnPredicate*> _child;
    mutable std::vector<uint8_t> _buff;
};

} // namespace starrocks::vectorized
