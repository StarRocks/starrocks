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

#include <cstdint>
#include <type_traits>

#include "column/nullable_column.h"
#include "storage/column_predicate.h"

namespace starrocks {
// Implementing a complete ColumnPredicate is very difficult, most of the ColumnPredicate logic is similar,
// we just need to implement similar apply operation can be more convenient to implement a new ColumnPredicate.

template <LogicalType field_type, class ColumnType, template <LogicalType> class ColumnOperator, typename... Args>
class ColumnOperatorPredicate final : public ColumnPredicate {
public:
    using SpecColumnOperator = ColumnOperator<field_type>;
    ColumnOperatorPredicate(const ColumnOperatorPredicate&) = delete;
    ColumnOperatorPredicate(const TypeInfoPtr& type_info, ColumnId id, Args&&... args)
            : ColumnPredicate(type_info, id), _predicate_operator(std::move(args)...) {}

    // evaluate
    uint8_t evaluate_at(int index, const ColumnType* column) const {
        return _predicate_operator.eval_at(column, index);
    }

    // evaluate with nullable
    uint8_t evaluate_at_nullable(int index, const uint8_t* null_data, const ColumnType* column) const {
        if constexpr (SpecColumnOperator::skip_null) {
            return !null_data[index] && _predicate_operator.eval_at(column, index);
        }
        return _predicate_operator.eval_at(column, index);
    }

    Status evaluate(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override {
        // get raw column
        const ColumnType* lowcard_column;
        if (column->is_nullable()) {
            // This is NullableColumn, get its data_column
            lowcard_column =
                    down_cast<const ColumnType*>(down_cast<const NullableColumn*>(column)->data_column().get());
        } else {
            lowcard_column = down_cast<const ColumnType*>(column);
        }
        if (!column->has_null()) {
            for (uint16_t i = from; i < to; i++) {
                sel[i] = evaluate_at(i, lowcard_column);
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                sel[i] = evaluate_at_nullable(i, null_data, lowcard_column);
            }
        }
        return Status::OK();
    }

    Status evaluate_and(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override {
        // get raw column
        const ColumnType* lowcard_column;
        if (column->is_nullable()) {
            // This is NullableColumn, get its data_column
            lowcard_column =
                    down_cast<const ColumnType*>(down_cast<const NullableColumn*>(column)->data_column().get());
        } else {
            lowcard_column = down_cast<const ColumnType*>(column);
        }
        if (!column->has_null()) {
            for (uint16_t i = from; i < to; i++) {
                sel[i] = (sel[i] && evaluate_at(i, lowcard_column));
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                sel[i] = (sel[i] && evaluate_at_nullable(i, null_data, lowcard_column));
            }
        }
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override {
        // get raw column
        const ColumnType* lowcard_column;
        if (column->is_nullable()) {
            // This is NullableColumn, get its data_column
            lowcard_column =
                    down_cast<const ColumnType*>(down_cast<const NullableColumn*>(column)->data_column().get());
        } else {
            lowcard_column = down_cast<const ColumnType*>(column);
        }
        if (!column->has_null()) {
            for (uint16_t i = from; i < to; i++) {
                sel[i] = (sel[i] || _predicate_operator.eval_at(lowcard_column, i));
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                sel[i] = (sel[i] || evaluate_at_nullable(i, null_data, lowcard_column));
            }
        }
        return Status::OK();
    }

    StatusOr<uint16_t> evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const override {
        // Get BinaryColumn
        const ColumnType* lowcard_column;
        if (column->is_nullable()) {
            // This is NullableColumn, get its data_column
            lowcard_column =
                    down_cast<const ColumnType*>(down_cast<const NullableColumn*>(column)->data_column().get());
        } else {
            lowcard_column = down_cast<const ColumnType*>(column);
        }

        uint16_t new_size = 0;
        if (!column->has_null()) {
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += evaluate_at(data_idx, lowcard_column);
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += evaluate_at_nullable(data_idx, null_data, lowcard_column);
            }
        }
        return new_size;
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        return _predicate_operator.zone_map_filter(detail);
    }

    PredicateType type() const override { return SpecColumnOperator::type(); }

    Datum value() const override { return _predicate_operator.value(); }

    std::vector<Datum> values() const override { return _predicate_operator.values(); }

    bool can_vectorized() const override { return SpecColumnOperator::can_vectorized(); }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange* range) const override {
        return _predicate_operator.seek_bitmap_dictionary(iter, range);
    }

    bool support_bloom_filter() const override { return SpecColumnOperator::support_bloom_filter(); }

    bool bloom_filter(const BloomFilter* bf) const override {
        DCHECK(support_bloom_filter()) << "Not support bloom filter";
        if constexpr (SpecColumnOperator::support_bloom_filter()) {
            return _predicate_operator.bloom_filter(bf);
        }
        return true;
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        return _predicate_operator.convert_to(output, target_type_info, obj_pool);
    }

    std::string debug_string() const override { return _predicate_operator.debug_string(); }

    bool padding_zeros(size_t len) override { return _predicate_operator.padding_zeros(len); }

private:
    SpecColumnOperator _predicate_operator;
};
} // namespace starrocks
