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

#include <cstdint>
#include <functional>
#include <vector>

#include "column/column.h" // Column
#include "column/datum.h"
#include "common/object_pool.h"
#include "storage/column_predicate.h"
#include "storage/olap_common.h" // ColumnId
#include "storage/range.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bloom_filter.h"
#include "storage/types.h"
#include "storage/zone_map_detail.h"
#include "util/string_parser.hpp"

namespace starrocks {
class BloomFilter;
class Slice;
class ObjectPool;
class ExprContext;
class RuntimeState;
class SlotDescriptor;
class BitmapIndexIterator;
class BloomFilter;
} // namespace starrocks

namespace starrocks {

template <LogicalType field_type>
using GeEval = std::greater_equal<typename CppTypeTraits<field_type>::CppType>;

template <LogicalType field_type>
using GtEval = std::greater<typename CppTypeTraits<field_type>::CppType>;

template <LogicalType field_type>
using LeEval = std::less_equal<typename CppTypeTraits<field_type>::CppType>;

template <LogicalType field_type>
using LtEval = std::less<typename CppTypeTraits<field_type>::CppType>;

template <LogicalType field_type>
using EqEval = std::equal_to<typename CppTypeTraits<field_type>::CppType>;

template <LogicalType field_type>
using NeEval = std::not_equal_to<typename CppTypeTraits<field_type>::CppType>;

template <LogicalType field_type, template <LogicalType> typename Predicate, typename ColumnPredicateBuilder>
static Status predicate_convert_to(Predicate<field_type> const& input_predicate,
                                   typename CppTypeTraits<field_type>::CppType const& value,
                                   ColumnPredicateBuilder column_predicate_builder, const ColumnPredicate** output,
                                   const TypeInfoPtr& target_type_info, ObjectPool* obj_pool) {
    using ValueType = typename CppTypeTraits<field_type>::CppType;
    const auto to_type = target_type_info->type();
    if (to_type == field_type) {
        *output = &input_predicate;
        return Status::OK();
    }
    auto cid = input_predicate.column_id();
    const auto type_info = input_predicate.type_info();
    std::string str = type_info->to_string(&value);
    *output = obj_pool->add(column_predicate_builder(target_type_info, cid, str));
    return Status::OK();
}

template <template <LogicalType> typename Predicate, template <LogicalType> typename BinaryPredicate>
static ColumnPredicate* new_column_predicate(const TypeInfoPtr& type_info, ColumnId id, const Slice& operand) {
    auto type = type_info->type();
    switch (type) {
    case TYPE_BOOLEAN:
        return new Predicate<TYPE_BOOLEAN>(type_info, id, string_to_int<int>(operand) != 0);
    case TYPE_TINYINT:
        return new Predicate<TYPE_TINYINT>(type_info, id, string_to_int<int8_t>(operand));
    case TYPE_UNSIGNED_TINYINT:
        return nullptr;
    case TYPE_SMALLINT:
        return new Predicate<TYPE_SMALLINT>(type_info, id, string_to_int<int16_t>(operand));
    case TYPE_UNSIGNED_SMALLINT:
        return nullptr;
    case TYPE_INT:
        return new Predicate<TYPE_INT>(type_info, id, string_to_int<int32_t>(operand));
    case TYPE_UNSIGNED_INT:
        return nullptr;
    case TYPE_BIGINT:
        return new Predicate<TYPE_BIGINT>(type_info, id, string_to_int<int64_t>(operand));
    case TYPE_UNSIGNED_BIGINT:
        return nullptr;
    case TYPE_LARGEINT:
        return new Predicate<TYPE_LARGEINT>(type_info, id, string_to_int<int128_t>(operand));
    case TYPE_FLOAT:
        return new Predicate<TYPE_FLOAT>(type_info, id, string_to_float<float>(operand));
    case TYPE_DOUBLE:
        return new Predicate<TYPE_DOUBLE>(type_info, id, string_to_float<double>(operand));
    case TYPE_DISCRETE_DOUBLE:
        return nullptr;
    case TYPE_DECIMAL: {
        decimal12_t value;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK(st.ok());
        return new Predicate<TYPE_DECIMAL>(type_info, id, value);
    }
    case TYPE_DECIMALV2: {
        DecimalV2Value value;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK(st.ok());
        return new Predicate<TYPE_DECIMALV2>(type_info, id, value);
    }
    case TYPE_DECIMAL32: {
        int32_t value;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK(st.ok());
        return new Predicate<TYPE_DECIMAL32>(type_info, id, value);
    }
    case TYPE_DECIMAL64: {
        int64_t value;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK(st.ok());
        return new Predicate<TYPE_DECIMAL64>(type_info, id, value);
    }
    case TYPE_DECIMAL128: {
        int128_t value;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK(st.ok());
        return new Predicate<TYPE_DECIMAL128>(type_info, id, value);
    }
    case TYPE_DATE_V1: {
        uint24_t value = 0;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK(st.ok());
        return new Predicate<TYPE_DATE_V1>(type_info, id, value);
    }
    case TYPE_DATE: {
        int32_t value = 0;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK(st.ok());
        return new Predicate<TYPE_DATE>(type_info, id, value);
    }
    case TYPE_DATETIME_V1: {
        uint64_t value = 0;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK(st.ok());
        return new Predicate<TYPE_DATETIME_V1>(type_info, id, value);
    }
    case TYPE_DATETIME: {
        int64_t value = 0;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK(st.ok());
        return new Predicate<TYPE_DATETIME>(type_info, id, value);
    }
    case TYPE_CHAR:
        return new BinaryPredicate<TYPE_CHAR>(type_info, id, operand);
    case TYPE_VARCHAR:
        return new BinaryPredicate<TYPE_VARCHAR>(type_info, id, operand);
    case TYPE_STRUCT:
    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_UNKNOWN:
    case TYPE_NONE:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_PERCENTILE:
    case TYPE_JSON:
    case TYPE_NULL:
    case TYPE_FUNCTION:
    case TYPE_TIME:
    case TYPE_BINARY:
    case TYPE_VARBINARY:
    case TYPE_MAX_VALUE:
        return nullptr;
        // No default to ensure newly added enumerator will be handled.
    }
    return nullptr;
}

// Base class for column predicate
template <LogicalType field_type, class Eval>
class ColumnPredicateCmpBase : public ColumnPredicate {
    using ValueType = typename CppTypeTraits<field_type>::CppType;

public:
    ColumnPredicateCmpBase(PredicateType predicate, const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : ColumnPredicate(type_info, id), _predicate(predicate), _value(value) {}

    ~ColumnPredicateCmpBase() override = default;

    template <typename Op>
    inline void t_evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
        auto* v = reinterpret_cast<const ValueType*>(column->raw_data());
        auto* sel = selection;
        auto eval = Eval();
        if (!column->has_null()) {
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(eval(v[i], _value)));
            }
        } else {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)((!is_null[i]) & eval(v[i], _value)));
            }
        }
    }

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAssignOp>(column, selection, from, to);
        return Status::OK();
    }

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAndOp>(column, selection, from, to);
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateOrOp>(column, selection, from, to);
        return Status::OK();
    }

    PredicateType type() const override { return _predicate; }

    Datum value() const override { return Datum(_value); }

    std::vector<Datum> values() const override { return std::vector<Datum>{Datum(_value)}; }

    bool can_vectorized() const override { return true; }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "(columnId(" << _column_id << ")" << _predicate << this->type_info()->to_string(&_value) << ")";
        return ss.str();
    }

protected:
    PredicateType _predicate;
    ValueType _value;
};

template <LogicalType field_type>
class ColumnGePredicate : public ColumnPredicateCmpBase<field_type, GeEval<field_type>> {
public:
    using ValueType = typename CppTypeTraits<field_type>::CppType;
    using Base = ColumnPredicateCmpBase<field_type, GeEval<field_type>>;

    ColumnGePredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kGE, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& max = detail.max_value();
        return this->type_info()->cmp(Datum(this->_value), max) <= 0;
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        range->clear();
        bool exact_match;
        Status s = iter->seek_dictionary(&this->_value, &exact_match);
        if (s.ok()) {
            rowid_t seeked_ordinal = iter->current_ordinal();
            rowid_t ordinal_limit = iter->bitmap_nums() - iter->has_null_bitmap();
            range->add(Range<>(seeked_ordinal, ordinal_limit));
        } else if (!s.is_not_found()) {
            return s;
        }
        return Status::OK();
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        InvertedIndexQueryType query_type = InvertedIndexQueryType::GREATER_EQUAL_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &this->_value, query_type, &roaring));
        *row_bitmap &= roaring;
        return Status::OK();
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        return predicate_convert_to<field_type>(*this, this->_value, new_column_ge_predicate, output, target_type_info,
                                                obj_pool);
    }
};

template <LogicalType field_type>
class ColumnGtPredicate : public ColumnPredicateCmpBase<field_type, GtEval<field_type>> {
public:
    using ValueType = typename CppTypeTraits<field_type>::CppType;
    using Base = ColumnPredicateCmpBase<field_type, GtEval<field_type>>;

    ColumnGtPredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kGT, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& max = detail.max_value();
        return this->type_info()->cmp(Datum(this->_value), max) < 0;
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        range->clear();
        bool exact_match = false;
        Status s = iter->seek_dictionary(&this->_value, &exact_match);
        if (s.ok()) {
            rowid_t seeked_ordinal = iter->current_ordinal() + exact_match;
            rowid_t ordinal_limit = iter->bitmap_nums() - iter->has_null_bitmap();
            range->add(Range<>(seeked_ordinal, ordinal_limit));
        } else if (!s.is_not_found()) {
            return s;
        }
        return Status::OK();
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        InvertedIndexQueryType query_type = InvertedIndexQueryType::GREATER_THAN_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &this->_value, query_type, &roaring));
        *row_bitmap &= roaring;
        return Status::OK();
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        return predicate_convert_to<field_type>(*this, this->_value, new_column_gt_predicate, output, target_type_info,
                                                obj_pool);
    }
};

template <LogicalType field_type>
class ColumnLePredicate : public ColumnPredicateCmpBase<field_type, LeEval<field_type>> {
public:
    using ValueType = typename CppTypeTraits<field_type>::CppType;
    using Base = ColumnPredicateCmpBase<field_type, LeEval<field_type>>;

    ColumnLePredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kLE, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& min = detail.min_or_null_value();
        const auto& max = detail.max_value();
        return (this->type_info()->cmp(Datum(this->_value), min) >= 0) & !max.is_null();
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        range->clear();
        bool exact_match = false;
        Status st = iter->seek_dictionary(&this->_value, &exact_match);
        if (st.ok()) {
            rowid_t seeked_ordinal = iter->current_ordinal() + exact_match;
            range->add(Range<>(0, seeked_ordinal));
        } else if (st.is_not_found()) {
            range->add(Range<>(0, iter->bitmap_nums() - iter->has_null_bitmap()));
            st = Status::OK();
        }
        return st;
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        InvertedIndexQueryType query_type = InvertedIndexQueryType::LESS_EQUAL_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &this->_value, query_type, &roaring));
        *row_bitmap &= roaring;
        return Status::OK();
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        return predicate_convert_to<field_type>(*this, this->_value, new_column_le_predicate, output, target_type_info,
                                                obj_pool);
    }
};

template <LogicalType field_type>
class ColumnLtPredicate : public ColumnPredicateCmpBase<field_type, LtEval<field_type>> {
public:
    using ValueType = typename CppTypeTraits<field_type>::CppType;
    using Base = ColumnPredicateCmpBase<field_type, LtEval<field_type>>;

    ColumnLtPredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kLT, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& min = detail.min_or_null_value();
        const auto& max = detail.max_value();
        return (this->type_info()->cmp(Datum(this->_value), min) > 0) & !max.is_null();
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        range->clear();
        bool exact_match = false;
        Status st = iter->seek_dictionary(&this->_value, &exact_match);
        if (st.ok()) {
            rowid_t seeked_ordinal = iter->current_ordinal();
            range->add(Range<>(0, seeked_ordinal));
        } else if (st.is_not_found()) {
            range->add(Range<>(0, iter->bitmap_nums() - iter->has_null_bitmap()));
            st = Status::OK();
        }
        return st;
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        InvertedIndexQueryType query_type = InvertedIndexQueryType::LESS_THAN_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &this->_value, query_type, &roaring));
        *row_bitmap &= roaring;
        return Status::OK();
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        return predicate_convert_to<field_type>(*this, this->_value, new_column_lt_predicate, output, target_type_info,
                                                obj_pool);
    }
};

template <LogicalType field_type>
class ColumnEqPredicate : public ColumnPredicateCmpBase<field_type, EqEval<field_type>> {
public:
    using ValueType = typename CppTypeTraits<field_type>::CppType;
    using Base = ColumnPredicateCmpBase<field_type, EqEval<field_type>>;

    ColumnEqPredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kEQ, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& min = detail.min_or_null_value();
        const auto& max = detail.max_value();
        const auto type_info = this->type_info();
        return type_info->cmp(Datum(this->_value), min) >= 0 && type_info->cmp(Datum(this->_value), max) <= 0;
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        range->clear();
        bool exact_match = false;
        Status s = iter->seek_dictionary(&this->_value, &exact_match);
        if (s.ok()) {
            if (exact_match) {
                rowid_t ordinal = iter->current_ordinal();
                range->add(Range<>(ordinal, ordinal + 1));
            }
        } else if (!s.is_not_found()) {
            return s;
        }
        return Status::OK();
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        InvertedIndexQueryType query_type = InvertedIndexQueryType::EQUAL_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &this->_value, query_type, &roaring));
        *row_bitmap &= roaring;
        return Status::OK();
    }

    bool support_bloom_filter() const override { return true; }

    bool bloom_filter(const BloomFilter* bf) const override {
        static_assert(field_type != TYPE_JSON, "TODO");
        static_assert(field_type != TYPE_HLL, "TODO");
        static_assert(field_type != TYPE_OBJECT, "TODO");
        static_assert(field_type != TYPE_PERCENTILE, "TODO");
        return bf->test_bytes(reinterpret_cast<const char*>(&this->_value), sizeof(this->_value));
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        return predicate_convert_to<field_type>(*this, this->_value, new_column_eq_predicate, output, target_type_info,
                                                obj_pool);
    }
};

template <LogicalType field_type>
class ColumnNePredicate : public ColumnPredicateCmpBase<field_type, NeEval<field_type>> {
public:
    using ValueType = typename CppTypeTraits<field_type>::CppType;
    using Base = ColumnPredicateCmpBase<field_type, NeEval<field_type>>;

    ColumnNePredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kNE, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override { return true; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        return Status::Cancelled("not-equal predicate not support bitmap index");
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        InvertedIndexQueryType query_type = InvertedIndexQueryType::EQUAL_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &this->_value, query_type, &roaring));
        *row_bitmap -= roaring;
        return Status::OK();
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        return predicate_convert_to<field_type>(*this, this->_value, new_column_ne_predicate, output, target_type_info,
                                                obj_pool);
    }
};

// Base class for binary column predicate
template <LogicalType field_type, class Eval>
class BinaryColumnPredicateCmpBase : public ColumnPredicate {
    using ValueType = Slice;

public:
    BinaryColumnPredicateCmpBase(PredicateType predicate_type, const TypeInfoPtr& type_info, ColumnId id,
                                 ValueType value)
            : ColumnPredicate(type_info, id),
              _predicate_type(predicate_type),
              _zero_padded_str(value.data, value.size),
              _value(_zero_padded_str) {}

    ~BinaryColumnPredicateCmpBase() override = default;

    template <typename Op>
    inline void t_evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
        auto* v = reinterpret_cast<const ValueType*>(column->raw_data());
        auto* sel = selection;
        auto eval = Eval();
        if (!column->has_null()) {
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(eval(v[i], _value)));
            }
        } else {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)((!is_null[i]) && eval(v[i], _value)));
            }
        }
    }

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAssignOp>(column, selection, from, to);
        return Status::OK();
    }

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAndOp>(column, selection, from, to);
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateOrOp>(column, selection, from, to);
        return Status::OK();
    }

    StatusOr<uint16_t> evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const override {
        // Get BinaryColumn
        const BinaryColumn* binary_column;
        if (column->is_nullable()) {
            // This is NullableColumn, get its data_column
            binary_column =
                    down_cast<const BinaryColumn*>(down_cast<const NullableColumn*>(column)->data_column().get());
        } else {
            binary_column = down_cast<const BinaryColumn*>(column);
        }

        uint16_t new_size = 0;
        auto eval = Eval();
        if (!column->has_null()) {
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += eval(binary_column->get_slice(data_idx), _value);
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !is_null[data_idx] && eval(binary_column->get_slice(data_idx), _value);
            }
        }
        return new_size;
    }

    PredicateType type() const override { return _predicate_type; }

    Datum value() const override { return Datum(Slice(_zero_padded_str)); }

    std::vector<Datum> values() const override { return std::vector<Datum>{Datum(_value)}; }

    bool can_vectorized() const override { return false; }

    bool support_bloom_filter() const override { return false; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        const auto to_type = target_type_info->type();
        if (to_type == field_type) {
            *output = this;
            return Status::OK();
        }
        CHECK(false) << "Not support, from_type=" << field_type << ", to_type=" << to_type;
        return Status::OK();
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "(columnId(" << _column_id << ")" << _predicate_type << _zero_padded_str << ")";
        return ss.str();
    }

    bool padding_zeros(size_t len) override {
        size_t old_sz = _zero_padded_str.size();
        _zero_padded_str.append(len > old_sz ? len - old_sz : 0, '\0');
        _value = Slice(_zero_padded_str.data(), old_sz);
        return true;
    }

protected:
    PredicateType _predicate_type;
    std::string _zero_padded_str;
    ValueType _value;
};

template <LogicalType field_type>
class BinaryColumnEqPredicate : public BinaryColumnPredicateCmpBase<field_type, EqEval<field_type>> {
public:
    using ValueType = Slice;
    using Base = BinaryColumnPredicateCmpBase<field_type, std::equal_to<ValueType>>;

    BinaryColumnEqPredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kEQ, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& min = detail.min_or_null_value();
        const auto& max = detail.max_value();
        const auto type_info = this->type_info();
        return type_info->cmp(Datum(this->_value), min) >= 0 && type_info->cmp(Datum(this->_value), max) <= 0;
    }

    bool support_bloom_filter() const override { return true; }

    bool bloom_filter(const BloomFilter* bf) const override {
        Slice padded(Base::_zero_padded_str);
        return bf->test_bytes(padded.data, padded.size);
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        // see the comment in `predicate_parser.cpp`.
        Slice padded_value(Base::_zero_padded_str);
        range->clear();
        bool exact_match = false;
        Status s = iter->seek_dictionary(&padded_value, &exact_match);
        if (s.ok()) {
            if (exact_match) {
                rowid_t ordinal = iter->current_ordinal();
                range->add(Range<>(ordinal, ordinal + 1));
            }
        } else if (!s.is_not_found()) {
            return s;
        }
        return Status::OK();
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        Slice padded_value(Base::_zero_padded_str);
        InvertedIndexQueryType query_type = InvertedIndexQueryType::EQUAL_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &padded_value, query_type, &roaring));
        *row_bitmap &= roaring;
        return Status::OK();
    }
};

template <LogicalType field_type>
class BinaryColumnGePredicate : public BinaryColumnPredicateCmpBase<field_type, GeEval<field_type>> {
public:
    using ValueType = Slice;
    using Base = BinaryColumnPredicateCmpBase<field_type, std::greater_equal<ValueType>>;

    BinaryColumnGePredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kGE, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& max = detail.max_value();
        return this->type_info()->cmp(Datum(this->_value), max) <= 0;
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        // Can NOT use `_value` here, see the comment in `predicate_parser.cpp`.
        Slice padded_value(Base::_zero_padded_str);

        range->clear();
        bool exact_match;
        Status s = iter->seek_dictionary(&padded_value, &exact_match);
        if (s.ok()) {
            rowid_t seeked_ordinal = iter->current_ordinal();
            rowid_t ordinal_limit = iter->bitmap_nums() - iter->has_null_bitmap();
            range->add(Range<>(seeked_ordinal, ordinal_limit));
        } else if (!s.is_not_found()) {
            return s;
        }
        return Status::OK();
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        Slice padded_value(Base::_zero_padded_str);
        InvertedIndexQueryType query_type = InvertedIndexQueryType::GREATER_EQUAL_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &padded_value, query_type, &roaring));
        *row_bitmap &= roaring;
        return Status::OK();
    }
};

template <LogicalType field_type>
class BinaryColumnGtPredicate : public BinaryColumnPredicateCmpBase<field_type, GtEval<field_type>> {
public:
    using ValueType = Slice;
    using Base = BinaryColumnPredicateCmpBase<field_type, std::greater<ValueType>>;

    BinaryColumnGtPredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kGT, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& max = detail.max_value();
        return this->type_info()->cmp(Datum(this->_value), max) < 0;
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        // Can NOT use `_value` here, see comment in predicate_parser.cpp.
        Slice padded_value(Base::_zero_padded_str);
        range->clear();
        bool exact_match = false;
        Status s = iter->seek_dictionary(&padded_value, &exact_match);
        if (s.ok()) {
            rowid_t seeked_ordinal = iter->current_ordinal() + exact_match;
            rowid_t ordinal_limit = iter->bitmap_nums() - iter->has_null_bitmap();
            range->add(Range<>(seeked_ordinal, ordinal_limit));
        } else if (!s.is_not_found()) {
            return s;
        }
        return Status::OK();
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        Slice padded_value(Base::_zero_padded_str);
        InvertedIndexQueryType query_type = InvertedIndexQueryType::GREATER_THAN_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &padded_value, query_type, &roaring));
        *row_bitmap &= roaring;
        return Status::OK();
    }
};

template <LogicalType field_type>
class BinaryColumnLtPredicate : public BinaryColumnPredicateCmpBase<field_type, LtEval<field_type>> {
public:
    using ValueType = Slice;
    using Base = BinaryColumnPredicateCmpBase<field_type, std::less<ValueType>>;

    BinaryColumnLtPredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kLT, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& min = detail.min_or_null_value();
        const auto& max = detail.max_value();
        const auto type_info = this->type_info();
        return (type_info->cmp(Datum(this->_value), min) > 0) & !max.is_null();
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        Slice padded_value(Base::_zero_padded_str);
        range->clear();
        bool exact_match = false;
        Status st = iter->seek_dictionary(&padded_value, &exact_match);
        if (st.ok()) {
            rowid_t seeked_ordinal = iter->current_ordinal();
            range->add(Range<>(0, seeked_ordinal));
        } else if (st.is_not_found()) {
            range->add(Range<>(0, iter->bitmap_nums() - iter->has_null_bitmap()));
            st = Status::OK();
        }
        return st;
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        Slice padded_value(Base::_zero_padded_str);
        InvertedIndexQueryType query_type = InvertedIndexQueryType::LESS_THAN_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &padded_value, query_type, &roaring));
        *row_bitmap &= roaring;
        return Status::OK();
    }
};

template <LogicalType field_type>
class BinaryColumnLePredicate : public BinaryColumnPredicateCmpBase<field_type, LeEval<field_type>> {
public:
    using ValueType = Slice;
    using Base = BinaryColumnPredicateCmpBase<field_type, std::less_equal<ValueType>>;

    BinaryColumnLePredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kLE, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& min = detail.min_or_null_value();
        const auto& max = detail.max_value();
        return (this->type_info()->cmp(Datum(this->_value), min) >= 0) & !max.is_null();
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        Slice padded_value(Base::_zero_padded_str);
        range->clear();
        bool exact_match = false;
        Status st = iter->seek_dictionary(&padded_value, &exact_match);
        if (st.ok()) {
            rowid_t seeked_ordinal = iter->current_ordinal() + exact_match;
            range->add(Range<>(0, seeked_ordinal));
        } else if (st.is_not_found()) {
            range->add(Range<>(0, iter->bitmap_nums() - iter->has_null_bitmap()));
            st = Status::OK();
        }
        return st;
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        Slice padded_value(Base::_zero_padded_str);
        InvertedIndexQueryType query_type = InvertedIndexQueryType::LESS_EQUAL_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &padded_value, query_type, &roaring));
        *row_bitmap &= roaring;
        return Status::OK();
    }
};

template <LogicalType field_type>
class BinaryColumnNePredicate : public BinaryColumnPredicateCmpBase<field_type, NeEval<field_type>> {
public:
    using ValueType = Slice;
    using Base = BinaryColumnPredicateCmpBase<field_type, std::not_equal_to<ValueType>>;

    BinaryColumnNePredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : Base(PredicateType::kNE, type_info, id, value) {}

    bool zone_map_filter(const ZoneMapDetail& detail) const override { return true; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        return Status::Cancelled("not-equal predicate not support bitmap index");
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        Slice padded_value(Base::_zero_padded_str);
        InvertedIndexQueryType query_type = InvertedIndexQueryType::EQUAL_QUERY;
        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &padded_value, query_type, &roaring));
        *row_bitmap -= roaring;
        return Status::OK();
    }
};

ColumnPredicate* new_column_ne_predicate(const TypeInfoPtr& type_info, ColumnId id, const Slice& operand) {
    return new_column_predicate<ColumnNePredicate, BinaryColumnNePredicate>(type_info, id, operand);
}

ColumnPredicate* new_column_eq_predicate(const TypeInfoPtr& type_info, ColumnId id, const Slice& operand) {
    return new_column_predicate<ColumnEqPredicate, BinaryColumnEqPredicate>(type_info, id, operand);
}

ColumnPredicate* new_column_lt_predicate(const TypeInfoPtr& type_info, ColumnId id, const Slice& operand) {
    return new_column_predicate<ColumnLtPredicate, BinaryColumnLtPredicate>(type_info, id, operand);
}

ColumnPredicate* new_column_le_predicate(const TypeInfoPtr& type_info, ColumnId id, const Slice& operand) {
    return new_column_predicate<ColumnLePredicate, BinaryColumnLePredicate>(type_info, id, operand);
}

ColumnPredicate* new_column_gt_predicate(const TypeInfoPtr& type_info, ColumnId id, const Slice& operand) {
    return new_column_predicate<ColumnGtPredicate, BinaryColumnGtPredicate>(type_info, id, operand);
}

ColumnPredicate* new_column_ge_predicate(const TypeInfoPtr& type_info, ColumnId id, const Slice& operand) {
    return new_column_predicate<ColumnGePredicate, BinaryColumnGePredicate>(type_info, id, operand);
}

ColumnPredicate* new_column_cmp_predicate(PredicateType predicate, const TypeInfoPtr& type, ColumnId id,
                                          const Slice& operand) {
    switch (predicate) {
    case PredicateType::kEQ:
        return new_column_eq_predicate(type, id, operand);
    case PredicateType::kNE:
        return new_column_ne_predicate(type, id, operand);
    case PredicateType::kLT:
        return new_column_lt_predicate(type, id, operand);
    case PredicateType::kLE:
        return new_column_le_predicate(type, id, operand);
    case PredicateType::kGT:
        return new_column_gt_predicate(type, id, operand);
    case PredicateType::kGE:
        return new_column_ge_predicate(type, id, operand);
    default:
        CHECK(false) << "not a cmp predicate";
    }
}

std::ostream& operator<<(std::ostream& os, PredicateType p) {
    switch (p) {
    case PredicateType::kUnknown:
        os << "unknown";
        break;
    case PredicateType::kEQ:
        os << "=";
        break;
    case PredicateType::kNE:
        os << "!=";
        break;
    case PredicateType::kGT:
        os << ">";
        break;
    case PredicateType::kGE:
        os << ">=";
        break;
    case PredicateType::kLT:
        os << "<";
        break;
    case PredicateType::kLE:
        os << "<=";
        break;

    case PredicateType::kInList:
        os << "IN";
        break;
    case PredicateType::kNotInList:
        os << "NOT IN";
        break;
    case PredicateType::kIsNull:
        os << "IS NULL";
        break;
    case PredicateType::kNotNull:
        os << "IS NOT NULL";
        break;
    case PredicateType::kAnd:
        os << "AND";
        break;
    case PredicateType::kOr:
        os << "OR";
        break;
    case PredicateType::kExpr:
        os << "expr";
        break;
    case PredicateType::kTrue:
        os << "true";
        break;
    case PredicateType::kMap:
        os << "map";
        break;
    default:
        CHECK(false) << "unknown predicate " << p;
    }

    return os;
}
} //namespace starrocks
