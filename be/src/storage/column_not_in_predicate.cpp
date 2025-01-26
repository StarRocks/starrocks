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

#include <utility>

#include "column/column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "storage/column_predicate.h"
#include "storage/in_predicate_utils.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "util/string_parser.hpp"

namespace starrocks {

template <LogicalType field_type>
class ColumnNotInPredicate final : public ColumnPredicate {
    using ValueType = typename CppTypeTraits<field_type>::CppType;

public:
    ColumnNotInPredicate(const TypeInfoPtr& type_info, ColumnId id, const std::vector<std::string>& strs)
            : ColumnPredicate(type_info, id), _values(predicate_internal::strings_to_hashset<field_type>(strs)) {}

    ColumnNotInPredicate(const TypeInfoPtr& type_info, ColumnId id, ItemHashSet<ValueType>&& values)
            : ColumnPredicate(type_info, id), _values(std::move(values)) {}

    ~ColumnNotInPredicate() override = default;

    template <typename Op>
    inline void t_evaluate(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
        auto* v = reinterpret_cast<const ValueType*>(column->raw_data());
        if (!column->has_null()) {
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(!_values.contains(v[i])));
            }
        } else {
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(!null_data[i] && !_values.contains(v[i])));
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
        auto* v = reinterpret_cast<const ValueType*>(column->raw_data());

        uint16_t new_size = 0;
        if (!column->has_null()) {
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !(_values.contains(v[data_idx]));
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !null_data[data_idx] && !(_values.contains(v[data_idx]));
            }
        }
        return new_size;
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override { return true; }

    bool support_bitmap_filter() const override { return false; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        return Status::Cancelled("not-equal predicate not support bitmap index");
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        InvertedIndexQueryType query_type = InvertedIndexQueryType::EQUAL_QUERY;
        roaring::Roaring indices;
        for (auto value : _values) {
            roaring::Roaring index;
            RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &value, query_type, &index));
            indices |= index;
        }
        *row_bitmap -= indices;
        return Status::OK();
    }

    PredicateType type() const override { return PredicateType::kNotInList; }

    bool can_vectorized() const override { return false; }

    std::vector<Datum> values() const override {
        std::vector<Datum> ret;
        ret.reserve(_values.size());
        for (const ValueType& value : _values) {
            ret.emplace_back(value);
        }
        return ret;
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        const auto to_type = target_type_info->type();
        if (to_type == field_type) {
            *output = this;
            return Status::OK();
        }

        if (to_type == TYPE_DECIMAL128) {
            std::vector<std::string> strs;
            const auto type_info = this->type_info();
            for (ValueType value : _values) {
                strs.emplace_back(type_info->to_string(&value));
            }
            *output = obj_pool->add(new_column_not_in_predicate(target_type_info, _column_id, strs));
            return Status::OK();
        }
        if constexpr (field_type == TYPE_DECIMAL128) {
            std::vector<std::string> strs;
            for (ValueType value : _values) {
                strs.emplace_back(DecimalV3Cast::to_string<ValueType>(value, 27, 9));
            }
            *output = obj_pool->add(new_column_not_in_predicate(target_type_info, _column_id, strs));
            return Status::OK();
        }
        const auto type_info = this->type_info();
        std::vector<std::string> strs;
        for (ValueType value : _values) {
            strs.emplace_back(type_info->to_string(&value));
        }
        *output = obj_pool->add(new_column_not_in_predicate(target_type_info, _column_id, strs));
        return Status::OK();
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "((columnId=" << _column_id << ")NOT IN(";
        int i = 0;
        for (auto& item : _values) {
            if (i++ != 0) {
                ss << ",";
            }
            ss << this->type_info()->to_string(&item);
        }
        ss << "))";
        return ss.str();
    }

private:
    ItemHashSet<ValueType> _values;
};

// Template specialization for binary column
template <LogicalType field_type>
class BinaryColumnNotInPredicate final : public ColumnPredicate {
public:
    BinaryColumnNotInPredicate(const TypeInfoPtr& type_info, ColumnId id, std::vector<std::string> strings)
            : ColumnPredicate(type_info, id), _zero_padded_strs(std::move(strings)) {
        for (const std::string& s : _zero_padded_strs) {
            _slices.emplace(Slice(s));
        }
    }

    ~BinaryColumnNotInPredicate() override = default;

    template <typename Op>
    inline void t_evaluate(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
        // Get BinaryColumn
        const BinaryColumn* binary_column;
        if (column->is_nullable()) {
            // This is NullableColumn, get its data_column
            binary_column =
                    down_cast<const BinaryColumn*>(down_cast<const NullableColumn*>(column)->data_column().get());
        } else {
            binary_column = down_cast<const BinaryColumn*>(column);
        }
        if (!column->has_null()) {
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(!(_slices.contains(binary_column->get_slice(i)))));
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (size_t i = from; i < to; i++) {
                sel[i] =
                        Op::apply(sel[i], (uint8_t)(!null_data[i] && !(_slices.contains(binary_column->get_slice(i)))));
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
        if (!column->has_null()) {
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !(_slices.contains(binary_column->get_slice(data_idx)));
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !null_data[data_idx] && !(_slices.contains(binary_column->get_slice(data_idx)));
            }
        }
        return new_size;
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override { return true; }

    bool support_bitmap_filter() const override { return false; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        return Status::Cancelled("not-equal predicate not support bitmap index");
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        InvertedIndexQueryType query_type = InvertedIndexQueryType::EQUAL_QUERY;
        roaring::Roaring indices;
        for (const std::string& s : _zero_padded_strs) {
            Slice padded_value(s);
            roaring::Roaring index;
            RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &padded_value, query_type, &index));
            indices |= index;
        }
        *row_bitmap -= indices;
        return Status::OK();
    }

    bool can_vectorized() const override { return false; }

    PredicateType type() const override { return PredicateType::kNotInList; }

    std::vector<Datum> values() const override {
        std::vector<Datum> ret;
        ret.reserve(_slices.size());
        for (const std::string& s : _zero_padded_strs) {
            ret.emplace_back(Slice(s));
        }
        return ret;
    }

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

    bool padding_zeros(size_t len) override {
        _slices.clear();
        for (auto& str : _zero_padded_strs) {
            size_t old_sz = str.size();
            str.append(len > old_sz ? len - old_sz : 0, '\0');
            _slices.emplace(str.data(), old_sz);
        }
        return true;
    }

private:
    std::vector<std::string> _zero_padded_strs;
    ItemHashSet<Slice> _slices;
};

ColumnPredicate* new_column_not_in_predicate(const TypeInfoPtr& type_info, ColumnId id,
                                             const std::vector<std::string>& strs) {
    auto type = type_info->type();
    switch (type) {
    case TYPE_BOOLEAN:
        return new ColumnNotInPredicate<TYPE_BOOLEAN>(type_info, id, strs);
    case TYPE_TINYINT:
        return new ColumnNotInPredicate<TYPE_TINYINT>(type_info, id, strs);
    case TYPE_SMALLINT:
        return new ColumnNotInPredicate<TYPE_SMALLINT>(type_info, id, strs);
    case TYPE_INT:
        return new ColumnNotInPredicate<TYPE_INT>(type_info, id, strs);
    case TYPE_BIGINT:
        return new ColumnNotInPredicate<TYPE_BIGINT>(type_info, id, strs);
    case TYPE_LARGEINT:
        return new ColumnNotInPredicate<TYPE_LARGEINT>(type_info, id, strs);
    case TYPE_DECIMAL:
        return new ColumnNotInPredicate<TYPE_DECIMAL>(type_info, id, strs);
    case TYPE_DECIMALV2:
        return new ColumnNotInPredicate<TYPE_DECIMALV2>(type_info, id, strs);
    case TYPE_DECIMAL32: {
        const auto scale = type_info->scale();
        using SetType = ItemHashSet<CppTypeTraits<TYPE_DECIMAL32>::CppType>;
        SetType values = predicate_internal::strings_to_decimal_set<TYPE_DECIMAL32>(scale, strs);
        return new ColumnNotInPredicate<TYPE_DECIMAL32>(type_info, id, std::move(values));
    }
    case TYPE_DECIMAL64: {
        const auto scale = type_info->scale();
        using SetType = ItemHashSet<CppTypeTraits<TYPE_DECIMAL64>::CppType>;
        SetType values = predicate_internal::strings_to_decimal_set<TYPE_DECIMAL64>(scale, strs);
        return new ColumnNotInPredicate<TYPE_DECIMAL64>(type_info, id, std::move(values));
    }
    case TYPE_DECIMAL128: {
        const auto scale = type_info->scale();
        using SetType = ItemHashSet<CppTypeTraits<TYPE_DECIMAL128>::CppType>;
        SetType values = predicate_internal::strings_to_decimal_set<TYPE_DECIMAL128>(scale, strs);
        return new ColumnNotInPredicate<TYPE_DECIMAL128>(type_info, id, std::move(values));
    }
    case TYPE_CHAR:
        return new BinaryColumnNotInPredicate<TYPE_CHAR>(type_info, id, strs);
    case TYPE_VARCHAR:
        return new BinaryColumnNotInPredicate<TYPE_VARCHAR>(type_info, id, strs);
    case TYPE_DATE_V1:
        return new ColumnNotInPredicate<TYPE_DATE_V1>(type_info, id, strs);
    case TYPE_DATE:
        return new ColumnNotInPredicate<TYPE_DATE>(type_info, id, strs);
    case TYPE_DATETIME_V1:
        return new ColumnNotInPredicate<TYPE_DATETIME_V1>(type_info, id, strs);
    case TYPE_DATETIME:
        return new ColumnNotInPredicate<TYPE_DATETIME>(type_info, id, strs);
    case TYPE_FLOAT:
        return new ColumnNotInPredicate<TYPE_FLOAT>(type_info, id, strs);
    case TYPE_DOUBLE:
        return new ColumnNotInPredicate<TYPE_DOUBLE>(type_info, id, strs);
    case TYPE_UNSIGNED_TINYINT:
    case TYPE_UNSIGNED_SMALLINT:
    case TYPE_UNSIGNED_INT:
    case TYPE_UNSIGNED_BIGINT:
    case TYPE_DISCRETE_DOUBLE:
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
    case TYPE_MAX_VALUE:
    case TYPE_VARBINARY:
        return nullptr;
        // No default to ensure newly added enumerator will be handled.
    }
    return nullptr;
}

} //namespace starrocks
