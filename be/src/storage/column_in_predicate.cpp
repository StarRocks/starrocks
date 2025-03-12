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

#include <type_traits>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "roaring/roaring.hh"
#include "storage/column_predicate.h"
#include "storage/in_predicate_utils.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "types/logical_type.h"
#include "util/bloom_filter.h"

namespace starrocks {

template <LogicalType field_type, typename ItemSet>
class ColumnInPredicate final : public ColumnPredicate {
    using ValueType = typename CppTypeTraits<field_type>::CppType;
    static_assert(std::is_same_v<ValueType, typename ItemSet::value_type>);

public:
    ColumnInPredicate(const TypeInfoPtr& type_info, ColumnId id, ItemSet values)
            : ColumnPredicate(type_info, id), _values(std::move(values)) {}

    ~ColumnInPredicate() override = default;

    template <typename Op>
    inline void t_evaluate(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
        auto* v = reinterpret_cast<const ValueType*>(column->raw_data());
        if (!column->has_null()) {
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(_values.contains(v[i])));
            }
        } else {
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(!null_data[i] && _values.contains(v[i])));
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
                new_size += _values.contains(v[data_idx]);
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !null_data[data_idx] && _values.contains(v[data_idx]);
            }
        }
        return new_size;
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& min = detail.min_or_null_value();
        const auto& max = detail.max_value();
        const auto type_info = this->type_info();
        for (const ValueType& v : _values) {
            if (type_info->cmp(Datum(v), min) >= 0 && type_info->cmp(Datum(v), max) <= 0) {
                return true;
            }
        }
        return false;
    }

    bool support_bitmap_filter() const override { return true; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        range->clear();
        for (auto value : _values) {
            bool exact_match = false;
            Status s = iter->seek_dictionary(&value, &exact_match);
            if (s.ok() && exact_match) {
                rowid_t seeked_ordinal = iter->current_ordinal();
                range->add(Range<>(seeked_ordinal, seeked_ordinal + 1));
            } else if (!s.ok() && !s.is_not_found()) {
                return s;
            }
        }
        return Status::OK();
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
        *row_bitmap &= indices;
        return Status::OK();
    }

    bool support_original_bloom_filter() const override { return true; }

    bool original_bloom_filter(const BloomFilter* bf) const override {
        static_assert(field_type != TYPE_HLL, "TODO");
        static_assert(field_type != TYPE_OBJECT, "TODO");
        static_assert(field_type != TYPE_PERCENTILE, "TODO");
        for (const ValueType& v : _values) {
            RETURN_IF(bf->test_bytes(reinterpret_cast<const char*>(&v), sizeof(v)), true);
        }
        return false;
    }

    PredicateType type() const override { return PredicateType::kInList; }

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

        auto type_info = this->type_info();
        std::vector<std::string> strs;
        for (ValueType value : _values) {
            strs.emplace_back(type_info->to_string(&value));
        }
        *output = obj_pool->add(new_column_in_predicate(target_type_info, _column_id, strs));
        return Status::OK();
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "((columnId=" << _column_id << ")IN(";
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
    ItemSet _values;
};

// Template specialization for binary column
template <LogicalType field_type>
class BinaryColumnInPredicate final : public ColumnPredicate {
public:
    BinaryColumnInPredicate(const TypeInfoPtr& type_info, ColumnId id, std::vector<std::string> strings)
            : ColumnPredicate(type_info, id), _zero_padded_strs(std::move(strings)) {
        for (const std::string& s : _zero_padded_strs) {
            _slices.emplace(Slice(s));
        }
    }

    ~BinaryColumnInPredicate() override = default;

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
                sel[i] = Op::apply(sel[i], (uint8_t)(_slices.contains(binary_column->get_slice(i))));
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(!null_data[i] && _slices.contains(binary_column->get_slice(i))));
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
                new_size += _slices.contains(binary_column->get_slice(data_idx));
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !null_data[data_idx] && _slices.contains(binary_column->get_slice(data_idx));
            }
        }
        return new_size;
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& min = detail.min_or_null_value();
        const auto& max = detail.max_value();
        const auto type_info = this->type_info();
        for (const Slice& v : _slices) {
            if (type_info->cmp(Datum(v), min) >= 0 && type_info->cmp(Datum(v), max) <= 0) {
                return true;
            }
        }
        return false;
    }

    bool support_bitmap_filter() const override { return true; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        range->clear();
        for (const std::string& s : _zero_padded_strs) {
            Slice padded_value(s);
            bool exact_match = false;
            Status st = iter->seek_dictionary(&padded_value, &exact_match);
            if (st.ok() && exact_match) {
                rowid_t seeked_ordinal = iter->current_ordinal();
                range->add(Range<>(seeked_ordinal, seeked_ordinal + 1));
            } else if (!st.ok() && !st.is_not_found()) {
                return st;
            }
        }
        return Status::OK();
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
        *row_bitmap &= indices;
        return Status::OK();
    }

    bool support_original_bloom_filter() const override { return true; }

    bool original_bloom_filter(const BloomFilter* bf) const override {
        for (const auto& str : _zero_padded_strs) {
            Slice v(str);
            RETURN_IF(bf->test_bytes(v.data, v.size), true);
        }
        return false;
    }

    bool can_vectorized() const override { return false; }

    PredicateType type() const override { return PredicateType::kInList; }

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

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "((columnId=" << _column_id << ")IN(";
        int i = 0;
        for (auto& item : _zero_padded_strs) {
            if (i++ != 0) {
                ss << ",";
            }
            ss << this->type_info()->to_string(&item);
        }
        ss << "))";
        return ss.str();
    }

private:
    std::vector<std::string> _zero_padded_strs;
    ItemHashSet<Slice> _slices;
};

class DictionaryCodeInPredicate final : public ColumnPredicate {
private:
    enum LogicOp { ASSIGN, AND, OR };

public:
    DictionaryCodeInPredicate(const TypeInfoPtr& type_info, ColumnId id, const std::vector<int32_t>& operands,
                              size_t size)
            : ColumnPredicate(type_info, id), _bit_mask(size) {
        for (auto item : operands) {
            DCHECK(item < size);
            _bit_mask[item] = 1;
        }
    }

    ~DictionaryCodeInPredicate() override = default;

    template <LogicOp Op>
    inline void t_evaluate(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
        const Int32Column* dict_code_column = down_cast<const Int32Column*>(ColumnHelper::get_data_column(column));
        const auto& data = dict_code_column->get_data();
        Filter filter(to - from, 1);

        if (column->has_null()) {
            const NullColumn* null_column = down_cast<const NullableColumn*>(column)->null_column().get();
            const auto& null_data = null_column->get_data();
            for (auto i = from; i < to; i++) {
                auto index = data[i] >= _bit_mask.size() ? 0 : data[i];
                filter[i - from] = (!null_data[i]) & _bit_mask[index];
            }
        } else {
            for (auto i = from; i < to; i++) {
                filter[i - from] = _bit_mask[data[i]];
            }
        }

        for (auto i = from; i < to; i++) {
            if constexpr (Op == ASSIGN) {
                sel[i] = filter[i - from];
            } else if constexpr (Op == AND) {
                sel[i] &= filter[i - from];
            } else {
                sel[i] |= filter[i - from];
            }
        }
    }

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ASSIGN>(column, selection, from, to);
        return Status::OK();
    }

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<AND>(column, selection, from, to);
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<OR>(column, selection, from, to);
        return Status::OK();
    }

    bool can_vectorized() const override { return false; }

    PredicateType type() const override { return PredicateType::kInList; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        const auto to_type = target_type_info->type();
        if (to_type == LogicalType::TYPE_INT) {
            *output = this;
            return Status::OK();
        }
        CHECK(false) << "Not support, from_type=" << LogicalType::TYPE_INT << ", to_type=" << to_type;
        return Status::OK();
    }

private:
    std::vector<uint8_t> _bit_mask;
};

template <template <typename, size_t...> typename Set, size_t... Args>
ColumnPredicate* new_column_in_predicate_generic(const TypeInfoPtr& type_info, ColumnId id,
                                                 const std::vector<std::string>& strs) {
    auto type = type_info->type();
    auto scale = type_info->scale();
    switch (type) {
    case TYPE_BOOLEAN: {
        using SetType = Set<CppTypeTraits<TYPE_BOOLEAN>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_BOOLEAN>(strs);
        return new ColumnInPredicate<TYPE_BOOLEAN, SetType>(type_info, id, std::move(values));
    }
    case TYPE_TINYINT: {
        using SetType = Set<CppTypeTraits<TYPE_TINYINT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_TINYINT>(strs);
        return new ColumnInPredicate<TYPE_TINYINT, SetType>(type_info, id, std::move(values));
    }
    case TYPE_SMALLINT: {
        using SetType = Set<CppTypeTraits<TYPE_SMALLINT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_SMALLINT>(strs);
        return new ColumnInPredicate<TYPE_SMALLINT, SetType>(type_info, id, std::move(values));
    }
    case TYPE_INT: {
        using SetType = Set<CppTypeTraits<TYPE_INT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_INT>(strs);
        return new ColumnInPredicate<TYPE_INT, SetType>(type_info, id, std::move(values));
    }
    case TYPE_BIGINT: {
        using SetType = Set<CppTypeTraits<TYPE_BIGINT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_BIGINT>(strs);
        return new ColumnInPredicate<TYPE_BIGINT, SetType>(type_info, id, std::move(values));
    }
    case TYPE_LARGEINT: {
        using SetType = Set<CppTypeTraits<TYPE_LARGEINT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_LARGEINT>(strs);
        return new ColumnInPredicate<TYPE_LARGEINT, SetType>(type_info, id, std::move(values));
    }
    case TYPE_DECIMAL: {
        using SetType = Set<CppTypeTraits<TYPE_DECIMAL>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_DECIMAL>(strs);
        return new ColumnInPredicate<TYPE_DECIMAL, SetType>(type_info, id, std::move(values));
    }
    case TYPE_DECIMALV2: {
        using SetType = Set<CppTypeTraits<TYPE_DECIMALV2>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_DECIMALV2>(strs);
        return new ColumnInPredicate<TYPE_DECIMALV2, SetType>(type_info, id, std::move(values));
    }
    case TYPE_DECIMAL32: {
        using SetType = Set<CppTypeTraits<TYPE_DECIMAL32>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_decimal_set<TYPE_DECIMAL32>(scale, strs);
        return new ColumnInPredicate<TYPE_DECIMAL32, SetType>(type_info, id, std::move(values));
    }
    case TYPE_DECIMAL64: {
        using SetType = Set<CppTypeTraits<TYPE_DECIMAL64>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_decimal_set<TYPE_DECIMAL64>(scale, strs);
        return new ColumnInPredicate<TYPE_DECIMAL64, SetType>(type_info, id, std::move(values));
    }
    case TYPE_DECIMAL128: {
        using SetType = Set<CppTypeTraits<TYPE_DECIMAL128>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_decimal_set<TYPE_DECIMAL128>(scale, strs);
        return new ColumnInPredicate<TYPE_DECIMAL128, SetType>(type_info, id, std::move(values));
    }
    case TYPE_CHAR:
        return new BinaryColumnInPredicate<TYPE_CHAR>(type_info, id, strs);
    case TYPE_VARCHAR:
        return new BinaryColumnInPredicate<TYPE_VARCHAR>(type_info, id, strs);
    case TYPE_DATE_V1: {
        using SetType = Set<CppTypeTraits<TYPE_DATE_V1>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_DATE_V1>(strs);
        return new ColumnInPredicate<TYPE_DATE_V1, SetType>(type_info, id, std::move(values));
    }
    case TYPE_DATE: {
        using SetType = Set<CppTypeTraits<TYPE_DATE>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_DATE>(strs);
        return new ColumnInPredicate<TYPE_DATE, SetType>(type_info, id, std::move(values));
    }
    case TYPE_DATETIME_V1: {
        using SetType = Set<CppTypeTraits<TYPE_DATETIME_V1>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_DATETIME_V1>(strs);
        return new ColumnInPredicate<TYPE_DATETIME_V1, SetType>(type_info, id, std::move(values));
    }
    case TYPE_DATETIME: {
        using SetType = Set<CppTypeTraits<TYPE_DATETIME>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_DATETIME>(strs);
        return new ColumnInPredicate<TYPE_DATETIME, SetType>(type_info, id, std::move(values));
    }
    case TYPE_FLOAT: {
        using SetType = Set<CppTypeTraits<TYPE_FLOAT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_FLOAT>(strs);
        return new ColumnInPredicate<TYPE_FLOAT, SetType>(type_info, id, std::move(values));
    }
    case TYPE_DOUBLE: {
        using SetType = Set<CppTypeTraits<TYPE_DOUBLE>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<TYPE_DOUBLE>(strs);
        return new ColumnInPredicate<TYPE_DOUBLE, SetType>(type_info, id, std::move(values));
    }
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
    case TYPE_VARBINARY:
    case TYPE_MAX_VALUE:
        return nullptr;
        // No default to ensure newly added enumerator will be handled.
    }
    return nullptr;
}
ColumnPredicate* new_column_in_predicate_small(const TypeInfoPtr& type_info, ColumnId id,
                                               const std::vector<std::string>& strs) {
    if (strs.size() == 3) {
        return new_column_in_predicate_generic<ArraySet, 3>(type_info, id, strs);
    } else if (strs.size() == 2) {
        return new_column_in_predicate_generic<ArraySet, 2>(type_info, id, strs);
    } else if (strs.size() == 1) {
        return new_column_in_predicate_generic<ArraySet, 1>(type_info, id, strs);
    }
    CHECK(false) << "unreachable path";
    return nullptr;
}

ColumnPredicate* new_column_in_predicate(const TypeInfoPtr& type_info, ColumnId id,
                                         const std::vector<std::string>& strs) {
    if (strs.size() > 3) {
        return new_column_in_predicate_generic<ItemHashSet>(type_info, id, strs);
    } else {
        return new_column_in_predicate_small(type_info, id, strs);
    }
}

ColumnPredicate* new_dictionary_code_in_predicate(const TypeInfoPtr& type, ColumnId id,
                                                  const std::vector<int32_t>& operands, size_t size) {
    DCHECK(is_integer_type(type->type()));
    if (operands.size() <= 3 || size > 1024) {
        std::vector<std::string> str_codes;
        str_codes.reserve(operands.size());
        for (int code : operands) {
            str_codes.emplace_back(std::to_string(code));
        }
        return new_column_in_predicate(type, id, str_codes);
    } else {
        return new DictionaryCodeInPredicate(type, id, operands, size);
    }
}

} //namespace starrocks
