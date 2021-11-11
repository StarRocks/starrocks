// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include <type_traits>

#include "column/binary_column.h"
#include "column/column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "roaring/roaring.hh"
#include "storage/rowset/segment_v2/bitmap_index_reader.h"
#include "storage/rowset/segment_v2/bloom_filter.h"
#include "storage/vectorized/column_predicate.h"
#include "storage/vectorized/in_predicate_utils.h"

namespace starrocks::vectorized {

template <FieldType field_type, typename ItemSet>
class ColumnInPredicate : public ColumnPredicate {
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

    void evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAssignOp>(column, selection, from, to);
    }

    void evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAndOp>(column, selection, from, to);
    }

    void evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateOrOp>(column, selection, from, to);
    }

    uint16_t evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const override {
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

    Status seek_bitmap_dictionary(segment_v2::BitmapIndexIterator* iter, SparseRange* range) const override {
        range->clear();
        for (auto value : _values) {
            bool exact_match = false;
            Status s = iter->seek_dictionary(&value, &exact_match);
            if (s.ok() && exact_match) {
                segment_v2::rowid_t seeked_ordinal = iter->current_ordinal();
                range->add(Range(seeked_ordinal, seeked_ordinal + 1));
            } else if (!s.ok() && !s.is_not_found()) {
                return s;
            }
        }
        return Status::OK();
    }

    bool support_bloom_filter() const override { return true; }

    bool bloom_filter(const segment_v2::BloomFilter* bf) const override {
        static_assert(field_type != OLAP_FIELD_TYPE_HLL, "TODO");
        static_assert(field_type != OLAP_FIELD_TYPE_OBJECT, "TODO");
        static_assert(field_type != OLAP_FIELD_TYPE_PERCENTILE, "TODO");
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
        ss << "(columnId=" << _column_id << ",In(";
        int i = 0;
        for (auto& item : _values) {
            if (i++ != 0) {
                ss << ",";
            }
            ss << this->type_info()->to_string(&item);
        }
        ss << ")";
        return ss.str();
    }

private:
    ItemSet _values;
};

// Template specialization for binary column
template <FieldType field_type>
class BinaryColumnInPredicate : public ColumnPredicate {
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

    void evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAssignOp>(column, selection, from, to);
    }

    void evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAndOp>(column, selection, from, to);
    }

    void evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateOrOp>(column, selection, from, to);
    }

    uint16_t evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const override {
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

    Status seek_bitmap_dictionary(segment_v2::BitmapIndexIterator* iter, SparseRange* range) const override {
        range->clear();
        for (const std::string& s : _zero_padded_strs) {
            Slice padded_value(s);
            bool exact_match = false;
            Status st = iter->seek_dictionary(&padded_value, &exact_match);
            if (st.ok() && exact_match) {
                segment_v2::rowid_t seeked_ordinal = iter->current_ordinal();
                range->add(Range(seeked_ordinal, seeked_ordinal + 1));
            } else if (!st.ok() && !st.is_not_found()) {
                return st;
            }
        }
        return Status::OK();
    }

    bool support_bloom_filter() const override { return true; }

    bool bloom_filter(const segment_v2::BloomFilter* bf) const override {
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

private:
    std::vector<std::string> _zero_padded_strs;
    ItemHashSet<Slice> _slices;
};

template <template <typename, size_t...> typename Set, size_t... Args>
ColumnPredicate* new_column_in_predicate_generic(const TypeInfoPtr& type_info, ColumnId id,
                                                 const std::vector<std::string>& strs) {
    auto type = type_info->type();
    auto scale = type_info->scale();
    switch (type) {
    case OLAP_FIELD_TYPE_BOOL: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_BOOL>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_BOOL>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_BOOL, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_TINYINT: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_TINYINT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_TINYINT>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_TINYINT, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_SMALLINT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_SMALLINT>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_SMALLINT, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_INT: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_INT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_INT>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_INT, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_BIGINT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_BIGINT>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_BIGINT, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_LARGEINT>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_LARGEINT, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_DECIMAL: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_DECIMAL>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_DECIMAL, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_DECIMAL_V2: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL_V2>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_DECIMAL_V2>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_DECIMAL_V2, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_DECIMAL32: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL32>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_decimal_set<OLAP_FIELD_TYPE_DECIMAL32>(scale, strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_DECIMAL32, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_DECIMAL64: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL64>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_decimal_set<OLAP_FIELD_TYPE_DECIMAL64>(scale, strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_DECIMAL64, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_DECIMAL128: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL128>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_decimal_set<OLAP_FIELD_TYPE_DECIMAL128>(scale, strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_DECIMAL128, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_CHAR:
        return new BinaryColumnInPredicate<OLAP_FIELD_TYPE_CHAR>(type_info, id, strs);
    case OLAP_FIELD_TYPE_VARCHAR:
        return new BinaryColumnInPredicate<OLAP_FIELD_TYPE_VARCHAR>(type_info, id, strs);
    case OLAP_FIELD_TYPE_DATE: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_DATE>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_DATE>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_DATE, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_DATE_V2: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_DATE_V2>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_DATE_V2>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_DATE_V2, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_DATETIME>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_DATETIME>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_DATETIME, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_TIMESTAMP: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_TIMESTAMP>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_TIMESTAMP>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_TIMESTAMP, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_FLOAT: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_FLOAT>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_FLOAT>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_FLOAT, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_DOUBLE: {
        using SetType = Set<CppTypeTraits<OLAP_FIELD_TYPE_DOUBLE>::CppType, (Args)...>;
        SetType values = predicate_internal::strings_to_set<OLAP_FIELD_TYPE_DOUBLE>(strs);
        return new ColumnInPredicate<OLAP_FIELD_TYPE_DOUBLE, SetType>(type_info, id, std::move(values));
    }
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_ARRAY:
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_UNKNOWN:
    case OLAP_FIELD_TYPE_NONE:
    case OLAP_FIELD_TYPE_HLL:
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_PERCENTILE:
    case OLAP_FIELD_TYPE_MAX_VALUE:
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

} //namespace starrocks::vectorized
