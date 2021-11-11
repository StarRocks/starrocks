// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include <cstdint>

#include "column/binary_column.h"
#include "column/column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "roaring/roaring.hh"
#include "storage/rowset/segment_v2/bitmap_index_reader.h"
#include "storage/types.h"
#include "storage/vectorized/column_predicate.h"

namespace starrocks::vectorized {

template <FieldType field_type>
class ColumnGePredicate : public ColumnPredicate {
    using ValueType = typename CppTypeTraits<field_type>::CppType;

public:
    ColumnGePredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : ColumnPredicate(type_info, id), _value(value) {}

    ~ColumnGePredicate() override = default;

    template <typename Op>
    inline void t_evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
        auto* v = reinterpret_cast<const ValueType*>(column->raw_data());
        auto* sel = selection;
        if (!column->has_null()) {
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(v[i] >= _value));
            }
        } else {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)((!is_null[i]) & (v[i] >= _value)));
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

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& max = detail.max_value();
        return this->type_info()->cmp(Datum(_value), max) <= 0;
    }

    Status seek_bitmap_dictionary(segment_v2::BitmapIndexIterator* iter, SparseRange* range) const override {
        range->clear();
        bool exact_match;
        Status s = iter->seek_dictionary(&_value, &exact_match);
        if (s.ok()) {
            segment_v2::rowid_t seeked_ordinal = iter->current_ordinal();
            segment_v2::rowid_t ordinal_limit = iter->bitmap_nums() - iter->has_null_bitmap();
            range->add(Range(seeked_ordinal, ordinal_limit));
        } else if (!s.is_not_found()) {
            return s;
        }
        return Status::OK();
    }

    PredicateType type() const override { return PredicateType::kGE; }

    Datum value() const override { return Datum(_value); }

    std::vector<Datum> values() const override { return std::vector<Datum>{Datum(_value)}; }

    bool can_vectorized() const override { return true; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        return predicate_convert_to<field_type>(*this, _value, new_column_ge_predicate, output, target_type_info,
                                                obj_pool);
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "(columnId(" << _column_id << ")>=" << this->type_info()->to_string(&_value) << ")";
        return ss.str();
    }

private:
    ValueType _value;
};

// BinaryColumnNePredicate use logical operators (&&, ||) instead of bit operators (&, |).
template <FieldType field_type>
class BinaryColumnGePredicate : public ColumnPredicate {
    using ValueType = Slice;

public:
    BinaryColumnGePredicate(const TypeInfoPtr& type_info, ColumnId id, ValueType value)
            : ColumnPredicate(type_info, id), _zero_padded_str(value.data, value.size), _value(_zero_padded_str) {}

    ~BinaryColumnGePredicate() override = default;

    template <typename Op>
    inline void t_evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
        auto* v = reinterpret_cast<const ValueType*>(column->raw_data());
        auto* sel = selection;
        if (!column->has_null()) {
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(v[i] >= _value));
            }
        } else {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)((!is_null[i]) && (v[i] >= _value)));
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
                new_size += binary_column->get_slice(data_idx) >= _value;
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !is_null[data_idx] && binary_column->get_slice(data_idx) >= _value;
            }
        }
        return new_size;
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& max = detail.max_value();
        return this->type_info()->cmp(Datum(_value), max) <= 0;
    }

    Status seek_bitmap_dictionary(segment_v2::BitmapIndexIterator* iter, SparseRange* range) const override {
        // Can NOT use `_value` here, see the comment in `predicate_parser.cpp`.
        Slice padded_value(_zero_padded_str);

        range->clear();
        bool exact_match;
        Status s = iter->seek_dictionary(&padded_value, &exact_match);
        if (s.ok()) {
            segment_v2::rowid_t seeked_ordinal = iter->current_ordinal();
            segment_v2::rowid_t ordinal_limit = iter->bitmap_nums() - iter->has_null_bitmap();
            range->add(Range(seeked_ordinal, ordinal_limit));
        } else if (!s.is_not_found()) {
            return s;
        }
        return Status::OK();
    }

    PredicateType type() const override { return PredicateType::kGE; }

    Datum value() const override { return Datum(Slice(_zero_padded_str)); }

    std::vector<Datum> values() const override { return std::vector<Datum>{Datum(_value)}; }

    bool can_vectorized() const override { return false; }

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
        size_t old_sz = _zero_padded_str.size();
        _zero_padded_str.append(len > old_sz ? len - old_sz : 0, '\0');
        _value = Slice(_zero_padded_str.data(), old_sz);
        return true;
    }

private:
    std::string _zero_padded_str;
    ValueType _value;
};

// declared in column_predicate.h.
ColumnPredicate* new_column_ge_predicate(const TypeInfoPtr& type_info, ColumnId id, const Slice& operand) {
    return new_column_predicate<ColumnGePredicate, BinaryColumnGePredicate>(type_info, id, operand);
}

} // namespace starrocks::vectorized
