// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <cstring>

#include "column/column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "roaring/roaring.hh"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bloom_filter.h"
#include "storage/vectorized/column_predicate.h"

namespace starrocks::vectorized {

class ColumnIsNullPredicate : public ColumnPredicate {
public:
    explicit ColumnIsNullPredicate(const TypeInfoPtr& type_info, ColumnId id) : ColumnPredicate(type_info, id) {}

    void evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            memcpy(&selection[from], &is_null[from], to - from);
        } else {
            memset(selection + from, 0, to - from);
        }
    }

    void evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                selection[i] &= is_null[i];
            }
        } else {
            memset(selection + from, 0, to - from);
        }
    }

    void evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                selection[i] |= is_null[i];
            }
        } else {
            // nothing to do.
        }
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& min = detail.min_or_null_value();
        return min.is_null();
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange* range) const override {
        range->clear();
        if (iter->has_null_bitmap()) {
            range->add(Range(iter->bitmap_nums() - 1, iter->bitmap_nums()));
        }
        return Status::OK();
    }

    bool support_bloom_filter() const override { return true; }

    bool bloom_filter(const BloomFilter* bf) const override { return bf->test_bytes(nullptr, 0); }

    PredicateType type() const override { return PredicateType::kIsNull; }

    bool can_vectorized() const override { return true; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& type_info,
                      ObjectPool* obj_pool) const override {
        *output = this;
        return Status::OK();
    }
};

class ColumnNotNullPredicate : public ColumnPredicate {
public:
    explicit ColumnNotNullPredicate(const TypeInfoPtr& type_info, ColumnId id) : ColumnPredicate(type_info, id) {}

    void evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                selection[i] = !is_null[i];
            }
        } else {
            memset(selection + from, 1, to - from);
        }
    }

    void evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                selection[i] &= !is_null[i];
            }
        } else {
            // nothing to do.
        }
    }

    void evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                selection[i] |= !is_null[i];
            }
        } else {
            memset(selection + from, 1, to - from);
        }
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& max = detail.max_value();
        return !max.is_null();
    }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange* range) const override {
        return Status::Cancelled("not null predicate not support bitmap index");
    }

    PredicateType type() const override { return PredicateType::kNotNull; }

    bool can_vectorized() const override { return true; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        *output = this;
        return Status::OK();
    }
};

ColumnPredicate* new_column_null_predicate(const TypeInfoPtr& type_info, ColumnId id, bool is_null) {
    if (is_null) {
        return new ColumnIsNullPredicate(type_info, id);
    }
    return new ColumnNotNullPredicate(type_info, id);
}

} // namespace starrocks::vectorized
