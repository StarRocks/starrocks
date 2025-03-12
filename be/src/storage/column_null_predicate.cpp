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

#include <cstring>

#include "column/column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "storage/column_predicate.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "util/bloom_filter.h"

namespace starrocks {

class ColumnIsNullPredicate final : public ColumnPredicate {
public:
    explicit ColumnIsNullPredicate(const TypeInfoPtr& type_info, ColumnId id) : ColumnPredicate(type_info, id) {}

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            memcpy(&selection[from], &is_null[from], to - from);
        } else {
            memset(selection + from, 0, to - from);
        }
        return Status::OK();
    }

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                selection[i] &= is_null[i];
            }
        } else {
            memset(selection + from, 0, to - from);
        }
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                selection[i] |= is_null[i];
            }
        } else {
            // nothing to do.
        }
        return Status::OK();
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& min = detail.min_or_null_value();
        return min.is_null();
    }

    bool support_bitmap_filter() const override { return true; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        range->clear();
        if (iter->has_null_bitmap()) {
            range->add(Range<>(iter->bitmap_nums() - 1, iter->bitmap_nums()));
        }
        return Status::OK();
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        roaring::Roaring null_roaring;
        RETURN_IF_ERROR(iterator->read_null(column_name, &null_roaring));
        *row_bitmap &= null_roaring;
        return Status::OK();
    }

    bool support_original_bloom_filter() const override { return true; }

    bool original_bloom_filter(const BloomFilter* bf) const override { return bf->test_bytes(nullptr, 0); }

    PredicateType type() const override { return PredicateType::kIsNull; }

    bool can_vectorized() const override { return true; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& type_info,
                      ObjectPool* obj_pool) const override {
        *output = this;
        return Status::OK();
    }

    std::string debug_string() const override { return strings::Substitute("(ColumnId($0) IS NULL)", _column_id); }
};

class ColumnNotNullPredicate final : public ColumnPredicate {
public:
    explicit ColumnNotNullPredicate(const TypeInfoPtr& type_info, ColumnId id) : ColumnPredicate(type_info, id) {}

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                selection[i] = !is_null[i];
            }
        } else {
            memset(selection + from, 1, to - from);
        }
        return Status::OK();
    }

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                selection[i] &= !is_null[i];
            }
        } else {
            // nothing to do.
        }
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (column->has_null()) {
            /* must use const uint8_t* to make vectorized effect, vector<uint8_t> not work */
            const uint8_t* is_null = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                selection[i] |= !is_null[i];
            }
        } else {
            memset(selection + from, 1, to - from);
        }
        return Status::OK();
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        const auto& max = detail.max_value();
        return !max.is_null();
    }

    bool support_bitmap_filter() const override { return false; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        return Status::Cancelled("not null predicate not support bitmap index");
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        roaring::Roaring null_roaring;
        RETURN_IF_ERROR(iterator->read_null(column_name, &null_roaring));
        *row_bitmap -= null_roaring;
        return Status::OK();
    }

    PredicateType type() const override { return PredicateType::kNotNull; }

    bool can_vectorized() const override { return true; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        *output = this;
        return Status::OK();
    }

    std::string debug_string() const override { return strings::Substitute("(ColumnId($0) IS NOT NULL)", _column_id); }
};

ColumnPredicate* new_column_null_predicate(const TypeInfoPtr& type_info, ColumnId id, bool is_null) {
    if (is_null) {
        return new ColumnIsNullPredicate(type_info, id);
    }
    return new ColumnNotNullPredicate(type_info, id);
}

} // namespace starrocks
