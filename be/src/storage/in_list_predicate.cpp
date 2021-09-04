// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/in_list_predicate.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/in_list_predicate.h"

#include "gutil/strings/substitute.h"
#include "runtime/decimalv2_value.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "storage/decimal12.h"
#include "storage/field.h"
#include "storage/uint24.h"
#include "storage/vectorized/convert_helper.h"

namespace starrocks {

using strings::Substitute;

#define IN_LIST_PRED_CONSTRUCTOR(CLASS)                             \
    template <class type>                                           \
    CLASS<type>::CLASS(uint32_t column_id, std::set<type>&& values) \
            : ColumnPredicate(column_id), _values(std::move(values)) {}

IN_LIST_PRED_CONSTRUCTOR(InListPredicate)
IN_LIST_PRED_CONSTRUCTOR(NotInListPredicate)

#define IN_LIST_PRED_EVALUATE(CLASS, OP)                                                               \
    template <class type>                                                                              \
    void CLASS<type>::evaluate(VectorizedRowBatch* batch) const {                                      \
        uint16_t n = batch->size();                                                                    \
        if (n == 0) {                                                                                  \
            return;                                                                                    \
        }                                                                                              \
        uint16_t* sel = batch->selected();                                                             \
        const type* col_vector = reinterpret_cast<const type*>(batch->column(_column_id)->col_data()); \
        uint16_t new_size = 0;                                                                         \
        if (batch->column(_column_id)->no_nulls()) {                                                   \
            if (batch->selected_in_use()) {                                                            \
                for (uint16_t j = 0; j != n; ++j) {                                                    \
                    uint16_t i = sel[j];                                                               \
                    sel[new_size] = i;                                                                 \
                    new_size += (_values.find(col_vector[i]) OP _values.end());                        \
                }                                                                                      \
                batch->set_size(new_size);                                                             \
            } else {                                                                                   \
                for (uint16_t i = 0; i != n; ++i) {                                                    \
                    sel[new_size] = i;                                                                 \
                    new_size += (_values.find(col_vector[i]) OP _values.end());                        \
                }                                                                                      \
                if (new_size < n) {                                                                    \
                    batch->set_size(new_size);                                                         \
                    batch->set_selected_in_use(true);                                                  \
                }                                                                                      \
            }                                                                                          \
        } else {                                                                                       \
            bool* is_null = batch->column(_column_id)->is_null();                                      \
            if (batch->selected_in_use()) {                                                            \
                for (uint16_t j = 0; j != n; ++j) {                                                    \
                    uint16_t i = sel[j];                                                               \
                    sel[new_size] = i;                                                                 \
                    new_size += (!is_null[i] && _values.find(col_vector[i]) OP _values.end());         \
                }                                                                                      \
                batch->set_size(new_size);                                                             \
            } else {                                                                                   \
                for (int i = 0; i != n; ++i) {                                                         \
                    sel[new_size] = i;                                                                 \
                    new_size += (!is_null[i] && _values.find(col_vector[i]) OP _values.end());         \
                }                                                                                      \
                if (new_size < n) {                                                                    \
                    batch->set_size(new_size);                                                         \
                    batch->set_selected_in_use(true);                                                  \
                }                                                                                      \
            }                                                                                          \
        }                                                                                              \
    }

IN_LIST_PRED_EVALUATE(InListPredicate, !=)
IN_LIST_PRED_EVALUATE(NotInListPredicate, ==)

#define IN_LIST_PRED_COLUMN_BLOCK_EVALUATE(CLASS, OP)                                                    \
    template <class type>                                                                                \
    void CLASS<type>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const {                \
        uint16_t new_size = 0;                                                                           \
        if (block->is_nullable()) {                                                                      \
            for (uint16_t i = 0; i < *size; ++i) {                                                       \
                uint16_t idx = sel[i];                                                                   \
                sel[new_size] = idx;                                                                     \
                const type* cell_value = reinterpret_cast<const type*>(block->cell(idx).cell_ptr());     \
                new_size += (!block->cell(idx).is_null() && _values.find(*cell_value) OP _values.end()); \
            }                                                                                            \
        } else {                                                                                         \
            for (uint16_t i = 0; i < *size; ++i) {                                                       \
                uint16_t idx = sel[i];                                                                   \
                sel[new_size] = idx;                                                                     \
                const type* cell_value = reinterpret_cast<const type*>(block->cell(idx).cell_ptr());     \
                new_size += (_values.find(*cell_value) OP _values.end());                                \
            }                                                                                            \
        }                                                                                                \
        *size = new_size;                                                                                \
    }

IN_LIST_PRED_COLUMN_BLOCK_EVALUATE(InListPredicate, !=)
IN_LIST_PRED_COLUMN_BLOCK_EVALUATE(NotInListPredicate, ==)

#define IN_LIST_PRED_BITMAP_EVALUATE(CLASS, OP)                                                            \
    template <class type>                                                                                  \
    Status CLASS<type>::evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators, \
                                 uint32_t num_rows, Roaring* result) const {                               \
        BitmapIndexIterator* iterator = iterators[_column_id];                                             \
        if (iterator == nullptr) {                                                                         \
            return Status::OK();                                                                           \
        }                                                                                                  \
        if (iterator->has_null_bitmap()) {                                                                 \
            Roaring null_bitmap;                                                                           \
            RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap));                                     \
            *result -= null_bitmap;                                                                        \
        }                                                                                                  \
        Roaring indices;                                                                                   \
        for (auto value : _values) {                                                                       \
            bool exact_match;                                                                              \
            Status s = iterator->seek_dictionary(&value, &exact_match);                                    \
            rowid_t seeked_ordinal = iterator->current_ordinal();                                          \
            if (!s.is_not_found()) {                                                                       \
                if (!s.ok()) {                                                                             \
                    return s;                                                                              \
                }                                                                                          \
                if (exact_match) {                                                                         \
                    Roaring index;                                                                         \
                    RETURN_IF_ERROR(iterator->read_bitmap(seeked_ordinal, &index));                        \
                    indices |= index;                                                                      \
                }                                                                                          \
            }                                                                                              \
        }                                                                                                  \
        *result OP indices;                                                                                \
        return Status::OK();                                                                               \
    }

IN_LIST_PRED_BITMAP_EVALUATE(InListPredicate, &=)
IN_LIST_PRED_BITMAP_EVALUATE(NotInListPredicate, -=)

#define DEFINE_CONVERT_TO_FUNCTION(CLASS)                                                                  \
    template <class type>                                                                                  \
    Status CLASS<type>::convert_to(const ColumnPredicate** output, FieldType from_type, FieldType to_type, \
                                   ObjectPool* obj_pool) const {                                           \
        if (from_type == to_type) {                                                                        \
            *output = this;                                                                                \
            return Status::OK();                                                                           \
        }                                                                                                  \
        auto converter = vectorized::get_field_converter(from_type, to_type);                              \
        if (converter == nullptr) {                                                                        \
            return Status::InternalError("Cannot get filed converter");                                    \
        }                                                                                                  \
        switch (to_type) {                                                                                 \
        default:                                                                                           \
            break;                                                                                         \
        case OLAP_FIELD_TYPE_DATE: {                                                                       \
            std::set<uint24_t> new_values;                                                                 \
            for (auto value : _values) {                                                                   \
                uint24_t new_value = 0;                                                                    \
                converter->convert(&new_value, &value);                                                    \
                new_values.insert(new_value);                                                              \
            }                                                                                              \
            *output = obj_pool->add(new CLASS<uint24_t>(_column_id, std::move(new_values)));               \
            return Status::OK();                                                                           \
        }                                                                                                  \
        case OLAP_FIELD_TYPE_DATE_V2: {                                                                    \
            std::set<int32_t> new_values;                                                                  \
            for (auto value : _values) {                                                                   \
                int32_t new_value = 0;                                                                     \
                converter->convert(&new_value, &value);                                                    \
                new_values.insert(new_value);                                                              \
            }                                                                                              \
            *output = obj_pool->add(new CLASS<int32_t>(_column_id, std::move(new_values)));                \
            return Status::OK();                                                                           \
        }                                                                                                  \
        case OLAP_FIELD_TYPE_DATETIME: {                                                                   \
            std::set<uint64_t> new_values;                                                                 \
            for (auto value : _values) {                                                                   \
                uint64_t new_value = 0;                                                                    \
                converter->convert(&new_value, &value);                                                    \
                new_values.insert(new_value);                                                              \
            }                                                                                              \
            *output = obj_pool->add(new CLASS<uint64_t>(_column_id, std::move(new_values)));               \
            return Status::OK();                                                                           \
        }                                                                                                  \
        case OLAP_FIELD_TYPE_TIMESTAMP: {                                                                  \
            std::set<int64_t> new_values;                                                                  \
            for (auto value : _values) {                                                                   \
                int64_t new_value = 0;                                                                     \
                converter->convert(&new_value, &value);                                                    \
                new_values.insert(new_value);                                                              \
            }                                                                                              \
            *output = obj_pool->add(new CLASS<int64_t>(_column_id, std::move(new_values)));                \
            return Status::OK();                                                                           \
        }                                                                                                  \
        case OLAP_FIELD_TYPE_DECIMAL: {                                                                    \
            std::set<decimal12_t> new_values;                                                              \
            for (auto value : _values) {                                                                   \
                decimal12_t new_value;                                                                     \
                converter->convert(&new_value, &value);                                                    \
                new_values.insert(new_value);                                                              \
            }                                                                                              \
            *output = obj_pool->add(new CLASS<decimal12_t>(_column_id, std::move(new_values)));            \
            return Status::OK();                                                                           \
        }                                                                                                  \
        case OLAP_FIELD_TYPE_DECIMAL_V2: {                                                                 \
            std::set<DecimalV2Value> new_values;                                                           \
            for (auto value : _values) {                                                                   \
                DecimalV2Value new_value;                                                                  \
                converter->convert(&new_value, &value);                                                    \
                new_values.insert(new_value);                                                              \
            }                                                                                              \
            *output = obj_pool->add(new CLASS<DecimalV2Value>(_column_id, std::move(new_values)));         \
            return Status::OK();                                                                           \
        }                                                                                                  \
        }                                                                                                  \
        return Status::NotSupported(                                                                       \
                Substitute("Don't support type convert from_type=$0, to_type=$1", from_type, to_type));    \
    }

DEFINE_CONVERT_TO_FUNCTION(InListPredicate);
DEFINE_CONVERT_TO_FUNCTION(NotInListPredicate);

#undef DEFINE_CONVERT_TO_FUNCTION

#define IN_LIST_PRED_CONSTRUCTOR_DECLARATION(CLASS)                                               \
    template CLASS<int8_t>::CLASS(uint32_t column_id, std::set<int8_t>&& values);                 \
    template CLASS<int16_t>::CLASS(uint32_t column_id, std::set<int16_t>&& values);               \
    template CLASS<int32_t>::CLASS(uint32_t column_id, std::set<int32_t>&& values);               \
    template CLASS<int64_t>::CLASS(uint32_t column_id, std::set<int64_t>&& values);               \
    template CLASS<int128_t>::CLASS(uint32_t column_id, std::set<int128_t>&& values);             \
    template CLASS<float>::CLASS(uint32_t column_id, std::set<float>&& values);                   \
    template CLASS<double>::CLASS(uint32_t column_id, std::set<double>&& values);                 \
    template CLASS<decimal12_t>::CLASS(uint32_t column_id, std::set<decimal12_t>&& values);       \
    template CLASS<DecimalV2Value>::CLASS(uint32_t column_id, std::set<DecimalV2Value>&& values); \
    template CLASS<StringValue>::CLASS(uint32_t column_id, std::set<StringValue>&& values);       \
    template CLASS<uint24_t>::CLASS(uint32_t column_id, std::set<uint24_t>&& values);             \
    template CLASS<uint64_t>::CLASS(uint32_t column_id, std::set<uint64_t>&& values);

IN_LIST_PRED_CONSTRUCTOR_DECLARATION(InListPredicate)
IN_LIST_PRED_CONSTRUCTOR_DECLARATION(NotInListPredicate)

#define IN_LIST_PRED_EVALUATE_DECLARATION(CLASS)                                 \
    template void CLASS<int8_t>::evaluate(VectorizedRowBatch* batch) const;      \
    template void CLASS<int16_t>::evaluate(VectorizedRowBatch* batch) const;     \
    template void CLASS<int32_t>::evaluate(VectorizedRowBatch* batch) const;     \
    template void CLASS<int64_t>::evaluate(VectorizedRowBatch* batch) const;     \
    template void CLASS<int128_t>::evaluate(VectorizedRowBatch* batch) const;    \
    template void CLASS<float>::evaluate(VectorizedRowBatch* batch) const;       \
    template void CLASS<double>::evaluate(VectorizedRowBatch* batch) const;      \
    template void CLASS<decimal12_t>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<StringValue>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<uint24_t>::evaluate(VectorizedRowBatch* batch) const;    \
    template void CLASS<uint64_t>::evaluate(VectorizedRowBatch* batch) const;

IN_LIST_PRED_EVALUATE_DECLARATION(InListPredicate)
IN_LIST_PRED_EVALUATE_DECLARATION(NotInListPredicate)

#define IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(CLASS)                                            \
    template void CLASS<int8_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const;      \
    template void CLASS<int16_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const;     \
    template void CLASS<int32_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const;     \
    template void CLASS<int64_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const;     \
    template void CLASS<int128_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const;    \
    template void CLASS<float>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const;       \
    template void CLASS<double>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const;      \
    template void CLASS<decimal12_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const; \
    template void CLASS<StringValue>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const; \
    template void CLASS<uint24_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const;    \
    template void CLASS<uint64_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const;

IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(InListPredicate)
IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(NotInListPredicate)

#define IN_LIST_PRED_BITMAP_EVALUATE_DECLARATION(CLASS)                                                                \
    template Status CLASS<int8_t>::evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators,  \
                                            uint32_t num_rows, Roaring* bitmap) const;                                 \
    template Status CLASS<int16_t>::evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators, \
                                             uint32_t num_rows, Roaring* bitmap) const;                                \
    template Status CLASS<int32_t>::evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators, \
                                             uint32_t num_rows, Roaring* bitmap) const;                                \
    template Status CLASS<int64_t>::evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators, \
                                             uint32_t num_rows, Roaring* bitmap) const;                                \
    template Status CLASS<int128_t>::evaluate(const Schema& schema,                                                    \
                                              const std::vector<BitmapIndexIterator*>& iterators, uint32_t num_rows,   \
                                              Roaring* bitmap) const;                                                  \
    template Status CLASS<float>::evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators,   \
                                           uint32_t num_rows, Roaring* bitmap) const;                                  \
    template Status CLASS<double>::evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators,  \
                                            uint32_t num_rows, Roaring* bitmap) const;                                 \
    template Status CLASS<decimal12_t>::evaluate(const Schema& schema,                                                 \
                                                 const std::vector<BitmapIndexIterator*>& iterators,                   \
                                                 uint32_t num_rows, Roaring* bitmap) const;                            \
    template Status CLASS<StringValue>::evaluate(const Schema& schema,                                                 \
                                                 const std::vector<BitmapIndexIterator*>& iterators,                   \
                                                 uint32_t num_rows, Roaring* bitmap) const;                            \
    template Status CLASS<uint24_t>::evaluate(const Schema& schema,                                                    \
                                              const std::vector<BitmapIndexIterator*>& iterators, uint32_t num_rows,   \
                                              Roaring* bitmap) const;                                                  \
    template Status CLASS<uint64_t>::evaluate(const Schema& schema,                                                    \
                                              const std::vector<BitmapIndexIterator*>& iterators, uint32_t num_rows,   \
                                              Roaring* bitmap) const;

IN_LIST_PRED_BITMAP_EVALUATE_DECLARATION(InListPredicate)
IN_LIST_PRED_BITMAP_EVALUATE_DECLARATION(NotInListPredicate)

} //namespace starrocks
