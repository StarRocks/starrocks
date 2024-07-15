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

#include "column/dictionary_column.h"

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

DictionaryColumn::DictionaryColumn(MutableColumnPtr&& data_column, MutableColumnPtr&& index_column)
        : _data_column(std::move(data_column)) {
    ColumnPtr ptr = std::move(index_column);
    _index_column = std::static_pointer_cast<IndexColumn>(ptr);
}

DictionaryColumn::DictionaryColumn(ColumnPtr data_column, IndexColumnPtr index_column)
        : _data_column(std::move(data_column)), _index_column(std::move(index_column)) {}

void DictionaryColumn::remove_first_n_values(size_t count) {
    _index_column->remove_first_n_values(count);
}

void DictionaryColumn::append_datum(const Datum& datum) {
    DCHECK(false);
    return;
}

void DictionaryColumn::append(const Column& src, size_t offset, size_t count) {
    DCHECK(false);
    return;
}

void DictionaryColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK(false);
    return;
}

void DictionaryColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    DCHECK(false);
    return;
}

ColumnPtr DictionaryColumn::replicate(const std::vector<uint32_t>& offsets) {
    return DictionaryColumn::create(this->_data_column->clone(),
                                    std::dynamic_pointer_cast<IndexColumn>(this->_index_column->replicate(offsets)));
}

size_t DictionaryColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    return _index_column->filter_range(filter, from, to);
}

int DictionaryColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    DCHECK(false);
    return -1;
}

int DictionaryColumn::equals(size_t left, const Column& rhs, size_t right, bool safe_eq) const {
    DCHECK(false);
    return -1;
}

uint32_t DictionaryColumn::serialize(size_t idx, uint8_t* pos) {
    DCHECK(false);
    return 0;
}

uint32_t DictionaryColumn::serialize_default(uint8_t* pos) {
    DCHECK(false);
    return 0;
}

size_t DictionaryColumn::serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval,
                                                     size_t start, size_t count) {
    DCHECK(false);
    return 0;
}

void DictionaryColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                       uint32_t max_one_row_size) {
    DCHECK(false);
    return;
}

const uint8_t* DictionaryColumn::deserialize_and_append(const uint8_t* pos) {
    DCHECK(false);
    return 0;
}

void DictionaryColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    DCHECK(false);
    return;
}

// Note: the hash function should be same with RawValue::get_hash_value_fvn
void DictionaryColumn::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    auto index_data = _index_column->get_data();
    for (size_t i = from; i < to; ++i) {
        size_t index = index_data[i];
        _data_column->fnv_hash(hash, index, index + 1);
    }
}

void DictionaryColumn::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    auto index_data = _index_column->get_data();
    for (size_t i = from; i < to; ++i) {
        size_t index = index_data[i];
        _data_column->crc32_hash(hash, index, index + 1);
    }
}

int64_t DictionaryColumn::xor_checksum(uint32_t from, uint32_t to) const {
    int64_t xor_checksum = 0;

    // The XOR of DictionaryColumn
    // XOR all the 8-bit integers one by one
    for (size_t i = from; i < to; ++i) {
        size_t index = _index_column->get_data()[i];
        xor_checksum ^= _data_column->xor_checksum(static_cast<uint32_t>(index), static_cast<uint32_t>(index + 1));
    }
    return xor_checksum;
}

void DictionaryColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    _data_column->put_mysql_row_buffer(buf, _index_column->get_data()[idx]);
}

void DictionaryColumn::check_or_die() const {
    _data_column->check_or_die();
    _index_column->check_or_die();
}

StatusOr<ColumnPtr> DictionaryColumn::upgrade_if_overflow() {
    if (_index_column->capacity_limit_reached()) {
        return Status::InternalError("Size of DictionaryColumn exceed the limit");
    }

    return upgrade_helper_func(&_data_column);
}

StatusOr<ColumnPtr> DictionaryColumn::downgrade() {
    return downgrade_helper_func(&_data_column);
}

} // namespace starrocks
