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

#include "column/adaptive_nullable_column.h"

#include "column/column_helper.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "simd/simd.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

size_t AdaptiveNullableColumn::null_count() const {
    switch (_state) {
    case State::kUninitialized: {
        return 0;
    }
    case State::kNull: {
        return _size;
    }
    default: {
        materialized_nullable();
        if (!_has_null) {
            return 0;
        }
        return SIMD::count_nonzero(_null_column->get_data());
    }
    }
}

size_t AdaptiveNullableColumn::null_count(size_t offset, size_t count) const {
    switch (_state) {
    case State::kUninitialized: {
        return 0;
    }
    case State::kNull: {
        return count - offset;
    }
    default: {
        materialized_nullable();
        if (!_has_null) {
            return 0;
        }
        return SIMD::count_nonzero(_null_column->get_data());
    }
    }
}

void AdaptiveNullableColumn::append_datum(const Datum& datum) {
    if (datum.is_null()) {
        append_nulls(1);
    } else {
        switch (_state) {
        case State::kUninitialized: {
            _state = State::kNotConstant;
            _size = 1;
            _data_column->append_datum(datum);
            break;
        }
        case State::kNotConstant: {
            _size++;
            _data_column->append_datum(datum);
            break;
        }
        case State::kMaterialized: {
            _data_column->append_datum(datum);
            null_column_data().emplace_back(0);
            DCHECK_EQ(_null_column->size(), _data_column->size());
            break;
        }
        default: {
            materialized_nullable();
            _data_column->append_datum(datum);
            null_column_data().emplace_back(0);
            DCHECK_EQ(_null_column->size(), _data_column->size());
            break;
        }
        }
    }
}

void AdaptiveNullableColumn::append(const Column& src, size_t offset, size_t count) {
    materialized_nullable();
    NullableColumn::append(src, offset, count);
}

void AdaptiveNullableColumn::append_shallow_copy(const Column& src, size_t offset, size_t count) {
    materialized_nullable();
    NullableColumn::append_shallow_copy(src, offset, count);
}

void AdaptiveNullableColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from,
                                              uint32_t size) {
    materialized_nullable();
    NullableColumn::append_selective(src, indexes, from, size);
}

void AdaptiveNullableColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size,
                                                         bool deep_copy) {
    materialized_nullable();
    NullableColumn::append_value_multiple_times(src, index, size, deep_copy);
}

bool AdaptiveNullableColumn::append_nulls(size_t count) {
    if (count == 0) {
        return true;
    }
    switch (_state) {
    case State::kUninitialized: {
        _state = State::kNull;
        _size = count;
        break;
    }
    case State::kNull: {
        _size += count;
        break;
    }
    case State::kMaterialized: {
        _data_column->append_default(count);
        null_column_data().insert(null_column_data().end(), count, 1);
        DCHECK_EQ(_null_column->size(), _data_column->size());
        _has_null = true;
        break;
    }
    default: {
        materialized_nullable();
        _data_column->append_default(count);
        null_column_data().insert(null_column_data().end(), count, 1);
        DCHECK_EQ(_null_column->size(), _data_column->size());
        _has_null = true;
        break;
    }
    }
    return true;
}

bool AdaptiveNullableColumn::append_strings(const Buffer<Slice>& strs) {
    if (_data_column->is_binary()) {
        switch (_state) {
        case State::kUninitialized: {
            _state = State::kNotConstant;
            std::ignore = _data_column->append_strings(strs);
            _size = strs.size();
            break;
        }
        case State::kNotConstant: {
            std::ignore = _data_column->append_strings(strs);
            _size += strs.size();
            break;
        }
        case State::kMaterialized: {
            std::ignore = _data_column->append_strings(strs);
            null_column_data().resize(_null_column->size() + strs.size(), 0);
            DCHECK_EQ(_null_column->size(), _data_column->size());
            break;
        }
        default: {
            materialized_nullable();
            std::ignore = _data_column->append_strings(strs);
            null_column_data().resize(_null_column->size() + strs.size(), 0);
            DCHECK_EQ(_null_column->size(), _data_column->size());
            break;
        }
        }
    } else {
        materialized_nullable();
        if (_data_column->append_strings(strs)) {
            null_column_data().resize(_null_column->size() + strs.size(), 0);
            return true;
        }
        DCHECK_EQ(_null_column->size(), _data_column->size());
        return false;
    }
    return true;
}

bool AdaptiveNullableColumn::append_strings_overflow(const Buffer<Slice>& strs, size_t max_length) {
    materialized_nullable();
    if (_data_column->append_strings_overflow(strs, max_length)) {
        null_column_data().resize(_null_column->size() + strs.size(), 0);
        return true;
    }
    DCHECK_EQ(_null_column->size(), _data_column->size());
    return false;
}

bool AdaptiveNullableColumn::append_continuous_strings(const Buffer<Slice>& strs) {
    materialized_nullable();
    if (_data_column->append_continuous_strings(strs)) {
        null_column_data().resize(_null_column->size() + strs.size(), 0);
        return true;
    }
    DCHECK_EQ(_null_column->size(), _data_column->size());
    return false;
}

bool AdaptiveNullableColumn::append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) {
    materialized_nullable();
    if (_data_column->append_continuous_fixed_length_strings(data, size, fixed_length)) {
        null_column_data().resize(_null_column->size() + size, 0);
        return true;
    }
    DCHECK_EQ(_null_column->size(), _data_column->size());
    return false;
}

size_t AdaptiveNullableColumn::append_numbers(const void* buff, size_t length) {
    materialized_nullable();
    size_t n;
    if ((n = _data_column->append_numbers(buff, length)) > 0) {
        null_column_data().insert(null_column_data().end(), n, 0);
    }
    DCHECK_EQ(_null_column->size(), _data_column->size());
    return n;
}

void AdaptiveNullableColumn::append_value_multiple_times(const void* value, size_t count) {
    materialized_nullable();
    _data_column->append_value_multiple_times(value, count);
    null_column_data().insert(null_column_data().end(), count, 0);
}

void AdaptiveNullableColumn::fill_null_with_default() {
    materialized_nullable();
    if (null_count() == 0) {
        return;
    }
    _data_column->fill_default(_null_column->get_data());
}

void AdaptiveNullableColumn::update_has_null() {
    materialized_nullable();
    _has_null = SIMD::contain_nonzero(_null_column->get_data(), 0);
}

void AdaptiveNullableColumn::update_rows(const Column& src, const uint32_t* indexes) {
    materialized_nullable();
    NullableColumn::update_rows(src, indexes);
}

int AdaptiveNullableColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    materialized_nullable();
    return NullableColumn::compare_at(left, right, rhs, nan_direction_hint);
}

uint32_t AdaptiveNullableColumn::serialize(size_t idx, uint8_t* pos) {
    materialized_nullable();
    return NullableColumn::serialize(idx, pos);
}

uint32_t AdaptiveNullableColumn::serialize_default(uint8_t* pos) {
    materialized_nullable();
    bool null = true;
    strings::memcpy_inlined(pos, &null, sizeof(bool));
    return sizeof(bool);
}

size_t AdaptiveNullableColumn::serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval,
                                                           size_t start, size_t count) {
    materialized_nullable();
    return NullableColumn::serialize_batch_at_interval(dst, byte_offset, byte_interval, start, count);
}

void AdaptiveNullableColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                             uint32_t max_one_row_size) {
    materialized_nullable();
    _data_column->serialize_batch_with_null_masks(dst, slice_sizes, chunk_size, max_one_row_size,
                                                  _null_column->get_data().data(), _has_null);
}

const uint8_t* AdaptiveNullableColumn::deserialize_and_append(const uint8_t* pos) {
    materialized_nullable();
    return NullableColumn::deserialize_and_append(pos);
}

void AdaptiveNullableColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    materialized_nullable();
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

void AdaptiveNullableColumn::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    materialized_nullable();
    return NullableColumn::fnv_hash(hash, from, to);
}

void AdaptiveNullableColumn::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    materialized_nullable();
    return NullableColumn::crc32_hash(hash, from, to);
}

int64_t AdaptiveNullableColumn::xor_checksum(uint32_t from, uint32_t to) const {
    materialized_nullable();
    return NullableColumn::xor_checksum(from, to);
}

void AdaptiveNullableColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    materialized_nullable();
    NullableColumn::put_mysql_row_buffer(buf, idx);
}

} // namespace starrocks
