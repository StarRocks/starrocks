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

#include "column/nullable_column.h"

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "simd/simd.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

NullableColumn::NullableColumn(MutableColumnPtr&& data_column, MutableColumnPtr&& null_column)
        : _data_column(std::move(data_column)), _has_null(false) {
    DCHECK(!_data_column->is_constant() && !_data_column->is_nullable())
            << "nullable column's data must be single column";
    DCHECK(!null_column->is_constant() && !null_column->is_nullable())
            << "nullable column's data must be single column";
    ColumnPtr ptr = std::move(null_column);
    _null_column = std::static_pointer_cast<NullColumn>(ptr);
    _has_null = SIMD::contain_nonzero(_null_column->get_data(), 0);
}

NullableColumn::NullableColumn(ColumnPtr data_column, NullColumnPtr null_column)
        : _data_column(std::move(data_column)),
          _null_column(std::move(null_column)),
          _has_null(SIMD::contain_nonzero(_null_column->get_data(), 0)) {
    DCHECK(!_data_column->is_constant() && !_data_column->is_nullable())
            << "nullable column's data must be single column";
    DCHECK(!_null_column->is_constant() && !_null_column->is_nullable())
            << "nullable column's data must be single column";
}

size_t NullableColumn::null_count() const {
    if (!_has_null) {
        return 0;
    }
    return SIMD::count_nonzero(_null_column->get_data());
}

size_t NullableColumn::null_count(size_t offset, size_t count) const {
    if (!_has_null) {
        return 0;
    }
    return SIMD::count_nonzero(_null_column->raw_data() + offset, count);
}

void NullableColumn::append_datum(const Datum& datum) {
    if (datum.is_null()) {
        append_nulls(1);
    } else {
        _data_column->append_datum(datum);
        null_column_data().emplace_back(0);
        DCHECK_EQ(_null_column->size(), _data_column->size());
    }
}

void NullableColumn::append(const Column& src, size_t offset, size_t count) {
    DCHECK_EQ(_null_column->size(), _data_column->size());

    if (src.is_nullable()) {
        const auto& c = down_cast<const NullableColumn&>(src);

        DCHECK_EQ(c._null_column->size(), c._data_column->size());

        _null_column->append(*c._null_column, offset, count);
        _data_column->append(*c._data_column, offset, count);
        _has_null = _has_null || SIMD::contain_nonzero(c._null_column->get_data(), offset, count);
    } else {
        _null_column->resize(_null_column->size() + count);
        _data_column->append(src, offset, count);
    }

    DCHECK_EQ(_null_column->size(), _data_column->size());
}

void NullableColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK_EQ(_null_column->size(), _data_column->size());
    size_t orig_size = _null_column->size();

    if (src.is_nullable()) {
        const auto& src_column = down_cast<const NullableColumn&>(src);

        DCHECK_EQ(src_column._null_column->size(), src_column._data_column->size());

        _null_column->append_selective(*src_column._null_column, indexes, from, size);
        _data_column->append_selective(*src_column._data_column, indexes, from, size);
        _has_null = _has_null || SIMD::contain_nonzero(_null_column->get_data(), orig_size, size);
    } else {
        _null_column->resize(orig_size + size);
        _data_column->append_selective(src, indexes, from, size);
    }

    DCHECK_EQ(_null_column->size(), _data_column->size());
}

void NullableColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    DCHECK_EQ(_null_column->size(), _data_column->size());
    size_t orig_size = _null_column->size();
    if (src.only_null()) {
        append_nulls(size);
    } else if (src.is_nullable()) {
        const auto& src_column = down_cast<const NullableColumn&>(src);

        DCHECK_EQ(src_column._null_column->size(), src_column._data_column->size());

        _null_column->append_value_multiple_times(*src_column._null_column, index, size);
        _data_column->append_value_multiple_times(*src_column._data_column, index, size);
        _has_null = _has_null || SIMD::contain_nonzero(_null_column->get_data(), orig_size, size);
    } else {
        _null_column->resize(orig_size + size);
<<<<<<< HEAD
        _data_column->append_selective_shallow_copy(src, indexes, from, size);
    }

    DCHECK_EQ(_null_column->size(), _data_column->size());
}

void NullableColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size, bool deep_copy) {
    DCHECK_EQ(_null_column->size(), _data_column->size());
    size_t orig_size = _null_column->size();

    if (src.is_nullable()) {
        const auto& src_column = down_cast<const NullableColumn&>(src);

        DCHECK_EQ(src_column._null_column->size(), src_column._data_column->size());

        _null_column->append_value_multiple_times(*src_column._null_column, index, size, deep_copy);
        _data_column->append_value_multiple_times(*src_column._data_column, index, size, deep_copy);
        _has_null = _has_null || SIMD::contain_nonzero(_null_column->get_data(), orig_size, size);
    } else {
        _null_column->resize(orig_size + size);
        _data_column->append_value_multiple_times(src, index, size, deep_copy);
    }

    DCHECK_EQ(_null_column->size(), _data_column->size());
}

void NullableColumn::append_shallow_copy(const Column& src, size_t offset, size_t count) {
    if (src.only_null()) {
        append_nulls(count);
    } else if (src.is_nullable()) {
        const auto& c = down_cast<const NullableColumn&>(src);

        DCHECK_EQ(c._null_column->size(), c._data_column->size());

        _null_column->append(*c._null_column, offset, count);
        _data_column->append_shallow_copy(*c._data_column, offset, count);
        _has_null = _has_null || SIMD::contain_nonzero(c._null_column->get_data(), offset, count);
    } else {
        _null_column->resize(_null_column->size() + count);
        _data_column->append_shallow_copy(src, offset, count);
=======
        _data_column->append_value_multiple_times(src, index, size);
>>>>>>> 44ae317858 ([Enhancement] BitmapValue support copy on write (#34047))
    }

    DCHECK_EQ(_null_column->size(), _data_column->size());
}

ColumnPtr NullableColumn::replicate(const std::vector<uint32_t>& offsets) {
    return NullableColumn::create(this->_data_column->replicate(offsets),
                                  std::dynamic_pointer_cast<NullColumn>(this->_null_column->replicate(offsets)));
}

bool NullableColumn::append_nulls(size_t count) {
    if (count == 0) {
        return true;
    }
    _data_column->resize_uninitialized(_data_column->size() + count);
    null_column_data().insert(null_column_data().end(), count, 1);
    DCHECK_EQ(_null_column->size(), _data_column->size());
    _has_null = true;
    return true;
}

bool NullableColumn::append_strings(const Buffer<Slice>& strs) {
    if (_data_column->append_strings(strs)) {
        null_column_data().resize(_null_column->size() + strs.size(), 0);
        return true;
    }
    DCHECK_EQ(_null_column->size(), _data_column->size());
    return false;
}

bool NullableColumn::append_strings_overflow(const Buffer<Slice>& strs, size_t max_length) {
    if (_data_column->append_strings_overflow(strs, max_length)) {
        null_column_data().resize(_null_column->size() + strs.size(), 0);
        return true;
    }
    DCHECK_EQ(_null_column->size(), _data_column->size());
    return false;
}

bool NullableColumn::append_continuous_strings(const Buffer<Slice>& strs) {
    if (_data_column->append_continuous_strings(strs)) {
        null_column_data().resize(_null_column->size() + strs.size(), 0);
        return true;
    }
    DCHECK_EQ(_null_column->size(), _data_column->size());
    return false;
}

bool NullableColumn::append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) {
    if (_data_column->append_continuous_fixed_length_strings(data, size, fixed_length)) {
        null_column_data().resize(_null_column->size() + size, 0);
        return true;
    }
    DCHECK_EQ(_null_column->size(), _data_column->size());
    return false;
}

size_t NullableColumn::append_numbers(const void* buff, size_t length) {
    size_t n;
    if ((n = _data_column->append_numbers(buff, length)) > 0) {
        null_column_data().insert(null_column_data().end(), n, 0);
    }
    DCHECK_EQ(_null_column->size(), _data_column->size());
    return n;
}

void NullableColumn::append_value_multiple_times(const void* value, size_t count) {
    _data_column->append_value_multiple_times(value, count);
    null_column_data().insert(null_column_data().end(), count, 0);
}

void NullableColumn::fill_null_with_default() {
    if (null_count() == 0) {
        return;
    }
    _data_column->fill_default(_null_column->get_data());
}

void NullableColumn::update_has_null() {
    _has_null = SIMD::contain_nonzero(_null_column->get_data(), 0);
}

Status NullableColumn::update_rows(const Column& src, const uint32_t* indexes) {
    DCHECK_EQ(_null_column->size(), _data_column->size());
    size_t replace_num = src.size();
    if (src.is_nullable()) {
        const auto& c = down_cast<const NullableColumn&>(src);
        RETURN_IF_ERROR(_null_column->update_rows(*c._null_column, indexes));
        RETURN_IF_ERROR(_data_column->update_rows(*c._data_column, indexes));
        // update rows may convert between null and not null, so we need count every times
        update_has_null();
    } else {
        auto new_null_column = NullColumn::create();
        new_null_column->get_data().insert(new_null_column->get_data().end(), replace_num, 0);
        RETURN_IF_ERROR(_null_column->update_rows(*new_null_column.get(), indexes));
        RETURN_IF_ERROR(_data_column->update_rows(src, indexes));
    }

    return Status::OK();
}

size_t NullableColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    auto s1 = _data_column->filter_range(filter, from, to);
    if (!_has_null) {
        _null_column->resize(s1);
    } else {
        auto s2 = _null_column->filter_range(filter, from, to);
        DCHECK_EQ(s1, s2);
        update_has_null();
    }
    return s1;
}

int NullableColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    if (immutable_null_column_data()[left]) {
        return rhs.is_null(right) ? 0 : nan_direction_hint;
    }
    if (rhs.is_nullable()) {
        const auto& nullable_rhs = down_cast<const NullableColumn&>(rhs);
        if (nullable_rhs.immutable_null_column_data()[right]) {
            return -nan_direction_hint;
        }
        const auto& rhs_data = *(nullable_rhs._data_column);
        return _data_column->compare_at(left, right, rhs_data, nan_direction_hint);
    } else {
        return _data_column->compare_at(left, right, rhs, nan_direction_hint);
    }
}

int NullableColumn::equals(size_t left, const Column& rhs, size_t right, bool safe_eq) const {
    if (immutable_null_column_data()[left]) {
        return safe_eq ? rhs.is_null(right) : EQUALS_NULL;
    }

    // left not null
    if (rhs.is_nullable()) {
        const auto& nullable_rhs = down_cast<const NullableColumn&>(rhs);
        if (nullable_rhs.immutable_null_column_data()[right]) {
            return safe_eq ? EQUALS_FALSE : EQUALS_NULL;
        }
        const auto& rhs_data = *(nullable_rhs._data_column);
        return _data_column->equals(left, rhs_data, right, safe_eq);
    } else {
        return _data_column->equals(left, rhs, right, safe_eq);
    }
}

uint32_t NullableColumn::serialize(size_t idx, uint8_t* pos) {
    // For nullable column don't have null column data and has_null is false.
    if (!_has_null) {
        strings::memcpy_inlined(pos, &_has_null, sizeof(bool));
        return sizeof(bool) + _data_column->serialize(idx, pos + sizeof(bool));
    }

    bool null = _null_column->get_data()[idx];
    strings::memcpy_inlined(pos, &null, sizeof(bool));

    if (null) {
        return sizeof(bool);
    }

    return sizeof(bool) + _data_column->serialize(idx, pos + sizeof(bool));
}

uint32_t NullableColumn::serialize_default(uint8_t* pos) {
    bool null = true;
    strings::memcpy_inlined(pos, &null, sizeof(bool));
    return sizeof(bool);
}

size_t NullableColumn::serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval, size_t start,
                                                   size_t count) {
    _null_column->serialize_batch_at_interval(dst, byte_offset, byte_interval, start, count);
    for (size_t i = start; i < start + count; i++) {
        if (_null_column->get_data()[i] == 0) {
            _data_column->serialize(i, dst + (i - start) * byte_interval + byte_offset + 1);
        } else {
            _data_column->serialize_default(dst + (i - start) * byte_interval + byte_offset + 1);
        }
    }
    return _null_column->type_size() + _data_column->type_size();
}

void NullableColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                     uint32_t max_one_row_size) {
    _data_column->serialize_batch_with_null_masks(dst, slice_sizes, chunk_size, max_one_row_size,
                                                  _null_column->get_data().data(), _has_null);
}

const uint8_t* NullableColumn::deserialize_and_append(const uint8_t* pos) {
    bool null;
    memcpy(&null, pos, sizeof(bool));
    pos += sizeof(bool);
    null_column_data().emplace_back(null);

    if (null == 0) {
        pos = _data_column->deserialize_and_append(pos);
    } else {
        _has_null = true;
        _data_column->append_default();
    }
    return pos;
}

void NullableColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

// Note: the hash function should be same with RawValue::get_hash_value_fvn
void NullableColumn::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    // fast path when _has_null is false
    if (!_has_null) {
        _data_column->fnv_hash(hash, from, to);
        return;
    }

    auto null_data = _null_column->get_data();
    uint32_t value = 0x9e3779b9;
    while (from < to) {
        uint32_t new_from = from + 1;
        while (new_from < to && null_data[from] == null_data[new_from]) {
            ++new_from;
        }
        if (null_data[from]) {
            for (uint32_t i = from; i < new_from; ++i) {
                hash[i] = hash[i] ^ (value + (hash[i] << 6) + (hash[i] >> 2));
            }
        } else {
            _data_column->fnv_hash(hash, from, new_from);
        }
        from = new_from;
    }
}

void NullableColumn::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    // fast path when _has_null is false
    if (!_has_null) {
        _data_column->crc32_hash(hash, from, to);
        return;
    }

    auto null_data = _null_column->get_data();
    // NULL is treat as 0 when crc32 hash for data loading
    static const int INT_VALUE = 0;
    while (from < to) {
        uint32_t new_from = from + 1;
        while (new_from < to && null_data[from] == null_data[new_from]) {
            ++new_from;
        }
        if (null_data[from]) {
            for (uint32_t i = from; i < new_from; ++i) {
                hash[i] = HashUtil::zlib_crc_hash(&INT_VALUE, 4, hash[i]);
            }
        } else {
            _data_column->crc32_hash(hash, from, new_from);
        }
        from = new_from;
    }
}

int64_t NullableColumn::xor_checksum(uint32_t from, uint32_t to) const {
    if (!_has_null) {
        return _data_column->xor_checksum(from, to);
    }

    int64_t xor_checksum = 0;
    size_t num = _null_column->size();
    uint8_t* src = _null_column->get_data().data();

    // The XOR of NullableColumn
    // XOR all the 8-bit integers one by one
    for (size_t i = 0; i < num; ++i) {
        xor_checksum ^= src[i];
        if (!src[i]) {
            xor_checksum ^= _data_column->xor_checksum(static_cast<uint32_t>(i), static_cast<uint32_t>(i + 1));
        }
    }
    return xor_checksum;
}

void NullableColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    if (_has_null && _null_column->get_data()[idx]) {
        buf->push_null();
    } else {
        _data_column->put_mysql_row_buffer(buf, idx);
    }
}

void NullableColumn::check_or_die() const {
    CHECK_EQ(_null_column->size(), _data_column->size());
    // when _has_null=true, the column may have no null value, so don't check.
    if (!_has_null) {
        CHECK(!SIMD::contain_nonzero(_null_column->get_data(), 0));
    }
    _data_column->check_or_die();
    _null_column->check_or_die();
}

StatusOr<ColumnPtr> NullableColumn::upgrade_if_overflow() {
    if (_null_column->capacity_limit_reached()) {
        return Status::InternalError("Size of NullableColumn exceed the limit");
    }

    return upgrade_helper_func(&_data_column);
}

StatusOr<ColumnPtr> NullableColumn::downgrade() {
    return downgrade_helper_func(&_data_column);
}

} // namespace starrocks
