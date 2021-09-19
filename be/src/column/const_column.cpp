// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "column/const_column.h"

#include <algorithm>

#include "column/column_helper.h"
#include "simd/simd.h"
#include "util/coding.h"

namespace starrocks::vectorized {

ConstColumn::ConstColumn(ColumnPtr data) : _data(std::move(data)), _size(0) {
    DCHECK(!_data->is_constant());
}

ConstColumn::ConstColumn(ColumnPtr data, size_t size) : _data(std::move(data)), _size(size) {
    DCHECK(!_data->is_constant());
}

void ConstColumn::append(const Column& src, size_t offset, size_t count) {
    if (_size == 0) {
        const auto& src_column = down_cast<const ConstColumn&>(src);
        _data->append(*src_column.data_column(), 0, 1);
    }
    _size += count;
}

void ConstColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    append(src, indexes[from], size);
}

void ConstColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    append(src, index, size);
}

void ConstColumn::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    DCHECK(_size > 0);
    for (uint32_t i = from; i < to; ++i) {
        _data->fnv_hash(&hash[i], 0, 1);
    }
}

void ConstColumn::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    DCHECK(false) << "Const column shouldn't call crc32 hash";
}

size_t ConstColumn::filter_range(const Column::Filter& filter, size_t from, size_t to) {
    size_t count = SIMD::count_nonzero(&filter[from], to - from);
    this->resize(from + count);
    return from + count;
}

int ConstColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    DCHECK(rhs.is_constant());
    const auto& rhs_data = static_cast<const ConstColumn&>(rhs)._data;
    return _data->compare_at(0, 0, *rhs_data, nan_direction_hint);
}

uint8_t* ConstColumn::serialize_column(uint8_t* dst) {
    encode_fixed64_le(dst, _size);
    dst += sizeof(size_t);

    return _data->serialize_column(dst);
}

const uint8_t* ConstColumn::deserialize_column(const uint8_t* src) {
    _size = decode_fixed64_le(src);
    src += sizeof(size_t);

    return _data->deserialize_column(src);
}

} // namespace starrocks::vectorized
