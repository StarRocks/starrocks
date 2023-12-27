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

#include "column/const_column.h"

#include <utility>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "simd/simd.h"
#include "util/coding.h"

namespace starrocks {

ConstColumn::ConstColumn(ColumnPtr data) : ConstColumn(std::move(data), 0) {}

ConstColumn::ConstColumn(ColumnPtr data, size_t size) : _data(std::move(data)), _size(size) {
    DCHECK(!_data->is_constant());
    if (_data->is_nullable() && size > 0 && !_data->is_null(0)) {
        _data = down_cast<NullableColumn*>(_data.get())->data_column();
    }
    DCHECK(_data->is_nullable() ? _size == 0 || _data->is_null(0) : true);
    if (_data->size() > 1) {
        _data->resize(1);
    }
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

ColumnPtr ConstColumn::replicate(const std::vector<uint32_t>& offsets) {
    return ConstColumn::create(this->_data->clone_shared(), offsets.back());
}

void ConstColumn::fill_default(const Filter& filter) {
    CHECK(false) << "ConstColumn does not support update";
}

void ConstColumn::update_rows(const Column& src, const uint32_t* indexes) {
    throw std::runtime_error("ConstColumn does not support update_rows");
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

int64_t ConstColumn::xor_checksum(uint32_t from, uint32_t to) const {
    DCHECK(false) << "Const column shouldn't call xor_checksum";
    return 0;
}

size_t ConstColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    size_t count = SIMD::count_nonzero(&filter[from], to - from);
    this->resize(from + count);
    return from + count;
}

int ConstColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    DCHECK(rhs.is_constant());
    const auto& rhs_data = static_cast<const ConstColumn&>(rhs)._data;
    return _data->compare_at(0, 0, *rhs_data, nan_direction_hint);
}

int ConstColumn::equals(size_t left, const Column& rhs, size_t right, bool safe_eq) const {
    const auto& rhs_data = static_cast<const ConstColumn&>(rhs)._data;
    return _data->equals(0, *rhs_data, right, safe_eq);
}

void ConstColumn::check_or_die() const {
    if (_size > 0) {
        CHECK_GE(_data->size(), 1);
    }
    _data->check_or_die();
}

StatusOr<ColumnPtr> ConstColumn::upgrade_if_overflow() {
    if (_size > Column::MAX_CAPACITY_LIMIT) {
        return Status::InternalError("Size of ConstColumn exceed the limit");
    }

    return upgrade_helper_func(&_data);
}

StatusOr<ColumnPtr> ConstColumn::downgrade() {
    return downgrade_helper_func(&_data);
}

} // namespace starrocks
