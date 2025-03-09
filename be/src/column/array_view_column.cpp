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

#include "column/array_view_column.h"

#include <cstdint>
#include <memory>
#include <stdexcept>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"

namespace starrocks {

StatusOr<ColumnPtr> ArrayViewColumn::replicate(const Buffer<uint32_t>& offsets) {
    auto dest_size = offsets.size() - 1;
    auto new_offsets = UInt32Column::create();
    auto new_lengths = UInt32Column::create();
    new_offsets->reserve(offsets.back());
    new_lengths->reserve(offsets.back());

    for (size_t i = 0; i < dest_size; i++) {
        uint32_t repeat_times = offsets[i + 1] - offsets[i];
        new_offsets->append_value_multiple_times(*_offsets, i, repeat_times);
        new_lengths->append_value_multiple_times(*_lengths, i, repeat_times);
    }
    return ArrayViewColumn::create(_elements, std::move(new_offsets), std::move(new_lengths));
}

void ArrayViewColumn::assign(size_t n, size_t idx) {
    DCHECK_GE(_offsets->size(), idx + 1);
    DCHECK_GE(_lengths->size(), idx + 1);
    _offsets->assign(n, idx);
    _lengths->assign(n, idx);
}

void ArrayViewColumn::append(const Column& src, size_t offset, size_t count) {
    const auto& array_view_column = down_cast<const ArrayViewColumn&>(src);
    const auto& src_offsets = array_view_column.offsets();
    const auto& src_lengths = array_view_column.lengths();

    if (_elements.get() == array_view_column._elements.get()) {
        // if these two array view column share the same elements, just append offset and lengths
        _offsets->append(src_offsets, offset, count);
        _lengths->append(src_lengths, offset, count);
    } else {
        // @TODO we can support it if necessary
#ifndef BE_TEST
        DCHECK(false) << "not support append data from another array";
#endif
        throw std::runtime_error("not support append data from another array");
    }
}

void ArrayViewColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        append(src, indexes[from + i], 1);
    }
}

void ArrayViewColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        append(src, index, 1);
    }
}

void ArrayViewColumn::remove_first_n_values(size_t count) {
    if (count > _offsets->size()) {
        count = _offsets->size();
    }
    _offsets->remove_first_n_values(count);
    _lengths->remove_first_n_values(count);
}

uint32_t ArrayViewColumn::max_one_element_serialize_size() const {
    DCHECK(false) << "ArrayViewColumn::max_one_element_serialize_size() is not supported";
    throw std::runtime_error("ArrayViewColumn::max_one_element_serialize_size() is not supported");
    return 0;
}

uint32_t ArrayViewColumn::serialize(size_t idx, uint8_t* pos) const {
    DCHECK(false) << "ArrayViewColumn::serialize() is not supported";
    throw std::runtime_error("ArrayViewColumn::serialize() is not supported");
    return 0;
}

uint32_t ArrayViewColumn::serialize_default(uint8_t* pos) const {
    DCHECK(false) << "ArrayViewColumn::serialize_default() is not supported";
    throw std::runtime_error("ArrayViewColumn::serialize_default() is not supported");
    return 0;
}

void ArrayViewColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                      uint32_t max_one_row_size) const {
    DCHECK(false) << "ArrayViewColumn::serialize_batch() is not supported";
    throw std::runtime_error("ArrayViewColumn::serialize_batch() is not supported");
}

const uint8_t* ArrayViewColumn::deserialize_and_append(const uint8_t* pos) {
    DCHECK(false) << "ArrayViewColumn::deserialize_and_append() is not supported";
    throw std::runtime_error("ArrayViewColumn::deserialize_and_append() is not supported");
    return nullptr;
}

void ArrayViewColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    DCHECK(false) << "ArrayViewColumn::deserialize_and_append_batch() is not supported";
    throw std::runtime_error("ArrayViewColumn::deserialize_and_append_batch() is not supported");
}

uint32_t ArrayViewColumn::serialize_size(size_t idx) const {
    DCHECK(false) << "ArrayViewColumn::serialize_size() is not supported";
    throw std::runtime_error("ArrayViewColumn::serialize_size() is not supported");
    return 0;
}

size_t ArrayViewColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    [[maybe_unused]] size_t offsets_size = _offsets->filter_range(filter, from, to);
    [[maybe_unused]] size_t lengths_size = _lengths->filter_range(filter, from, to);
    DCHECK_EQ(offsets_size, lengths_size);
    return offsets_size;
}

int ArrayViewColumn::compare_at(size_t left, size_t right, const Column& right_column, int nan_direction_hint) const {
    // @TODO support compare with ArrayColumn
    const auto& rhs = down_cast<const ArrayViewColumn&>(right_column);
    size_t lhs_offset = _offsets->get_data()[left];
    size_t lhs_length = _lengths->get_data()[left];
    size_t rhs_offset = rhs._offsets->get_data()[right];
    size_t rhs_length = rhs._lengths->get_data()[right];

    size_t min_size = std::min(lhs_length, rhs_length);
    for (size_t i = 0; i < min_size; i++) {
        int res = _elements->compare_at(lhs_offset + i, rhs_offset + i, *rhs._elements, nan_direction_hint);
        if (res != 0) {
            return res;
        }
    }
    return lhs_length < rhs_length ? -1 : (lhs_length == rhs_length ? 0 : 1);
}

void ArrayViewColumn::compare_column(const Column& rhs, std::vector<int8_t>* output) const {
    if (size() != rhs.size()) {
        throw std::runtime_error("Two input columns in comparison must have the same size");
    }
    size_t rows = size();
    output->resize(rows);
    for (size_t i = 0; i < rows; i++) {
        int res = compare_at(i, i, rhs, 1);
        (*output)[i] = (res > 0) - (res < 0);
    }
}

int ArrayViewColumn::equals(size_t left, const Column& right_column, size_t right, bool safe_eq) const {
    const auto& rhs = down_cast<const ArrayViewColumn&>(right_column);
    size_t lhs_offset = _offsets->get_data()[left];
    size_t lhs_length = _lengths->get_data()[left];
    size_t rhs_offset = rhs._offsets->get_data()[right];
    size_t rhs_length = rhs._lengths->get_data()[right];

    if (lhs_length != rhs_length) {
        return EQUALS_FALSE;
    }

    int ret = EQUALS_TRUE;
    for (size_t i = 0; i < lhs_length; i++) {
        int tmp = _elements->equals(lhs_offset + i, *rhs._elements, rhs_offset + i, safe_eq);
        if (tmp == EQUALS_FALSE) {
            return EQUALS_FALSE;
        } else if (tmp == EQUALS_NULL) {
            ret = EQUALS_NULL;
        }
    }
    return safe_eq ? EQUALS_TRUE : ret;
}

Datum ArrayViewColumn::get(size_t idx) const {
    DCHECK_LT(idx, _offsets->size()) << "idx should be less than offsets size";
    size_t offset = _offsets->get_data()[idx];
    size_t length = _lengths->get_data()[idx];

    DatumArray res(length);
    for (size_t i = 0; i < length; ++i) {
        res[i] = _elements->get(offset + i);
    }
    return {res};
}

void ArrayViewColumn::fnv_hash_at(uint32_t* seed, uint32_t idx) const {
    DCHECK(false) << "ArrayViewColumn::fnv_hash_at() is not supported";
    throw std::runtime_error("ArrayViewColumn::fnv_hash_at() is not supported");
}

void ArrayViewColumn::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    DCHECK(false) << "ArrayViewColumn::fnv_hash() is not supported";
    throw std::runtime_error("ArrayViewColumn::fnv_hash() is not supported");
}

void ArrayViewColumn::crc32_hash_at(uint32_t* seed, uint32_t idx) const {
    DCHECK(false) << "ArrayViewColumn::crc32_hash_at() is not supported";
    throw std::runtime_error("ArrayViewColumn::crc32_hash_at() is not supported");
}

void ArrayViewColumn::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    DCHECK(false) << "ArrayViewColumn::crc32_hash() is not supported";
    throw std::runtime_error("ArrayViewColumn::crc32_hash() is not supported");
}

int64_t ArrayViewColumn::xor_checksum(uint32_t from, uint32_t to) const {
    DCHECK(false) << "ArrayViewColumn::xor_checksum() is not supported";
    throw std::runtime_error("ArrayViewColumn::xor_checksum() is not supported");
}

void ArrayViewColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    DCHECK(false) << "ArrayViewColumn::put_mysql_row_buffer() is not supported";
    throw std::runtime_error("ArrayViewColumn::put_mysql_row_buffer() is not supported");
}

size_t ArrayViewColumn::get_element_null_count(size_t idx) const {
    size_t offset = _offsets->get_data()[idx];
    size_t length = _lengths->get_data()[idx];
    auto nullable_column = down_cast<const NullableColumn*>(_elements.get());
    return nullable_column->null_count(offset, length);
}

size_t ArrayViewColumn::get_element_size(size_t idx) const {
    DCHECK_LT(idx, _lengths->size());
    return _lengths->get_data()[idx];
}

void ArrayViewColumn::check_or_die() const {
    DCHECK(_elements);
    DCHECK(_offsets);
    DCHECK(_lengths);
    DCHECK_EQ(_offsets->size(), _lengths->size());
    for (size_t i = 0; i < _offsets->size(); i++) {
        uint32_t offset = _offsets->get_data()[i];
        uint32_t length = _lengths->get_data()[i];
        DCHECK_LE(offset + length, _elements->size());
    }
}

std::string ArrayViewColumn::debug_item(size_t idx) const {
    DCHECK_LT(idx, size());
    uint32_t offset = _offsets->get_data()[idx];
    uint32_t length = _lengths->get_data()[idx];

    std::stringstream ss;
    ss << "[";
    for (uint32_t i = 0; i < length; i++) {
        ss << (i > 0 ? "," : "") << _elements->debug_item(offset + i);
    }
    ss << "]";
    return ss.str();
}

std::string ArrayViewColumn::debug_string() const {
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < size(); i++) {
        ss << (i > 0 ? "," : "") << debug_item(i);
    }
    ss << "]";
    return ss.str();
}

MutableColumnPtr ArrayViewColumn::clone_empty() const {
    return create(_elements, UInt32Column::create(), UInt32Column::create());
}

void ArrayViewColumn::swap_column(Column& rhs) {
    auto& array_view_column = down_cast<ArrayViewColumn&>(rhs);
    _offsets->swap_column(*array_view_column._offsets);
    _lengths->swap_column(*array_view_column._lengths);
    _elements->swap_column(*array_view_column._elements);
}

void ArrayViewColumn::reset_column() {
    _offsets->reset_column();
    _lengths->reset_column();
}

ColumnPtr ArrayViewColumn::to_array_column() const {
    auto array_elements = _elements->clone_empty();
    auto array_offsets = UInt32Column::create();
    array_offsets->reserve(_offsets->size() + 1);
    array_offsets->append(0);
    uint32_t last_offset = 0;
    size_t num_rows = _offsets->size();
    for (size_t i = 0; i < num_rows; i++) {
        uint32_t offset = _offsets->get_data()[i];
        uint32_t length = _lengths->get_data()[i];
        // append lement
        array_elements->append(*_elements, offset, length);
        array_offsets->append(last_offset + length);
        last_offset += length;
    }
    return ArrayColumn::create(std::move(array_elements), std::move(array_offsets));
}

// for example,
// array column: [[1,2],null,[],[4]]
//  null_data [0,1,0,0]
//  elements column: [1,2,3,4]
//  offsets column: [0, 2, 2, 2, 3]

// array view column: [[1,2], null, [], [4]]
// null_data[0,1,0,0]
//  elements column: [1,2,3,4]
//  offsets column: [0,2,2,3]
//  length column:  [2,0,0,1]
ColumnPtr ArrayViewColumn::from_array_column(const ColumnPtr& column) {
    if (!column->is_array()) {
        throw std::runtime_error("input column must be an array column");
    }
    auto view_offsets = UInt32Column::create();
    auto view_lengths = UInt32Column::create();
    view_offsets->reserve(column->size());
    view_lengths->reserve(column->size());
    ColumnPtr view_elements;

    if (column->is_nullable()) {
        auto nullable_column = down_cast<const NullableColumn*>(column.get());
        DCHECK(nullable_column != nullptr);
        const auto& null_data = nullable_column->null_column()->get_data();
        auto array_column = down_cast<const ArrayColumn*>(nullable_column->data_column().get());
        const auto& array_offsets = array_column->offsets().get_data();

        view_elements = array_column->elements_column();

        for (size_t i = 0; i < column->size(); i++) {
            uint32_t offset = array_offsets[i];
            uint32_t length = null_data[i] ? 0 : (array_offsets[i + 1] - offset);
            view_offsets->append(offset);
            view_lengths->append(length);
        }
        auto ret = NullableColumn::create(
                ArrayViewColumn::create(std::move(view_elements), std::move(view_offsets), std::move(view_lengths)),
                nullable_column->null_column()->as_mutable_ptr());
        ret->check_or_die();
        return ret;
    }

    auto array_column = down_cast<const ArrayColumn*>(column.get());
    view_elements = array_column->elements_column();
    const auto& array_offsets = array_column->offsets().get_data();

    for (size_t i = 0; i < column->size(); i++) {
        uint32_t offset = array_offsets[i];
        uint32_t length = array_offsets[i + 1] - offset;
        view_offsets->append(offset);
        view_lengths->append(length);
    }
    return ArrayViewColumn::create(std::move(view_elements), std::move(view_offsets), std::move(view_lengths));
}

ColumnPtr ArrayViewColumn::to_array_column(const ColumnPtr& column) {
    if (!column->is_array_view()) {
        throw std::runtime_error("input column must be an array view column");
    }

    if (column->is_nullable()) {
        auto nullable_column = down_cast<const NullableColumn*>(column.get());
        DCHECK(nullable_column != nullptr);
        auto array_view_column = down_cast<const ArrayViewColumn*>(nullable_column->data_column().get());
        auto array_column = array_view_column->to_array_column();
        return NullableColumn::create(std::move(array_column)->as_mutable_ptr(),
                                      nullable_column->null_column()->as_mutable_ptr());
    }
    auto array_view_column = down_cast<const ArrayViewColumn*>(column.get());
    return array_view_column->to_array_column();
}
} // namespace starrocks