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

#include "column/binary_column.h"
#include "column/column_view/column_view.h"

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include "column/bytes.h"
#include "column/german_string_column.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "gutil/bits.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/substitute.h"
#include "util/hash_util.hpp"
#include "util/mysql_row_buffer.h"
#include "util/raw_container.h"

namespace starrocks {

void GermanStringColumn::check_or_die() const {}

void GermanStringColumn::append(const GermanString& str) {
    _data.emplace_back(str, _allocator.allocate(str.len));
}

void GermanStringColumn::append(const Column& src, size_t offset, size_t count) {
    DCHECK(offset + count <= src.size());
    const auto& b = down_cast<const GermanStringColumn&>(src);
    for (auto i = offset; i < offset + count; ++i) {
        const auto& str = b._data[i];
        _data.emplace_back(str, _allocator.allocate(str.len));
    }
}

void GermanStringColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK(!src.is_binary_view());
    auto& src_column = down_cast<const GermanStringColumn&>(src);
    _data.reserve(_data.size() + size);
    for (auto i = 0; i < size; ++i) {
        uint32_t row_idx = indexes[from + i];
        const auto& str = src_column._data[row_idx];
        _data.emplace_back(str, _allocator.allocate(str.len));
    }
}

void GermanStringColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    auto& src_column = down_cast<const GermanStringColumn&>(src);
    DCHECK(index < src_column.size());
    const auto& str = src_column._data[index];
    _data.reserve(_data.size() + size);
    _data.emplace_back(str, _allocator.allocate(str.len));
    auto& first_str = _data.back();
    _data.insert(_data.end(), size - 1, first_str);
}

StatusOr<ColumnPtr> GermanStringColumn::replicate(const Buffer<uint32_t>& offsets) {
    auto dest = GermanStringColumn::create();
    auto* dest_column = down_cast<GermanStringColumn*>(dest.get());
    dest_column->reserve(offsets.back());
    auto src_size = offsets.size() - 1; // this->size() may be large than offsets->size() -1
    for (auto i = 0; i < src_size; ++i) {
        dest_column->append_value_multiple_times(*this, i, offsets[i + 1] - offsets[i]);
    }
    return dest;
}

void GermanStringColumn::_append_string(const char* str, size_t len) {
    _data.emplace_back(str, len, _allocator.allocate(len));
}

void GermanStringColumn::append_string(const char* str, size_t len) {
    _append_string(str, len);
}

void GermanStringColumn::append_string(const std::string& str) {
    _append_string(str.data(), str.size());
}

bool GermanStringColumn::append_strings(const GermanString* data, size_t size) {
    _data.reserve(_data.size() + size);
    for (size_t i = 0; i < size; i++) {
        append(data[i]);
    }
    return true;
}

void GermanStringColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* str = reinterpret_cast<const GermanString*>(value);
    _data.reserve(_data.size() + count);
    _data.emplace_back(*str, _allocator.allocate(str->len));
    auto& first_str = _data.back();
    for (int i = 1; i < count; ++i) {
        _data.emplace_back(first_str);
    }
}

void GermanStringColumn::fill_default(const Filter& filter) {
    std::vector<uint32_t> indexes;
    for (size_t i = 0; i < filter.size(); i++) {
        auto len = _data[i].len;
        if (filter[i] == 1 && len > 0) {
            indexes.push_back(static_cast<uint32_t>(i));
        }
    }
    if (indexes.empty()) {
        return;
    }
    auto default_column = clone_empty();
    default_column->append_default(indexes.size());
    update_rows(*default_column, indexes.data());
}

void GermanStringColumn::update_rows(const Column& src, const uint32_t* indexes) {
    const auto& src_column = down_cast<const GermanStringColumn&>(src);
    size_t replace_num = src.size();
    for (size_t i = 0; i < replace_num; ++i) {
        const auto& src_str = src_column._data[i];
        auto& dst_str = _data[indexes[i]];
        if (src_str.len <= dst_str.len) {
            new (&dst_str) GermanString(src_str, reinterpret_cast<char*>(dst_str.long_rep.ptr));
        } else {
            new (&dst_str) GermanString(src_str, _allocator.allocate(src_str.len));
        }
    }
}

void GermanStringColumn::assign(size_t n, size_t idx) {
    if (n == 0) {
        return; // nothing to do
    }
    GermanString gs(_data[idx], _allocator.allocate(_data[idx].len));
    _data.clear();
    _allocator.clear();
    _data.reserve(n);
    _data.insert(_data.end(), n, gs);
}

void GermanStringColumn::remove_first_n_values(size_t count) {
    DCHECK_LE(count, _data.size());
    size_t remain_size = _data.size() - count;

    ColumnPtr column = cut(count, remain_size);
    auto* german_string_column = down_cast<const GermanStringColumn*>(column.get());
    *this = std::move(*german_string_column);
}

ColumnPtr GermanStringColumn::cut(size_t start, size_t length) const {
    auto result = this->create();

    if (start >= size() || length == 0) {
        return result;
    }

    auto* result_column = down_cast<GermanStringColumn*>(result.get());
    size_t end = std::min(start + length, _data.size());
    result_column->reserve(end - start);
    for (size_t i = start; i < end; ++i) {
        const auto& str = _data[i];
        result_column->_data.emplace_back(str, result_column->_allocator.allocate(str.len));
    }
    return result;
}

size_t GermanStringColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    auto column = GermanStringColumn::create();
    auto* tmp_column = down_cast<GermanStringColumn*>(column.get());
    for (auto i = 0; i < _data.size(); ++i) {
        if (i < from || i >= to || filter[i]) {
            tmp_column->append(*this, i, 1);
            continue;
        }
    }
    *this = std::move(*tmp_column);
    return _data.size();
}

int GermanStringColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    const auto& right_column = down_cast<const GermanStringColumn&>(rhs);
    const auto& lhs_str = this->_data[left];
    const auto& rhs_str = right_column._data[right];
    return lhs_str.compare(rhs_str);
}

uint32_t GermanStringColumn::max_one_element_serialize_size() const {
    auto num_rows = _data.size();
    uint32_t max_size = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        // it's safe to cast, because max size of one string is 2^32
        max_size = std::max(max_size, _data[i].len);
    }
    // TODO: may be overflow here, i will solve it later
    return max_size + sizeof(uint32_t);
}

uint32_t GermanStringColumn::serialize_default(uint8_t* pos) const {
    // max size of one string is 2^32, so use uint32_t not T
    uint32_t binary_size = 0;
    strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
    return sizeof(uint32_t);
}

void GermanStringColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                         uint32_t max_one_row_size) const {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

const uint8_t* GermanStringColumn::deserialize_and_append(const uint8_t* pos) {
    // max size of one string is 2^32, so use uint32_t not T
    uint32_t string_size{};
    strings::memcpy_inlined(&string_size, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    _append_string(reinterpret_cast<const char*>(pos), string_size);
    return pos + string_size;
}

void GermanStringColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

void GermanStringColumn::serialize_batch_with_null_masks(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                                         uint32_t max_one_row_size, const uint8_t* null_masks,
                                                         bool has_null) const {
    uint32_t* sizes = slice_sizes.data();

    if (!has_null) {
        for (size_t i = 0; i < chunk_size; ++i) {
            memcpy(dst + i * max_one_row_size + sizes[i], &has_null, sizeof(bool));
            sizes[i] += static_cast<uint32_t>(sizeof(bool)) +
                        serialize(i, dst + i * max_one_row_size + sizes[i] + sizeof(bool));
        }
    } else {
        for (size_t i = 0; i < chunk_size; ++i) {
            memcpy(dst + i * max_one_row_size + sizes[i], null_masks + i, sizeof(bool));
            sizes[i] += sizeof(bool);

            if (!null_masks[i]) {
                sizes[i] += serialize(i, dst + i * max_one_row_size + sizes[i]);
            }
        }
    }
}

void GermanStringColumn::deserialize_and_append_batch_nullable(Buffer<Slice>& srcs, size_t chunk_size,
                                                               Buffer<uint8_t>& is_nulls, bool& has_null) {
    ColumnFactory<Column, GermanStringColumn>::deserialize_and_append_batch_nullable(srcs, chunk_size, is_nulls,
                                                                                     has_null);
}

void GermanStringColumn::fnv_hash(uint32_t* hashes, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        hashes[i] = _data[i].fnv_hash(hashes[i]);
    }
}

void GermanStringColumn::fnv_hash_with_selection(uint32_t* hashes, uint8_t* selection, uint16_t from,
                                                 uint16_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        if (!selection[i]) {
            continue;
        }
        hashes[i] = _data[i].fnv_hash(hashes[i]);
    }
}

void GermanStringColumn::fnv_hash_selective(uint32_t* hashes, uint16_t* sel, uint16_t sel_size) const {
    for (uint16_t i = 0; i < sel_size; i++) {
        uint16_t idx = sel[i];
        hashes[idx] = _data[idx].fnv_hash(hashes[idx]);
    }
}

void GermanStringColumn::crc32_hash(uint32_t* hashes, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        hashes[i] = _data[i].crc32_hash(hashes[i]);
    }
}

void GermanStringColumn::crc32_hash_with_selection(uint32_t* hashes, uint8_t* selection, uint16_t from,
                                                   uint16_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        if (!selection[i]) {
            continue;
        }
        hashes[i] = _data[i].crc32_hash(hashes[i]);
    }
}

void GermanStringColumn::crc32_hash_selective(uint32_t* hashes, uint16_t* sel, uint16_t sel_size) const {
    for (uint16_t i = 0; i < sel_size; i++) {
        uint16_t idx = sel[i];
        hashes[idx] = _data[i].crc32_hash(hashes[idx]);
    }
}

int64_t GermanStringColumn::xor_checksum(uint32_t from, uint32_t to) const {
    // The XOR of BinaryColumn
    // For one string, treat it as a number of 64-bit integers and 8-bit integers.
    // XOR all of the integers to get a checksum for one string.
    // XOR all of the checksums to get xor_checksum.
    int64_t xor_checksum = 0;

    for (size_t i = from; i < to; ++i) {
        auto str = static_cast<std::string>(_data[i]);
        const auto* src = reinterpret_cast<const uint8_t*>(str.data());
        auto num = str.size();
#ifdef __AVX2__
        // AVX2 intructions can improve the speed of XOR procedure of one string.
        __m256i avx2_checksum = _mm256_setzero_si256();
        size_t step = sizeof(__m256i) / sizeof(uint8_t);

        while (num >= step) {
            const __m256i left = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
            avx2_checksum = _mm256_xor_si256(left, avx2_checksum);
            src += step;
            num -= step;
        }
        auto* checksum_vec = reinterpret_cast<int64_t*>(&avx2_checksum);
        size_t eight_byte_step = sizeof(__m256i) / sizeof(int64_t);
        for (size_t j = 0; j < eight_byte_step; ++j) {
            xor_checksum ^= checksum_vec[j];
        }
#endif

        while (num >= 8) {
            xor_checksum ^= *reinterpret_cast<const int64_t*>(src);
            src += 8;
            num -= 8;
        }
        for (size_t j = 0; j < num; ++j) {
            xor_checksum ^= src[j];
        }
    }

    return xor_checksum;
}

void GermanStringColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    auto str = static_cast<std::string>(_data[idx]);
    buf->push_string(str.data(), str.size());
}

std::string GermanStringColumn::debug_item(size_t idx) const {
    std::string s;
    auto str = static_cast<std::string>(_data[idx]);
    s.reserve(str.size() + 2);
    s.push_back('\'');
    s.append(str.data(), str.size());
    s.push_back('\'');
    return s;
}

bool GermanStringColumn::append_strings(const Slice* data, size_t size) {
    _data.reserve(_data.size() + size);
    for (size_t i = 0; i < size; i++) {
        _append_string(data[i].data, data[i].size);
    }
    return true;
}

bool GermanStringColumn::append_strings_overflow(const Slice* data, size_t size, size_t max_length) {
    return append_strings(data, size);
}

bool GermanStringColumn::append_continuous_strings(const Slice* data, size_t size) {
    return append_strings(data, size);
}

bool GermanStringColumn::append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) {
    _data.reserve(_data.size() + size);
    for (size_t i = 0; i < size; ++i) {
        _append_string(data + i * fixed_length, fixed_length);
    }
    return true;
}

std::string GermanStringColumn::raw_item_value(size_t idx) const {
    return static_cast<std::string>(_data[idx]);
}

StatusOr<ColumnPtr> GermanStringColumn::upgrade_if_overflow() {
    return const_cast<const GermanStringColumn*>(this)->get_ptr();
}

StatusOr<ColumnPtr> GermanStringColumn::downgrade() {
    return const_cast<const GermanStringColumn*>(this)->get_ptr();
}

bool GermanStringColumn::has_large_column() const {
    return false;
}

Status GermanStringColumn::capacity_limit_reached() const {
    return Status::OK();
}

template <typename T>
ColumnPtr GermanStringColumn::_to_binary_impl(size_t num_bytes) const {
    using BinColType = BinaryColumnBase<T>;
    auto result_column = BinColType::create(this->size());
    auto* binary_column = down_cast<BinColType*>(result_column.get());
    auto& bytes = binary_column->get_bytes();
    auto& offsets = binary_column->get_offset();
    bytes.resize(num_bytes);
    offsets[0] = 0;
    auto* p = bytes.data();
    const auto num_rows = this->size();
    for (auto i = 0; i < num_rows; ++i) {
        const auto& str = _data[i];
        offsets[i + 1] = offsets[i] + str.len;
        if (str.is_inline()) {
            strings::memcpy_inlined(p, str.short_rep.str, str.len);
        } else {
            strings::memcpy_inlined(p, str.long_rep.prefix, GermanString::PREFIX_LENGTH);
            auto* remaining = reinterpret_cast<const char*>(str.long_rep.ptr);
            strings::memcpy_inlined(p + GermanString::PREFIX_LENGTH, remaining, str.len - GermanString::PREFIX_LENGTH);
        }
        p += str.len;
    }
    return result_column;
}

ColumnPtr GermanStringColumn::to_binary() const {
    const auto num_bytes = this->get_binary_size();
    if (num_bytes > std::numeric_limits<uint32_t>::max()) {
        return this->_to_binary_impl<uint64_t>(num_bytes);
    } else {
        return this->_to_binary_impl<uint32_t>(num_bytes);
    }
}

void GermanStringColumn::from_binary(const starrocks::BinaryColumn& binary_column) {
    _data.clear();
    _allocator.clear();
    const auto& bytes = binary_column.get_bytes();
    const auto& offsets = binary_column.get_offset();
    size_t num_rows = offsets.size() - 1;
    _data.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        auto* p = reinterpret_cast<const char*>(bytes.data() + offsets[i]);
        size_t len = offsets[i + 1] - offsets[i];
        _data.emplace_back(p, len, _allocator.allocate(len));
    }
}

size_t GermanStringColumn::get_binary_size() const {
    size_t total_size = 0;
    const auto num_rows = _data.size();
    for (size_t i = 0; i < num_rows; ++i) {
        total_size += _data[i].len;
    }
    return total_size;
}

} // namespace starrocks
