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

#include <gutil/strings/fastmem.h>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/sort_helper.h"
#include "gutil/casts.h"
#include "storage/decimal12.h"
#include "types/large_int_value.h"
#include "util/hash_util.hpp"
#include "util/mysql_row_buffer.h"
#include "util/value_generator.h"

namespace starrocks {

template <typename T>
StatusOr<ColumnPtr> FixedLengthColumnBase<T>::upgrade_if_overflow() {
    RETURN_IF_ERROR(capacity_limit_reached());
    return nullptr;
}

template <typename T>
void FixedLengthColumnBase<T>::append(const Column& src, size_t offset, size_t count) {
    const auto& num_src = down_cast<const FixedLengthColumnBase<T>&>(src);
    _data.insert(_data.end(), num_src._data.begin() + offset, num_src._data.begin() + offset + count);
}

template <typename T>
void FixedLengthColumnBase<T>::append_selective(const Column& src, const uint32_t* indexes, uint32_t from,
                                                uint32_t size) {
    const T* src_data = reinterpret_cast<const T*>(src.raw_data());
    size_t orig_size = _data.size();
    _data.resize(orig_size + size);
    for (size_t i = 0; i < size; ++i) {
        _data[orig_size + i] = src_data[indexes[from + i]];
    }
}

template <typename T>
void FixedLengthColumnBase<T>::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    const T* src_data = reinterpret_cast<const T*>(src.raw_data());
    size_t orig_size = _data.size();
    _data.resize(orig_size + size);
    for (size_t i = 0; i < size; ++i) {
        _data[orig_size + i] = src_data[index];
    }
}

//TODO(fzh): optimize copy using SIMD
template <typename T>
StatusOr<ColumnPtr> FixedLengthColumnBase<T>::replicate(const Buffer<uint32_t>& offsets) {
    auto dest = this->clone_empty();
    auto& dest_data = down_cast<FixedLengthColumnBase<T>&>(*dest);
    dest_data._data.resize(offsets.back());
    size_t orig_size = offsets.size() - 1; // this->size() may be large than offsets->size() -1
    for (auto i = 0; i < orig_size; ++i) {
        for (auto j = offsets[i]; j < offsets[i + 1]; ++j) {
            dest_data._data[j] = _data[i];
        }
    }
    return dest;
}

template <typename T>
void FixedLengthColumnBase<T>::fill_default(const Filter& filter) {
    T val = DefaultValueGenerator<T>::next_value();
    for (size_t i = 0; i < filter.size(); i++) {
        if (filter[i] == 1) {
            _data[i] = val;
        }
    }
}

template <typename T>
Status FixedLengthColumnBase<T>::fill_range(const std::vector<T>& ids, const Filter& filter) {
    DCHECK_EQ(filter.size(), _data.size());
    size_t j = 0;
    for (size_t i = 0; i < _data.size(); ++i) {
        if (filter[i] == 1) {
            _data[i] = ids[j];
            ++j;
        }
    }
    DCHECK_EQ(j, ids.size());

    return Status::OK();
}

template <typename T>
void FixedLengthColumnBase<T>::update_rows(const Column& src, const uint32_t* indexes) {
    const T* src_data = reinterpret_cast<const T*>(src.raw_data());
    size_t replace_num = src.size();
    for (uint32_t i = 0; i < replace_num; ++i) {
        DCHECK_LT(indexes[i], _data.size());
        _data[indexes[i]] = src_data[i];
    }
}

template <typename T>
size_t FixedLengthColumnBase<T>::filter_range(const Filter& filter, size_t from, size_t to) {
    auto size = ColumnHelper::filter_range<T>(filter, _data.data(), from, to);
    this->resize(size);
    return size;
}

template <typename T>
int FixedLengthColumnBase<T>::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    DCHECK_LT(left, _data.size());
    DCHECK_LT(right, rhs.size());
    DCHECK(dynamic_cast<const FixedLengthColumnBase<T>*>(&rhs) != nullptr);
    T x = _data[left];
    T y = down_cast<const FixedLengthColumnBase<T>&>(rhs)._data[right];
    return SorterComparator<T>::compare(x, y);
}

template <typename T>
uint32_t FixedLengthColumnBase<T>::serialize(size_t idx, uint8_t* pos) {
    memcpy(pos, &_data[idx], sizeof(T));
    return sizeof(T);
}

template <typename T>
uint32_t FixedLengthColumnBase<T>::serialize_default(uint8_t* pos) {
    ValueType value{};
    memcpy(pos, &value, sizeof(T));
    return sizeof(T);
}

template <typename T>
void FixedLengthColumnBase<T>::serialize_batch(uint8_t* __restrict__ dst, Buffer<uint32_t>& slice_sizes,
                                               size_t chunk_size, uint32_t max_one_row_size) {
    uint32_t* sizes = slice_sizes.data();
    T* __restrict__ src = _data.data();

    for (size_t i = 0; i < chunk_size; ++i) {
        memcpy(dst + i * max_one_row_size + sizes[i], src + i, sizeof(T));
    }

    for (size_t i = 0; i < chunk_size; i++) {
        sizes[i] += sizeof(T);
    }
}

template <typename T>
void FixedLengthColumnBase<T>::serialize_batch_with_null_masks(uint8_t* __restrict__ dst, Buffer<uint32_t>& slice_sizes,
                                                               size_t chunk_size, uint32_t max_one_row_size,
                                                               uint8_t* null_masks, bool has_null) {
    uint32_t* sizes = slice_sizes.data();
    T* __restrict__ src = _data.data();

    if (!has_null) {
        for (size_t i = 0; i < chunk_size; ++i) {
            memcpy(dst + i * max_one_row_size + sizes[i], &has_null, sizeof(bool));
            memcpy(dst + i * max_one_row_size + sizes[i] + sizeof(bool), src + i, sizeof(T));
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            sizes[i] += sizeof(bool) + sizeof(T);
        }
    } else {
        for (size_t i = 0; i < chunk_size; ++i) {
            memcpy(dst + i * max_one_row_size + sizes[i], null_masks + i, sizeof(bool));
            if (!null_masks[i]) {
                memcpy(dst + i * max_one_row_size + sizes[i] + sizeof(bool), src + i, sizeof(T));
            }
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            sizes[i] += static_cast<uint32_t>(sizeof(bool) + (1 - null_masks[i]) * sizeof(T));
        }
    }
}

template <typename T>
size_t FixedLengthColumnBase<T>::serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval,
                                                             size_t start, size_t count) {
    const size_t value_size = sizeof(T);
    const auto& key_data = get_data();
    uint8_t* buf = dst + byte_offset;
    for (size_t i = start; i < start + count; ++i) {
        strings::memcpy_inlined(buf, &key_data[i], value_size);
        buf = buf + byte_interval;
    }
    return value_size;
}

template <typename T>
const uint8_t* FixedLengthColumnBase<T>::deserialize_and_append(const uint8_t* pos) {
    T value{};
    memcpy(&value, pos, sizeof(T));
    _data.emplace_back(value);
    return pos + sizeof(T);
}

template <typename T>
void FixedLengthColumnBase<T>::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    raw::make_room(&_data, chunk_size);
    for (size_t i = 0; i < chunk_size; ++i) {
        memcpy(&_data[i], srcs[i].data, sizeof(T));
        srcs[i].data = srcs[i].data + sizeof(T);
    }
}

template <typename T>
void FixedLengthColumnBase<T>::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        hash[i] = HashUtil::fnv_hash(&_data[i], sizeof(ValueType), hash[i]);
    }
}

// Must same with RawValue::zlib_crc32
template <typename T>
void FixedLengthColumnBase<T>::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        if constexpr (IsDate<T> || IsTimestamp<T>) {
            std::string str = _data[i].to_string();
            hash[i] = HashUtil::zlib_crc_hash(str.data(), static_cast<int32_t>(str.size()), hash[i]);
        } else if constexpr (IsDecimal<T>) {
            int64_t int_val = _data[i].int_value();
            int32_t frac_val = _data[i].frac_value();
            uint32_t seed = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), hash[i]);
            hash[i] = HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), seed);
        } else {
            hash[i] = HashUtil::zlib_crc_hash(&_data[i], sizeof(ValueType), hash[i]);
        }
    }
}

template <typename T>
int64_t FixedLengthColumnBase<T>::xor_checksum(uint32_t from, uint32_t to) const {
    int64_t xor_checksum = 0;
    if constexpr (IsDate<T>) {
        for (size_t i = from; i < to; ++i) {
            xor_checksum ^= _data[i].to_date_literal();
        }
    } else if constexpr (IsTimestamp<T>) {
        for (size_t i = from; i < to; ++i) {
            xor_checksum ^= _data[i].to_timestamp_literal();
        }
    } else if constexpr (IsDecimal<T>) {
        for (size_t i = from; i < to; ++i) {
            xor_checksum ^= _data[i].int_value();
            xor_checksum ^= _data[i].frac_value();
        }
    } else if constexpr (is_signed_integer<T>) {
        const T* src = reinterpret_cast<const T*>(_data.data());
        for (size_t i = from; i < to; ++i) {
            if constexpr (std::is_same_v<T, int128_t>) {
                xor_checksum ^= static_cast<int64_t>(src[i] >> 64);
                xor_checksum ^= static_cast<int64_t>(src[i] & ULLONG_MAX);
            } else {
                xor_checksum ^= src[i];
            }
        }
    }

    return xor_checksum;
}

template <typename T>
void FixedLengthColumnBase<T>::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    if constexpr (IsDecimal<T>) {
        buf->push_decimal(_data[idx].to_string());
    } else if constexpr (IsDate<T>) {
        buf->push_date(_data[idx], is_binary_protocol);
    } else if constexpr (IsTimestamp<T>) {
        buf->push_timestamp(_data[idx], is_binary_protocol);
    } else if constexpr (std::is_arithmetic_v<T>) {
        buf->push_number(_data[idx], is_binary_protocol);
    } else {
        std::string s = _data[idx].to_string();
        buf->push_string(s.data(), s.size());
    }
}

template <typename T>
void FixedLengthColumnBase<T>::remove_first_n_values(size_t count) {
    size_t remain_size = _data.size() - count;
    memmove(_data.data(), _data.data() + count, remain_size * sizeof(T));
    _data.resize(remain_size);
}

template <typename T>
std::string FixedLengthColumnBase<T>::debug_item(size_t idx) const {
    std::stringstream ss;
    if constexpr (sizeof(T) == 1) {
        // for bool, int8_t
        ss << (int)_data[idx];
    } else {
        ss << _data[idx];
    }
    return ss.str();
}

template <>
std::string FixedLengthColumnBase<int128_t>::debug_item(size_t idx) const {
    std::stringstream ss;
    starrocks::operator<<(ss, _data[idx]);
    return ss.str();
}

template <typename T>
std::string FixedLengthColumnBase<T>::debug_string() const {
    std::stringstream ss;
    ss << "[";
    size_t size = this->size();
    for (size_t i = 0; i + 1 < size; ++i) {
        ss << debug_item(i) << ", ";
    }
    if (size > 0) {
        ss << debug_item(size - 1);
    }
    ss << "]";
    return ss.str();
}

template <typename T>
std::string FixedLengthColumnBase<T>::get_name() const {
    if constexpr (IsDecimal<T>) {
        return "decimal";
    } else if constexpr (IsDate<T>) {
        return "date";
    } else if constexpr (IsTimestamp<T>) {
        return "timestamp";
    } else if constexpr (IsInt128<T>) {
        return "int128";
    } else if constexpr (std::is_floating_point_v<T>) {
        return "float-" + std::to_string(sizeof(T));
    } else {
        return "integral-" + std::to_string(sizeof(T));
    }
}

template class FixedLengthColumnBase<uint8_t>;
template class FixedLengthColumnBase<uint16_t>;
template class FixedLengthColumnBase<uint32_t>;
template class FixedLengthColumnBase<uint64_t>;

template class FixedLengthColumnBase<int8_t>;
template class FixedLengthColumnBase<int16_t>;
template class FixedLengthColumnBase<int32_t>;
template class FixedLengthColumnBase<int64_t>;
template class FixedLengthColumnBase<int96_t>;
template class FixedLengthColumnBase<int128_t>;

template class FixedLengthColumnBase<float>;
template class FixedLengthColumnBase<double>;

template class FixedLengthColumnBase<uint24_t>;
template class FixedLengthColumnBase<decimal12_t>;

template class FixedLengthColumnBase<DateValue>;
template class FixedLengthColumnBase<DecimalV2Value>;
template class FixedLengthColumnBase<TimestampValue>;

} // namespace starrocks
