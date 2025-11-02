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

#include "column/fixed_length_column_base.h"

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "exec/sorting/sort_helper.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/substitute.h"
#include "simd/gather.h"
#include "storage/decimal12.h"
#include "types/int256.h"
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
    DCHECK(this != &src);

    auto& datas = get_data();
    const size_t orig_size = datas.size();
    raw::stl_vector_resize_uninitialized(&datas, orig_size + count);

    const T* src_data = reinterpret_cast<const T*>(src.raw_data());
    strings::memcpy_inlined(datas.data() + orig_size, src_data + offset, count * sizeof(T));
}

template <typename T>
void FixedLengthColumnBase<T>::append_selective(const Column& src, const uint32_t* indexes, uint32_t from,
                                                uint32_t size) {
    DCHECK(this != &src);

    indexes += from;
    auto& datas = get_data();
    const size_t orig_size = datas.size();
    raw::stl_vector_resize_uninitialized(&datas, orig_size + size);
    auto* dest_data = datas.data() + orig_size;

    const T* src_data = reinterpret_cast<const T*>(src.raw_data());
    SIMDGather::gather(dest_data, src_data, indexes, size);
}

template <typename T>
void FixedLengthColumnBase<T>::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    DCHECK(this != &src);

    auto& datas = get_data();

    size_t orig_size = datas.size();
    datas.resize(orig_size + size);

    const auto& src_col = down_cast<const FixedLengthColumnBase<T>&>(src);
    const auto src_datas = src_col.immutable_data();
    const T* src_data = src_datas.data();

    for (size_t i = 0; i < size; ++i) {
        datas[orig_size + i] = src_data[index];
    }
}

template <typename T>
size_t FixedLengthColumnBase<T>::append_numbers(const ContainerResource& res) {
    bool could_apply_opt = config::enable_zero_copy_from_page_cache && res.owned() && res.is_aligned<T>();
    if (could_apply_opt && empty() && _resource.empty()) {
        DCHECK(res.length() % sizeof(ValueType) == 0);
        _resource.acquire(res);
        _resource.set_data(res.data());
        _resource.set_length(res.length() / sizeof(ValueType));
        return _resource.length();
    } else {
        return append_numbers(res.data(), res.length());
    }
}

template <typename T>
void FixedLengthColumnBase<T>::append_default() {
    auto& datas = get_data();
    datas.emplace_back(DefaultValueGenerator<ValueType>::next_value());
}

template <typename T>
void FixedLengthColumnBase<T>::append_default(size_t count) {
    auto& datas = get_data();
    datas.resize(datas.size() + count, DefaultValueGenerator<ValueType>::next_value());
}

//TODO(fzh): optimize copy using SIMD
template <typename T>
StatusOr<ColumnPtr> FixedLengthColumnBase<T>::replicate(const Buffer<uint32_t>& offsets) {
    auto dest = this->clone_empty();
    auto& dest_data = down_cast<FixedLengthColumnBase<T>&>(*dest);
    auto& dest_datas = dest_data.get_data();

    const auto datas = this->immutable_data();
    dest_datas.resize(offsets.back());
    size_t orig_size = offsets.size() - 1; // this->size() may be large than offsets->size() -1
    for (auto i = 0; i < orig_size; ++i) {
        for (auto j = offsets[i]; j < offsets[i + 1]; ++j) {
            dest_datas[j] = datas[i];
        }
    }
    return dest;
}

template <typename T>
void FixedLengthColumnBase<T>::fill_default(const Filter& filter) {
    auto& datas = get_data();

    T val = DefaultValueGenerator<T>::next_value();
    for (size_t i = 0; i < filter.size(); i++) {
        if (filter[i] == 1) {
            datas[i] = val;
        }
    }
}

template <typename T>
Status FixedLengthColumnBase<T>::fill_range(const std::vector<T>& ids, const Filter& filter) {
    auto& datas = get_data();

    DCHECK_EQ(filter.size(), datas.size());
    size_t j = 0;
    for (size_t i = 0; i < datas.size(); ++i) {
        if (filter[i] == 1) {
            datas[i] = ids[j];
            ++j;
        }
    }
    DCHECK_EQ(j, ids.size());

    return Status::OK();
}

template <typename T>
void FixedLengthColumnBase<T>::update_rows(const Column& src, const uint32_t* indexes) {
    auto& datas = get_data();

    const auto& src_col = down_cast<const FixedLengthColumnBase<T>&>(src);
    const auto src_datas = src_col.immutable_data();
    const T* src_data = src_datas.data();

    size_t replace_num = src.size();
    for (uint32_t i = 0; i < replace_num; ++i) {
        DCHECK_LT(indexes[i], _data.size());
        datas[indexes[i]] = src_data[i];
    }
}

template <typename T>
size_t FixedLengthColumnBase<T>::filter_range(const Filter& filter, size_t from, size_t to) {
    // TODO: FIXME
    const auto src = immutable_data();
    raw::stl_vector_resize_uninitialized(&_data, src.size());
    auto size = ColumnHelper::filter_range<T>(filter, _data.data(), src.data(), from, to);
    _data.resize(size);
    _resource.reset();
    return size;
}

template <typename T>
int FixedLengthColumnBase<T>::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    const auto lhs_datas = this->immutable_data();
    const auto rhs_datas = down_cast<const FixedLengthColumnBase<T>&>(rhs).immutable_data();
    DCHECK_LT(left, lhs_datas.size());
    DCHECK_LT(right, rhs_datas.size());
    T x = lhs_datas[left];
    T y = rhs_datas[right];
    return SorterComparator<T>::compare(x, y);
}

template <typename T>
uint32_t FixedLengthColumnBase<T>::serialize(size_t idx, uint8_t* pos) const {
    const auto datas = this->immutable_data();
    memcpy(pos, &datas[idx], sizeof(T));
    return sizeof(T);
}

template <typename T>
uint32_t FixedLengthColumnBase<T>::serialize_default(uint8_t* pos) const {
    ValueType value{};
    memcpy(pos, &value, sizeof(T));
    return sizeof(T);
}

template <typename T>
void FixedLengthColumnBase<T>::serialize_batch(uint8_t* __restrict__ dst, Buffer<uint32_t>& slice_sizes,
                                               size_t chunk_size, uint32_t max_one_row_size) const {
    uint32_t* sizes = slice_sizes.data();
    const T* __restrict__ src = this->immutable_data().data();

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
                                                               const uint8_t* null_masks, bool has_null) const {
    uint32_t* sizes = slice_sizes.data();
    const T* __restrict__ src = this->immutable_data().data();

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
                                                             uint32_t max_row_size, size_t start, size_t count) const {
    const size_t value_size = sizeof(T);
    DCHECK_EQ(max_row_size, value_size);
    const auto key_data = this->immutable_data();
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
    this->get_data().emplace_back(value);
    return pos + sizeof(T);
}

template <typename T>
void FixedLengthColumnBase<T>::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    auto& datas = this->get_data();
    raw::make_room(&datas, chunk_size);
    for (size_t i = 0; i < chunk_size; ++i) {
        memcpy(&datas[i], srcs[i].data, sizeof(T));
        srcs[i].data = srcs[i].data + sizeof(T);
    }
}

template <typename T>
void FixedLengthColumnBase<T>::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    const auto datas = this->immutable_data();
    for (uint32_t i = from; i < to; ++i) {
        hash[i] = HashUtil::fnv_hash(&datas[i], sizeof(ValueType), hash[i]);
    }
}

template <typename T>
void FixedLengthColumnBase<T>::fnv_hash_with_selection(uint32_t* hash, uint8_t* selection, uint16_t from,
                                                       uint16_t to) const {
    const auto datas = this->immutable_data();
    for (uint16_t i = from; i < to; i++) {
        if (selection[i]) {
            hash[i] = HashUtil::fnv_hash(&datas[i], sizeof(ValueType), hash[i]);
        }
    }
}
template <typename T>
void FixedLengthColumnBase<T>::fnv_hash_selective(uint32_t* hash, uint16_t* sel, uint16_t sel_size) const {
    const auto datas = this->immutable_data();
    for (uint16_t i = 0; i < sel_size; i++) {
        hash[sel[i]] = HashUtil::fnv_hash(&datas[sel[i]], sizeof(ValueType), hash[sel[i]]);
    }
}

// Must same with RawValue::zlib_crc32
template <typename T>
void FixedLengthColumnBase<T>::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    const auto datas = this->immutable_data();
    for (uint32_t i = from; i < to; ++i) {
        if constexpr (IsDate<T> || IsTimestamp<T>) {
            std::string str = datas[i].to_string();
            hash[i] = HashUtil::zlib_crc_hash(str.data(), static_cast<int32_t>(str.size()), hash[i]);
        } else if constexpr (IsDecimal<T>) {
            int64_t int_val = datas[i].int_value();
            int32_t frac_val = datas[i].frac_value();
            uint32_t seed = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), hash[i]);
            hash[i] = HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), seed);
        } else {
            hash[i] = HashUtil::zlib_crc_hash(&datas[i], sizeof(ValueType), hash[i]);
        }
    }
}

template <typename T>
void FixedLengthColumnBase<T>::crc32_hash_with_selection(uint32_t* hash, uint8_t* selection, uint16_t from,
                                                         uint16_t to) const {
    const auto datas = this->immutable_data();
    for (uint16_t i = from; i < to; i++) {
        if (!selection[i]) {
            continue;
        }
        if constexpr (IsDate<T> || IsTimestamp<T>) {
            std::string str = datas[i].to_string();
            hash[i] = HashUtil::zlib_crc_hash(str.data(), static_cast<int32_t>(str.size()), hash[i]);
        } else if constexpr (IsDecimal<T>) {
            int64_t int_val = datas[i].int_value();
            int32_t frac_val = datas[i].frac_value();
            uint32_t seed = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), hash[i]);
            hash[i] = HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), seed);
        } else {
            hash[i] = HashUtil::zlib_crc_hash(&datas[i], sizeof(ValueType), hash[i]);
        }
    }
}

template <typename T>
void FixedLengthColumnBase<T>::crc32_hash_selective(uint32_t* hash, uint16_t* sel, uint16_t sel_size) const {
    const auto datas = this->immutable_data();

    for (uint16_t i = 0; i < sel_size; i++) {
        if constexpr (IsDate<T> || IsTimestamp<T>) {
            std::string str = datas[sel[i]].to_string();
            hash[sel[i]] = HashUtil::zlib_crc_hash(str.data(), static_cast<int32_t>(str.size()), hash[sel[i]]);
        } else if constexpr (IsDecimal<T>) {
            int64_t int_val = datas[sel[i]].int_value();
            int32_t frac_val = datas[sel[i]].frac_value();
            uint32_t seed = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), hash[sel[i]]);
            hash[sel[i]] = HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), seed);
        } else {
            hash[sel[i]] = HashUtil::zlib_crc_hash(&datas[sel[i]], sizeof(ValueType), hash[sel[i]]);
        }
    }
}

template <typename T>
void FixedLengthColumnBase<T>::murmur_hash3_x86_32(uint32_t* hash, uint32_t from, uint32_t to) const {
    const auto datas = this->immutable_data();
    for (uint32_t i = from; i < to; ++i) {
        uint32_t hash_value = 0;
        if constexpr (IsDate<T>) {
            // Julian Day -> epoch day
            // TODO, This is not a good place to do a project, this is just for test.
            // If we need to make it more general, we should do this project in `IcebergMurmurHashProject`
            // but consider that use date type column as bucket transform is rare, we can do it later.
            int64_t long_value = datas[i].julian() - date::UNIX_EPOCH_JULIAN;
            hash_value = HashUtil::murmur_hash3_32(&long_value, sizeof(int64_t), 0);
        } else if constexpr (std::is_same<T, int32_t>::value) {
            // Integer and long hash results must be identical for all integer values.
            // This ensures that schema evolution does not change bucket partition values if integer types are promoted.
            int64_t long_value = datas[i];
            hash_value = HashUtil::murmur_hash3_32(&long_value, sizeof(int64_t), 0);
        } else if constexpr (std::is_same<T, int64_t>::value) {
            hash_value = HashUtil::murmur_hash3_32(&datas[i], sizeof(ValueType), 0);
        } else {
            // for decimal/timestamp type, the storage is very different from iceberg,
            // and consider they are merely used, these types are forbidden by fe
            DCHECK(false);
            return;
        }
        hash[i] = hash_value;
    }
}

template <typename T>
int64_t FixedLengthColumnBase<T>::xor_checksum(uint32_t from, uint32_t to) const {
    const auto datas = this->immutable_data();

    int64_t xor_checksum = 0;
    if constexpr (IsDate<T>) {
        for (size_t i = from; i < to; ++i) {
            xor_checksum ^= datas[i].to_date_literal();
        }
    } else if constexpr (IsTimestamp<T>) {
        for (size_t i = from; i < to; ++i) {
            xor_checksum ^= datas[i].to_timestamp_literal();
        }
    } else if constexpr (IsDecimal<T>) {
        for (size_t i = from; i < to; ++i) {
            xor_checksum ^= datas[i].int_value();
            xor_checksum ^= datas[i].frac_value();
        }
    } else if constexpr (is_signed_integer<T>) {
        const T* src = reinterpret_cast<const T*>(datas.data());
        for (size_t i = from; i < to; ++i) {
            if constexpr (std::is_same_v<T, int128_t>) {
                xor_checksum ^= static_cast<int64_t>(src[i] >> 64);
                xor_checksum ^= static_cast<int64_t>(src[i] & ULLONG_MAX);
            } else if constexpr (std::is_same_v<T, int256_t>) {
                xor_checksum ^= static_cast<int64_t>(src[i].high >> 64);
                xor_checksum ^= static_cast<int64_t>(src[i].high & ULLONG_MAX);
                xor_checksum ^= static_cast<int64_t>(src[i].low >> 64);
                xor_checksum ^= static_cast<int64_t>(src[i].low & ULLONG_MAX);
            } else {
                xor_checksum ^= src[i];
            }
        }
    }

    return xor_checksum;
}

template <typename T>
void FixedLengthColumnBase<T>::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    const auto datas = this->immutable_data();
    if constexpr (IsDecimal<T>) {
        buf->push_decimal(datas[idx].to_string());
    } else if constexpr (IsDate<T>) {
        buf->push_date(datas[idx], is_binary_protocol);
    } else if constexpr (IsTimestamp<T>) {
        buf->push_timestamp(datas[idx], is_binary_protocol);
    } else if constexpr (std::is_arithmetic_v<T>) {
        buf->push_number(datas[idx], is_binary_protocol);
    } else {
        std::string s = datas[idx].to_string();
        buf->push_string(s.data(), s.size());
    }
}

template <typename T>
void FixedLengthColumnBase<T>::remove_first_n_values(size_t count) {
    // TODO: avoid memcpy here
    auto& datas = this->get_data();
    size_t remain_size = datas.size() - count;
    memmove(_data.data(), _data.data() + count, remain_size * sizeof(T));
    _data.resize(remain_size);
}

template <typename T>
std::string FixedLengthColumnBase<T>::debug_item(size_t idx) const {
    const auto datas = this->immutable_data();
    std::stringstream ss;
    if constexpr (sizeof(T) == 1) {
        // for bool, int8_t
        ss << (int)datas[idx];
    } else {
        ss << datas[idx];
    }
    return ss.str();
}

template <>
std::string FixedLengthColumnBase<int128_t>::debug_item(size_t idx) const {
    const auto datas = this->immutable_data();
    std::stringstream ss;
    starrocks::operator<<(ss, datas[idx]);
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
Status FixedLengthColumnBase<T>::capacity_limit_reached() const {
    if (_data.size() > Column::MAX_CAPACITY_LIMIT) {
        return Status::CapacityLimitExceed(strings::Substitute("row count of fixed length column exceend the limit: $0",
                                                               std::to_string(Column::MAX_CAPACITY_LIMIT)));
    }
    return Status::OK();
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
    } else if constexpr (IsInt256<T>) {
        return "int256";
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
template class FixedLengthColumnBase<int256_t>;

template class FixedLengthColumnBase<float>;
template class FixedLengthColumnBase<double>;

template class FixedLengthColumnBase<uint24_t>;
template class FixedLengthColumnBase<decimal12_t>;

template class FixedLengthColumnBase<DateValue>;
template class FixedLengthColumnBase<DecimalV2Value>;
template class FixedLengthColumnBase<TimestampValue>;

} // namespace starrocks
