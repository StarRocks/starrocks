// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include <gutil/strings/fastmem.h>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "gutil/casts.h"
#include "runtime/large_int_value.h"
#include "storage/decimal12.h"
#include "storage/uint24.h"
#include "util/coding.h"
#include "util/hash_util.hpp"
#include "util/mysql_row_buffer.h"
#include "util/types.h"

namespace starrocks::vectorized {

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

template <typename T>
size_t FixedLengthColumnBase<T>::filter_range(const Column::Filter& filter, size_t from, size_t to) {
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
    if constexpr (IsDate<T>) {
        return x.julian() - y.julian();
    } else if constexpr (IsTimestamp<T>) {
        Timestamp v = x.timestamp() - y.timestamp();
        // Implicitly converting Timestamp to int may give wrong result.
        if (v == 0) {
            return 0;
        } else {
            return v > 0 ? 1 : -1;
        }
    } else {
        // uint8/int8_t, uint16/int16_t, uint32/int32_t, int64, int128, float, double, Decimal, ...
        if (x > y) {
            return 1;
        } else if (x < y) {
            return -1;
        } else {
            return 0;
        }
    }
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
            sizes[i] += sizeof(bool) + (1 - null_masks[i]) * sizeof(T);
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
void FixedLengthColumnBase<T>::deserialize_and_append_batch(std::vector<Slice>& srcs, size_t batch_size) {
    raw::make_room(&_data, batch_size);
    for (size_t i = 0; i < batch_size; ++i) {
        memcpy(&_data[i], srcs[i].data, sizeof(T));
        srcs[i].data = srcs[i].data + sizeof(T);
    }
}

template <typename T>
uint8_t* FixedLengthColumnBase<T>::serialize_column(uint8_t* dst) {
    uint32_t size = byte_size();
    encode_fixed32_le(dst, size);
    dst += sizeof(uint32_t);

    strings::memcpy_inlined(dst, _data.data(), size);
    dst += size;
    return dst;
}

template <typename T>
const uint8_t* FixedLengthColumnBase<T>::deserialize_column(const uint8_t* src) {
    uint32_t size = decode_fixed32_le(src);
    src += sizeof(uint32_t);

    raw::make_room(&_data, size / sizeof(ValueType));
    strings::memcpy_inlined(_data.data(), src, size);
    src += size;
    return src;
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
            hash[i] = HashUtil::zlib_crc_hash(str.data(), str.size(), hash[i]);
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
void FixedLengthColumnBase<T>::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    if constexpr (IsDecimal<T>) {
        buf->push_decimal(_data[idx].to_string());
    } else if constexpr (std::is_arithmetic_v<T>) {
        buf->push_number(_data[idx]);
    } else {
        // date/datetime or something else.
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
std::string FixedLengthColumnBase<T>::debug_item(uint32_t idx) const {
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
std::string FixedLengthColumnBase<int128_t>::debug_item(uint32_t idx) const {
    std::stringstream ss;
    starrocks::operator<<(ss, _data[idx]);
    return ss.str();
}

template <typename T>
std::string FixedLengthColumnBase<T>::debug_string() const {
    std::stringstream ss;
    ss << "[";
    for (int i = 0; i < _data.size() - 1; ++i) {
        ss << debug_item(i) << ", ";
    }
    ss << debug_item(_data.size() - 1) << "]";
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

} // namespace starrocks::vectorized
