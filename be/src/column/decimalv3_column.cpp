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

#include "column/decimalv3_column.h"

#include "column/fixed_length_column.h"

namespace starrocks {

template <typename T>
DecimalV3Column<T>::DecimalV3Column(size_t num_rows) {
    this->resize_uninitialized(num_rows);
}

template <typename T>
DecimalV3Column<T>::DecimalV3Column(int precision, int scale) : _precision(precision), _scale(scale) {
    DCHECK(0 <= _scale && _scale <= _precision && _precision <= decimal_precision_limit<T>);
}

template <typename T>
DecimalV3Column<T>::DecimalV3Column(int precision, int scale, size_t num_rows) : DecimalV3Column(precision, scale) {
    this->resize_uninitialized(num_rows);
}
template <typename T>
bool DecimalV3Column<T>::is_decimal() const {
    return true;
}
template <typename T>
bool DecimalV3Column<T>::is_numeric() const {
    return true;
}
template <typename T>
void DecimalV3Column<T>::set_precision(int precision) {
    this->_precision = precision;
}
template <typename T>
void DecimalV3Column<T>::set_scale(int scale) {
    this->_scale = scale;
}
template <typename T>
int DecimalV3Column<T>::precision() const {
    return _precision;
}
template <typename T>
int DecimalV3Column<T>::scale() const {
    return _scale;
}

template <typename T>
MutableColumnPtr DecimalV3Column<T>::clone_empty() const {
    return this->create_mutable(_precision, _scale);
}

template <typename T>
void DecimalV3Column<T>::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    auto& data = this->get_data();
    auto s = DecimalV3Cast::to_string<T>(data[idx], _precision, _scale);
    buf->push_decimal(s);
}

template <typename T>
std::string DecimalV3Column<T>::debug_item(size_t idx) const {
    auto& data = this->get_data();
    return DecimalV3Cast::to_string<T>(data[idx], _precision, _scale);
}

template <typename T>
void DecimalV3Column<T>::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    const auto& data = this->get_data();
    // When decimal-v2 columns are used as distribution keys and users try to upgrade
    // decimal-v2 column to decimal-v3 by schema change, decimal128(27,9) shall be the
    // only acceptable target type, so keeping result of crc32_hash on type decimal128(27,9)
    // compatible with type decimal-v2 is required in order to keep data layout consistency.
    if constexpr (std::is_same_v<T, int128_t>) {
        if (_precision == 27 && _scale == 9) {
            for (uint32_t i = from; i < to; ++i) {
                auto& decimal_v2_value = (DecimalV2Value&)(data[i]);
                int64_t int_val = decimal_v2_value.int_value();
                int32_t frac_val = decimal_v2_value.frac_value();
                uint32_t seed = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), hash[i]);
                hash[i] = HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), seed);
            }
            return;
        }
    }
    for (uint32_t i = from; i < to; ++i) {
        hash[i] = HashUtil::zlib_crc_hash(&data[i], sizeof(T), hash[i]);
    }
}

template <typename T>
int64_t DecimalV3Column<T>::xor_checksum(uint32_t from, uint32_t to) const {
    // The XOR of DecimalV3Column
    // XOR all the decimals one by one
    auto& data = this->get_data();
    int64_t xor_checksum = 0;
    const T* src = reinterpret_cast<const T*>(data.data());

    for (size_t i = from; i < to; ++i) {
        if constexpr (std::is_same_v<T, int128_t>) {
            xor_checksum ^= static_cast<int64_t>(src[i] >> 64);
            xor_checksum ^= static_cast<int64_t>(src[i] & ULLONG_MAX);
        } else {
            xor_checksum ^= src[i];
        }
    }

    return xor_checksum;
}

template class DecimalV3Column<int32_t>;
template class DecimalV3Column<int64_t>;
template class DecimalV3Column<int128_t>;
} // namespace starrocks
