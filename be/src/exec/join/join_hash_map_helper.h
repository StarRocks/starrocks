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

#pragma once

#include <cmath>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exec/join/join_hash_map_helper.h"
#include "exec/join/join_hash_table_descriptor.h"
#include "util/phmap/phmap.h"

namespace starrocks {

// IEEE-754 canonical quiet NaN values (same as Java's Float/Double.floatToIntBits for NaN)
constexpr uint32_t CANONICAL_FLOAT_NAN_BITS = 0x7FC00000u;
constexpr uint64_t CANONICAL_DOUBLE_NAN_BITS = 0x7FF8000000000000ULL;

// Normalize NaN values in a float/double column for Iceberg equality delete semantics.
// In Iceberg, NaN == NaN must be true for equality deletes.
// This function replaces all NaN values with a canonical NaN so they hash and compare equal.
template <typename T>
ColumnPtr normalize_nan_in_column(const ColumnPtr& col) {
    static_assert(std::is_floating_point_v<T>, "T must be float or double");

    using ColumnType = typename RunTimeTypeTraits<RunTimeTypeLimits<T>::logical_type>::ColumnType;

    Column* data_col = col.get();
    bool is_nullable = col->is_nullable();

    if (is_nullable) {
        auto* nullable = down_cast<NullableColumn*>(col.get());
        data_col = nullable->data_column().get();
    }

    auto* typed_col = down_cast<ColumnType*>(data_col);
    const auto& src_data = typed_col->get_data();

    // Check if there are any NaN values
    bool has_nan = false;
    for (size_t i = 0; i < src_data.size(); i++) {
        if (std::isnan(src_data[i])) {
            has_nan = true;
            break;
        }
    }

    if (!has_nan) {
        return col;
    }

    // Create a copy and normalize NaN values
    ColumnPtr result;
    if (is_nullable) {
        auto* nullable = down_cast<NullableColumn*>(col.get());
        auto new_data_col = typed_col->clone_empty();
        auto* new_typed_col = down_cast<ColumnType*>(new_data_col.get());
        auto& dest_data = new_typed_col->get_data();
        dest_data.resize(src_data.size());

        constexpr T canonical_nan = []() {
            if constexpr (std::is_same_v<T, float>) {
                uint32_t bits = CANONICAL_FLOAT_NAN_BITS;
                return *reinterpret_cast<float*>(&bits);
            } else {
                uint64_t bits = CANONICAL_DOUBLE_NAN_BITS;
                return *reinterpret_cast<double*>(&bits);
            }
        }();

        for (size_t i = 0; i < src_data.size(); i++) {
            dest_data[i] = std::isnan(src_data[i]) ? canonical_nan : src_data[i];
        }

        result = NullableColumn::create(std::move(new_data_col), nullable->null_column());
    } else {
        auto new_col = typed_col->clone_empty();
        auto* new_typed_col = down_cast<ColumnType*>(new_col.get());
        auto& dest_data = new_typed_col->get_data();
        dest_data.resize(src_data.size());

        constexpr T canonical_nan = []() {
            if constexpr (std::is_same_v<T, float>) {
                uint32_t bits = CANONICAL_FLOAT_NAN_BITS;
                return *reinterpret_cast<float*>(&bits);
            } else {
                uint64_t bits = CANONICAL_DOUBLE_NAN_BITS;
                return *reinterpret_cast<double*>(&bits);
            }
        }();

        for (size_t i = 0; i < src_data.size(); i++) {
            dest_data[i] = std::isnan(src_data[i]) ? canonical_nan : src_data[i];
        }

        result = std::move(new_col);
    }

    return result;
}

// Normalize NaN values based on column type
inline ColumnPtr normalize_float_nan(const ColumnPtr& col, LogicalType type) {
    if (type == TYPE_FLOAT) {
        return normalize_nan_in_column<float>(col);
    } else if (type == TYPE_DOUBLE) {
        return normalize_nan_in_column<double>(col);
    }
    return col;
}

template <class T, size_t Size = sizeof(T)>
struct JoinKeyHash {
    static constexpr uint32_t CRC_SEED = 0x811C9DC5;
    uint32_t operator()(const T& value, uint32_t num_buckets, uint32_t num_log_buckets) const {
        const size_t hash = crc_hash_32(&value, sizeof(T), CRC_SEED);
        return hash & (num_buckets - 1);
    }
};

/// Apply multiplicative hashing for 4-byte or 8-byte keys.
/// It only needs to perform arithmetic operations on the key as a whole, so the compiler can automatically vectorize it.
template <typename T>
struct JoinKeyHash<T, 4> {
    uint32_t operator()(T value, uint32_t num_buckets, uint32_t num_log_buckets) const {
        static constexpr uint32_t a = 2654435761u;
        uint32_t v = *reinterpret_cast<uint32_t*>(&value);
        v ^= v >> (32 - num_log_buckets);
        const uint32_t fraction = v * a;
        return fraction >> (32 - num_log_buckets);
    }
};

template <typename T>
struct JoinKeyHash<T, 8> {
    uint32_t operator()(T value, uint32_t num_buckets, uint32_t num_log_buckets) const {
        static constexpr uint64_t a = 11400714819323198485ull;
        uint64_t v = *reinterpret_cast<uint64_t*>(&value);
        v ^= v >> (64 - num_log_buckets);
        const uint64_t fraction = v * a;
        return fraction >> (64 - num_log_buckets);
    }
};

template <>
struct JoinKeyHash<Slice> {
    static const uint32_t CRC_SEED = 0x811C9DC5;
    uint32_t operator()(const Slice& slice, uint32_t num_buckets, uint32_t num_log_buckets) const {
        const size_t hash = crc_hash_32(slice.data, slice.size, CRC_SEED);
        return hash & (num_buckets - 1);
    }
};

class JoinHashMapHelper {
public:
    // maxinum bucket size
    const static uint32_t MAX_BUCKET_SIZE = 1 << 31;

    static uint32_t calc_bucket_size(uint32_t size) {
        size_t expect_bucket_size = static_cast<size_t>(size) + (size - 1) / 4;
        // Limit the maximum hash table bucket size.
        if (expect_bucket_size >= MAX_BUCKET_SIZE) {
            return MAX_BUCKET_SIZE;
        }
        return phmap::priv::NormalizeCapacity(expect_bucket_size) + 1;
    }

    template <typename CppType>
    static uint32_t calc_bucket_num(const CppType& value, uint32_t bucket_size, uint32_t num_log_buckets) {
        using HashFunc = JoinKeyHash<CppType>;

        return HashFunc()(value, bucket_size, num_log_buckets);
    }

    template <typename CppType>
    static void calc_bucket_nums(const Buffer<CppType>& data, uint32_t bucket_size, uint32_t num_log_buckets,
                                 Buffer<uint32_t>* buckets, uint32_t start, uint32_t count) {
        DCHECK(count <= buckets->size());
        for (size_t i = 0; i < count; i++) {
            (*buckets)[i] = calc_bucket_num<CppType>(data[start + i], bucket_size, num_log_buckets);
        }
    }

    template <typename CppType>
    static std::pair<uint32_t, uint8_t> calc_bucket_num_and_fp(const CppType& value, uint32_t bucket_size,
                                                               uint32_t num_log_buckets) {
        static constexpr uint64_t FP_BITS = 7;
        using HashFunc = JoinKeyHash<CppType>;
        const uint64_t hash = HashFunc()(value, bucket_size << FP_BITS, num_log_buckets + FP_BITS);
        return {hash >> FP_BITS, (hash & 0x7F) | 0x80};
    }

    static Slice get_hash_key(const Columns& key_columns, size_t row_idx, uint8_t* buffer) {
        size_t byte_size = 0;
        for (const auto& key_column : key_columns) {
            byte_size += key_column->serialize(row_idx, buffer + byte_size);
        }
        return {buffer, byte_size};
    }

    // combine keys into fixed size key by column.
    template <LogicalType LT>
    static void serialize_fixed_size_key_column(const Columns& key_columns, Column* fixed_size_key_column,
                                                const std::vector<uint32_t>& serialized_fixed_size_key_bytes,
                                                uint32_t start, uint32_t count) {
        DCHECK_EQ(serialized_fixed_size_key_bytes.size(), key_columns.size());

        using CppType = typename RunTimeTypeTraits<LT>::CppType;
        using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

        auto& data = down_cast<ColumnType*>(fixed_size_key_column)->get_data();
        auto* buf = reinterpret_cast<uint8_t*>(&data[start]);

        constexpr size_t byte_interval = sizeof(CppType);
        std::memset(buf, 0, count * byte_interval);

        size_t byte_offset = 0;
        for (uint32_t i = 0; i < key_columns.size(); i++) {
            const auto& key_col = key_columns[i];
            const uint32_t max_row_size = serialized_fixed_size_key_bytes[i];
            const size_t offset =
                    key_col->serialize_batch_at_interval(buf, byte_offset, byte_interval, max_row_size, start, count);
            byte_offset += offset;
        }
    }
};

} // namespace starrocks