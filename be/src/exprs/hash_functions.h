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

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "util/xxh3.h"

namespace starrocks {
class HashFunctions {
public:
    /**
     * @param columns: [BinaryColumn, ...]
     * @return IntColumn
     */
    DEFINE_VECTORIZED_FN(murmur_hash3_32);

    /**
     * @param columns: [BinaryColumn, ...]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(xx_hash3_64);

    /**
     * @param columns: [BinaryColumn, ...]
     * @return LargeIntColumn
     */
    DEFINE_VECTORIZED_FN(xx_hash3_128);
};

inline StatusOr<ColumnPtr> HashFunctions::murmur_hash3_32(FunctionContext* context, const starrocks::Columns& columns) {
    std::vector<ColumnViewer<TYPE_VARCHAR>> viewers;

    viewers.reserve(columns.size());
    for (const auto& column : columns) {
        viewers.emplace_back(column);
    }

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_INT> builder(size);
    for (int row = 0; row < size; ++row) {
        uint32_t seed = HashUtil::MURMUR3_32_SEED;
        bool has_null = false;
        for (const auto& viewer : viewers) {
            if (viewer.is_null(row)) {
                has_null = true;
                break;
            }

            auto slice = viewer.value(row);
            seed = HashUtil::murmur_hash3_32(slice.data, slice.size, seed);
        }

        builder.append(seed, has_null);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

inline StatusOr<ColumnPtr> HashFunctions::xx_hash3_64(FunctionContext* context, const starrocks::Columns& columns) {
    std::vector<ColumnViewer<TYPE_VARCHAR>> column_viewers;

    column_viewers.reserve(columns.size());
    for (const auto& column : columns) {
        column_viewers.emplace_back(column);
    }

    const uint64_t default_xxhash_seed = HashUtil::XXHASH3_64_SEED;

    size_t row_size = columns[0]->size();
    std::vector<uint64_t> seeds_vec(row_size, default_xxhash_seed);
    std::vector<bool> is_null_vec(row_size, false);

    for (const auto& viewer : column_viewers) {
        for (size_t row = 0; row < row_size; ++row) {
            if (is_null_vec[row]) {
                continue;
            }

            if (viewer.is_null(row)) {
                is_null_vec[row] = true;
                continue;
            }

            auto slice = viewer.value(row);
            uint64_t seed = seeds_vec[row];
            seeds_vec[row] = HashUtil::xx_hash3_64(slice.data, slice.size, seed);
        }
    }

    ColumnBuilder<TYPE_BIGINT> builder(row_size);
    for (int row = 0; row < row_size; ++row) {
        builder.append(seeds_vec[row], is_null_vec[row]);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

inline StatusOr<ColumnPtr> HashFunctions::xx_hash3_128(FunctionContext* context, const starrocks::Columns& columns) {
    std::vector<ColumnViewer<TYPE_VARCHAR>> column_viewers;

    column_viewers.reserve(columns.size());
    for (const auto& column : columns) {
        column_viewers.emplace_back(column);
    }

    size_t row_size = columns[0]->size();
    const uint64_t default_xxhash_seed = HashUtil::XXHASH3_64_SEED;
    std::vector<XXH3_state_t> states(row_size);
    std::vector<bool> is_null_vec(row_size, false);

    for (size_t i = 0; i < row_size; i++) {
        XXH_errorcode code = XXH3_128bits_reset_withSeed(&(states[i]), default_xxhash_seed);
        if (code != XXH_OK) {
            return Status::InternalError("init xxh3 state failed");
        }
    }

    for (const auto& viewer : column_viewers) {
        for (size_t row = 0; row < row_size; ++row) {
            if (is_null_vec[row]) {
                continue;
            }

            if (viewer.is_null(row)) {
                is_null_vec[row] = true;
                continue;
            }

            auto slice = viewer.value(row);
            XXH_errorcode code = XXH3_128bits_update(&(states[row]), slice.data, slice.size);
            if (code != XXH_OK) {
                return Status::InternalError("update xxh3 state failed");
            }
        }
    }

    ColumnBuilder<TYPE_LARGEINT> builder(row_size);
    for (int row = 0; row < row_size; ++row) {
        XXH128_hash_t value = XXH3_128bits_digest(&states[row]);
        int128_t res = ((int128_t)value.high64 << 64) | (uint64_t)value.low64;
        builder.append(res, is_null_vec[row]);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks
