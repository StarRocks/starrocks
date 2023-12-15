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

namespace starrocks {
class HashFunctions {
public:
    /**
     * @param columns: [BinaryColumn, ...]
     * @return IntColumn
     */
    DEFINE_VECTORIZED_FN(murmur_hash3_32);
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

} // namespace starrocks
