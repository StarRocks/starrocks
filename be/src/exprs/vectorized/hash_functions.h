// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "exprs/vectorized/function_helper.h"
#include "udf/udf.h"

namespace starrocks {
namespace vectorized {
class HashFunctions {
public:
    /**
     * @param columns: [BinaryColumn, ...]
     * @return IntColumn
     */
    DEFINE_VECTORIZED_FN(murmur_hash3_32);
};

inline StatusOr<ColumnPtr> HashFunctions::murmur_hash3_32(FunctionContext* context,
                                                          const starrocks::vectorized::Columns& columns) {
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

} // namespace vectorized
} // namespace starrocks
